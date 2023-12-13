use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::sync::Arc;

use models::predicate::domain::{TimeRange, TimeRanges};
use models::FieldId;
use snafu::ResultExt;
use trace::{error, info, trace};
use utils::BloomFilter;

use super::iterator::BufferedIterator;
use crate::compaction::CompactReq;
use crate::context::GlobalContext;
use crate::error::{self, Result};
use crate::summary::{CompactMeta, VersionEdit};
use crate::tseries_family::TseriesFamily;
use crate::tsm::{
    self, BlockMeta, BlockMetaIterator, DataBlock, EncodedDataBlock, IndexIterator, IndexMeta,
    TsmReader, TsmWriter, WriteTsmError, WriteTsmResult,
};
use crate::{ColumnFileId, Error, LevelId, TseriesFamilyId};

pub async fn run_compaction_job(
    request: CompactReq,
    kernel: Arc<GlobalContext>,
) -> Result<Option<(VersionEdit, HashMap<ColumnFileId, Arc<BloomFilter>>)>> {
    info!(
        "Compaction: Running compaction job on ts_family: {} and files: [ {} ]",
        request.ts_family_id,
        request
            .files
            .iter()
            .map(|f| {
                format!(
                    "{{ Level-{}, file_id: {}, time_range: {}-{} }}",
                    f.level(),
                    f.file_id(),
                    f.time_range().min_ts,
                    f.time_range().max_ts
                )
            })
            .collect::<Vec<String>>()
            .join(", ")
    );

    if request.files.is_empty() {
        // Nothing to compact
        return Ok(None);
    }

    // Buffers all tsm-files and it's indexes for this compaction
    let mut tsm_readers = Vec::new();
    for col_file in request.files.iter() {
        let tsm_reader = request.version.get_tsm_reader(col_file.file_path()).await?;
        tsm_readers.push(tsm_reader);
    }

    let max_block_size = TseriesFamily::MAX_DATA_BLOCK_SIZE as usize;
    let mut iter = CompactState::new(tsm_readers, request.time_range, max_block_size);
    let mut writer_wrapper = WriterWrapper::new(&request, kernel.clone());

    let mut previous_merged_block: Option<CompactingBlock> = None;
    let mut fid = iter.curr_fid;
    while let Some(blk_meta_group) = iter.next().await {
        trace!("selected merging meta group: {blk_meta_group}");
        if fid.is_some() && fid != iter.curr_fid {
            // Iteration of next field id, write previous merged block.
            if let Some(blk) = previous_merged_block.take() {
                // Write the small previous merged block.
                writer_wrapper.write(blk).await?;
            }
        }

        fid = iter.curr_fid;
        let mut compacting_blks = blk_meta_group
            .merge_with_previous_block(
                previous_merged_block.take(),
                max_block_size,
                &request.time_range,
            )
            .await?;
        if compacting_blks.len() == 1 && compacting_blks[0].len() < max_block_size {
            // The only one data block too small, try to extend the next compacting blocks.
            previous_merged_block = Some(compacting_blks.remove(0));
            continue;
        }

        let last_blk_idx = compacting_blks.len() - 1;
        for (i, blk) in compacting_blks.into_iter().enumerate() {
            if i == last_blk_idx && blk.len() < max_block_size {
                // The last data block too small, try to extend to
                // the next compacting blocks (current field id).
                previous_merged_block = Some(blk);
                break;
            }
            writer_wrapper.write(blk).await?;
        }
    }
    if let Some(blk) = previous_merged_block {
        writer_wrapper.write(blk).await?;
    }

    let (mut version_edit, file_metas) = writer_wrapper.close().await?;
    for file in request.files {
        version_edit.del_file(file.level(), file.file_id(), file.is_delta());
    }

    info!(
        "Compaction: Compact finished, version edits: {:?}",
        version_edit
    );
    Ok(Some((version_edit, file_metas)))
}

pub(crate) struct CompactState {
    tsm_readers: Vec<Arc<TsmReader>>,
    /// The TimeRange for delta files to partly compact with other files.
    out_time_range: TimeRange,
    /// The TimeRanges for delta files to partly compact with other files.
    out_time_ranges: Arc<TimeRanges>,
    /// Maximum values in generated CompactingBlock
    max_data_block_size: usize,

    compacting_files: BinaryHeap<CompactingFile>,
    /// Temporarily stored index of `TsmReader` in self.tsm_readers,
    /// and `BlockMetaIterator` of current field_id.
    tmp_tsm_blk_meta_iters: Vec<(usize, BlockMetaIterator)>,
    /// When a TSM file at index i is ended, finished_idxes[i] is set to true.
    finished_readers: Vec<bool>,
    /// How many finished_idxes is set to true.
    finished_reader_cnt: usize,
    curr_fid: Option<FieldId>,

    merging_blk_meta_groups: VecDeque<CompactingBlockMetaGroup>,
}

impl CompactState {
    pub(crate) fn new(
        tsm_readers: Vec<Arc<TsmReader>>,
        out_time_range: TimeRange,
        max_data_block_size: usize,
    ) -> Self {
        let compacting_files: BinaryHeap<CompactingFile> = tsm_readers
            .iter()
            .enumerate()
            .map(|(i, r)| CompactingFile::new(i, r.clone()))
            .collect();
        let compacting_files_cnt = compacting_files.len();

        let out_time_ranges = Arc::new(TimeRanges::new(vec![out_time_range]));
        Self {
            tsm_readers,
            compacting_files,
            out_time_range,
            out_time_ranges,
            max_data_block_size,
            tmp_tsm_blk_meta_iters: Vec::with_capacity(compacting_files_cnt),
            finished_readers: vec![false; compacting_files_cnt],
            finished_reader_cnt: 0_usize,
            curr_fid: None,
            merging_blk_meta_groups: VecDeque::new(),
        }
    }

    pub(crate) async fn next(&mut self) -> Option<CompactingBlockMetaGroup> {
        if let Some(g) = self.merging_blk_meta_groups.pop_front() {
            return Some(g);
        }

        // For each tsm-file, get next index reader for current iteration field id
        self.next_field_id();

        trace!(
            "selected {} blocks meta iterators",
            self.tmp_tsm_blk_meta_iters.len()
        );
        if self.tmp_tsm_blk_meta_iters.is_empty() {
            trace!("iteration field_id {:?} is finished", self.curr_fid);
            self.curr_fid = None;
            return None;
        }

        // Get all of block_metas of this field id, and merge these blocks
        self.fetch_merging_block_meta_groups();

        if let Some(g) = self.merging_blk_meta_groups.pop_front() {
            return Some(g);
        }
        None
    }

    pub fn curr_fid(&self) -> Option<FieldId> {
        self.curr_fid
    }
}

impl CompactState {
    /// Update tmp_tsm_blk_meta_iters for field id in next iteration.
    fn next_field_id(&mut self) {
        trace!("===============================");
        self.curr_fid = None;

        if let Some(f) = self.compacting_files.peek() {
            if self.curr_fid.is_none() {
                trace!(
                    "selected new field {:?} from file {} as current field id",
                    f.field_id,
                    f.tsm_reader.file_id()
                );
                self.curr_fid = f.field_id
            }
        } else {
            // TODO finished
            trace!("no file to select, mark finished");
            self.finished_reader_cnt += 1;
        }
        self.tmp_tsm_blk_meta_iters.clear();
        let mut loop_field_id;
        let mut loop_file_i;
        while let Some(mut f) = self.compacting_files.pop() {
            loop_field_id = f.field_id;
            loop_file_i = f.i;
            if self.curr_fid == loop_field_id {
                if let Some(idx_meta) = f.peek() {
                    trace!(
                        "for delta file {loop_file_i}, put block iterator with time_range {:?}",
                        &self.out_time_range
                    );
                    self.tmp_tsm_blk_meta_iters.push((
                        loop_file_i,
                        idx_meta.block_iterator_opt(self.out_time_ranges.clone()),
                    ));

                    trace!("merging idx_meta({}): field_id: {}, field_type: {:?}, block_count: {}, time_range: {:?}",
                        self.tmp_tsm_blk_meta_iters.len(),
                        idx_meta.field_id(),
                        idx_meta.field_type(),
                        idx_meta.block_count(),
                        idx_meta.time_range(),
                    );
                    f.next();
                    self.compacting_files.push(f);
                } else {
                    // This tsm-file has been finished
                    trace!("file {} is finished.", loop_file_i);
                    self.finished_readers[loop_file_i] = true;
                    self.finished_reader_cnt += 1;
                }
            } else {
                self.compacting_files.push(f);
                break;
            }
        }
    }

    /// Collect merging `DataBlock`s.
    fn fetch_merging_block_meta_groups(&mut self) -> bool {
        if self.tmp_tsm_blk_meta_iters.is_empty() {
            return false;
        }
        let field_id = match self.curr_fid {
            Some(fid) => fid,
            None => return false,
        };

        let mut blk_metas: Vec<CompactingBlockMeta> =
            Vec::with_capacity(self.tmp_tsm_blk_meta_iters.len());
        // Get all block_meta, and check if it's tsm file has a related tombstone file.
        for (tsm_reader_idx, blk_iter) in self.tmp_tsm_blk_meta_iters.iter_mut() {
            for blk_meta in blk_iter.by_ref() {
                let tsm_reader_ptr = self.tsm_readers[*tsm_reader_idx].clone();
                blk_metas.push(CompactingBlockMeta::new(
                    *tsm_reader_idx,
                    tsm_reader_ptr,
                    blk_meta,
                ));
            }
        }
        // Sort by field_id, min_ts and max_ts.
        blk_metas.sort();

        let mut blk_meta_groups: Vec<CompactingBlockMetaGroup> =
            Vec::with_capacity(blk_metas.len());
        for blk_meta in blk_metas {
            blk_meta_groups.push(CompactingBlockMetaGroup::new(field_id, blk_meta));
        }
        // Compact blk_meta_groups.
        let mut i = 0;
        loop {
            let mut head_idx = i;
            // Find the first non-empty as head.
            for (off, bmg) in blk_meta_groups[i..].iter().enumerate() {
                if !bmg.is_empty() {
                    head_idx += off;
                    break;
                }
            }
            if head_idx >= blk_meta_groups.len() - 1 {
                // There no other blk_meta_group to merge with the last one.
                break;
            }
            let mut head = blk_meta_groups[head_idx].clone();
            i = head_idx + 1;
            for bmg in blk_meta_groups[i..].iter_mut() {
                if bmg.is_empty() {
                    continue;
                }
                if head.overlaps(bmg) {
                    head.append(bmg);
                }
            }
            blk_meta_groups[head_idx] = head;
        }
        let blk_meta_groups: VecDeque<CompactingBlockMetaGroup> = blk_meta_groups
            .into_iter()
            .filter(|l| !l.is_empty())
            .collect();
        trace!(
            "selected merging meta groups: {}",
            blk_meta_groups
                .iter()
                .map(|g| g.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        );

        self.merging_blk_meta_groups = blk_meta_groups;

        true
    }
}

/// Temporary compacting data block meta, holding the priority of the source tsm file,
/// the tsm reader and the meta of data block.
#[derive(Clone)]
pub(crate) struct CompactingBlockMeta {
    priority: usize,
    reader: Arc<TsmReader>,
    meta: BlockMeta,
}

impl PartialEq for CompactingBlockMeta {
    fn eq(&self, other: &Self) -> bool {
        self.reader.file_id() == other.reader.file_id() && self.meta == other.meta
    }
}

impl Eq for CompactingBlockMeta {}

impl PartialOrd for CompactingBlockMeta {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CompactingBlockMeta {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.meta.cmp(&other.meta)
    }
}

impl Display for CompactingBlockMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {{ len: {}, min_ts: {}, max_ts: {} }}",
            self.meta.field_type(),
            self.meta.count(),
            self.meta.min_ts(),
            self.meta.max_ts(),
        )
    }
}

impl CompactingBlockMeta {
    pub fn new(priority: usize, tsm_reader: Arc<TsmReader>, block_meta: BlockMeta) -> Self {
        Self {
            priority,
            reader: tsm_reader,
            meta: block_meta,
        }
    }

    pub fn time_range(&self) -> TimeRange {
        self.meta.time_range()
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.meta.min_ts() <= other.meta.max_ts() && self.meta.max_ts() >= other.meta.min_ts()
    }

    pub fn overlaps_time_range(&self, time_range: &TimeRange) -> bool {
        self.meta.min_ts() <= time_range.max_ts && self.meta.max_ts() >= time_range.min_ts
    }

    /// Read data block of block meta from reader.
    pub async fn get_data_block(&self, time_range: &TimeRange) -> Result<Option<DataBlock>> {
        // It's impossible that the reader got None by meta,
        // or blk.intersection(time_range) returned None.
        self.reader
            .get_data_block(&self.meta)
            .await
            .map(|blk| blk.intersection(time_range))
            .context(error::ReadTsmSnafu)
    }

    /// Read raw data of block meta from reader.
    pub async fn get_raw_data(&self, dst: &mut Vec<u8>) -> Result<usize> {
        self.reader
            .get_raw_data(&self.meta, dst)
            .await
            .context(error::ReadTsmSnafu)
    }

    pub fn has_tombstone(&self) -> bool {
        self.reader.has_tombstone()
    }
}

///
#[derive(Clone)]
pub(crate) struct CompactingBlockMetaGroup {
    field_id: FieldId,
    blk_metas: Vec<CompactingBlockMeta>,
    time_range: TimeRange,
}

impl CompactingBlockMetaGroup {
    pub fn new(field_id: FieldId, blk_meta: CompactingBlockMeta) -> Self {
        let time_range = blk_meta.time_range();
        Self {
            field_id,
            blk_metas: vec![blk_meta],
            time_range,
        }
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.time_range.overlaps(&other.time_range)
    }

    pub fn append(&mut self, other: &mut CompactingBlockMetaGroup) {
        self.blk_metas.append(&mut other.blk_metas);
        self.time_range.merge(&other.time_range);
    }

    pub async fn merge_with_previous_block(
        mut self,
        previous_block: Option<CompactingBlock>,
        max_block_size: usize,
        out_time_range: &TimeRange,
    ) -> Result<Vec<CompactingBlock>> {
        if self.blk_metas.is_empty() {
            return Ok(vec![]);
        }
        self.blk_metas
            .sort_by(|a, b| a.priority.cmp(&b.priority).reverse());

        let mut merged_block = Option::<DataBlock>::None;
        if self.blk_metas.len() == 1
            && !self.blk_metas[0].has_tombstone()
            && !self.blk_metas[0].overlaps_time_range(out_time_range)
        {
            // Only one compacting block and has no tombstone, write as raw block.
            trace!("only one compacting block without tombstone and time_range is entirely included by target level, handled as raw block");
            let head_meta = &self.blk_metas[0].meta;
            let mut buf = Vec::with_capacity(head_meta.size() as usize);
            let data_len = self.blk_metas[0].get_raw_data(&mut buf).await?;
            buf.truncate(data_len);

            if head_meta.size() >= max_block_size as u64 {
                // Raw data block is full, so do not merge with the previous, directly return.
                let mut merged_blks = Vec::new();
                if let Some(prev_compacting_block) = previous_block {
                    merged_blks.push(prev_compacting_block);
                }
                merged_blks.push(CompactingBlock::raw(
                    self.blk_metas[0].priority,
                    head_meta.clone(),
                    buf,
                ));

                return Ok(merged_blks);
            } else if let Some(prev_compacting_block) = previous_block {
                // Raw block is not full, so decode and merge with compacting_block.
                let decoded_raw_block = tsm::decode_data_block(
                    &buf,
                    head_meta.field_type(),
                    head_meta.val_off() - head_meta.offset(),
                )
                .context(error::ReadTsmSnafu)?;
                if let Some(mut data_block) = prev_compacting_block.decode(&self.time_range)? {
                    data_block.extend(decoded_raw_block);
                    merged_block = Some(data_block);
                }
            } else {
                // Raw block is not full, but nothing to merge with, directly return.
                return Ok(vec![CompactingBlock::raw(
                    self.blk_metas[0].priority,
                    head_meta.clone(),
                    buf,
                )]);
            }
        } else {
            // One block with tombstone or multi compacting blocks, decode and merge these data block.
            trace!(
                "there are {} compacting blocks, need to decode and merge",
                self.blk_metas.len()
            );

            let (mut head_block, mut head_i) = (Option::<DataBlock>::None, 0_usize);
            for (i, meta) in self.blk_metas.iter().enumerate() {
                if let Some(blk) = meta.get_data_block(out_time_range).await? {
                    head_block = Some(blk);
                    head_i = i;
                }
            }
            if let Some(head_blk) = head_block.take() {
                if let Some(prev_compacting_block) = previous_block {
                    if let Some(mut data_block) = prev_compacting_block.decode(&self.time_range)? {
                        data_block.extend(head_blk);
                        head_block = Some(data_block);
                    }
                }
                if let Some(head_blk) = head_block.take() {
                    for blk_meta in self.blk_metas.iter_mut().skip(head_i + 1) {
                        // Merge decoded data block.
                        if let Some(blk) = blk_meta.get_data_block(out_time_range).await? {
                            head_block = Some(head_blk.merge(blk));
                        }
                    }
                }
            }

            merged_block = head_block;
        }
        if let Some(blk) = merged_block {
            chunk_merged_block(self.field_id, blk, max_block_size)
        } else {
            Ok(vec![])
        }
    }

    pub fn into_compacting_block_metas(self) -> Vec<CompactingBlockMeta> {
        self.blk_metas
    }

    pub fn is_empty(&self) -> bool {
        self.blk_metas.is_empty()
    }

    pub fn len(&self) -> usize {
        self.blk_metas.len()
    }
}

impl Display for CompactingBlockMetaGroup {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{field_id: {}, blk_metas: [", self.field_id)?;
        if !self.blk_metas.is_empty() {
            write!(f, "{}", &self.blk_metas[0])?;
            for b in self.blk_metas.iter().skip(1) {
                write!(f, ", {}", b)?;
            }
        }
        write!(f, "]}}")
    }
}

fn chunk_merged_block(
    field_id: FieldId,
    data_block: DataBlock,
    max_block_size: usize,
) -> Result<Vec<CompactingBlock>> {
    let mut merged_blks = Vec::new();
    if max_block_size == 0 || data_block.len() < max_block_size {
        // Data block elements less than max_block_size, do not encode it.
        // Try to merge with the next CompactingBlockMetaGroup.
        merged_blks.push(CompactingBlock::decoded(0, field_id, data_block));
    } else {
        // Data block is so big that split into multi CompactingBlock
        let len = data_block.len();
        let mut start = 0;
        let mut end = len.min(max_block_size);
        while start < len {
            // Encode decoded data blocks into chunks.
            let encoded_blk =
                EncodedDataBlock::encode(&data_block, start, end).map_err(|e| Error::WriteTsm {
                    source: WriteTsmError::Encode { source: e },
                })?;
            merged_blks.push(CompactingBlock::encoded(0, field_id, encoded_blk));

            start = end;
            end = len.min(start + max_block_size);
        }
    }

    Ok(merged_blks)
}

/// Temporary compacting data block.
/// - priority: When merging two (timestamp, value) pair with the same
/// timestamp from two data blocks, pair from data block with lower
/// priority will be discarded.
#[derive(Debug, PartialEq)]
pub(crate) enum CompactingBlock {
    Decoded {
        priority: usize,
        field_id: FieldId,
        data_block: DataBlock,
    },
    Encoded {
        priority: usize,
        field_id: FieldId,
        data_block: EncodedDataBlock,
    },
    Raw {
        priority: usize,
        meta: BlockMeta,
        raw: Vec<u8>,
    },
}

impl Display for CompactingBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CompactingBlock::Decoded {
                priority,
                field_id,
                data_block,
            } => {
                write!(f, "p: {priority}, f: {field_id}, block: {data_block}")
            }
            CompactingBlock::Encoded {
                priority,
                field_id,
                data_block,
            } => {
                write!(f, "p: {priority}, f: {field_id}, block: {data_block}")
            }
            CompactingBlock::Raw {
                priority,
                meta,
                raw,
            } => {
                write!(
                    f,
                    "p: {priority}, f: {}, block: {}: {{ len: {}, min_ts: {}, max_ts: {}, raw_len: {} }}",
                    meta.field_id(),
                    meta.field_type(),
                    meta.count(),
                    meta.min_ts(),
                    meta.max_ts(),
                    raw.len(),
                )
            }
        }
    }
}

impl CompactingBlock {
    pub fn decoded(priority: usize, field_id: FieldId, data_block: DataBlock) -> CompactingBlock {
        Self::Decoded {
            priority,
            field_id,
            data_block,
        }
    }

    pub fn encoded(
        priority: usize,
        field_id: FieldId,
        data_block: EncodedDataBlock,
    ) -> CompactingBlock {
        Self::Encoded {
            priority,
            field_id,
            data_block,
        }
    }

    pub fn raw(priority: usize, meta: BlockMeta, raw: Vec<u8>) -> CompactingBlock {
        CompactingBlock::Raw {
            priority,
            meta,
            raw,
        }
    }

    pub fn decode(self, out_time_range: &TimeRange) -> Result<Option<DataBlock>> {
        let data_block = match self {
            CompactingBlock::Decoded { data_block, .. } => data_block,
            CompactingBlock::Encoded { data_block, .. } => {
                data_block.decode().context(error::DecodeSnafu)?
            }
            CompactingBlock::Raw { raw, meta, .. } => {
                tsm::decode_data_block(&raw, meta.field_type(), meta.val_off() - meta.offset())
                    .context(error::ReadTsmSnafu)?
            }
        };
        if data_block
            .time_range()
            .is_some_and(|tr| out_time_range.overlaps(&tr.into()))
        {
            Ok(data_block.intersection(out_time_range))
        } else {
            Ok(None)
        }
    }

    pub fn len(&self) -> usize {
        match self {
            CompactingBlock::Decoded { data_block, .. } => data_block.len(),
            CompactingBlock::Encoded { data_block, .. } => data_block.count as usize,
            CompactingBlock::Raw { meta, .. } => meta.count() as usize,
        }
    }
}

struct CompactingFile {
    i: usize,
    tsm_reader: Arc<TsmReader>,
    index_iter: BufferedIterator<IndexIterator>,
    field_id: Option<FieldId>,
}

impl CompactingFile {
    fn new(i: usize, tsm_reader: Arc<TsmReader>) -> Self {
        let mut index_iter = BufferedIterator::new(tsm_reader.index_iterator());
        let first_field_id = index_iter.peek().map(|i| i.field_id());
        Self {
            i,
            tsm_reader,
            index_iter,
            field_id: first_field_id,
        }
    }

    fn next(&mut self) -> Option<&IndexMeta> {
        let idx_meta = self.index_iter.next();
        idx_meta.map(|i| self.field_id.replace(i.field_id()));
        idx_meta
    }

    fn peek(&mut self) -> Option<&IndexMeta> {
        self.index_iter.peek()
    }
}

impl Eq for CompactingFile {}

impl PartialEq for CompactingFile {
    fn eq(&self, other: &Self) -> bool {
        self.tsm_reader.file_id() == other.tsm_reader.file_id() && self.field_id == other.field_id
    }
}

impl Ord for CompactingFile {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.field_id.cmp(&other.field_id).reverse()
    }
}

impl PartialOrd for CompactingFile {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Returns if r1 (min_ts, max_ts) overlaps r2 (min_ts, max_ts)
fn overlaps_tuples(r1: (i64, i64), r2: (i64, i64)) -> bool {
    r1.0 <= r2.1 && r1.1 >= r2.0
}

pub(crate) struct WriterWrapper {
    // Init values.
    ts_family_id: TseriesFamilyId,
    out_level: LevelId,
    out_time_range: TimeRange,
    max_file_size: u64,
    tsm_dir: PathBuf,
    delta_dir: PathBuf,
    context: Arc<GlobalContext>,

    // Temporary values.
    tsm_writer_full: bool,
    tsm_writer: Option<TsmWriter>,

    // Result values.
    version_edit: VersionEdit,
    file_metas: HashMap<ColumnFileId, Arc<BloomFilter>>,
}

impl WriterWrapper {
    pub fn new(request: &CompactReq, context: Arc<GlobalContext>) -> Self {
        Self {
            ts_family_id: request.ts_family_id,
            out_level: request.out_level,
            out_time_range: request.time_range,
            max_file_size: request
                .version
                .storage_opt()
                .level_max_file_size(request.out_level),
            tsm_dir: request
                .storage_opt
                .tsm_dir(&request.tenant_database, request.ts_family_id),
            delta_dir: request
                .storage_opt
                .delta_dir(&request.tenant_database, request.ts_family_id),
            context,

            tsm_writer_full: false,
            tsm_writer: None,

            version_edit: VersionEdit::new(request.ts_family_id),
            file_metas: HashMap::new(),
        }
    }

    pub async fn close(mut self) -> Result<(VersionEdit, HashMap<ColumnFileId, Arc<BloomFilter>>)> {
        self.close_writer().await?;
        Ok((self.version_edit, self.file_metas))
    }

    /// Write CompactingBlock to TsmWriter, fill file_metas and version_edit.
    pub async fn write(&mut self, blk: CompactingBlock) -> Result<()> {
        match blk {
            CompactingBlock::Decoded {
                field_id,
                data_block,
                ..
            } => {
                if let Some(tr) = data_block.time_range() {
                    if tr.1 <= self.out_time_range.max_ts {
                        self.write_tsm_data_block(field_id, &data_block).await?;
                    } else if let Some(blk) = data_block.intersection(&self.out_time_range) {
                        if !blk.is_empty() {
                            self.write_tsm_data_block(field_id, &blk).await?;
                        }
                    }
                }
            }
            CompactingBlock::Encoded {
                priority,
                field_id,
                data_block,
                ..
            } => {
                if let Some(tr) = data_block.time_range {
                    if tr.max_ts <= self.out_time_range.max_ts {
                        self.write_tsm_encoded_data_block(field_id, &data_block)
                            .await?;
                    } else if let Some(decoded_blk) =
                        CompactingBlock::encoded(priority, field_id, data_block)
                            .decode(&self.out_time_range)?
                    {
                        if let Some(blk) = decoded_blk.intersection(&self.out_time_range) {
                            if !blk.is_empty() {
                                self.write_tsm_data_block(field_id, &blk).await?;
                            }
                        }
                    }
                }
            }
            CompactingBlock::Raw {
                priority,
                meta,
                raw,
                ..
            } => {
                let tr = meta.time_range();
                if tr.max_ts <= self.out_time_range.max_ts {
                    self.write_tsm_raw_data_block(&meta, &raw).await?;
                } else {
                    let field_id = meta.field_id();
                    if let Some(blk) =
                        CompactingBlock::raw(priority, meta, raw).decode(&self.out_time_range)?
                    {
                        if !blk.is_empty() {
                            self.write_tsm_data_block(field_id, &blk).await?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn write_tsm_data_block(
        &mut self,
        field_id: FieldId,
        data_block: &DataBlock,
    ) -> Result<usize> {
        let write_ret = self
            .tsm_writer_mut()
            .await?
            .write_block(field_id, data_block)
            .await;
        if let Err(WriteTsmError::MaxFileSizeExceed { write_size, .. }) = write_ret {
            self.tsm_writer_full = true;
            return Ok(write_size);
        }
        Self::warp_write_tsm_result(write_ret)
    }

    async fn write_tsm_encoded_data_block(
        &mut self,
        field_id: FieldId,
        data_block: &EncodedDataBlock,
    ) -> Result<usize> {
        let write_ret = self
            .tsm_writer_mut()
            .await?
            .write_encoded_block(field_id, data_block)
            .await;
        if let Err(WriteTsmError::MaxFileSizeExceed { write_size, .. }) = write_ret {
            self.tsm_writer_full = true;
            return Ok(write_size);
        }
        Self::warp_write_tsm_result(write_ret)
    }

    async fn write_tsm_raw_data_block(
        &mut self,
        block_meta: &BlockMeta,
        data_block: &[u8],
    ) -> Result<usize> {
        let write_ret = self
            .tsm_writer_mut()
            .await?
            .write_raw(block_meta, data_block)
            .await;
        if let Err(WriteTsmError::MaxFileSizeExceed { write_size, .. }) = write_ret {
            self.tsm_writer_full = true;
            return Ok(write_size);
        }
        Self::warp_write_tsm_result(write_ret)
    }

    async fn tsm_writer_mut(&mut self) -> Result<&mut TsmWriter> {
        if self.tsm_writer_full {
            self.close_writer().await?;
            self.new_writer().await
        } else {
            match self.tsm_writer {
                Some(ref mut w) => Ok(w),
                None => self.new_writer().await,
            }
        }
    }

    async fn new_writer(&mut self) -> Result<&mut TsmWriter> {
        let writer = tsm::new_tsm_writer(
            &self.tsm_dir,
            self.context.file_id_next(),
            false,
            self.max_file_size,
        )
        .await?;
        info!(
            "Compaction: File: {} been created (level: {}).",
            writer.sequence(),
            self.out_level,
        );

        self.tsm_writer_full = false;
        Ok(self.tsm_writer.insert(writer))
    }

    async fn close_writer(&mut self) -> Result<()> {
        if let Some(mut tsm_writer) = self.tsm_writer.take() {
            tsm_writer
                .write_index()
                .await
                .context(error::WriteTsmSnafu)?;
            tsm_writer.finish().await.context(error::WriteTsmSnafu)?;

            info!(
                "Compaction: File: {} write finished (level: {}, {} B).",
                tsm_writer.sequence(),
                self.out_level,
                tsm_writer.size()
            );

            let file_id = tsm_writer.sequence();
            let cm = CompactMeta {
                file_id,
                file_size: tsm_writer.size(),
                tsf_id: self.ts_family_id,
                level: self.out_level,
                min_ts: tsm_writer.min_ts(),
                max_ts: tsm_writer.max_ts(),
                high_seq: 0,
                low_seq: 0,
                is_delta: false,
            };
            self.version_edit.add_file(cm, tsm_writer.max_ts());
            let bloom_filter = tsm_writer.into_bloom_filter();
            self.file_metas.insert(file_id, Arc::new(bloom_filter));
        }

        Ok(())
    }

    fn warp_write_tsm_result<T: Default>(write_result: WriteTsmResult<T>) -> Result<T> {
        match write_result {
            Ok(size) => Ok(size),
            Err(WriteTsmError::WriteIO { source }) => {
                // TODO try re-run compaction on other time.
                error!("Failed compaction: IO error when write tsm: {:?}", source);
                Err(Error::IO { source })
            }
            Err(WriteTsmError::Encode { source }) => {
                // TODO try re-run compaction on other time.
                error!(
                    "Failed compaction: encoding error when write tsm: {:?}",
                    source
                );
                Err(Error::Encode { source })
            }
            Err(WriteTsmError::Finished { path }) => {
                error!(
                    "Failed compaction: Trying write already finished tsm file: '{}'",
                    path.display()
                );
                Err(Error::WriteTsm {
                    source: WriteTsmError::Finished { path },
                })
            }
            Err(WriteTsmError::MaxFileSizeExceed { .. }) => {
                // This error should be already handled before, ignore.
                error!("WriteTsmError::MaxFileSizeExceed should be handled before.");
                Ok(T::default())
            }
        }
    }
}

#[cfg(test)]
mod test {
    use models::{FieldId, PhysicalDType as ValueType};

    use super::*;
    use crate::compaction::test::{
        check_column_file, create_options, generate_data_block, prepare_compaction,
        write_data_block_desc, TsmSchema,
    };
    use crate::file_system::file_manager;
    use crate::tsm::codec::DataBlockEncoding;

    #[test]
    fn test_chunk_merged_block() {
        let data_block = DataBlock::U64 {
            ts: vec![0, 1, 2, 10, 11, 12, 100, 101, 102, 1000, 1001, 1002],
            val: vec![0, 3, 6, 30, 33, 36, 300, 303, 306, 3000, 3003, 3006],
            enc: DataBlockEncoding::default(),
        };
        let field_id = 1;
        // Trying to chunk with no chunk size
        {
            let chunks = chunk_merged_block(field_id, data_block.clone(), 0).unwrap();
            assert_eq!(chunks.len(), 1);
            assert_eq!(
                chunks[0],
                CompactingBlock::decoded(0, 1, data_block.clone())
            );
        }
        // Trying to chunk with too big chunk size
        {
            let chunks = chunk_merged_block(field_id, data_block.clone(), 100).unwrap();
            assert_eq!(chunks.len(), 1);
            assert_eq!(
                chunks[0],
                CompactingBlock::decoded(0, 1, data_block.clone())
            );
        }
        // Trying to chunk with chunk size that can divide data block exactly
        {
            let chunks = chunk_merged_block(field_id, data_block.clone(), 4).unwrap();
            assert_eq!(chunks.len(), 3);
            assert_eq!(
                chunks[0],
                CompactingBlock::encoded(
                    0,
                    field_id,
                    EncodedDataBlock::encode(&data_block, 0, 4).unwrap()
                )
            );
            assert_eq!(
                chunks[1],
                CompactingBlock::encoded(
                    0,
                    field_id,
                    EncodedDataBlock::encode(&data_block, 4, 8).unwrap()
                )
            );
            assert_eq!(
                chunks[2],
                CompactingBlock::encoded(
                    0,
                    field_id,
                    EncodedDataBlock::encode(&data_block, 8, 12).unwrap()
                )
            );
        }
        // Trying to chunk with chunk size that cannot divide data block exactly
        {
            let chunks = chunk_merged_block(field_id, data_block.clone(), 5).unwrap();
            assert_eq!(chunks.len(), 3);
            assert_eq!(
                chunks[0],
                CompactingBlock::encoded(
                    0,
                    field_id,
                    EncodedDataBlock::encode(&data_block, 0, 5).unwrap()
                )
            );
            assert_eq!(
                chunks[1],
                CompactingBlock::encoded(
                    0,
                    field_id,
                    EncodedDataBlock::encode(&data_block, 5, 10).unwrap()
                )
            );
            assert_eq!(
                chunks[2],
                CompactingBlock::encoded(
                    0,
                    field_id,
                    EncodedDataBlock::encode(&data_block, 10, 12).unwrap()
                )
            );
        }
    }

    /// Test compaction on level-0 (delta compaction) with multi-field.
    #[tokio::test]
    async fn test_delta_compaction() {
        #[rustfmt::skip]
        let data_desc: [TsmSchema; 3] = [
            // [( tsm_data:  tsm_sequence, vec![(ValueType, FieldId, Timestamp_Begin, Timestamp_end)],
            //    tombstone: vec![(FieldId, MinTimestamp, MaxTimestamp)]
            // )]
            (1, vec![
                // 1, 1~2500
                (ValueType::Unsigned, 1, 1, 1000), (ValueType::Unsigned, 1, 1001, 2000),  (ValueType::Unsigned, 1, 2001, 2500),
                // 2, 1~1500
                (ValueType::Integer, 2, 1, 1000), (ValueType::Integer, 2, 1001, 1500),
                // 3, 1~1500
                (ValueType::Boolean, 3, 1, 1000), (ValueType::Boolean, 3, 1001, 1500),
            ], vec![]),
            (2, vec![
                // 1, 2001~4500
                (ValueType::Unsigned, 1, 2001, 3000), (ValueType::Unsigned, 1, 3001, 4000), (ValueType::Unsigned, 1, 4001, 4500),
                // 2, 1001~3000
                (ValueType::Integer, 2, 1001, 2000), (ValueType::Integer, 2, 2001, 3000),
                // 3, 1001~2500
                (ValueType::Boolean, 3, 1001, 2000), (ValueType::Boolean, 3, 2001, 2500),
                // 4, 1~1500
                (ValueType::Float, 4, 1, 1000), (ValueType::Float, 4, 1001, 1500),
            ], vec![]),
            (3, vec![
                // 1, 4001~6500
                (ValueType::Unsigned, 1, 4001, 5000), (ValueType::Unsigned, 1, 5001, 6000), (ValueType::Unsigned, 1, 6001, 6500),
                // 2, 3001~5000
                (ValueType::Integer, 2, 3001, 4000), (ValueType::Integer, 2, 4001, 5000),
                // 3, 2001~3500
                (ValueType::Boolean, 3, 2001, 3000), (ValueType::Boolean, 3, 3001, 3500),
                // 4. 1001~2500
                (ValueType::Float, 4, 1001, 2000), (ValueType::Float, 4, 2001, 2500),
            ], vec![]),
        ];
        #[rustfmt::skip]
        let expected_data_target_level: HashMap<FieldId, Vec<DataBlock>> = HashMap::from(
            [
                // 1, 1~6500
                (1, vec![
                    generate_data_block(ValueType::Unsigned, vec![(1, 1000)]),
                    generate_data_block(ValueType::Unsigned, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Unsigned, vec![(2001, 3000)]),
                ]),
                // 2, 1~5000
                (2, vec![
                    generate_data_block(ValueType::Integer, vec![(1, 1000)]),
                    generate_data_block(ValueType::Integer, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Integer, vec![(2001, 3000)]),
                ]),
                // 3, 1~3500
                (3, vec![
                    generate_data_block(ValueType::Boolean, vec![(1, 1000)]),
                    generate_data_block(ValueType::Boolean, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Boolean, vec![(2001, 3000)]),
                ]),
                // 4, 1~2500
                (4, vec![
                    generate_data_block(ValueType::Float, vec![(1, 1000)]),
                    generate_data_block(ValueType::Float, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Float, vec![(2001, 2500)]),
                ]),
            ]
        );
        #[rustfmt::skip]
        let expected_data_delta_level: HashMap<FieldId, Vec<DataBlock>> = HashMap::from(
            [
                // 1, 1~6500
                (1, vec![
                    generate_data_block(ValueType::Unsigned, vec![(3001, 4000)]),
                    generate_data_block(ValueType::Unsigned, vec![(4001, 5000)]),
                    generate_data_block(ValueType::Unsigned, vec![(5001, 6000)]),
                    generate_data_block(ValueType::Unsigned, vec![(6001, 6500)]),
                ]),
                // 2, 1~5000
                (2, vec![
                    generate_data_block(ValueType::Integer, vec![(3001, 4000)]),
                    generate_data_block(ValueType::Integer, vec![(4001, 5000)]),
                ]),
                // 3, 1~3500
                (3, vec![
                    generate_data_block(ValueType::Boolean, vec![(3001, 3500)]),
                ]),
            ]
        );

        let dir = "/tmp/test/delta_compaction/1";
        let _ = std::fs::remove_dir_all(dir);
        let tenant_database = Arc::new("cnosdb.dba".to_string());
        let opt = create_options(dir.to_string());
        let tsm_dir = opt.storage.tsm_dir(&tenant_database, 1);
        if !file_manager::try_exists(&tsm_dir) {
            std::fs::create_dir_all(&tsm_dir).unwrap();
        }
        let delta_dir = opt.storage.delta_dir(&tenant_database, 1);
        if !file_manager::try_exists(&delta_dir) {
            std::fs::create_dir_all(&delta_dir).unwrap();
        }
        let max_level_ts = 9000;

        let column_files = write_data_block_desc(&delta_dir, &data_desc).await;
        let next_file_id = 4_u64;
        let (mut compact_req, kernel) = prepare_compaction(
            tenant_database,
            opt,
            next_file_id,
            column_files,
            max_level_ts,
        );
        compact_req.in_level = 0;
        compact_req.out_level = 2;
        compact_req.time_range = TimeRange::new(1, 3000);

        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();

        check_column_file(
            &delta_dir,
            version_edit.clone(),
            expected_data_delta_level,
            0,
        )
        .await;
        check_column_file(tsm_dir, version_edit, expected_data_target_level, 2).await;
    }
}
