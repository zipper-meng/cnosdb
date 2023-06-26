use std::collections::BTreeMap;
use std::io::IoSlice;
use std::path::{Path, PathBuf};

use models::{FieldId, Timestamp};
use snafu::ResultExt;
use utils::BloomFilter;

use crate::error::{self, Result as TskvResult};
use crate::file_system::file::cursor::FileCursor;
use crate::file_system::file::IFile;
use crate::file_system::file_manager;
use crate::file_utils;
use crate::tsm::{
    BlockEntry, BlockMeta, DataBlock, EncodedDataBlock, IndexEntry, TsmError, TsmResult,
    BLOCK_META_SIZE, BLOOM_FILTER_BITS, INDEX_META_SIZE,
};

// A TSM file is composed for four sections: header, blocks, index and the footer.
//
// ┌────────┬────────────────────────────────────┬─────────────┬──────────────┐
// │ Header │               Blocks               │    Index    │    Footer    │
// │5 bytes │              N bytes               │   N bytes   │   8 bytes    │
// └────────┴────────────────────────────────────┴─────────────┴──────────────┘
//
// ┌───────────────────┐
// │      Header       │
// ├─────────┬─────────┤
// │  Magic  │ Version │
// │ 4 bytes │ 1 byte  │
// └─────────┴─────────┘
//
// ┌───────────────────────────────────────┐
// │               Blocks                  │
// ├───────────────────┬───────────────────┤
// │                Block                  │
// ├─────────┬─────────┼─────────┬─────────┼
// │  CRC    │ ts      │  CRC    │  value  │
// │ 4 bytes │ N bytes │ 4 bytes │ N bytes │
// └─────────┴─────────┴─────────┴─────────┴
//
// ┌───────────────────────────────────────────────────────────────────────────────┐
// │                               Index                                           │
// ├─────────┬──────┬───────┬─────────┬─────────┬────────┬────────┬────────┬───────┤
// │ fieldId │ Type │ Count │Min Time │Max Time │ count  │ Offset │  Size  │Valoff │
// │ 8 bytes │1 byte│2 bytes│ 8 bytes │ 8 bytes │4 bytes │8 bytes │8 bytes │8 bytes│
// └─────────┴──────┴───────┴─────────┴─────────┴────────┴────────┴────────┴───────┘
//
// ┌─────────────────────────┐
// │ Footer                  │
// ├───────────────┬─────────┤
// │ Bloom Filter  │Index Ofs│
// │ 8 bytes       │ 8 bytes │
// └───────────────┴─────────┘

const HEADER_LEN: u64 = 5;
const TSM_MAGIC: [u8; 4] = 0x01346613_u32.to_be_bytes();
const VERSION: [u8; 1] = [1];

struct IndexBuf {
    index_offset: u64,
    buf: BTreeMap<FieldId, IndexEntry>,
    bloom_filter: BloomFilter,
}

impl IndexBuf {
    pub fn new() -> Self {
        Self {
            index_offset: 0,
            buf: BTreeMap::new(),
            bloom_filter: BloomFilter::new(BLOOM_FILTER_BITS),
        }
    }

    pub fn set_index_offset(&mut self, index_offset: u64) {
        self.index_offset = index_offset;
    }

    pub fn insert_block_meta(&mut self, ie: IndexEntry, be: BlockEntry) {
        let fid = ie.field_id;
        let idx = self.buf.entry(fid).or_insert(ie);
        idx.blocks.push(be);
        self.bloom_filter.insert(&fid.to_be_bytes()[..]);
    }

    pub async fn write_to(&self, writer: &mut FileCursor) -> TsmResult<usize> {
        let mut size = 0_usize;

        let mut buf = vec![0_u8; BLOCK_META_SIZE];
        for (_, idx) in self.buf.iter() {
            idx.encode(&mut buf[..INDEX_META_SIZE])?;
            writer
                .write(&buf[..INDEX_META_SIZE])
                .await
                .with_context(|_| super::WriteSnafu {
                    path: writer.open_path().clone(),
                })?;
            size += INDEX_META_SIZE;
            for blk in idx.blocks.iter() {
                blk.encode(&mut buf);
                writer
                    .write(&buf[..])
                    .await
                    .with_context(|_| super::WriteSnafu {
                        path: writer.open_path().clone(),
                    })?;
                size += BLOCK_META_SIZE;
            }
        }

        Ok(size)
    }
}

/// TSM file writer.
///
/// # Examples
/// ```ignore
/// let path = "/tmp/tsm_writer/test_write.tsm";
/// let file = file_manager::create_file(path);
/// // Create a new TSM file, write header.
/// let mut writer = TsmWriter::open(file).unwrap();
/// // Write blocks.
/// writer.write_block(1, &DataBlock::I64{ ts: vec![1], val: vec![1] }).unwrap();
/// // Write index and footer.
/// writer.write_index().unwrap();
/// // Sync to disk.
/// writer.flush().unwrap();
/// ```
pub struct TsmWriter {
    tmp_path: PathBuf,
    final_path: PathBuf,
    finished: bool,

    writer: FileCursor,
    /// Store tsm sequence for debug
    sequence: u64,
    /// Store is_delta for debug
    is_delta: bool,
    /// Store min_ts for version edit
    min_ts: Timestamp,
    /// Store max_ts for version edit
    max_ts: Timestamp,
    size: u64,
    max_size: u64,
    index_buf: IndexBuf,
}

impl TsmWriter {
    pub async fn open(
        path: impl AsRef<Path>,
        sequence: u64,
        is_delta: bool,
        max_size: u64,
    ) -> TsmResult<Self> {
        let final_path: PathBuf = path.as_ref().into();
        let mut tmp_path_str = final_path.as_os_str().to_os_string();
        tmp_path_str.push(".tmp");
        let tmp_path = PathBuf::from(tmp_path_str);

        let writer = file_manager::create_file(&tmp_path)
            .await
            .context(super::OpenSnafu)?
            .into();
        let mut w = Self {
            tmp_path,
            final_path,
            finished: false,
            writer,
            sequence,
            is_delta,
            min_ts: Timestamp::MAX,
            max_ts: Timestamp::MIN,
            size: 0,
            max_size,
            index_buf: IndexBuf::new(),
        };
        let size = write_header_to(&mut w.writer).await?;
        w.size += size as u64;
        Ok(w)
    }

    pub fn finished(&self) -> bool {
        self.finished
    }

    pub fn path(&self) -> PathBuf {
        if self.finished {
            self.final_path.clone()
        } else {
            self.tmp_path.clone()
        }
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn is_delta(&self) -> bool {
        self.is_delta
    }

    pub fn min_ts(&self) -> Timestamp {
        self.min_ts
    }

    pub fn max_ts(&self) -> Timestamp {
        self.max_ts
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// Write a DataBlock to tsm file. If the max_size is greater than 0,
    /// then check if the current size exceeded, if so return Err(MaxFileSizeExceed).
    pub async fn write_block(&mut self, field_id: FieldId, block: &DataBlock) -> TsmResult<usize> {
        if self.finished {
            return Err(TsmError::Finished {
                path: self.final_path.clone(),
            });
        }
        if let Some((min_ts, max_ts)) = block.time_range() {
            self.min_ts = self.min_ts.min(min_ts);
            self.max_ts = self.max_ts.max(max_ts);
        }

        let size = write_block_to(&mut self.writer, &mut self.index_buf, field_id, block).await?;
        self.size += size as u64;
        if self.max_size > 0 && self.size >= self.max_size {
            Err(TsmError::MaxFileSizeExceed {
                max_size: self.max_size,
                cur_size: self.size,
                write_size: size,
            })
        } else {
            Ok(size)
        }
    }

    /// Write a EncodedDataBlock to tsm file. If the max_size is greater than 0,
    /// then check if the current size exceeded, if so return Err(MaxFileSizeExceed).
    pub async fn write_encoded_block(
        &mut self,
        field_id: FieldId,
        block: &EncodedDataBlock,
    ) -> TsmResult<usize> {
        if self.finished {
            return Err(TsmError::Finished {
                path: self.final_path.clone(),
            });
        }
        if let Some(time_range) = block.time_range {
            self.min_ts = self.min_ts.min(time_range.min_ts);
            self.max_ts = self.max_ts.max(time_range.max_ts);
        }

        let size =
            write_encoded_block_to(&mut self.writer, &mut self.index_buf, field_id, block).await?;
        self.size += size as u64;
        if self.max_size > 0 && self.size >= self.max_size {
            Err(TsmError::MaxFileSizeExceed {
                max_size: self.max_size,
                cur_size: self.size,
                write_size: size,
            })
        } else {
            Ok(size)
        }
    }

    /// Write a u8 slice to tsm file. If the max_size is greater than 0,
    /// then check if the current size exceeded, if so return Err(MaxFileSizeExceed).
    pub async fn write_raw(&mut self, block_meta: &BlockMeta, block: &[u8]) -> TsmResult<usize> {
        if self.finished {
            return Err(TsmError::Finished {
                path: self.final_path.clone(),
            });
        }
        self.min_ts = self.min_ts.min(block_meta.min_ts());
        self.max_ts = self.max_ts.max(block_meta.max_ts());

        let size =
            write_raw_data_to(&mut self.writer, &mut self.index_buf, block_meta, block).await?;
        self.size += size as u64;
        if self.max_size > 0 && self.size >= self.max_size {
            Err(TsmError::MaxFileSizeExceed {
                max_size: self.max_size,
                cur_size: self.size,
                write_size: size,
            })
        } else {
            Ok(size)
        }
    }

    pub async fn write_index(&mut self) -> TsmResult<usize> {
        if self.finished {
            return Err(TsmError::Finished {
                path: self.final_path.clone(),
            });
        }

        self.index_buf.set_index_offset(self.writer.pos());
        let len1 = self.index_buf.write_to(&mut self.writer).await?;
        let len2 = write_footer_to(
            &mut self.writer,
            &self.index_buf.bloom_filter,
            self.index_buf.index_offset,
        )
        .await?;

        Ok(len1 + len2)
    }

    pub async fn finish(&mut self) -> TsmResult<()> {
        if self.finished {
            return Err(TsmError::Finished {
                path: self.final_path.clone(),
            });
        }
        self.writer
            .sync_data()
            .await
            .with_context(|_| super::SyncSnafu {
                path: self.writer.open_path().clone(),
            })?;
        std::fs::rename(&self.tmp_path, &self.final_path).with_context(|_| super::RenameSnafu {
            old: self.tmp_path.clone(),
            new: self.final_path.clone(),
        })?;
        self.finished = true;
        Ok(())
    }

    /// Get a cloned `BloomFilter` from currently buffered index data.
    pub fn bloom_filter_cloned(&self) -> BloomFilter {
        self.index_buf.bloom_filter.clone()
    }
}

pub async fn new_tsm_writer(
    dir: impl AsRef<Path>,
    tsm_sequence: u64,
    is_delta: bool,
    max_size: u64,
) -> TskvResult<TsmWriter> {
    let tsm_path = if is_delta {
        file_utils::make_delta_file_name(dir, tsm_sequence)
    } else {
        file_utils::make_tsm_file_name(dir, tsm_sequence)
    };
    TsmWriter::open(tsm_path, tsm_sequence, is_delta, max_size)
        .await
        .context(error::TsmSnafu)
}

pub async fn write_header_to(writer: &mut FileCursor) -> TsmResult<usize> {
    writer
        .write_vec(
            [
                IoSlice::new(TSM_MAGIC.as_slice()),
                IoSlice::new(VERSION.as_slice()),
            ]
            .as_mut_slice(),
        )
        .await
        .with_context(|_| super::WriteSnafu {
            path: writer.open_path().clone(),
        })
}

async fn write_raw_data_to(
    writer: &mut FileCursor,
    index_buf: &mut IndexBuf,
    block_meta: &BlockMeta,
    block: &[u8],
) -> TsmResult<usize> {
    let mut size = 0_usize;
    let offset = writer.pos();
    writer
        .write(block)
        .await
        .map(|s| {
            size += s;
        })
        .with_context(|_| super::WriteSnafu {
            path: writer.open_path().clone(),
        })?;

    index_buf.insert_block_meta(
        IndexEntry {
            field_id: block_meta.field_id(),
            field_type: block_meta.field_type(),
            blocks: vec![],
        },
        BlockEntry::with_block_meta(block_meta, offset, block.len() as u64),
    );

    Ok(size)
}

async fn write_block_to(
    writer: &mut FileCursor,
    index_buf: &mut IndexBuf,
    field_id: FieldId,
    block: &DataBlock,
) -> TsmResult<usize> {
    if block.is_empty() {
        return Ok(0);
    }

    let offset = writer.pos();

    // TODO Make encoding result streamable
    let (ts_buf, data_buf) = block
        .encode(0, block.len(), block.encodings())
        .context(super::EncodeSnafu)?;

    let size = writer
        .write_vec(
            [
                IoSlice::new(crc32fast::hash(&ts_buf).to_be_bytes().as_slice()),
                IoSlice::new(&ts_buf),
                IoSlice::new(crc32fast::hash(&data_buf).to_be_bytes().as_slice()),
                IoSlice::new(&data_buf),
            ]
            .as_mut_slice(),
        )
        .await
        .with_context(|_| super::WriteSnafu {
            path: writer.open_path().clone(),
        })?;

    index_buf.insert_block_meta(
        IndexEntry {
            field_id,
            field_type: block.field_type(),
            blocks: vec![],
        },
        BlockEntry::with_block(block, offset, size as u64, ts_buf.len() as u64)
            .expect("data_block is not empty"),
    );

    Ok(size)
}

async fn write_encoded_block_to(
    writer: &mut FileCursor,
    index_buf: &mut IndexBuf,
    field_id: FieldId,
    block: &EncodedDataBlock,
) -> TsmResult<usize> {
    if block.count == 0 || block.ts.is_empty() {
        return Ok(0);
    }

    let offset = writer.pos();
    let size = writer
        .write_vec(
            [
                IoSlice::new(crc32fast::hash(&block.ts).to_be_bytes().as_slice()),
                IoSlice::new(&block.ts),
                IoSlice::new(crc32fast::hash(&block.val).to_be_bytes().as_slice()),
                IoSlice::new(&block.val),
            ]
            .as_mut_slice(),
        )
        .await
        .with_context(|_| super::WriteSnafu {
            path: writer.open_path().clone(),
        })?;

    let time_range = block.time_range.expect("EncodedDataBlock is not empty");

    index_buf.insert_block_meta(
        IndexEntry {
            field_id,
            field_type: block.field_type,
            blocks: vec![],
        },
        BlockEntry {
            min_ts: time_range.min_ts,
            max_ts: time_range.max_ts,
            count: block.count,
            offset,
            size: size as u64,
            // Encoded timestamps block need a 4-byte crc checksum together.
            val_offset: offset + block.ts.len() as u64 + 4,
        },
    );

    Ok(size)
}

async fn write_footer_to(
    writer: &mut FileCursor,
    bloom_filter: &BloomFilter,
    index_offset: u64,
) -> TsmResult<usize> {
    let size = writer
        .write_vec(
            [
                IoSlice::new(bloom_filter.bytes()),
                IoSlice::new(index_offset.to_be_bytes().as_slice()),
            ]
            .as_mut_slice(),
        )
        .await
        .with_context(|_| super::WriteSnafu {
            path: writer.open_path().clone(),
        })?;
    Ok(size)
}

#[cfg(test)]
pub mod tsm_writer_tests {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};

    use models::FieldId;
    use snafu::ResultExt;

    use crate::file_system::file_manager::{self};
    use crate::file_utils::{self, make_tsm_file_name};
    use crate::tsm::codec::DataBlockEncoding;
    use crate::tsm::tsm_reader_tests::read_and_check;
    use crate::tsm::{self, DataBlock, TsmError, TsmReader, TsmResult, TsmWriter};

    const TEST_PATH: &str = "/tmp/test/tsm_writer";

    pub async fn write_to_tsm(
        path: impl AsRef<Path>,
        data: &HashMap<FieldId, Vec<DataBlock>>,
    ) -> TsmResult<()> {
        let tsm_seq =
            file_utils::get_tsm_file_id_by_path(&path).context(tsm::InvalidFileNameSnafu)?;
        let path = path.as_ref();
        let dir = path.parent().unwrap();
        if !file_manager::try_exists(dir) {
            std::fs::create_dir_all(dir).map_err(|e| TsmError::Other {
                path: PathBuf::new(),
                message: format!("Failed to create directory '{}'", dir.display()),
            })?;
        }
        let mut writer = TsmWriter::open(path, tsm_seq, false, 0).await?;
        for (fid, blks) in data.iter() {
            for blk in blks.iter() {
                writer.write_block(*fid, blk).await?;
            }
        }
        writer.write_index().await?;
        writer.finish().await
    }

    #[tokio::test]
    async fn test_tsm_write_fast() {
        #[rustfmt::skip]
        let data: HashMap<FieldId, Vec<DataBlock>> = HashMap::from([
            (1, vec![DataBlock::U64 { ts: vec![2, 3, 4], val: vec![12, 13, 15], enc: DataBlockEncoding::default() }]),
            (2, vec![DataBlock::U64 { ts: vec![2, 3, 4], val: vec![101, 102, 103], enc: DataBlockEncoding::default() }]),
        ]);

        let tsm_file = make_tsm_file_name(TEST_PATH, 0);
        write_to_tsm(&tsm_file, &data).await.unwrap();

        let reader = TsmReader::open(tsm_file).await.unwrap();
        read_and_check(&reader, &data).await.unwrap();
    }

    #[tokio::test]
    async fn test_tsm_write_1() {
        let mut ts_1: Vec<i64> = Vec::new();
        let mut val_1: Vec<i64> = Vec::new();
        for i in 1..1001 {
            ts_1.push(i as i64);
            val_1.push(i as i64);
        }
        let mut ts_2: Vec<i64> = Vec::new();
        let mut val_2: Vec<i64> = Vec::new();
        for i in 1001..2001 {
            ts_2.push(i as i64);
            val_2.push(i as i64);
        }

        #[rustfmt::skip]
        let data = HashMap::from([
            (1, vec![
                DataBlock::I64 { ts: ts_1, val: val_1, enc: DataBlockEncoding::default() },
                DataBlock::I64 { ts: ts_2, val: val_2, enc: DataBlockEncoding::default() },
            ]),
        ]);

        let tsm_file = make_tsm_file_name(TEST_PATH, 1);
        write_to_tsm(&tsm_file, &data).await.unwrap();

        let reader = TsmReader::open(tsm_file).await.unwrap();
        read_and_check(&reader, &data).await.unwrap();
    }
}
