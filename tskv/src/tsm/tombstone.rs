//! # Tombstone file
//!
//! A tombstone file is a [`record_file`].
//!
//! ## Record Data (v1)
//! ```text
//! +------------+------------+---------------+---------------+
//! | 0: 4 bytes | 0: 4 bytes | 8: 8 bytes    | 16: 8 bytes   |
//! +------------+------------+---------------+---------------+
//! | series_id  | column_id  | min_timestamp | max_timestamp |
//! +------------+------------+---------------+---------------+
//! ```
//!
//! ## Record Data (v2)
//! ```text
//! # field_typ = FIELD_TYPE_ONE(0x00)
//! +------------------------------------------------+------------------------------------
//! | TOMBSTONE_HEADER_ONE                           | time_ranges: time_rnages_num
//! +-----------------+------------+-----------------+---------------+---------------+----
//! | 0: 1 byte       | 1: 4 bytes | 9: 4 bytes      | 8: 8 bytes    | 16: 8 bytes   | ...
//! +-----------------+------------+-----------------+---------------+---------------+----
//! | field_typ(0x00) | series_id  | time_ranges_num | min_timestamp | max_timestamp | ...
//! +-----------------+------------+-----------------+---------------+---------------+----
//!
//! # field_typ = FIELD_TYPE_ALL(0x01)
//! +-----------------------------------+------------------------------------
//! | TOMBSTONE_HEADER_ALL              | time_ranges: time_rnages_num
//! +-----------------+-----------------+---------------+---------------+----
//! | 0: 1 byte       | 9: 4 bytes      | 8: 8 bytes    | 16: 8 bytes   | ...
//! +-----------------+-----------------+---------------+---------------+----
//! | field_typ(0x01) | time_ranges_num | min_timestamp | max_timestamp | ...
//! +-----------------+-----------------+---------------+---------------+----
//! ```

use std::collections::HashMap;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use models::predicate::domain::{TimeRange, TimeRanges};
use models::{ColumnId, SeriesId};
use parking_lot::Mutex;
use tokio::sync::Mutex as AsyncMutex;

use crate::file_system::file_manager;
use crate::record_file::{self, RecordDataType};
use crate::{byte_utils, file_utils, Error, Result};

pub const TOMBSTONE_FILE_SUFFIX: &str = "tombstone";
const FOOTER_MAGIC_NUMBER: u32 = u32::from_be_bytes([b'r', b'o', b'm', b'b']);
const FOOTER_MAGIC_NUMBER_LEN: usize = 4;
const RECORD_VERSION_V1: u8 = 1_u8;
const RECORD_VERSION_V2: u8 = 2_u8;
const MAX_RECORD_LEN: usize = 1024 * 128;
const MAX_RECORD_TIME_RANGES_NUM: u32 = 1000_u32;
const MAX_TOMBSTONE_LEN: usize = 25;

/// Tombstone v2: entry key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TombstoneKey {
    /// Represents a series data.
    ///
    /// By: `DELETE FROM <table>`, or `DROP TABLE <table>`.
    Series(SeriesId),

    /// Represents a column of a series data.
    ///
    /// By: `ALTER TABLE <table> DROP COLUMN <column>`.
    Column(SeriesId, ColumnId),

    /// Represents all series data in a tsm file.
    /// By: Delta compaction.
    All,
}

impl TombstoneKey {
    const TYPE_SERIES: u8 = 0x00;
    const TYPE_COLUMN: u8 = 0x01;
    const TYPE_ALL: u8 = 0x0F;

    fn encode_len(&self) -> usize {
        match self {
            TombstoneKey::Series(_) => 5,
            TombstoneKey::Column(_, _) => 9,
            TombstoneKey::All => 1,
        }
    }

    pub fn encode(&self, buffer: &mut Vec<u8>) {
        match self {
            Self::Series(series_id) => {
                buffer.push(Self::TYPE_SERIES);
                buffer.extend_from_slice(series_id.to_be_bytes().as_slice());
            }
            Self::Column(series_id, column_id) => {
                buffer.push(Self::TYPE_COLUMN);
                buffer.extend_from_slice(series_id.to_be_bytes().as_slice());
                buffer.extend_from_slice(column_id.to_be_bytes().as_slice());
            }
            Self::All => {
                buffer.push(Self::TYPE_ALL);
            }
        }
    }
}

/// Tombstone v2: entry key and time_ranges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TombstoneEntry {
    pub key: TombstoneKey,
    pub time_ranges: Vec<TimeRange>,
}

impl TombstoneEntry {
    const ENTRY_LEGACY_LEN: usize = 24; // sid:4 + cid:4 + min_ts:8 + max_ts:8

    const ENTRY_HEADER_LEN_SERIES: usize = 9; // type:1 + sid:4 + time_range_num:4
    const ENTRY_HEADER_LEN_COLUMN: usize = 13; // type:1 + sid:4 + cid:4 + time_range_num:4
    const ENTRY_HEADER_LEN_ALL: usize = 5; // type:1 + time_range_num:4

    pub fn decode(data: &[u8], buffer: &mut Vec<Self>) -> Result<()> {
        let mut slice: &[u8] = data;
        while !slice.is_empty() {
            match slice[0] {
                TombstoneKey::TYPE_SERIES => {
                    if slice.len() < Self::ENTRY_HEADER_LEN_SERIES {
                        return Err(Error::InvalidData {
                            message: format!(
                                "buf_len too small for tomb_header_series: {} (<{})",
                                slice.len(),
                                Self::ENTRY_HEADER_LEN_SERIES
                            ),
                        });
                    }
                    slice = Self::decode_entry_type_series(slice, buffer)?;
                }
                TombstoneKey::TYPE_COLUMN => {
                    if slice.len() < Self::ENTRY_HEADER_LEN_COLUMN {
                        return Err(Error::InvalidData {
                            message: format!(
                                "buf_len too small for tomb_header_column: {} (<{})",
                                slice.len(),
                                Self::ENTRY_HEADER_LEN_COLUMN
                            ),
                        });
                    }
                    slice = Self::decode_entry_type_column(slice, buffer)?;
                }
                TombstoneKey::TYPE_ALL => {
                    if slice.len() < Self::ENTRY_HEADER_LEN_ALL {
                        return Err(Error::InvalidData {
                            message: format!(
                                "buf_len too small for tomb_header_all: {} (<{})",
                                slice.len(),
                                Self::ENTRY_HEADER_LEN_ALL
                            ),
                        });
                    }
                    slice = Self::decode_entry_type_all(slice, buffer)?;
                }
                _ => {
                    return Err(Error::InvalidData {
                        message: format!("invalid field type: {}", slice[0]),
                    });
                }
            }
        }

        Ok(())
    }

    /// Decode `sid: u32` and `time_range_num: u32` and `time_ranges: [(i64, i64); time_range_num]` from data.
    fn decode_entry_type_series<'a>(data: &'a [u8], buffer: &mut Vec<Self>) -> Result<&'a [u8]> {
        let series_id = byte_utils::decode_be_u32(&data[1..5]);
        let key = TombstoneKey::Series(series_id);
        let (data_rem, time_ranges) = Self::decode_time_ranges(&data[5..])?;
        buffer.push(Self { key, time_ranges });
        Ok(data_rem)
    }

    /// Decode `sid: u32`, `cid: u32` and `time_range_num: u32` and `time_ranges: [(i64, i64); time_range_num]` from data.
    fn decode_entry_type_column<'a>(data: &'a [u8], buffer: &mut Vec<Self>) -> Result<&'a [u8]> {
        let series_id = byte_utils::decode_be_u32(&data[1..5]);
        let column_id = byte_utils::decode_be_u32(&data[5..9]);
        let key = TombstoneKey::Column(series_id, column_id);
        let (data_rem, time_ranges) = Self::decode_time_ranges(&data[9..])?;
        buffer.push(Self { key, time_ranges });
        Ok(data_rem)
    }

    /// Decode `time_range_num: u32` and `time_ranges: [(i64, i64); time_range_num]` from data.
    fn decode_entry_type_all<'a>(data: &'a [u8], buffer: &mut Vec<Self>) -> Result<&'a [u8]> {
        let key = TombstoneKey::All;
        let (data_rem, time_ranges) = Self::decode_time_ranges(&data[1..])?;
        buffer.push(Self { key, time_ranges });
        Ok(data_rem)
    }

    /// Decode `time_range_num: u32` and `time_ranges: [(i64, i64); time_range_num]` from data.
    fn decode_time_ranges(data: &[u8]) -> Result<(&[u8], Vec<TimeRange>)> {
        let time_ranges_num = byte_utils::decode_be_u32(&data[0..4]) as usize;
        let time_ranges_len = (8 + 8) * time_ranges_num;
        let time_ranges_sli = &data[4..];
        if time_ranges_sli.len() < time_ranges_len {
            return Err(Error::InvalidData {
                message: format!(
                    "buf_len too small for tomb_time_ranges: {} (<{time_ranges_len})",
                    data.len()
                ),
            });
        }
        let mut time_ranges: Vec<TimeRange> = Vec::with_capacity(time_ranges_num);

        let mut pos = 0;
        for _ in 0..time_ranges_num {
            let min_ts = byte_utils::decode_be_i64(&time_ranges_sli[pos..]);
            let max_ts = byte_utils::decode_be_i64(&time_ranges_sli[pos + 8..]);
            time_ranges.push((min_ts, max_ts).into());
            pos += 16;
        }

        Ok((&time_ranges_sli[time_ranges_len..], time_ranges))
    }

    /// Encode `key: (u8, opt_u32)` and `time_range_num: u32` and `time_ranges: [(i64, i64); time_range_num]` to buffer,
    /// and return the number of encoded time_ranges.
    pub fn encode(
        &self,
        time_ranges_off: usize,
        max_time_ranges_num: usize,
        buffer: &mut Vec<u8>,
    ) -> std::result::Result<(), usize> {
        if time_ranges_off >= self.time_ranges.len() {
            return Ok(());
        }

        let time_ranges: &[TimeRange] = &self.time_ranges[time_ranges_off..];
        let time_ranges_num: usize = time_ranges.len().min(max_time_ranges_num);

        self.key.encode(buffer);
        buffer.extend_from_slice((time_ranges_num as u32).to_be_bytes().as_slice());
        for time_range in time_ranges[..time_ranges_num].iter() {
            buffer.extend_from_slice(time_range.min_ts.to_be_bytes().as_slice());
            buffer.extend_from_slice(time_range.max_ts.to_be_bytes().as_slice());
        }

        if time_ranges_num < time_ranges.len() {
            Err(time_ranges_num)
        } else {
            Ok(())
        }
    }

    /// Decode `seires_id: u32`, `column_id: u32` and `time_range: (i64, i64)` from data.
    pub fn decode_legacy_v1(data: &[u8]) -> Result<((SeriesId, ColumnId), TimeRange)> {
        if data.len() < Self::ENTRY_LEGACY_LEN {
            return Err(Error::InvalidData {
                message: format!("field length too small: {}", data.len()),
            });
        }
        let series_id = byte_utils::decode_be_u32(&data[0..4]);
        let column_id = byte_utils::decode_be_u32(&data[4..8]);
        let min_ts = byte_utils::decode_be_i64(&data[8..16]);
        let max_ts = byte_utils::decode_be_i64(&data[16..24]);
        Ok(((series_id, column_id), TimeRange::new(min_ts, max_ts)))
    }
}

/// Tombstones for a tsm file
///
/// - file_name: _%06d.tombstone
/// - header: b"TOMB" 4 bytes
/// - loop begin
///   - field_type: u8 1 byte
///   - series_id: opt u32 4 bytes
///   - time_range_num: u32 4 bytes
///   - loop begin
///     - min: i64 8 bytes
///     - max: i64 8 bytes
///   - loop end
/// - loop end
pub struct TsmTombstone {
    /// Tombstone caches.
    cache: Mutex<TsmTombstoneCache>,

    path: PathBuf,
    /// Async record file writer.
    ///
    /// If you want to use self::writer and self::tombstones at the same time,
    /// lock writer first then tombstones.
    writer: Arc<AsyncMutex<Option<record_file::Writer>>>,
}

impl TsmTombstone {
    pub async fn open(path: impl AsRef<Path>, tsm_file_id: u64) -> Result<Self> {
        let path = file_utils::make_tsm_tombstone_file(path, tsm_file_id);
        let (mut reader, writer) = if file_manager::try_exists(&path) {
            (
                Some(record_file::Reader::open(&path).await?),
                Some(record_file::Writer::open(&path, RecordDataType::Tombstone).await?),
            )
        } else {
            (None, None)
        };

        let cache = if let Some(r) = reader.as_mut() {
            TsmTombstoneCache::load_from(r, false).await?
        } else {
            TsmTombstoneCache::default()
        };

        Ok(Self {
            cache: Mutex::new(cache),
            path,
            writer: Arc::new(AsyncMutex::new(writer)),
        })
    }

    #[cfg(test)]
    pub async fn with_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let parent = path.parent().expect("a valid tsm/tombstone file path");
        let tsm_file_id = file_utils::get_tsm_file_id_by_path(path)?;
        Self::open(parent, tsm_file_id).await
    }

    pub fn is_empty(&self) -> bool {
        self.cache.lock().is_empty()
    }

    pub async fn add_range(
        &mut self,
        series_ids: &[SeriesId],
        column_ids: &[ColumnId],
        time_range: &TimeRange,
    ) -> Result<()> {
        let mut tomb_entries = Vec::with_capacity(series_ids.len());
        for cid in column_ids.iter().copied() {
            for sid in series_ids {
                tomb_entries.push(TombstoneEntry {
                    key: TombstoneKey::Column(*sid, cid),
                    time_ranges: vec![*time_range],
                });
            }
            self.add_tombstone(std::mem::replace(
                &mut tomb_entries,
                Vec::with_capacity(series_ids.len()),
            ))
            .await?;
            tomb_entries.clear();
        }
        Ok(())
    }

    pub async fn add_tombstone(&mut self, tombstone_entries: Vec<TombstoneEntry>) -> Result<()> {
        let mut writer_lock: tokio::sync::MutexGuard<'_, Option<record_file::Writer>> =
            self.writer.lock().await;
        if writer_lock.is_none() {
            *writer_lock =
                Some(record_file::Writer::open(&self.path, RecordDataType::Tombstone).await?);
        }
        let writer = writer_lock
            .as_mut()
            .expect("initialized record file writer");

        let mut write_buf = Vec::with_capacity(MAX_RECORD_LEN);
        let mut tomb_tmp: HashMap<TombstoneKey, Vec<TimeRange>> =
            HashMap::with_capacity(tombstone_entries.len());
        for mut tomb_entry in tombstone_entries {
            write_tombstone_buffer(writer, &mut write_buf, &tomb_entry).await?;
            tomb_tmp
                .entry(tomb_entry.key)
                .or_default()
                .append(&mut tomb_entry.time_ranges);
        }
        if !write_buf.is_empty() {
            write_tombstone_record(writer, &write_buf).await?;
        }
        self.cache.lock().insert_batch(tomb_tmp);

        Ok(())
    }

    #[cfg(test)]
    pub async fn add_range_legacy_v1(
        &mut self,
        columns: &[(SeriesId, ColumnId)],
        time_range: &TimeRange,
    ) -> Result<()> {
        use crate::record_file::RecordDataVersion;
        let mut writer_lock = self.writer.lock().await;
        if writer_lock.is_none() {
            *writer_lock =
                Some(record_file::Writer::open(&self.path, RecordDataType::Tombstone).await?);
        }
        let writer = writer_lock
            .as_mut()
            .expect("initialized record file writer");

        let mut write_buf = [0_u8; 24];
        let mut tomb_tmp: HashMap<TombstoneKey, Vec<TimeRange>> =
            HashMap::with_capacity(columns.len());
        for (series_id, column_id) in columns.iter().copied() {
            write_buf[0..4].copy_from_slice((series_id).to_be_bytes().as_slice());
            write_buf[4..8].copy_from_slice((column_id).to_be_bytes().as_slice());
            write_buf[8..16].copy_from_slice(time_range.min_ts.to_be_bytes().as_slice());
            write_buf[16..24].copy_from_slice(time_range.max_ts.to_be_bytes().as_slice());
            writer
                .write_record(
                    RecordDataVersion::V1 as u8,
                    RecordDataType::Tombstone as u8,
                    &[&write_buf],
                )
                .await?;

            tomb_tmp
                .entry(TombstoneKey::Column(series_id, column_id))
                .or_default()
                .push(*time_range);
        }
        self.cache.lock().insert_batch(tomb_tmp);
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        if let Some(w) = self.writer.lock().await.as_mut() {
            w.sync().await?;
        }
        Ok(())
    }

    pub(crate) fn cache(&self) -> impl Deref<Target = TsmTombstoneCache> + '_ {
        self.cache.lock()
    }

    pub(crate) fn series_excluded_cloned(&self) -> HashMap<SeriesId, TimeRanges> {
        self.cache.lock().series_excluded().clone()
    }

    pub(crate) fn columns_excluded_cloned(&self) -> HashMap<(SeriesId, ColumnId), TimeRanges> {
        self.cache.lock().columns_excluded().clone()
    }

    pub(crate) fn all_excluded_cloned(&self) -> TimeRanges {
        self.cache.lock().all_excluded().clone()
    }

    pub fn overlaps_series_time_range(&self, series_id: SeriesId, time_range: &TimeRange) -> bool {
        self.cache
            .lock()
            .overlaps_series_time_range(series_id, time_range)
    }

    pub fn overlaps_column_time_range(
        &self,
        series_id: SeriesId,
        column_id: ColumnId,
        time_range: &TimeRange,
    ) -> bool {
        self.cache
            .lock()
            .overlaps_column_time_range(series_id, column_id, time_range)
    }

    /// Returns all tombstone `TimeRange`s that overlaps the given `TimeRange`.
    /// Returns None if there is nothing to return, or `TimeRange`s is empty.
    pub fn get_overlapped_time_ranges(
        &self,
        series_id: SeriesId,
        column_id: ColumnId,
        time_range: &TimeRange,
    ) -> Vec<TimeRange> {
        self.cache
            .lock()
            .overlapped_time_ranges(series_id, column_id, time_range)
    }
}

async fn write_tombstone_buffer(
    writer: &mut record_file::Writer,
    write_buf: &mut Vec<u8>,
    tombstome_entry: &TombstoneEntry,
) -> Result<()> {
    let mut time_ranges_off = 0_usize;
    while let Err(tr_num) = tombstome_entry.encode(
        time_ranges_off,
        MAX_RECORD_TIME_RANGES_NUM as usize,
        write_buf,
    ) {
        time_ranges_off += tr_num;
        if write_buf.len() >= MAX_RECORD_LEN {
            write_tombstone_record(writer, write_buf).await?;
            write_buf.clear();
        }
    }
    if write_buf.len() >= MAX_RECORD_LEN {
        write_tombstone_record(writer, write_buf).await?;
        write_buf.clear();
    }

    Ok(())
}

async fn write_tombstone_record(writer: &mut record_file::Writer, data: &[u8]) -> Result<usize> {
    writer
        .write_record(RECORD_VERSION_V2, RecordDataType::Tombstone as u8, &[data])
        .await
}

/// Generate pathbuf by tombstone path.
/// - For tombstone file: /tmp/test/000001.tombstone, return /tmp/test/000001.compact.tmp
pub fn tombstone_compact_tmp_path(tombstone_path: &Path) -> Result<PathBuf> {
    match tombstone_path.file_name().map(|os_str| {
        let mut s = os_str.to_os_string();
        s.push(".compact.tmp");
        s
    }) {
        Some(name) => {
            let mut p = tombstone_path.to_path_buf();
            p.set_file_name(name);
            Ok(p)
        }
        None => Err(Error::InvalidFileName {
            file_name: tombstone_path.display().to_string(),
            message: "invalid tombstone file name".to_string(),
        }),
    }
}

#[derive(Clone)]
pub struct TsmTombstoneCache {
    /// The excluded time ranges of each series.
    /// Got data by executing DELETE_FROM_TABLE or DROP_TABLE statements.
    series_excluded: HashMap<SeriesId, TimeRanges>,

    /// The excluded time ranges of each column of series.
    /// Got data by executing ALTER_TABLE_DROP_COLUMN statements.
    columns_excluded: HashMap<(SeriesId, ColumnId), TimeRanges>,

    /// The excluded time range of all fields.
    /// Got data by compacting part of current TSM file with another TSM file.
    all_excluded: TimeRanges,
}

impl Default for TsmTombstoneCache {
    fn default() -> Self {
        Self {
            series_excluded: HashMap::new(),
            columns_excluded: HashMap::new(),
            all_excluded: TimeRanges::empty(),
        }
    }
}

impl TsmTombstoneCache {
    #[cfg(test)]
    pub fn with_all_excluded(all_excluded: TimeRange) -> Self {
        Self {
            all_excluded: TimeRanges::new(vec![all_excluded]),
            ..Default::default()
        }
    }

    ///  Append time_range, but if time_range is None, does nothing.
    pub fn insert(&mut self, tombstone_key: TombstoneKey, time_range: TimeRange) {
        if time_range.is_none() || self.all_excluded.includes(&time_range) {
            return;
        }
        match tombstone_key {
            TombstoneKey::Series(series_id) => {
                // Adding for one SERIES: merge with existing tombstones.
                self.series_excluded
                    .entry(series_id)
                    .or_insert_with(TimeRanges::empty)
                    .push(time_range);
            }
            TombstoneKey::Column(series_id, column_id) => {
                // Adding for one COLUMN: merge with existing tombstones.
                self.columns_excluded
                    .entry((series_id, column_id))
                    .or_insert_with(TimeRanges::empty)
                    .push(time_range);
            }
            TombstoneKey::All => {
                // Adding for ALL: just merge with all_excluded.
                self.all_excluded.push(time_range);
            }
        }
    }

    pub fn insert_batch(&mut self, batch: HashMap<TombstoneKey, Vec<TimeRange>>) {
        for (key, time_ranges) in batch {
            match key {
                TombstoneKey::Series(series_id) => {
                    self.series_excluded
                        .entry(series_id)
                        .or_insert_with(TimeRanges::empty)
                        .extend_from_slice(&time_ranges);
                }
                TombstoneKey::Column(series_id, column_id) => {
                    self.columns_excluded
                        .entry((series_id, column_id))
                        .or_insert_with(TimeRanges::empty)
                        .extend_from_slice(&time_ranges);
                }
                TombstoneKey::All => {
                    self.all_excluded.extend_from_slice(&time_ranges);
                }
            }
        }
    }

    pub fn compact(&mut self) {
        let mut all_excluded = true;

        for time_ranges in self.series_excluded.values_mut() {
            if self.all_excluded.includes_time_ranges(time_ranges) {
                time_ranges.clear();
                continue;
            }
            // Compact time ranges of series with all_excluded.
            all_excluded = false;
        }
        if all_excluded {
            self.series_excluded.clear();
        }

        all_excluded = true;

        for ((series_id, _), time_ranges) in self.columns_excluded.iter_mut() {
            if self.all_excluded.includes_time_ranges(time_ranges) {
                time_ranges.clear();
                continue;
            }
            if let Some(series_time_ranges) = self.series_excluded.get(series_id) {
                if series_time_ranges.includes_time_ranges(time_ranges) {
                    time_ranges.clear();
                    continue;
                }
            }

            // Compact time ranges of columns with all_excluded.
            all_excluded = false;
        }
        if all_excluded {
            self.columns_excluded.clear();
        }
    }

    pub fn overlaps_series_time_range(&self, series_id: SeriesId, time_range: &TimeRange) -> bool {
        if self.all_excluded.overlaps(time_range) {
            return true;
        }
        if let Some(time_ranges) = self.series_excluded.get(&series_id) {
            for t in time_ranges.time_ranges() {
                if t.overlaps(time_range) {
                    return true;
                }
            }
        }
        false
    }

    pub fn overlaps_column_time_range(
        &self,
        series_id: SeriesId,
        column_id: ColumnId,
        time_range: &TimeRange,
    ) -> bool {
        if self.overlaps_series_time_range(series_id, time_range) {
            return true;
        }
        if let Some(time_ranges) = self.columns_excluded.get(&(series_id, column_id)) {
            for t in time_ranges.time_ranges() {
                if t.overlaps(time_range) {
                    return true;
                }
            }
        }
        false
    }

    pub fn overlapped_time_ranges(
        &self,
        series_id: SeriesId,
        column_id: ColumnId,
        time_range: &TimeRange,
    ) -> Vec<TimeRange> {
        let mut trs = Vec::new();
        if self.all_excluded.includes(time_range) {
            trs.extend(self.all_excluded.time_ranges());
        }
        if let Some(time_ranges) = self.series_excluded.get(&series_id) {
            for t in time_ranges.time_ranges() {
                if t.overlaps(time_range) {
                    trs.push(t);
                }
            }
        }
        if let Some(time_ranges) = self.columns_excluded.get(&(series_id, column_id)) {
            for t in time_ranges.time_ranges() {
                if t.overlaps(time_range) {
                    trs.push(t);
                }
            }
        }
        trs
    }

    pub fn check_all_fields_excluded_time_range(&self, time_range: &TimeRange) -> bool {
        self.all_excluded.includes(time_range)
    }

    pub async fn load(path: impl AsRef<Path>) -> Result<Option<Self>> {
        let mut reader = if file_manager::try_exists(&path) {
            record_file::Reader::open(&path).await?
        } else {
            return Ok(None);
        };

        let cache = Self::load_from(&mut reader, true).await?;
        Ok(Some(cache))
    }

    /// Load tombstones from a record file, if `just_stop_if_error` is true,
    /// errors except EOF only stops the read of remaining data and return.
    pub async fn load_from(
        reader: &mut record_file::Reader,
        just_stop_if_error: bool,
    ) -> Result<Self> {
        let mut columns_excluded: HashMap<(SeriesId, ColumnId), TimeRanges> = HashMap::new();
        let mut series_excluded: HashMap<SeriesId, TimeRanges> = HashMap::new();
        let mut all_excluded = TimeRanges::empty();
        let mut buffer: Vec<TombstoneEntry> = Vec::new();
        let mut next_record_pos = 0_u64;
        loop {
            let record = match reader.read_record().await {
                Ok(r) => {
                    // TODO(zipper): add pos in errors from record reader.
                    next_record_pos =
                        r.pos + r.data.len() as u64 + record_file::RECORD_HEADER_LEN as u64;
                    r
                }
                Err(Error::Eof) => break,
                Err(e) => {
                    if just_stop_if_error {
                        trace::error!(
                            "Invalid tombstone record in '{}' at pos {next_record_pos}: {e}",
                            reader.path().display(),
                        );
                        break;
                    } else {
                        return Err(e);
                    }
                }
            };

            buffer.clear();
            if record.data_version == RECORD_VERSION_V1 {
                // In version v1, each record only contains one tombstone.
                let (sid_cid, time_range) = TombstoneEntry::decode_legacy_v1(&record.data)?;
                columns_excluded
                    .entry(sid_cid)
                    .or_insert_with(TimeRanges::empty)
                    .push(time_range);
            } else {
                // In version v2, each record may contain multiple tombstones.
                match TombstoneEntry::decode(&record.data, &mut buffer) {
                    Ok(_) => {
                        for TombstoneEntry { key, time_ranges } in buffer.iter() {
                            match *key {
                                TombstoneKey::Series(series_id) => {
                                    series_excluded
                                        .entry(series_id)
                                        .or_insert_with(TimeRanges::empty)
                                        .extend_from_slice(time_ranges.as_slice());
                                }
                                TombstoneKey::Column(series_id, column_id) => {
                                    columns_excluded
                                        .entry((series_id, column_id))
                                        .or_insert_with(TimeRanges::empty)
                                        .extend_from_slice(time_ranges.as_slice());
                                }
                                TombstoneKey::All => {
                                    all_excluded.extend_from_slice(time_ranges.as_slice())
                                }
                            };
                        }
                    }
                    Err(e) => {
                        trace::error!(
                            "Invalid tombstone record in '{}' at pos {}: {e}",
                            reader.path().display(),
                            record.pos
                        );
                    }
                }
            }
        }

        Ok(Self {
            columns_excluded,
            series_excluded,
            all_excluded,
        })
    }

    pub async fn save_to(&self, writer: &mut record_file::Writer) -> Result<()> {
        let mut write_buf = Vec::with_capacity(MAX_RECORD_LEN);
        // Save series_excluded tombstone.
        for (series_id, time_ranges) in self.series_excluded.iter() {
            write_tombstone_buffer(
                writer,
                &mut write_buf,
                &TombstoneEntry {
                    key: TombstoneKey::Series(*series_id),
                    time_ranges: time_ranges.time_ranges().collect::<Vec<_>>(),
                },
            )
            .await?;
        }
        // Save column_excluded tombstones.
        for ((series_id, column_id), time_ranges) in self.columns_excluded.iter() {
            write_tombstone_buffer(
                writer,
                &mut write_buf,
                &TombstoneEntry {
                    key: TombstoneKey::Column(*series_id, *column_id),
                    time_ranges: time_ranges.time_ranges().collect::<Vec<_>>(),
                },
            )
            .await?;
        }
        // Save all_excluded tombstone.
        write_tombstone_buffer(
            writer,
            &mut write_buf,
            &TombstoneEntry {
                key: TombstoneKey::All,
                time_ranges: self.all_excluded.time_ranges().collect::<Vec<_>>(),
            },
        )
        .await?;

        if !write_buf.is_empty() {
            write_tombstone_record(writer, &write_buf).await?;
        }

        Ok(())
    }

    /// Check if there is no field-time_range being excluded.
    pub fn is_empty(&self) -> bool {
        self.series_excluded.is_empty()
            && self.columns_excluded.is_empty()
            && self.all_excluded.is_empty()
    }

    /// Immutably borrow `series_excluded`.
    pub fn series_excluded(&self) -> &HashMap<SeriesId, TimeRanges> {
        &self.series_excluded
    }

    /// Immutably borrow `columns_excluded`.
    pub fn columns_excluded(&self) -> &HashMap<(SeriesId, ColumnId), TimeRanges> {
        &self.columns_excluded
    }

    /// Immutably borrow `all_excluded`.
    pub fn all_excluded(&self) -> &TimeRanges {
        &self.all_excluded
    }
}

#[cfg(test)]
pub mod test {
    use std::path::{Path, PathBuf};

    use models::predicate::domain::{TimeRange, TimeRanges};
    use models::{ColumnId, SeriesId, Timestamp};

    use super::{TombstoneEntry, TombstoneKey, TsmTombstone, TsmTombstoneCache};
    use crate::file_system::file_manager;
    use crate::record_file::{self, RecordDataType, RecordDataVersion};

    #[test]
    fn test_tombstone_entry_codec() {
        let mut buffer = Vec::new();
        let mut tomb_entries: Vec<TombstoneEntry> = Vec::new();
        {
            let tombstone = TombstoneEntry {
                key: TombstoneKey::Series(1),
                time_ranges: vec![(2, 3).into(), (4, 5).into()],
            };
            buffer.clear();
            assert_eq!(tombstone.encode(0, 1, &mut buffer), Err(1));
            assert_eq!(buffer.len(), 25);
            assert_eq!(tombstone.encode(1, 1, &mut buffer), Ok(()));
            #[rustfmt::skip]
            assert_eq!(buffer, vec![
                0x00,
                0, 0, 0, 1,
                0, 0, 0, 1,
                0, 0, 0, 0, 0, 0, 0, 2,
                0, 0, 0, 0, 0, 0, 0, 3,
                0x00,
                0, 0, 0, 1,
                0, 0, 0, 1,
                0, 0, 0, 0, 0, 0, 0, 4,
                0, 0, 0, 0, 0, 0, 0, 5,
            ]);
            tomb_entries.clear();
            TombstoneEntry::decode(&buffer, &mut tomb_entries).unwrap();
            assert_eq!(tomb_entries.len(), 2);
            assert_eq!(
                tomb_entries[0],
                TombstoneEntry {
                    key: TombstoneKey::Series(1),
                    time_ranges: vec![(2, 3).into()],
                }
            );
            assert_eq!(
                tomb_entries[1],
                TombstoneEntry {
                    key: TombstoneKey::Series(1),
                    time_ranges: vec![(4, 5).into()],
                }
            );
        }
        {
            let tombstone = TombstoneEntry {
                key: TombstoneKey::Column(1, 2),
                time_ranges: vec![(3, 4).into(), (5, 6).into(), (7, 8).into()],
            };
            buffer.clear();
            assert_eq!(tombstone.encode(0, 1, &mut buffer), Err(1));
            assert_eq!(buffer.len(), 29);
            assert_eq!(tombstone.encode(1, 2, &mut buffer), Ok(()));
            #[rustfmt::skip]
            assert_eq!(buffer, vec![
                0x01,
                0, 0, 0, 1,
                0, 0, 0, 2,
                0, 0, 0, 1,
                0, 0, 0, 0, 0, 0, 0, 3,
                0, 0, 0, 0, 0, 0, 0, 4,
                0x01,
                0, 0, 0, 1,
                0, 0, 0, 2,
                0, 0, 0, 2,
                0, 0, 0, 0, 0, 0, 0, 5,
                0, 0, 0, 0, 0, 0, 0, 6,
                0, 0, 0, 0, 0, 0, 0, 7,
                0, 0, 0, 0, 0, 0, 0, 8,
            ]);
            tomb_entries.clear();
            TombstoneEntry::decode(&buffer, &mut tomb_entries).unwrap();
            assert_eq!(tomb_entries.len(), 2);
            assert_eq!(
                tomb_entries[0],
                TombstoneEntry {
                    key: TombstoneKey::Column(1, 2),
                    time_ranges: vec![(3, 4).into()],
                }
            );
            assert_eq!(
                tomb_entries[1],
                TombstoneEntry {
                    key: TombstoneKey::Column(1, 2),
                    time_ranges: vec![(5, 6).into(), (7, 8).into()],
                }
            );
        }
        {
            let tombstone = TombstoneEntry {
                key: TombstoneKey::All,
                time_ranges: vec![(1, 2).into()],
            };
            buffer.clear();
            assert_eq!(tombstone.encode(0, 100, &mut buffer), Ok(()));
            #[rustfmt::skip]
            assert_eq!(buffer, vec![
                0x0F,
                0, 0, 0, 1,
                0, 0, 0, 0, 0, 0, 0, 1,
                0, 0, 0, 0, 0, 0, 0, 2,
            ]);
            tomb_entries.clear();
            TombstoneEntry::decode(&buffer, &mut tomb_entries).unwrap();
            assert_eq!(tomb_entries.len(), 1);
            assert_eq!(
                tomb_entries[0],
                TombstoneEntry {
                    key: TombstoneKey::All,
                    time_ranges: vec![(1, 2).into()],
                }
            );
        }
    }

    pub struct TombstoneLegacyV1 {
        series_id: SeriesId,
        column_id: ColumnId,
        time_range: TimeRange,
    }

    pub async fn write_to_tsm_tombstone_v1(
        path: impl AsRef<Path>,
        tombstones: &[TombstoneLegacyV1],
    ) {
        let mut writer = record_file::Writer::open(path, record_file::RecordDataType::Tombstone)
            .await
            .unwrap();

        let mut write_buf = [0_u8; 24];
        // Save field_excluded tombstones.
        for tombstone in tombstones.iter() {
            write_buf[0..4].copy_from_slice(tombstone.series_id.to_be_bytes().as_slice());
            write_buf[4..8].copy_from_slice(tombstone.column_id.to_be_bytes().as_slice());
            write_buf[8..16].copy_from_slice(tombstone.time_range.min_ts.to_be_bytes().as_slice());
            write_buf[16..24].copy_from_slice(tombstone.time_range.max_ts.to_be_bytes().as_slice());
            writer
                .write_record(
                    RecordDataVersion::V1 as u8,
                    RecordDataType::Tombstone as u8,
                    &[&write_buf],
                )
                .await
                .unwrap();
        }
        writer.close().await.unwrap();
    }

    pub async fn write_to_tsm_tombstone_v2(path: impl AsRef<Path>, data: &TsmTombstoneCache) {
        let mut writer = record_file::Writer::open(path, record_file::RecordDataType::Tombstone)
            .await
            .unwrap();
        data.save_to(&mut writer).await.unwrap();
        writer.close().await.unwrap();
    }

    /// Check if tombstone contains any overlapped time ranges.
    fn check_tombstone(
        tombstone: &TsmTombstone,
        series_id: SeriesId,
        column_id: Option<ColumnId>,
        ranges_overlapped: Vec<(Timestamp, Timestamp)>,
        ranges_not_overlapped: Vec<(Timestamp, Timestamp)>,
    ) {
        let iter = ranges_overlapped
            .into_iter()
            .map(|r| (r, true))
            .chain(ranges_not_overlapped.into_iter().map(|r| (r, false)));
        for (range, expected_overlapped) in iter {
            let time_range = range.into();
            let actual_overlapped = if let Some(cid) = column_id {
                tombstone.overlaps_column_time_range(series_id, cid, &time_range)
            } else {
                tombstone.overlaps_series_time_range(series_id, &time_range)
            };
            assert_eq!(
                expected_overlapped, actual_overlapped,
                "sid: {series_id}, cid: {column_id:?}, time_range: {time_range}, overlapped: expected: {}, actual: {}",
                expected_overlapped, actual_overlapped,
            );
        }
    }

    #[tokio::test]
    async fn test_read_legacy_v1() {
        let dir = PathBuf::from("/tmp/test/tombstone/read_legacy_v1");
        let _ = std::fs::remove_dir_all(&dir);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        let path = dir.join("_000001.tombstone");
        #[rustfmt::skip]
        write_to_tsm_tombstone_v1(&path, &[
            TombstoneLegacyV1 { series_id: 1, column_id: 2, time_range: (11, 21).into() },
            TombstoneLegacyV1 { series_id: 3, column_id: 4, time_range: (31, 41).into() },
            TombstoneLegacyV1 { series_id: 5, column_id: 6, time_range: (51, 61).into() },
        ]).await;

        let tombstone = TsmTombstone::with_path(&path).await.unwrap();
        check_tombstone(&tombstone, 1, Some(2), vec![(11, 21)], vec![(31, 41)]);
        check_tombstone(&tombstone, 3, Some(4), vec![(31, 41)], vec![(51, 61)]);
        check_tombstone(&tombstone, 5, Some(6), vec![(51, 61)], vec![(11, 21)]);
    }

    #[tokio::test]
    async fn test_read_write_1() {
        let dir = PathBuf::from("/tmp/test/tombstone/read_write_1");
        let _ = std::fs::remove_dir_all(&dir);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        {
            let mut tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
            tombstone
                .add_range(&[1], &[2], &(1, 100).into())
                .await
                .unwrap();
            tombstone
                .add_range(&[3], &[4], &(1, 100).into())
                .await
                .unwrap();
            tombstone.flush().await.unwrap();

            assert_eq!(tombstone.all_excluded_cloned(), TimeRanges::empty());
            let columns_excluded = tombstone.columns_excluded_cloned();
            assert_eq!(
                columns_excluded.get(&(1, 2)),
                Some(&TimeRanges::new(vec![(1, 100).into()]))
            );
            assert_eq!(
                columns_excluded.get(&(3, 4)),
                Some(&TimeRanges::new(vec![(1, 100).into()]))
            );
            check_tombstone(
                &tombstone,
                1,
                Some(2),
                vec![(2, 99), (1, 100), (0, 101), (0, 1), (100, 101)],
                vec![(101, 103), (-1, 0), (101, 102)],
            );
        }

        let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        check_tombstone(
            &tombstone,
            3,
            Some(4),
            vec![(2, 99), (1, 100), (0, 101), (0, 1), (100, 101)],
            vec![(101, 103), (-1, 0), (101, 102)],
        );
    }

    #[tokio::test]
    async fn test_read_write_2() {
        let dir = PathBuf::from("/tmp/test/tombstone/read_write_2".to_string());
        let _ = std::fs::remove_dir_all(&dir);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        {
            let mut tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
            #[rustfmt::skip]
            tombstone.add_tombstone(vec![
                TombstoneEntry { key: TombstoneKey::Series(1), time_ranges: vec![(1, 100).into()] },
                TombstoneEntry { key: TombstoneKey::Series(2), time_ranges: vec![(90, 110).into()] },
                TombstoneEntry { key: TombstoneKey::Column(2, 3), time_ranges: vec![(101, 200).into()] },
                TombstoneEntry { key: TombstoneKey::All, time_ranges: vec![(301, 400).into()] },
            ]).await.unwrap();
            tombstone.flush().await.unwrap();
            drop(tombstone);
        }

        let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();

        check_tombstone(
            &tombstone,
            1,
            None,
            vec![(0, 1), (1, 2), (1, 101), (100, 101)],
            vec![(-1, 0), (101, 103)],
        );
        check_tombstone(
            &tombstone,
            1,
            Some(999),
            vec![(0, 1), (1, 2), (1, 101), (100, 101)],
            vec![(-1, 0), (101, 103)],
        );

        #[rustfmt::skip]
        check_tombstone(
            &tombstone,
            2,
            Some(3),
            vec![(80, 100), (90, 110), (101, 102), (101, 200), (150, 200), (199, 201)],
            vec![(80, 89), (201, 202)],
        );
        #[rustfmt::skip]
        check_tombstone(
            &tombstone,
            2,
            Some(999),
            vec![(80, 100), (90, 110), (101, 102), (101, 200), (200, 500), (350, 360)],
            vec![(150, 200), (199, 201)],
        );

        check_tombstone(
            &tombstone,
            99,
            Some(999),
            vec![(200, 500), (350, 360)],
            vec![(150, 200), (401, 450)],
        );
    }

    #[tokio::test]
    async fn test_read_write_3() {
        let dir = PathBuf::from("/tmp/test/tombstone/read_write_3".to_string());
        let _ = std::fs::remove_dir_all(&dir);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        let mut tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        for i in 0..10000 {
            tombstone
                .add_range(
                    &[0],
                    &[3 * i as u32 + 1, 3 * i as u32 + 2, 3 * i as u32 + 3],
                    &TimeRange::new(i as i64 * 2, i as i64 * 2 + 100),
                )
                .await
                .unwrap();
        }
        tombstone.flush().await.unwrap();
        drop(tombstone);

        let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        assert!(tombstone.overlaps_column_time_range(
            0,
            1,
            &TimeRange {
                max_ts: 2,
                min_ts: 99
            }
        ));
        assert!(tombstone.overlaps_column_time_range(
            0,
            2,
            &TimeRange {
                max_ts: 3,
                min_ts: 100
            }
        ));
        assert!(!tombstone.overlaps_column_time_range(
            0,
            3,
            &TimeRange {
                max_ts: 4,
                min_ts: 101
            }
        ));
    }

    #[test]
    fn test_tombstone_cache() {
        let mut cache = TsmTombstoneCache::default();
        cache.insert(TombstoneKey::All, (3, 4).into());
        for (field, time_range) in [
            (TombstoneKey::Series(1), (1, 2)),
            (TombstoneKey::Series(2), (10, 20)),
            (TombstoneKey::Series(1), (10, 20)),
            (TombstoneKey::Series(2), (100, 200)),
            (TombstoneKey::Series(1), (100, 200)),
            (TombstoneKey::Series(2), (1000, 2000)),
        ] {
            cache.insert(field, time_range.into());
        }
        cache.insert(TombstoneKey::All, (3000, 4000).into());

        assert_eq!(
            cache.series_excluded.get(&1),
            Some(&TimeRanges::new(vec![
                (1, 2).into(),
                (10, 20).into(),
                (100, 200).into(),
            ]))
        );
        assert_eq!(
            cache.series_excluded.get(&2),
            Some(&TimeRanges::new(vec![
                (10, 20).into(),
                (100, 200).into(),
                (1000, 2000).into(),
            ]))
        );
        assert_eq!(
            cache.all_excluded(),
            &TimeRanges::new(vec![(3, 4).into(), (3000, 4000).into()])
        );
    }
}
