pub mod check;
mod compact;
mod delta_compact;
mod flush;
mod iterator;
pub mod job;
mod picker;

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
pub use flush::run_flush_memtable_job;
use models::predicate::domain::TimeRange;
use parking_lot::RwLock;
pub use picker::{pick_delta_compaction, pick_level_compaction};
use utils::BloomFilter;

use crate::context::GlobalContext;
use crate::kv_option::StorageOptions;
use crate::memcache::MemCache;
use crate::tseries_family::{ColumnFile, LevelInfo, Version};
use crate::{ColumnFileId, LevelId, TseriesFamilyId, VersionEdit};

#[cfg(test)]
pub mod test {
    pub use super::compact::test::{
        check_column_file, create_options, generate_data_block, prepare_compaction,
        read_data_blocks_from_column_file, write_data_block_desc, write_data_blocks_to_column_file,
        TsmSchema,
    };
    pub use super::flush::flush_tests::default_table_schema;
}

pub enum CompactTask {
    Normal(TseriesFamilyId),
    Cold(TseriesFamilyId),
    Delta(TseriesFamilyId),
}

#[derive(Debug, Clone)]
pub struct CompactReq {
    ts_family_id: TseriesFamilyId,
    tenant_database: Arc<String>,
    storage_opt: Arc<StorageOptions>,

    files: Vec<Arc<ColumnFile>>,
    version: Arc<Version>,
    in_level: LevelId,
    out_level: LevelId,
    time_range: TimeRange,
}

impl std::fmt::Display for CompactReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tenant_database: {}, ts_family: {}, files: [",
            self.tenant_database, self.ts_family_id
        )?;
        if !self.files.is_empty() {
            write!(
                f,
                "{{ Level-{}, file_id: {}, time_range: {}-{} }}",
                self.files[0].level(),
                self.files[0].file_id(),
                self.files[0].time_range().min_ts,
                self.files[0].time_range().max_ts
            )?;
            for file in self.files.iter().skip(1) {
                write!(
                    f,
                    ", {{ Level-{}, file_id: {}, time_range: {}-{} }}",
                    file.level(),
                    file.file_id(),
                    file.time_range().min_ts,
                    file.time_range().max_ts
                )?;
            }
        }
        write!(f, "]")
    }
}

#[derive(Debug, Clone)]
pub struct FlushReq {
    pub ts_family_id: TseriesFamilyId,
    pub mems: Vec<Arc<RwLock<MemCache>>>,
    pub force_flush: bool,
    pub low_seq_no: u64,
    pub high_seq_no: u64,
}

impl std::fmt::Display for FlushReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FlushReq on vnode: {}, low_seq_no: {}, high_seq_no: {} caches_num: {}, force_flush: {}",
            self.ts_family_id,
            self.low_seq_no,
            self.high_seq_no,
            self.mems.len(),
            self.force_flush,
        )
    }
}

fn format_level_infos(levels: &[LevelInfo]) -> String {
    levels
        .iter()
        .map(|l| format!("{l}"))
        .collect::<Vec<String>>()
        .join(", ")
}

fn format_column_files(files: &[Arc<ColumnFile>]) -> String {
    files
        .iter()
        .map(|f| format!("{f}"))
        .collect::<Vec<String>>()
        .join(", ")
}

const PICKER_CONTEXT_DATETIME_FORMAT: &str = "%d%m%Y_%H%M%S_%3f";

fn context_datetime() -> String {
    Utc::now()
        .format(PICKER_CONTEXT_DATETIME_FORMAT)
        .to_string()
}

pub async fn run_compaction_job(
    request: CompactReq,
    kernel: Arc<GlobalContext>,
) -> crate::Result<Option<(VersionEdit, HashMap<ColumnFileId, Arc<BloomFilter>>)>> {
    if request.in_level == 0 {
        delta_compact::run_compaction_job(request, kernel).await
    } else {
        compact::run_compaction_job(request, kernel).await
    }
}
