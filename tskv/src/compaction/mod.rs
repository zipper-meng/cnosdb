pub mod check;
mod compact;
mod flush;
mod iterator;
pub mod job;
mod picker;

use std::sync::Arc;

use chrono::Utc;
pub use compact::*;
pub use flush::*;
use models::predicate::domain::TimeRange;
use parking_lot::RwLock;
pub use picker::*;

use crate::kv_option::StorageOptions;
use crate::memcache::MemCache;
use crate::tseries_family::{ColumnFile, LevelInfo, Version};
use crate::{LevelId, TseriesFamilyId};

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
