pub mod check;
mod compact;
mod delta_compact;
mod flush;
mod iterator;
pub mod job;
mod picker;

use std::sync::Arc;

pub use compact::*;
pub use flush::*;
use parking_lot::RwLock;
pub use picker::*;

use crate::kv_option::StorageOptions;
use crate::memcache::MemCache;
use crate::tseries_family::{ColumnFile, Version};
use crate::{LevelId, TseriesFamilyId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompactTask {
    /// Compact the files in the in_level into the out_level.
    Normal(TseriesFamilyId),
    /// Compact the files in level-0 to larger files.
    Delta(TseriesFamilyId),
}

impl CompactTask {
    pub fn ts_family_id(&self) -> TseriesFamilyId {
        match self {
            CompactTask::Normal(ts_family_id) => *ts_family_id,
            CompactTask::Delta(ts_family_id) => *ts_family_id,
        }
    }

    fn priority(&self) -> usize {
        match self {
            CompactTask::Delta(_) => 1,
            CompactTask::Normal(_) => 2,
        }
    }
}

impl Ord for CompactTask {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority().cmp(&other.priority())
    }
}

impl PartialOrd for CompactTask {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Display for CompactTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompactTask::Normal(ts_family_id) => write!(f, "Normal({})", ts_family_id),
            CompactTask::Delta(ts_family_id) => write!(f, "Delta({})", ts_family_id),
        }
    }
}

pub struct CompactReq {
    pub ts_family_id: TseriesFamilyId,
    pub database: Arc<String>,
    storage_opt: Arc<StorageOptions>,

    files: Vec<Arc<ColumnFile>>,
    version: Arc<Version>,
    pub out_level: LevelId,
}

pub struct FlushReq {
    pub owner: String,
    pub ts_family_id: TseriesFamilyId,
    pub mems: Vec<Arc<RwLock<MemCache>>>,
    pub low_seq_no: u64,
    pub high_seq_no: u64,
}

impl std::fmt::Display for FlushReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FlushReq owner: {}, on vnode: {}, low_seq_no: {}, high_seq_no: {} caches_num: {}",
            self.owner,
            self.ts_family_id,
            self.low_seq_no,
            self.high_seq_no,
            self.mems.len(),
        )
    }
}
