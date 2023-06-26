pub mod check;
mod compact;
mod flush;
mod iterator;
pub mod job;
mod picker;

use std::sync::Arc;

pub use compact::*;
pub use flush::*;
use models::meta_data::VnodeId;
use parking_lot::RwLock;
pub use picker::*;

use crate::error::GenericError;
use crate::kv_option::StorageOptions;
use crate::memcache::MemCache;
use crate::tseries_family::{ColumnFile, Version};
use crate::tsm::TsmError;
use crate::LevelId;

#[derive(snafu::Snafu, Debug)]
pub enum CompactError {
    #[snafu(display("Failed to open summary: {source}"))]
    Open {
        source: TsmError,
    },

    #[snafu(display("Compaction failed to read TSM: {source}"))]
    ReadTsm {
        source: TsmError,
    },

    #[snafu(display("Failed to write summary: {source}"))]
    WriteTsm {
        source: TsmError,
    },

    #[snafu(display("Failed to sync summary: {source}"))]
    Sync {
        source: TsmError,
    },

    Encode {
        source: GenericError,
    },

    Decode {
        source: GenericError,
    },
}

pub type CompactResult<T> = Result<T, CompactError>;

pub enum CompactTask {
    Vnode(VnodeId),
    ColdVnode(VnodeId),
}

pub struct CompactReq {
    pub ts_family_id: VnodeId,
    pub database: Arc<String>,
    storage_opt: Arc<StorageOptions>,

    files: Vec<Arc<ColumnFile>>,
    version: Arc<Version>,
    pub out_level: LevelId,
}

#[derive(Debug)]
pub struct FlushReq {
    pub ts_family_id: VnodeId,
    pub mems: Vec<Arc<RwLock<MemCache>>>,
    pub force_flush: bool,
}
