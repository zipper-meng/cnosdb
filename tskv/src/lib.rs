#![allow(dead_code)]
#![allow(unreachable_patterns, unused_variables)]
#![allow(unused_imports)]

pub use error::{Error, Result};
pub use kv_option::Options;
pub use kvcore::TsKv;
pub use summary::{print_summary_statistics, Summary, VersionEdit};
pub use tseries_family::TimeRange;
pub use tsm::print_tsm_statistics;

mod background_task;
pub mod byte_utils;
mod compaction;
mod context;
pub mod database;
pub mod engine;
pub mod error;
pub mod file_system;
pub mod file_utils;
pub mod index;
pub mod iterator;
pub mod kv_option;
mod kvcore;
mod memcache;
mod record_file;
mod schema;
mod summary;
mod tseries_family;
mod tsm;
mod version_set;
mod wal;

pub type ColumnFileId = u64;
type TseriesFamilyId = u32;
type LevelId = u32;
