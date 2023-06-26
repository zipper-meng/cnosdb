mod block;
pub mod codec;
mod index;
mod reader;
mod tombstone;
mod writer;

pub use block::*;
pub use index::*;
pub use reader::*;
pub use tombstone::{Tombstone, TsmTombstone};
pub use writer::*;

use self::tombstone::TombstoneError;
use crate::error::GenericError;
use crate::file_system::FileSystemError;

// MAX_BLOCK_VALUES is the maximum number of values a TSM block can store.
pub(crate) const MAX_BLOCK_VALUES: u32 = 1000;

const HEADER_SIZE: usize = 5;
const INDEX_META_SIZE: usize = 11;
const BLOCK_META_SIZE: usize = 44;
const BLOOM_FILTER_SIZE: usize = 64;
const BLOOM_FILTER_BITS: u64 = 512; // 64 * 8
const FOOTER_SIZE: usize = BLOOM_FILTER_SIZE + 8; // 72

pub trait BlockReader {
    fn decode(&mut self, block: &BlockMeta) -> crate::error::Result<DataBlock>;
}

#[derive(snafu::Snafu, Debug)]
pub enum TsmError {
    #[snafu(display("Failed to open TSM: {source}"))]
    Open { source: FileSystemError },

    #[snafu(display("Failed to read TSM '{}': {source}", path.display()))]
    Read {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to write TSM: {source}"))]
    Write {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to sync TSM '{}': {source}", path.display()))]
    Sync {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to rename TSM '{}' to '{}': {source}", old.display(), new.display()))]
    Rename {
        old: std::path::PathBuf,
        new: std::path::PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("TSM file is lost: {}", reason))]
    FileNotFound { reason: String },

    #[snafu(display("Invalid TSM file name: {source}"))]
    InvalidFileName { source: GenericError },

    #[snafu(display("Invalid TSM file: {source}"))]
    InvalidFile { source: GenericError },

    #[snafu(display("Error of TSM index: {source}"))]
    Index { source: GenericError },

    #[snafu(display("Failed to encode TSM data block: {source}"))]
    Encode { source: GenericError },

    #[snafu(display("Failed to decode TSM data block: {source}"))]
    Decode { source: GenericError },

    #[snafu(display("TSM Data block crc check failed"))]
    CrcCheck,

    #[snafu(display(
        "File size ({cur_size} B) exceed max_file_size ({max_size} B) after write {write_size} B"
    ))]
    MaxFileSizeExceed {
        cur_size: u64,
        max_size: u64,
        write_size: usize,
    },

    #[snafu(display("Writing to finished TSM writer '{}'", path.display()))]
    Finished { path: std::path::PathBuf },

    #[snafu(display("Error of TSM file: '{}': {}", path.display(), message))]
    Other {
        path: std::path::PathBuf,
        message: String,
    },

    #[snafu(display("Error of tombstone: {source}"))]
    Tombstone { source: TombstoneError },
}

pub type TsmResult<T> = Result<T, TsmError>;

impl From<TombstoneError> for TsmError {
    fn from(e: TombstoneError) -> Self {
        Self::Tombstone { source: e }
    }
}
