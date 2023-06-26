//! # Record file
//! ```text
//! +--------+------------+------------+------------+-----+--------+
//! |   4    | block_size | block_size | block_size | ... |   32   |
//! +--------+------------+------------+------------+-----+--------+
//! | "RECO" |   Record   |   Record   |   Record   | ... | Footer |
//! +--------+------------+------------+------------+-----+--------+
//! ```
//!
//! ## Record
//! ```text
//! +--------------+--------------+-----------+------------+--------------+---------------+
//! | 0: 4 bytes   | 4: 1 byte    | 5: 1 byte | 6: 4 bytes | 10: 4 bytes  | 14: data_size |
//! +--------------+--------------+-----------+------------+--------------+---------------+
//! | magic_number | data_version | data_type | data_size  | crc32_number |     data      |
//! +--------------+--------------+-----------+------------+--------------+---------------+
//! ```
//!
//! The crc32_number is hash(data_version + data_type + data_size + data)
//!
//! ## Footer
//!
//! ### Wal
//! ```text
//! +------------+--------------+---------------+--------------+--------------+
//! | 0: 4 bytes | 4: 4 bytes   | 8: 8 bytes   | 16: 8 bytes  | 24: 8 bytes   |
//! +------------+--------------+---------------+--------------+--------------+
//! | "walo"     | crc32_number | padding_zeros | min_sequence | max_sequence |
//! +------------+--------------+---------------+--------------+--------------+
//! ```
//!
//! The crc32_number is:
//! - If `file_len > (magic_len + 1024 + footer_len)`: `hash(file[magic_len..magic_len + 1024])`
//! - If `file_len <= (magic_len + 1024 + footer_len)`: `hash(file[magic_len..file_len - footer_len])`

mod reader;
mod record;
mod writer;

use std::fmt::Display;

use num_enum::{IntoPrimitive, TryFromPrimitive};
pub use reader::*;
pub use record::*;
pub use writer::*;

use crate::file_system::FileSystemError;

pub const FILE_MAGIC_NUMBER: u32 = u32::from_be_bytes([b'R', b'E', b'C', b'O']);
pub const FILE_MAGIC_NUMBER_LEN: usize = 4;
pub const FILE_FOOTER_LEN: usize = 32;
pub const FILE_FOOTER_MAGIC_NUMBER_LEN: usize = 4;
pub const FILE_FOOTER_CRC32_NUMBER_LEN: usize = 4;

/// If file_len > file_crc_source_len, footer crc32_number is
/// hash(file[header_len..header_len + 1024]), otherwise
/// hash(file[header_len..file_len - footer_len])
pub fn file_crc_source_len(file_len: u64, file_footer_len: usize) -> usize {
    if file_len > (FILE_MAGIC_NUMBER_LEN + 1024 + file_footer_len) as u64 {
        1024
    } else {
        (file_len - (FILE_MAGIC_NUMBER_LEN + file_footer_len) as u64) as usize
    }
}

pub const RECORD_MAGIC_NUMBER: u32 = u32::from_be_bytes([b'F', b'l', b'O', b'g']);
pub const RECORD_MAGIC_NUMBER_LEN: usize = 4;
pub const RECORD_DATA_VERSION_LEN: usize = 1;
pub const RECORD_DATA_TYPE_LEN: usize = 1;
pub const RECORD_DATA_SIZE_LEN: usize = 4;
pub const RECORD_CRC32_NUMBER_LEN: usize = 4;
pub const RECORD_HEADER_LEN: usize = 14; // 4 + 1 + 1 + 4 + 4
pub const BLOCK_SIZE: usize = 4096;

pub const READER_BUF_SIZE: usize = 1024 * 1024 * 64; //64MB

#[derive(Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum RecordDataVersion {
    V1 = 1,
}

#[derive(Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum RecordDataType {
    Summary = 1,
    Tombstone = 4,
    Wal = 8,
    IndexLog = 16,
}

impl Display for RecordDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordDataType::Summary => write!(f, "summary"),
            RecordDataType::Tombstone => write!(f, "tombstone"),
            RecordDataType::Wal => write!(f, "WAL"),
            RecordDataType::IndexLog => write!(f, "indexlog"),
        }
    }
}

#[derive(snafu::Snafu, Debug)]
pub enum RecordFileError {
    #[snafu(display("Internal handled: the end of the record file"))]
    Eof,

    #[snafu(display("Internal handled: cannot detect the footer of the record file"))]
    NoFooter,

    #[snafu(display("Failed to open record file '{}': {}", path.display(), source))]
    OpenFile {
        path: std::path::PathBuf,
        source: FileSystemError,
    },

    #[snafu(display("Failed to create record file '{}': {}", path.display(), source))]
    CreateFile {
        path: std::path::PathBuf,
        source: FileSystemError,
    },

    #[snafu(display("Failed to read record file '{}': {}", path.display(), source))]
    ReadFile {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to seek record file '{}': {}", path.display(), source))]
    SeekFile {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to write record file '{}': {}", path.display(), source))]
    WriteFile {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to sync record file: {}", source))]
    SyncFile {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Error of record file: '{}': {}", path.display(), message))]
    Other {
        path: std::path::PathBuf,
        message: String,
    },

    #[snafu(display("Failed to read record file '{}': pos ({pos}) is too large (> {file_len})", path.display()))]
    PosOverflow {
        path: std::path::PathBuf,
        pos: u64,
        file_len: u64,
    },

    #[snafu(display("Failed to write record file '{}': record(type: {data_type}) has len ({data_len}) that is not a valid u32", path.display()))]
    LenOverFlow {
        path: std::path::PathBuf,
        data_type: u8,
        data_len: u64,
    },
}

pub type RecordFileResult<T> = Result<T, RecordFileError>;
