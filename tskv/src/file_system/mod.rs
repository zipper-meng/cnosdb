// #![deny(dead_code)]
// #![deny(non_snake_case)]
// #![deny(unused_imports)]
// #![deny(unused_must_use)]

use async_trait::async_trait;
use tokio::fs::File;

pub(crate) mod file;
pub mod file_manager;
pub mod queue;

#[async_trait]
pub trait DataBlock {
    async fn write(&self, file: &mut File) -> crate::Result<usize>;
    async fn read(&mut self, file: &mut File) -> crate::Result<usize>;
}

#[derive(snafu::Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum FileSystemError {
    #[snafu(display("Unable to open async file '{}': {}", path.display(), source))]
    OpenFile {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Unable to create async file '{}': {}", path.display(), source))]
    CreateFile {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Unable to create directory '{}': {}", path.display(), source))]
    CreateDirectory {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Error with read async file '{}': {}", path.display(), source))]
    ReadFile {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Unable to write async file '{}': {}", path.display(), source))]
    WriteFile {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Unable to sync async file: {}", source))]
    SyncFile { source: std::io::Error },

    #[snafu(display("Failed to rename file '{}' to '{}': {}", old.display(), new.display(), source))]
    RenameFile {
        old: std::path::PathBuf,
        new: std::path::PathBuf,
        source: std::io::Error,
    },
}

pub type FileSystemResult<T> = Result<T, FileSystemError>;
