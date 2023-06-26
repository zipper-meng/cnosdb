use std::path::PathBuf;

use datafusion::arrow::error::ArrowError;
use error_code::{ErrorCode, ErrorCoder};
use http_protocol::response::ErrorResponse;
use meta::error::MetaError;
use protos::PointsError;
use snafu::Snafu;
use tonic::{Code, Status};

use crate::index::IndexError;
use crate::schema::error::SchemaError;
use crate::tsm::TsmError;
use crate::{file_system, record_file, summary, tsm, wal};

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type GenericError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Snafu, Debug, ErrorCoder)]
#[snafu(visibility(pub))]
#[error_code(mod_code = "02")]
pub enum Error {
    ErrorResponse {
        error: ErrorResponse,
    },

    Network {
        source: Status,
    },

    Meta {
        source: MetaError,
    },

    #[snafu(display("Invalid flatbuffers: {}", source))]
    #[error_code(code = 1)]
    InvalidFlatbuffer {
        source: flatbuffers::InvalidFlatbuffer,
    },

    #[snafu(display("Fields can't be empty"))]
    #[error_code(code = 2)]
    InvalidPoint,

    #[snafu(display("{}", reason))]
    #[error_code(code = 3)]
    CommonError {
        reason: String,
    },

    #[snafu(display("DataSchemaError: {}", source))]
    #[error_code(code = 4)]
    Schema {
        source: SchemaError,
    },

    #[snafu(display("Memory Exhausted Retry Later"))]
    #[error_code(code = 5)]
    MemoryExhausted,

    #[error_code(code = 6)]
    Arrow {
        source: ArrowError,
    },

    // Internal Error
    #[snafu(display("{}", source))]
    IO {
        source: std::io::Error,
    },

    #[snafu(display("Unable to open file '{}': {}", path.display(), source))]
    OpenFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Unable to create file '{}': {}", path.display(), source))]
    CreateFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Error with read file '{}': {}", path.display(), source))]
    ReadFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Unable to write file '{}': {}", path.display(), source))]
    WriteFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Unable to sync file: {}", source))]
    SyncFile {
        source: std::io::Error,
    },

    #[snafu(display("File {} has wrong name format: {}", file_name, message))]
    InvalidFileName {
        file_name: String,
        message: String,
    },

    #[snafu(display("File '{}' has wrong format: {}", path.display(), message))]
    InvalidFileFormat {
        path: PathBuf,
        message: String,
    },

    /// Failed to send someting to a channel
    #[snafu(display("{source}"))]
    ChannelSend {
        source: ChannelSendError,
    },

    /// Failed to receive something from a channel
    #[snafu(display("{source}"))]
    ChannelReceive {
        source: ChannelReceiveError,
    },

    #[snafu(display("tskv::file_system: {source}"))]
    FileSystem {
        source: file_system::FileSystemError,
    },

    #[snafu(display("tskv::record_file: {source}"))]
    RecordFile {
        source: record_file::RecordFileError,
    },

    /// Failed to write to WAL.
    #[snafu(display("tskv::wal: {source}"))]
    Wal {
        source: wal::WalError,
    },

    #[snafu(display("tskv::record_file: {source}"))]
    Summary {
        source: summary::SummaryError,
    },

    #[snafu(display("tskv::tsm: {source}"))]
    Tsm {
        source: tsm::TsmError,
    },

    #[snafu(display("Failed to do encode: {}", source))]
    Encode {
        source: GenericError,
    },

    #[snafu(display("Faield to do decode: {}", source))]
    Decode {
        source: GenericError,
    },

    #[snafu(display("Index: : {}", source))]
    IndexErr {
        source: crate::index::IndexError,
    },

    #[snafu(display("character set error"))]
    ErrCharacterSet,

    #[snafu(display("Invalid parameter : {}", reason))]
    InvalidParam {
        reason: String,
    },

    #[snafu(display("file has no footer"))]
    NoFooter,

    #[snafu(display("unable to transform: {}", reason))]
    Transform {
        reason: String,
    },

    #[snafu(display("invalid points : '{}'", source))]
    Points {
        source: PointsError,
    },
}

impl From<PointsError> for Error {
    fn from(value: PointsError) -> Self {
        Error::Points { source: value }
    }
}

impl From<SchemaError> for Error {
    fn from(value: SchemaError) -> Self {
        match value {
            SchemaError::Meta { source } => Self::Meta { source },
            other => Error::Schema { source: other },
        }
    }
}

impl From<IndexError> for Error {
    fn from(value: IndexError) -> Self {
        Error::IndexErr { source: value }
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::IO { source: value }
    }
}

impl From<MetaError> for Error {
    fn from(source: MetaError) -> Self {
        Error::Meta { source }
    }
}

impl From<ArrowError> for Error {
    fn from(source: ArrowError) -> Self {
        Error::Arrow { source }
    }
}

impl Error {
    pub fn error_code(&self) -> &dyn ErrorCode {
        match self {
            Error::Meta { source } => source.error_code(),
            Error::ErrorResponse { error } => error,
            _ => self,
        }
    }

    pub fn invalid_vnode(&self) -> bool {
        match self {
            Self::Tsm { source } => {
                matches!(
                    source,
                    TsmError::CrcCheck
                        | TsmError::FileNotFound { .. }
                        | TsmError::InvalidFile { .. }
                )
            }
            _ => false,
        }
    }
}

// default conversion from CoordinatorError to tonic treats everything
// other than `Status` as an internal error
impl From<Error> for Status {
    fn from(value: Error) -> Self {
        let error_resp = ErrorResponse::new(value.error_code());

        match serde_json::to_string(&error_resp) {
            Ok(err) => Status::internal(err),
            Err(err) => {
                let error_str = format!("Serialize TskvError, error: {}", err);
                trace::error!(error_str);
                Status::unknown(error_str)
            }
        }
    }
}

impl From<Status> for Error {
    fn from(source: Status) -> Self {
        let error_message = source.message();
        match source.code() {
            Code::Internal => {
                match serde_json::from_str::<ErrorResponse>(error_message) {
                    Ok(error) => Error::ErrorResponse { error },
                    Err(err) => {
                        // Deserialization tskv error failed, maybe a bug
                        Error::CommonError {
                            reason: err.to_string(),
                        }
                    }
                }
            }
            Code::Unknown => {
                // The server will return this exception if serialization of tskv error fails, maybe a bug
                trace::error!("The server will return this exception if serialization of tskv error fails, maybe a bug");
                Error::CommonError {
                    reason: error_message.to_string(),
                }
            }
            _ => Self::Network { source },
        }
    }
}

#[test]
fn test_error_code() {
    let e = Error::Schema {
        source: SchemaError::ColumnAlreadyExists {
            name: "".to_string(),
        },
    };
    assert!(e.code().starts_with("02"));
}

#[derive(Snafu, Debug)]
pub enum ChannelSendError {
    #[snafu(display("Failed to send a WAL task"))]
    WalTask,
}

#[derive(Snafu, Debug)]
pub enum ChannelReceiveError {
    #[snafu(display("Failed to receive write WAL result: {source}"))]
    WriteWalResult {
        source: tokio::sync::oneshot::error::RecvError,
    },
}

#[derive(Snafu, Debug)]
pub enum WalReadError {}
