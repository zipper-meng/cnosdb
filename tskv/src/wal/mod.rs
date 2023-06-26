mod wal;

use models::meta_data::VnodeId;
use models::schema::Precision;
use tokio::sync::oneshot;
pub use wal::*;

use crate::error::{self, Error as TskvError, Result as TskvResult};
use crate::record_file::RecordFileError;

#[derive(snafu::Snafu, Debug)]
pub enum WalError {
    /// WAL file is truncated, it's because CnosDB didn't shutdown properly.
    ///
    /// This error is handled by WAL module:
    /// just stop the current WAL file reading, go to the next WAL file.
    #[snafu(display("Internal handled: WAL truncated"))]
    WalTruncated,

    #[snafu(display("Failed to read WAL: {source}"))]
    Read { source: RecordFileError },

    #[snafu(display("Failed to write WAL: {source}"))]
    Write { source: RecordFileError },

    #[snafu(display("Failed to sync WAL: {source}"))]
    Sync { source: RecordFileError },
}

pub type WalResult<T> = Result<T, WalError>;

/// A channel sender that send write WAL result: `(seq_no: u64, written_size: usize)`
type WriteWalResultSender = oneshot::Sender<WalResult<(u64, usize)>>;
type WriteWalResultReceiver = oneshot::Receiver<WalResult<(u64, usize)>>;

pub enum WalTask {
    Write {
        tenant: String,
        vnode_id: VnodeId,
        precision: Precision,
        points: Vec<u8>,
        cb: WriteWalResultSender,
    },
    DeleteVnode {
        tenant: String,
        database: String,
        vnode_id: VnodeId,
        cb: WriteWalResultSender,
    },
    DeleteTable {
        tenant: String,
        database: String,
        table: String,
        cb: WriteWalResultSender,
    },
}

impl WalTask {
    pub fn new_write(
        tenant: String,
        vnode_id: VnodeId,
        precision: Precision,
        points: Vec<u8>,
    ) -> (WalTask, WriteWalResultReceiver) {
        let (cb, rx) = oneshot::channel();
        (
            WalTask::Write {
                tenant,
                vnode_id,
                precision,
                points,
                cb,
            },
            rx,
        )
    }

    pub fn new_delete_vnode(
        tenant: String,
        database: String,
        vnode_id: VnodeId,
    ) -> (WalTask, WriteWalResultReceiver) {
        let (cb, rx) = oneshot::channel();
        (
            WalTask::DeleteVnode {
                tenant,
                database,
                vnode_id,
                cb,
            },
            rx,
        )
    }

    pub fn new_delete_table(
        tenant: String,
        database: String,
        table: String,
    ) -> (WalTask, WriteWalResultReceiver) {
        let (cb, rx) = oneshot::channel();
        (
            WalTask::DeleteTable {
                tenant,
                database,
                table,
                cb,
            },
            rx,
        )
    }

    pub fn wal_entry_type(&self) -> WalEntryType {
        match self {
            WalTask::Write { .. } => WalEntryType::Write,
            WalTask::DeleteVnode { .. } => WalEntryType::DeleteVnode,
            WalTask::DeleteTable { .. } => WalEntryType::DeleteTable,
        }
    }

    fn write_wal_result_sender(self) -> WriteWalResultSender {
        match self {
            WalTask::Write { cb, .. } => cb,
            WalTask::DeleteVnode { cb, .. } => cb,
            WalTask::DeleteTable { cb, .. } => cb,
        }
    }

    pub fn fail(self, e: WalError) -> TskvResult<()> {
        self.write_wal_result_sender()
            .send(Err(e))
            .map_err(|_| TskvError::ChannelSend {
                source: error::ChannelSendError::WalTask,
            })
    }
}
