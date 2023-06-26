//! # WAL file
//!
//! A WAL file is a [`record_file`].
//!
//! ## Record Data
//! ```text
//! # type = Write
//! +------------+------------+------------+------------+--------------+-----------------+-----------+
//! | 0: 1 byte  | 1: 8 bytes | 9: 4 bytes | 13: 1 byte | 14: 8 bytes  | 22: tenant_size |  n bytes  |
//! +------------+------------+------------+------------+--------------+-----------------+-----------+
//! |    type    |  sequence  |  vnode_id  |  precision | tenant_size  |  tenant         |   data    |
//! +------------+------------+------------+------------+--------------+-----------------+-----------+
//!
//! # type = DeleteVnode
//! +------------+------------+------------+-------------+-------------+----------+
//! | 0: 1 byte  | 1: 8 bytes | 9: 4 bytes | 9: 8 bytes  | 13: n bytes | n bytes  |
//! +------------+------------+------------+-------------+-------------+----------+
//! |    type    |  sequence  |  vnode_id  | tenant_size |  tenant     | database |
//! +------------+------------+------------+-------------+-------------+----------+
//!
//! # type = DeleteTable
//! +------------+------------+-------------+---------------+-----------------+---------------+---------+
//! | 0: 1 byte  | 1: 8 bytes | 9: 8 bytes  | 17: 4 bytes   | 25: tenant_size | database_size | n bytes |
//! +------------+------------+-------------+---------------+-----------------+---------------+---------+
//! |    type    |  sequence  | tenant_size | database_size |  tenant         |  database     | table   |
//! +------------+------------+-------------+---------------+-----------------+---------------+---------+
//! ```
//!
//! ## Footer
//! ```text
//! +------------+---------------+--------------+--------------+
//! | 0: 4 bytes | 4: 12 bytes   | 16: 8 bytes  | 24: 8 bytes  |
//! +------------+---------------+--------------+--------------+
//! | "walo"     | padding_zeros | min_sequence | max_sequence |
//! +------------+---------------+--------------+--------------+
//! ```

use std::collections::HashMap;
use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::string::String;
use std::sync::Arc;

use models::codec::Encoding;
use models::meta_data::VnodeId;
use models::schema::Precision;
use protos::kv_service::{Meta, WritePointsRequest};
use protos::models_helper::print_points;
use snafu::ResultExt;
use trace::{debug, error, info, warn};

use super::{WalError, WalResult, WalTask};
use crate::byte_utils::{decode_be_u32, decode_be_u64};
use crate::context::GlobalSequenceContext;
use crate::error::Result as TskvResult;
use crate::file_system::file_manager;
use crate::kv_option::WalOptions;
use crate::record_file::{self, RecordDataType, RecordDataVersion, RecordFileError};
use crate::tsm::codec::get_str_codec;
use crate::{error, file_utils, Engine};

const ENTRY_TYPE_LEN: usize = 1;
const ENTRY_SEQUENCE_LEN: usize = 8;
/// 9 - type(1) + sequence(8)
const ENTRY_HEADER_LEN: usize = 9;

const ENTRY_VNODE_ID_LEN: usize = 4;
const ENTRY_PRECISION_LEN: usize = 1;
const ENTRY_TENANT_SIZE_LEN: usize = 8;
const ENTRY_DATABASE_SIZE_LEN: usize = 4;
const ENTRY_TABLE_SIZE_LEN: usize = 4;

const FOOTER_MAGIC_NUMBER: u32 = u32::from_be_bytes([b'w', b'a', b'l', b'o']);
const FOOTER_MAGIC_NUMBER_LEN: usize = 4;

const SEGMENT_MAGIC: [u8; 4] = [0x57, 0x47, 0x4c, 0x00];

#[repr(u8)]
#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub enum WalEntryType {
    Write = 1,
    DeleteVnode = 2,
    DeleteTable = 3,
    Unknown = 127,
}

impl From<u8> for WalEntryType {
    fn from(typ: u8) -> Self {
        match typ {
            1 => WalEntryType::Write,
            2 => WalEntryType::DeleteVnode,
            3 => WalEntryType::DeleteTable,
            _ => WalEntryType::Unknown,
        }
    }
}

impl Display for WalEntryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalEntryType::Write => write!(f, "write(1)"),
            WalEntryType::DeleteVnode => write!(f, "delete_vnode(2)"),
            WalEntryType::DeleteTable => write!(f, "delete_table(3)"),
            WalEntryType::Unknown => write!(f, "unknown"),
        }
    }
}

pub struct WalEntryBlock<'a> {
    pub typ: WalEntryType,
    buf: Vec<u8>,
    entry: WalEntry<'a>,
}

impl<'a> WalEntryBlock<'_> {
    pub fn new(buf: Vec<u8>) -> Self {
        if buf.is_empty() {
            return Self {
                typ: WalEntryType::Unknown,
                buf,
                entry: WalEntry::Unknown,
            };
        }

        let entry_type: WalEntryType = buf[0].into();
        let entry = match entry_type {
            WalEntryType::Write => WalEntry::Write(WriteBlock::new(&buf[ENTRY_HEADER_LEN..])),
            WalEntryType::DeleteVnode => {
                WalEntry::DeleteVnode(DeleteVnodeBlock(&buf[ENTRY_HEADER_LEN..]))
            }
            WalEntryType::DeleteTable => {
                WalEntry::DeleteTable(DeleteTableBlock::new(&buf[ENTRY_HEADER_LEN..]))
            }
            WalEntryType::Unknown => WalEntry::Unknown,
        };
        Self {
            typ: entry_type,
            buf,
            entry,
        }
    }

    pub fn seq(&self) -> u64 {
        decode_be_u64(&self.buf[1..9])
    }
}

enum WalEntry<'a> {
    Write(WriteBlock<'a>),
    DeleteVnode(DeleteVnodeBlock<'a>),
    DeleteTable(DeleteTableBlock<'a>),
    Unknown,
}

/// ENTRY_VNODE_ID_LEN + ENTRY_PRECISION_LEN + ENTRY_TENANT_SIZE_LEN + tenant + data
struct WriteBlock<'a> {
    buf: &'a [u8],
    tenant_size: usize,
}

impl<'a> WriteBlock<'_> {
    pub fn new(buf: &[u8]) -> Self {
        let tenatn_size_pos = ENTRY_VNODE_ID_LEN + ENTRY_PRECISION_LEN;
        let tenant_size =
            decode_be_u64(&buf[tenatn_size_pos..tenatn_size_pos + ENTRY_TENANT_SIZE_LEN]) as usize;
        Self { buf, tenant_size }
    }

    pub fn check_buf_size(size: usize) -> bool {
        size >= ENTRY_VNODE_ID_LEN + ENTRY_PRECISION_LEN + ENTRY_TENANT_SIZE_LEN
    }

    pub fn vnode_id(&self) -> VnodeId {
        decode_be_u32(&self.buf[..ENTRY_VNODE_ID_LEN])
    }

    pub fn precision(&self) -> Precision {
        Precision::from(self.buf[ENTRY_VNODE_ID_LEN])
    }

    pub fn tenant(&self) -> &[u8] {
        let tenatn_pos = ENTRY_VNODE_ID_LEN + ENTRY_PRECISION_LEN + ENTRY_TENANT_SIZE_LEN;
        &self.buf[tenatn_pos..tenatn_pos + self.tenant_size]
    }

    pub fn points(&self) -> &[u8] {
        &self.buf
            [ENTRY_VNODE_ID_LEN + ENTRY_PRECISION_LEN + ENTRY_TENANT_SIZE_LEN + self.tenant_size..]
    }
}

/// ENTRY_VNODE_ID_LEN + tenant
struct DeleteVnodeBlock<'a>(&'a [u8]);

impl<'a> DeleteVnodeBlock<'_> {
    pub fn check_buf_size(size: usize) -> bool {
        size > ENTRY_VNODE_ID_LEN
    }

    pub fn vnode_id(&self) -> VnodeId {
        decode_be_u32(&self.0[0..ENTRY_VNODE_ID_LEN])
    }

    pub fn tenant(&self) -> &[u8] {
        &self.0[ENTRY_VNODE_ID_LEN..ENTRY_VNODE_ID_LEN + ENTRY_TENANT_SIZE_LEN]
    }

    pub fn database(&self) -> &[u8] {
        let database_pos = ENTRY_VNODE_ID_LEN + ENTRY_TENANT_SIZE_LEN;
        &self.0[database_pos..database_pos + ENTRY_DATABASE_SIZE_LEN]
    }
}

/// ENTRY_TENANT_SIZE_LEN + ENTRY_DATABASE_SIZE_LEN + tenant + database + table
struct DeleteTableBlock<'a> {
    buf: &'a [u8],
    tenant_len: usize,
    database_len: usize,
}

impl<'a> DeleteTableBlock<'_> {
    pub fn new(buf: &[u8]) -> Self {
        let tenant_len = decode_be_u64(&buf[..ENTRY_TENANT_SIZE_LEN]) as usize;
        let database_len =
            decode_be_u64(&buf[ENTRY_TENANT_SIZE_LEN..ENTRY_DATABASE_SIZE_LEN]) as usize;
        Self {
            buf,
            tenant_len,
            database_len,
        }
    }

    pub fn check_buf_size(size: usize) -> bool {
        size >= ENTRY_TENANT_SIZE_LEN + ENTRY_DATABASE_SIZE_LEN
    }

    pub fn tenant(&self) -> &[u8] {
        let tenant_pos = ENTRY_TENANT_SIZE_LEN + ENTRY_DATABASE_SIZE_LEN;
        &self.buf[tenant_pos..tenant_pos + self.tenant_len]
    }

    pub fn database(&self) -> &[u8] {
        let database_pos = ENTRY_TENANT_SIZE_LEN + ENTRY_DATABASE_SIZE_LEN + self.tenant_len;
        &self.buf[database_pos..database_pos + self.database_len]
    }

    pub fn table(&self) -> &[u8] {
        &self.buf[ENTRY_TENANT_SIZE_LEN
            + ENTRY_DATABASE_SIZE_LEN
            + self.tenant_len
            + self.database_len..]
    }
}

fn build_footer(min_sequence: u64, max_sequence: u64) -> [u8; record_file::FILE_FOOTER_LEN] {
    let mut footer = [0_u8; record_file::FILE_FOOTER_LEN];
    footer[0..4].copy_from_slice(&FOOTER_MAGIC_NUMBER.to_be_bytes());
    footer[16..24].copy_from_slice(&min_sequence.to_be_bytes());
    footer[24..32].copy_from_slice(&max_sequence.to_be_bytes());
    footer
}

/// Reads a wal file and parse footer, returns sequence range
async fn read_footer(path: impl AsRef<Path>) -> WalResult<Option<(u64, u64)>> {
    if file_manager::try_exists(&path) {
        let reader = WalReader::open(path).await?;
        Ok(Some((reader.min_sequence, reader.max_sequence)))
    } else {
        Ok(None)
    }
}

struct WalWriter {
    id: u64,
    inner: record_file::Writer,
    size: u64,
    path: PathBuf,
    config: Arc<WalOptions>,

    buf: Vec<u8>,
    min_sequence: u64,
    max_sequence: u64,
}

impl WalWriter {
    /// Opens a wal file at path, returns a WalWriter with id and config.
    /// If wal file doesn't exist, create new wal file and set it's min_log_sequence(default 0).
    pub async fn open(
        config: Arc<WalOptions>,
        id: u64,
        path: impl AsRef<Path>,
        min_seq: u64,
    ) -> WalResult<Self> {
        let path = path.as_ref();

        // Use min_sequence existing in file, otherwise in parameter
        let (writer, min_sequence, max_sequence) = if file_manager::try_exists(path) {
            let writer = record_file::Writer::open(path, RecordDataType::Wal)
                .await
                .context(super::WriteSnafu)?;
            let (min_sequence, max_sequence) = match writer.footer() {
                Some(footer) => WalReader::parse_footer(footer).unwrap_or((min_seq, min_seq)),
                None => (min_seq, min_seq),
            };
            (writer, min_sequence, max_sequence)
        } else {
            (
                record_file::Writer::open(path, RecordDataType::Wal)
                    .await
                    .context(super::WriteSnafu)?,
                min_seq,
                min_seq,
            )
        };

        let size = writer.file_size();

        Ok(Self {
            id,
            inner: writer,
            size,
            path: PathBuf::from(path),
            config,
            buf: Vec::new(),
            min_sequence,
            max_sequence,
        })
    }

    /// Writes data, returns data sequence and data size.
    pub async fn write(
        &mut self,
        tenant: String,
        vnode_id: VnodeId,
        precision: Precision,
        points: Vec<u8>,
    ) -> WalResult<(u64, usize)> {
        let seq = self.max_sequence;
        let tenant_len = tenant.len() as u64;

        let written_size = self
            .inner
            .write_record(
                RecordDataVersion::V1 as u8,
                RecordDataType::Wal as u8,
                [
                    &[WalEntryType::Write as u8][..],
                    &seq.to_be_bytes(),
                    &vnode_id.to_be_bytes(),
                    &(precision as u8).to_be_bytes(),
                    &tenant_len.to_be_bytes(),
                    tenant.as_bytes(),
                    &points,
                ]
                .as_slice(),
            )
            .await
            .context(super::WriteSnafu)?;

        if self.config.sync {
            self.inner.sync().await.context(super::SyncSnafu)?;
        }
        // write & fsync succeed
        self.max_sequence += 1;
        self.size += written_size as u64;
        Ok((seq, written_size))
    }

    pub async fn delete_vnode(
        &mut self,
        tenant: String,
        database: String,
        vnode_id: VnodeId,
    ) -> WalResult<(u64, usize)> {
        let seq = self.max_sequence;
        let tenant_len = tenant.len() as u64;

        let written_size = self
            .inner
            .write_record(
                RecordDataVersion::V1 as u8,
                RecordDataType::Wal as u8,
                [
                    &[WalEntryType::DeleteVnode as u8][..],
                    &seq.to_be_bytes(),
                    &vnode_id.to_be_bytes(),
                    &tenant_len.to_be_bytes(),
                    tenant.as_bytes(),
                    database.as_bytes(),
                ]
                .as_slice(),
            )
            .await
            .context(super::WriteSnafu)?;

        if self.config.sync {
            self.inner.sync().await.context(super::SyncSnafu)?;
        }
        // write & fsync succeed
        self.max_sequence += 1;
        self.size += written_size as u64;
        Ok((seq, written_size))
    }

    pub async fn delete_table(
        &mut self,
        tenant: String,
        database: String,
        table: String,
    ) -> WalResult<(u64, usize)> {
        let seq = self.max_sequence;
        let tenant_len = tenant.len() as u64;
        let database_len = database.len() as u64;

        let written_size = self
            .inner
            .write_record(
                RecordDataVersion::V1 as u8,
                RecordDataType::Wal as u8,
                [
                    &[WalEntryType::DeleteTable as u8][..],
                    &seq.to_be_bytes(),
                    &tenant_len.to_be_bytes(),
                    &database_len.to_be_bytes(),
                    tenant.as_bytes(),
                    database.as_bytes(),
                    table.as_bytes(),
                ]
                .as_slice(),
            )
            .await
            .context(super::WriteSnafu)?;

        if self.config.sync {
            self.inner.sync().await.context(super::SyncSnafu)?;
        }
        // write & fsync succeed
        self.max_sequence += 1;
        self.size += written_size as u64;
        Ok((seq, written_size))
    }

    pub async fn sync(&self) -> WalResult<()> {
        self.inner.sync().await.context(super::SyncSnafu)
    }

    pub async fn close(mut self) -> WalResult<usize> {
        info!(
            "Closing wal with sequence: [{}, {})",
            self.min_sequence, self.max_sequence
        );
        let footer = build_footer(self.min_sequence, self.max_sequence);
        let size = self
            .inner
            .write_footer(footer)
            .await
            .context(super::WriteSnafu)?;
        self.inner.close().await.context(super::WriteSnafu)?;
        Ok(size)
    }
}

pub struct WalManager {
    config: Arc<WalOptions>,
    global_seq_ctx: Arc<GlobalSequenceContext>,
    current_dir: PathBuf,
    current_file: WalWriter,
    total_file_size: u64,
    old_file_max_sequence: HashMap<u64, u64>,
}

unsafe impl Send for WalManager {}

unsafe impl Sync for WalManager {}

impl WalManager {
    pub async fn open(
        config: Arc<WalOptions>,
        global_seq_ctx: Arc<GlobalSequenceContext>,
    ) -> TskvResult<Self> {
        if !file_manager::try_exists(&config.path) {
            std::fs::create_dir_all(&config.path).unwrap();
        }
        let base_path = config.path.to_path_buf();

        let mut total_file_size = 0_u64;
        let mut old_file_max_sequence: HashMap<u64, u64> = HashMap::new();
        let file_names = file_manager::list_file_names(&config.path);
        for f in file_names {
            let file_path = base_path.join(&f);
            match tokio::fs::metadata(&file_path).await {
                Ok(m) => {
                    total_file_size += m.len();
                }
                Err(e) => error!("Failed to get WAL file metadata for '{}': {:?}", &f, e),
            }
            match read_footer(file_path).await {
                Ok(Some((_, max_seq))) => match file_utils::get_wal_file_id(&f) {
                    Ok(file_id) => {
                        old_file_max_sequence.insert(file_id, max_seq);
                    }
                    Err(e) => error!("Failed to parse WAL file name for '{}': {:?}", &f, e),
                },
                Ok(None) => warn!("Failed to parse WAL file footer for '{}'", &f),
                Err(e) => warn!("Failed to parse WAL file footer for '{}': {:?}", &f, e),
            }
        }

        // Create a new wal file every time it starts.
        let (pre_max_seq, next_file_id) =
            match file_utils::get_max_sequence_file_name(&config.path, file_utils::get_wal_file_id)
            {
                Some((_, id)) => {
                    let path = file_utils::make_wal_file(&config.path, id);
                    let (_, max_seq) = read_footer(&path)
                        .await
                        .context(error::WalSnafu)?
                        .unwrap_or((1_u64, 1_u64));
                    (max_seq + 1, id + 1)
                }
                None => (1_u64, 1_u64),
            };

        let new_wal = file_utils::make_wal_file(&config.path, next_file_id);
        let current_file = WalWriter::open(config.clone(), next_file_id, new_wal, pre_max_seq)
            .await
            .context(error::WalSnafu)?;
        total_file_size += current_file.size;
        info!("WAL '{}' starts write", current_file.id);
        let current_dir = config.path.clone();
        Ok(WalManager {
            config,
            global_seq_ctx,
            current_dir,
            current_file,
            old_file_max_sequence,
            total_file_size,
        })
    }

    async fn roll_wal_file(&mut self, max_file_size: u64) -> WalResult<()> {
        if self.current_file.size > max_file_size {
            info!(
                "WAL '{}' is full at seq '{}', begin rolling.",
                self.current_file.id, self.current_file.max_sequence
            );

            let new_file_id = self.current_file.id + 1;
            let new_file_name = file_utils::make_wal_file(&self.config.path, new_file_id);

            let new_file = WalWriter::open(
                self.config.clone(),
                new_file_id,
                new_file_name,
                self.current_file.max_sequence,
            )
            .await?;
            // Total WALs size add WAL header size.
            self.total_file_size += new_file.size;

            let mut old_file = std::mem::replace(&mut self.current_file, new_file);
            if old_file.max_sequence <= old_file.min_sequence {
                old_file.max_sequence = old_file.min_sequence;
            } else {
                old_file.max_sequence -= 1;
            }
            self.old_file_max_sequence
                .insert(old_file.id, old_file.max_sequence);
            // Total WALs size add WAL footer size.
            self.total_file_size += old_file.close().await? as u64;

            info!(
                "WAL '{}' starts write at seq {}",
                self.current_file.id, self.current_file.max_sequence
            );

            self.check_to_delete().await;
        }
        Ok(())
    }

    pub async fn check_to_delete(&mut self) {
        let min_seq = self.global_seq_ctx.min_seq();
        let mut old_files_to_delete: Vec<u64> = Vec::new();
        for (old_file_id, old_file_max_seq) in self.old_file_max_sequence.iter() {
            if *old_file_max_seq < min_seq {
                old_files_to_delete.push(*old_file_id);
            }
        }

        if !old_files_to_delete.is_empty() {
            for file_id in old_files_to_delete {
                let file_path = file_utils::make_wal_file(&self.config.path, file_id);
                debug!("Removing wal file '{}'", file_path.display());
                let file_size = match tokio::fs::metadata(&file_path).await {
                    Ok(m) => m.len(),
                    Err(e) => {
                        error!(
                            "Failed to get WAL file metadata for '{}': {:?}",
                            file_path.display(),
                            e
                        );
                        0
                    }
                };
                if let Err(e) = tokio::fs::remove_file(&file_path).await {
                    error!("Failed to remove file '{}': {:?}", file_path.display(), e);
                }
                // Remove max_sequence record for deleted file.
                self.old_file_max_sequence.remove(&file_id);
                // Subtract deleted file size.
                self.total_file_size -= file_size;
            }
        }
    }

    /// Checks if wal file is full then writes data. Return data sequence and data size.
    pub async fn write(&mut self, wal_task: WalTask) {
        if let Err(e) = self.roll_wal_file(self.config.max_file_size).await {
            error!("Failed to roll WAL file: {}", e);
            if wal_task.fail(e).is_err() {
                error!("Failed to send roll WAL error to tskv");
            }
            return;
        }
        let (write_ret, cb) = match wal_task {
            WalTask::Write {
                tenant,
                vnode_id,
                precision,
                points,
                cb,
            } => (
                self.current_file
                    .write(tenant, vnode_id, precision, points)
                    .await,
                cb,
            ),
            WalTask::DeleteVnode {
                tenant,
                database,
                vnode_id,
                cb,
            } => (
                self.current_file
                    .delete_vnode(tenant, database, vnode_id)
                    .await,
                cb,
            ),
            WalTask::DeleteTable {
                tenant,
                database,
                table,
                cb,
            } => (
                self.current_file
                    .delete_table(tenant, database, table)
                    .await,
                cb,
            ),
        };
        let send_ret = match write_ret {
            Ok((seq, size)) => {
                self.total_file_size += size as u64;
                cb.send(Ok((seq, size)))
            }
            Err(e) => cb.send(Err(e)),
        };
        if let Err(e) = send_ret {
            // WAL job closed, leaving this write request.
            warn!("send WAL write result failed: {:?}", e);
        }
    }

    pub async fn recover(&self, engine: &impl Engine) -> TskvResult<()> {
        let vnode_last_seq_map = self.global_seq_ctx.cloned();
        let min_log_seq = self.global_seq_ctx.min_seq();
        warn!("Recover: reading wal from seq '{}'", min_log_seq);

        let wal_files = file_manager::list_file_names(&self.current_dir);
        // TODO: Parallel get min_sequence at first.
        for file_name in wal_files {
            let path = self.current_dir.join(&file_name);
            if !file_manager::try_exists(&path) {
                continue;
            }
            let mut reader = WalReader::open(&path).await.context(error::WalSnafu)?;
            if reader.is_empty() {
                continue;
            }
            // If this file has no footer, try to read all it's records.
            // If max_sequence of this file is greater than min_log_seq, read all it's records.
            if reader.max_sequence == 0 || reader.max_sequence >= min_log_seq {
                info!(
                    "Recover: reading wal '{}' for seq {} to {}",
                    file_name, reader.min_sequence, reader.max_sequence
                );
                Self::read_wal_to_engine(&mut reader, engine, min_log_seq, &vnode_last_seq_map)
                    .await?;
            }
        }
        Ok(())
    }

    async fn read_wal_to_engine(
        reader: &mut WalReader,
        engine: &impl Engine,
        min_log_seq: u64,
        vnode_last_seq_map: &HashMap<VnodeId, u64>,
    ) -> TskvResult<bool> {
        let mut seq_gt_min_seq = false;
        let decoder = get_str_codec(Encoding::Zstd);
        let mut decoded_data = Vec::new();
        loop {
            match reader.next_wal_entry().await {
                Ok(Some(wal_entry_blk)) => {
                    let seq = wal_entry_blk.seq();
                    if seq < min_log_seq {
                        continue;
                    }
                    seq_gt_min_seq = true;
                    match wal_entry_blk.entry {
                        WalEntry::Write(blk) => {
                            decoded_data.truncate(0);
                            decoder
                                .decode(blk.points(), &mut decoded_data)
                                .context(error::DecodeSnafu)?;
                            if decoded_data.is_empty() {
                                continue;
                            }
                            let vnode_id = blk.vnode_id();
                            if let Some(tsf_last_seq) = vnode_last_seq_map.get(&vnode_id) {
                                // If `seq_no` of TsFamily is greater than or equal to `seq`,
                                // it means that data was writen to tsm.
                                if *tsf_last_seq >= seq {
                                    continue;
                                }
                            }
                            let tenant =
                                unsafe { String::from_utf8_unchecked(blk.tenant().to_vec()) };
                            let precision = blk.precision();
                            let req = WritePointsRequest {
                                version: 1,
                                meta: Some(Meta {
                                    tenant,
                                    user: None,
                                    password: None,
                                }),
                                points: decoded_data[0].to_vec(),
                            };
                            engine
                                .write_from_wal(vnode_id, precision, req, seq)
                                .await
                                .unwrap();
                        }
                        WalEntry::DeleteVnode(blk) => {
                            let vnode_id = blk.vnode_id();
                            let tenant =
                                unsafe { String::from_utf8_unchecked(blk.tenant().to_vec()) };
                            let database =
                                unsafe { String::from_utf8_unchecked(blk.database().to_vec()) };
                            engine
                                .remove_tsfamily(&tenant, &database, vnode_id)
                                .await
                                .unwrap();
                        }
                        WalEntry::DeleteTable(blk) => {
                            // TODO(zipper) may we only delete data in memcache?
                            let tenant =
                                unsafe { String::from_utf8_unchecked(blk.tenant().to_vec()) };
                            let database =
                                unsafe { String::from_utf8_unchecked(blk.database().to_vec()) };
                            let table =
                                unsafe { String::from_utf8_unchecked(blk.table().to_vec()) };
                            engine.drop_table(&tenant, &database, &table).await.unwrap();
                        }
                        _ => {}
                    };
                }
                Ok(None) | Err(WalError::WalTruncated) => {
                    break;
                }
                Err(e) => {
                    panic!(
                        "Failed to recover from {}: {:?}",
                        reader.path().display(),
                        e
                    );
                }
            }
        }
        Ok(seq_gt_min_seq)
    }

    pub async fn sync(&self) -> TskvResult<()> {
        self.current_file.sync().await.context(error::WalSnafu)
    }

    /// Close current record file, return count of bytes appended as footer.
    pub async fn close(self) -> TskvResult<usize> {
        self.current_file.close().await.context(error::WalSnafu)
    }

    pub fn current_seq_no(&self) -> u64 {
        self.current_file.max_sequence
    }

    pub fn sync_interval(&self) -> std::time::Duration {
        self.config.sync_interval
    }

    pub fn is_total_file_size_exceed(&self) -> bool {
        self.total_file_size >= self.config.flush_trigger_total_file_size
    }

    pub fn total_file_size(&self) -> u64 {
        self.total_file_size
    }
}

pub struct WalReader {
    inner: record_file::Reader,
    /// Min write sequence in the wal file, may be 0 if wal file is new or
    /// CnosDB was crushed or force-killed.
    min_sequence: u64,
    /// Max write sequence in the wal file, may be 0 if wal file is new or
    /// CnosDB was crushed or force-killed.
    max_sequence: u64,
}

impl WalReader {
    pub async fn open(path: impl AsRef<Path>) -> WalResult<Self> {
        let reader = record_file::Reader::open(&path)
            .await
            .context(super::ReadSnafu)?;

        let (min_sequence, max_sequence) = match reader.footer() {
            Some(footer) => Self::parse_footer(footer).unwrap_or((0_u64, 0_u64)),
            None => (0_u64, 0_u64),
        };

        Ok(Self {
            inner: reader,
            min_sequence,
            max_sequence,
        })
    }

    /// Parses wal footer, returns sequence range.
    pub fn parse_footer(footer: [u8; record_file::FILE_FOOTER_LEN]) -> Option<(u64, u64)> {
        let magic_number = decode_be_u32(&footer[0..4]);
        if magic_number != FOOTER_MAGIC_NUMBER {
            // There is no footer in wal file.
            return None;
        }
        let min_sequence = decode_be_u64(&footer[16..24]);
        let max_sequence = decode_be_u64(&footer[24..32]);
        Some((min_sequence, max_sequence))
    }

    pub async fn next_wal_entry(&mut self) -> WalResult<Option<WalEntryBlock>> {
        let data = match self.inner.read_record().await {
            Ok(r) => r.data,
            Err(RecordFileError::Eof) => {
                return Ok(None);
            }
            Err(e) => {
                error!("Error reading wal: {:?}", e);
                return Err(WalError::WalTruncated);
            }
        };
        Ok(Some(WalEntryBlock::new(data)))
    }

    pub fn path(&self) -> &PathBuf {
        self.inner.path()
    }

    pub fn len(&self) -> u64 {
        self.inner.len()
    }

    /// If this record file has some records in it.
    pub fn is_empty(&self) -> bool {
        match self
            .len()
            .checked_sub((record_file::FILE_MAGIC_NUMBER_LEN + record_file::FILE_FOOTER_LEN) as u64)
        {
            Some(d) => d == 0,
            None => true,
        }
    }
}

pub async fn print_wal_statistics(path: impl AsRef<Path>) {
    use protos::models as fb_models;

    let mut reader = WalReader::open(path).await.unwrap();
    let decoder = get_str_codec(Encoding::Zstd);
    loop {
        match reader.next_wal_entry().await {
            Ok(Some(entry_block)) => {
                println!("============================================================");
                println!("Seq: {}, Typ: {}", entry_block.seq(), entry_block.typ);
                match entry_block.entry {
                    WalEntry::Write(blk) => {
                        println!(
                            "Tenant: {}, VnodeId: {}, Precision: {}",
                            std::str::from_utf8(blk.tenant()).unwrap(),
                            blk.vnode_id(),
                            blk.precision(),
                        );
                        let ety_points = blk.points();
                        let mut data_buf = Vec::with_capacity(ety_points.len());
                        decoder.decode(ety_points, &mut data_buf).unwrap();
                        match flatbuffers::root::<fb_models::Points>(&data_buf[0]) {
                            Ok(points) => {
                                print_points(points);
                            }
                            Err(e) => panic!("unexpected data: '{:?}'", e),
                        }
                    }
                    WalEntry::DeleteVnode(blk) => {
                        println!(
                            "Tenant: {}, Database: {}, VnodeId: {}",
                            std::str::from_utf8(blk.tenant()).unwrap(),
                            std::str::from_utf8(blk.database()).unwrap(),
                            blk.vnode_id(),
                        );
                    }
                    WalEntry::DeleteTable(blk) => {
                        println!(
                            "Tenant: {}, VnodeId: {}, Precision: {}",
                            std::str::from_utf8(blk.tenant()).unwrap(),
                            std::str::from_utf8(blk.database()).unwrap(),
                            std::str::from_utf8(blk.table()).unwrap(),
                        );
                    }
                    WalEntry::Unknown => {
                        println!("Unknown WAL entry type.");
                    }
                }
            }
            Ok(None) => {
                println!("============================================================");
                break;
            }
            Err(WalError::WalTruncated) => {
                println!("============================================================");
                println!("WAL file truncated");
                break;
            }
            Err(e) => {
                panic!("Failed to read wal file: {:?}", e);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use core::panic;
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;

    use memory_pool::GreedyMemoryPool;
    use meta::model::meta_admin::AdminMeta;
    use meta::model::MetaRef;
    use metrics::metric_register::MetricsRegister;
    use minivec::MiniVec;
    use models::codec::Encoding;
    use models::schema::{Precision, TenantOptions};
    use models::Timestamp;
    use protos::models::FieldType;
    use protos::{models as fb_models, models_helper, FbSchema};
    use serial_test::serial;
    use tokio::runtime;
    use trace::init_default_global_tracing;

    use crate::context::GlobalSequenceContext;
    use crate::file_system::file_manager::list_file_names;
    use crate::kv_option::WalOptions;
    use crate::memcache::test::get_one_series_cache_data;
    use crate::memcache::FieldVal;
    use crate::tsm::codec::get_str_codec;
    use crate::wal::wal::WalEntry;
    use crate::wal::{WalError, WalManager, WalReader, WalResult, WalTask};
    use crate::{error, kv_option, Engine, Error, TsKv};

    fn random_write_data() -> Vec<u8> {
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let ptr = models_helper::create_random_points_with_delta(&mut fbb, 5);
        fbb.finish(ptr, None);
        fbb.finished_data().to_vec()
    }

    /// Generate flatbuffers data and memcache data
    #[allow(clippy::type_complexity)]
    fn const_write_data(
        start_timestamp: i64,
        num: usize,
    ) -> (Vec<u8>, HashMap<String, Vec<(Timestamp, FieldVal)>>) {
        let mut fa_data: Vec<(Timestamp, FieldVal)> = Vec::with_capacity(num);
        let mut fb_data: Vec<(Timestamp, FieldVal)> = Vec::with_capacity(num);
        for i in start_timestamp..start_timestamp + num as i64 {
            fa_data.push((i, FieldVal::Integer(100)));
            fb_data.push((i, FieldVal::Bytes(MiniVec::from("b"))));
        }
        let map = HashMap::from([("fa".to_string(), fa_data), ("fb".to_string(), fb_data)]);

        let mut tags_names: HashMap<&str, usize> = HashMap::new();
        tags_names.insert("ta", 0);
        tags_names.insert("tb", 1);

        let mut fields: HashMap<&str, usize> = HashMap::new();
        fields.insert("fa", 0);
        fields.insert("fb", 1);

        let schema = FbSchema::new(
            tags_names,
            fields,
            vec![FieldType::Integer, FieldType::String],
        );

        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let ptr = models_helper::create_const_points(
            &mut fbb,
            schema,
            "dba",
            "tba",
            vec![("ta", "a"), ("tb", "b")],
            vec![("fa", &100_u64.to_be_bytes()), ("fb", b"b")],
            start_timestamp,
            num,
        );
        fbb.finish(ptr, None);
        (fbb.finished_data().to_vec(), map)
    }

    async fn check_wal_files(
        wal_dir: impl AsRef<Path>,
        data: Vec<Vec<u8>>,
        is_flatbuffers: bool,
    ) -> WalResult<()> {
        let wal_dir = wal_dir.as_ref();
        let wal_files = list_file_names(wal_dir);
        let mut data_iter = data.iter();
        for wal_file in wal_files {
            let path = wal_dir.join(wal_file);

            let mut reader = WalReader::open(&path).await.unwrap();
            let decoder = get_str_codec(Encoding::Zstd);
            println!("Reading data from wal file '{}'", path.display());
            loop {
                match reader.next_wal_entry().await {
                    Ok(Some(entry_block)) => {
                        println!("Reading entry from wal file '{}'", path.display());
                        match entry_block.entry {
                            WalEntry::Write(entry) => {
                                let ety_data = entry.points();
                                let ori_data = match data_iter.next() {
                                    Some(d) => d,
                                    None => {
                                        panic!("unexpected data to compare that is less than file count.")
                                    }
                                };
                                if is_flatbuffers {
                                    let mut data_buf = Vec::new();
                                    decoder.decode(ety_data, &mut data_buf).unwrap();
                                    assert_eq!(data_buf[0].as_slice(), ori_data.as_slice());
                                    if let Err(e) =
                                        flatbuffers::root::<fb_models::Points>(&data_buf[0])
                                    {
                                        panic!(
                                            "unexpected data in wal file, ignored file '{}' because '{}'",
                                            wal_dir.display(),
                                            e
                                        );
                                    }
                                } else {
                                    assert_eq!(ety_data, ori_data.as_slice());
                                }
                            }
                            WalEntry::DeleteVnode(_) => todo!(),
                            WalEntry::DeleteTable(_) => todo!(),
                            WalEntry::Unknown => todo!(),
                        }
                    }
                    Ok(None) => {
                        println!("Reae none from wal file '{}'", path.display());
                        break;
                    }
                    Err(WalError::WalTruncated) => {
                        println!("WAL file truncated: {}", path.display());
                        return Err(WalError::WalTruncated);
                    }
                    Err(e) => {
                        panic!("Failed to recover from {}: {:?}", path.display(), e);
                    }
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_read_and_write() {
        let dir = "/tmp/test/wal/1".to_string();
        let _ = std::fs::remove_dir_all(dir.clone()); // Ignore errors
        let mut global_config = config::get_config_for_test();
        global_config.wal.path = dir.clone();
        let wal_config = WalOptions::from(&global_config);

        let tenant = "cnosdb".to_string();
        let mut mgr = WalManager::open(Arc::new(wal_config), GlobalSequenceContext::empty())
            .await
            .unwrap();
        let mut data_vec = Vec::new();
        for i in 1..=10_u64 {
            let data = b"hello".to_vec();
            data_vec.push(data.clone());
            let (wal_task, rx) = WalTask::new_write(tenant.clone(), 0, Precision::NS, data);
            mgr.write(wal_task).await;
            let (seq, _) = rx.await.unwrap().unwrap();
            assert_eq!(i, seq)
        }
        mgr.close().await.unwrap();

        check_wal_files(&dir, data_vec, false).await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_roll_wal_file() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");

        let dir = "/tmp/test/wal/2".to_string();
        let _ = std::fs::remove_dir_all(dir.clone()); // Ignore errors
        let mut global_config = config::get_config_for_test();
        global_config.wal.path = dir.clone();
        // Argument max_file_size is so small that there must a new wal file created.
        global_config.wal.max_file_size = 1;
        global_config.wal.sync = false;
        global_config.wal.flush_trigger_total_file_size = 100;
        let wal_config = WalOptions::from(&global_config);

        let tenant = "cnosdb".to_string();
        let min_seq_no = 6;

        let gcs = GlobalSequenceContext::empty();
        gcs.set_min_seq(min_seq_no);

        let mut mgr = WalManager::open(Arc::new(wal_config), gcs).await.unwrap();
        let mut data_vec: Vec<Vec<u8>> = Vec::new();
        for seq in 1..=10 {
            let data = format!("{}", seq).into_bytes();
            if seq >= min_seq_no {
                // Data in file_id that less than version_set_min_seq_no will be deleted.
                data_vec.push(data.clone());
            }
            let (wal_task, rx) = WalTask::new_write(tenant.clone(), 0, Precision::NS, data);
            mgr.write(wal_task).await;
            let (write_seq, _) = rx.await.unwrap().unwrap();
            assert_eq!(seq, write_seq)
        }
        assert_eq!(mgr.total_file_size(), 364);
        assert!(mgr.is_total_file_size_exceed());
        mgr.close().await.unwrap();

        check_wal_files(dir, data_vec, false).await.unwrap();
    }

    #[tokio::test]
    async fn test_read_truncated() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let dir = "/tmp/test/wal/3".to_string();
        let _ = std::fs::remove_dir_all(dir.clone()); // Ignore errors
        let mut global_config = config::get_config_for_test();
        global_config.wal.path = dir.clone();
        let wal_config = WalOptions::from(&global_config);

        let tenant = "cnosdb".to_string();
        let mut mgr = WalManager::open(Arc::new(wal_config), GlobalSequenceContext::empty())
            .await
            .unwrap();
        let coder = get_str_codec(Encoding::Zstd);
        let mut data_vec: Vec<Vec<u8>> = Vec::new();

        for _i in 0..10 {
            let data = random_write_data();
            data_vec.push(data.clone());

            let mut enc_points = Vec::new();
            coder
                .encode(&[&data], &mut enc_points)
                .map_err(|_| Error::ChannelSend {
                    source: crate::error::ChannelSendError::WalTask,
                })
                .unwrap();
            let (wal_task, rx) = WalTask::new_write(tenant.clone(), 0, Precision::NS, enc_points);
            mgr.write(wal_task).await;
            rx.await.unwrap().unwrap();
        }
        // Do not close wal manager, so footer won't write.

        check_wal_files(dir, data_vec, true).await.unwrap();
    }

    #[test]
    #[serial]
    fn test_recover_from_wal() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let rt = Arc::new(runtime::Runtime::new().unwrap());
        let dir = "/tmp/test/wal/4/wal";
        let _ = std::fs::remove_dir_all(dir);
        let mut global_config = config::get_config_for_test();
        global_config.wal.path = dir.to_string();
        global_config.storage.path = "/tmp/test/wal/4".to_string();
        let wal_config = WalOptions::from(&global_config);

        let mut wrote_data: HashMap<String, Vec<(Timestamp, FieldVal)>> = HashMap::new();
        rt.block_on(async {
            let tenant = "cnosdb".to_string();
            let mut mgr = WalManager::open(Arc::new(wal_config), GlobalSequenceContext::empty())
                .await
                .unwrap();
            let coder = get_str_codec(Encoding::Zstd);
            let mut data_vec: Vec<Vec<u8>> = Vec::new();

            for i in 1..=10 {
                let (data, mem_data) = const_write_data(i, 1);
                data_vec.push(data.clone());

                for (col_name, values) in mem_data {
                    wrote_data
                        .entry(col_name)
                        .or_default()
                        .extend(values.into_iter());
                }

                let mut enc_points = Vec::new();
                coder
                    .encode(&[&data], &mut enc_points)
                    .map_err(|_| Error::ChannelSend {
                        source: error::ChannelSendError::WalTask,
                    })
                    .unwrap();
                let (wal_task, rx) =
                    WalTask::new_write(tenant.clone(), 10, Precision::NS, enc_points);
                mgr.write(wal_task).await;
                rx.await.unwrap().unwrap();
            }
            mgr.close().await.unwrap();
            check_wal_files(dir, data_vec, true).await.unwrap();
        });
        let rt_2 = rt.clone();
        rt.block_on(async {
            let memory_pool = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
            let opt = kv_option::Options::from(&global_config);
            let meta_manager: MetaRef = AdminMeta::new(global_config.clone()).await;

            meta_manager.add_data_node().await.unwrap();

            let _ = meta_manager
                .create_tenant("cnosdb".to_string(), TenantOptions::default())
                .await;
            let tskv = TsKv::open(
                meta_manager,
                opt,
                rt_2,
                memory_pool,
                Arc::new(MetricsRegister::default()),
            )
            .await
            .unwrap();
            let ver = tskv
                .get_db_version("cnosdb", "dba", 10)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(ver.ts_family_id, 10);

            let cached_data = get_one_series_cache_data(ver.caches.mut_cache.clone());
            // fa, fb
            assert_eq!(cached_data.len(), 2);
            assert_eq!(wrote_data.len(), 2);
            assert_eq!(wrote_data, cached_data);
        });
    }

    #[test]
    fn test_get_test_config() {
        let _ = config::get_config_for_test();
    }
}
