//! # MAL (meta write ahead log) file
//!
//! A MAL file is a [`record_file`].
//!
//! ## Record Data
//! ```text
//! # type = DeleteVnode
//! +------------+------------+------------+-------------+-------------+----------+
//! | 0: 1 byte  | 1: 8 bytes | 9: 4 bytes | 13: 8 bytes | 21: n bytes | n bytes  |
//! +------------+------------+------------+-------------+-------------+----------+
//! |    type    |  sequence  |  vnode_id  | tenant_size |  tenant     | database |
//! +------------+------------+------------+-------------+-------------+----------+
//!
//! # type = DeleteTable
//! +------------+------------+-------------+---------------+-----------------+---------------+---------+
//! | 0: 1 byte  | 1: 8 bytes | 9: 8 bytes  | 17: 4 bytes   | 21: tenant_size | database_size | n bytes |
//! +------------+------------+-------------+---------------+-----------------+---------------+---------+
//! |    type    |  sequence  | tenant_size | database_size |  tenant         |  database     | table   |
//! +------------+------------+-------------+---------------+-----------------+---------------+---------+
//! ```
//!
//! ## Footer
//! ```text
//! +------------+---------------+
//! | 0: 4 bytes | 4: 28 bytes   |
//! +------------+---------------+
//! | "malo"     | padding_zeros |
//! +------------+---------------+
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;

use models::meta_data::VnodeId;

use super::{ENTRY_DATABASE_SIZE_LEN, ENTRY_HEADER_LEN, ENTRY_TENANT_SIZE_LEN, ENTRY_VNODE_ID_LEN};
use crate::byte_utils::{decode_be_u32, decode_be_u64};
use crate::error::{Error, Result};
use crate::kv_option::WalOptions;
use crate::record_file::{self, RecordDataType, RecordDataVersion};

const MAL_FOOTER_MAGIC_NUMBER: u32 = u32::from_be_bytes([b'm', b'a', b'l', b'o']);
const MAL_FOOTER_MAGIC_NUMBER_LEN: usize = 4;

pub struct MalEntryBlock {
    pub typ: MalEntryType,
    pub seq: u64,
    pub entry: MalEntry,
}

impl MalEntryBlock {
    pub fn new(buf: Vec<u8>) -> Self {
        if buf.len() < ENTRY_HEADER_LEN {
            return Self {
                typ: MalEntryType::Unknown,
                seq: 0,
                entry: MalEntry::Unknown,
            };
        }
        let seq = decode_be_u64(&buf[1..9]);
        let entry_type: MalEntryType = buf[0].into();
        let entry: MalEntry = match entry_type {
            MalEntryType::DeleteVnode => MalEntry::DeleteVnode(DeleteVnodeBlock::new(buf)),
            MalEntryType::DeleteTable => MalEntry::DeleteTable(DeleteTableBlock::new(buf)),
            MalEntryType::Unknown => MalEntry::Unknown,
        };
        Self {
            typ: entry_type,
            seq,
            entry,
        }
    }
}

#[repr(u8)]
#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub enum MalEntryType {
    DeleteVnode = 11,
    DeleteTable = 21,
    Unknown = 127,
}

impl From<u8> for MalEntryType {
    fn from(typ: u8) -> Self {
        match typ {
            11 => MalEntryType::DeleteVnode,
            21 => MalEntryType::DeleteTable,
            _ => MalEntryType::Unknown,
        }
    }
}

impl std::fmt::Display for MalEntryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MalEntryType::DeleteVnode => write!(f, "delete_vnode"),
            MalEntryType::DeleteTable => write!(f, "delete_table"),
            MalEntryType::Unknown => write!(f, "unknown"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MalEntry {
    DeleteVnode(DeleteVnodeBlock),
    DeleteTable(DeleteTableBlock),
    Unknown,
}

/// buf:
/// - header: ENTRY_HEADER_LEN
/// - vnode_id: ENTRY_VNODE_ID_LEN
/// - tenant_size: ENTRY_TENANT_SIZE_LEN
/// - tenant: tenant_size
/// - database: ..
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteVnodeBlock {
    buf: Vec<u8>,
    tenant_size: usize,
}

impl DeleteVnodeBlock {
    pub fn new(buf: Vec<u8>) -> Self {
        let tenant_len = decode_be_u64(
            &buf[ENTRY_HEADER_LEN + ENTRY_VNODE_ID_LEN
                ..ENTRY_HEADER_LEN + ENTRY_VNODE_ID_LEN + ENTRY_TENANT_SIZE_LEN],
        ) as usize;
        Self {
            buf,
            tenant_size: tenant_len,
        }
    }

    pub fn check_buf_size(size: usize) -> bool {
        size > ENTRY_HEADER_LEN + ENTRY_VNODE_ID_LEN + ENTRY_TENANT_SIZE_LEN
    }

    pub fn vnode_id(&self) -> VnodeId {
        decode_be_u32(&self.buf[ENTRY_HEADER_LEN..ENTRY_HEADER_LEN + ENTRY_VNODE_ID_LEN])
    }

    pub fn tenant(&self) -> &[u8] {
        let tenant_pos = ENTRY_HEADER_LEN + ENTRY_VNODE_ID_LEN + ENTRY_TENANT_SIZE_LEN;
        &self.buf[tenant_pos..tenant_pos + self.tenant_size]
    }

    pub fn database(&self) -> &[u8] {
        let database_pos =
            ENTRY_HEADER_LEN + ENTRY_VNODE_ID_LEN + ENTRY_TENANT_SIZE_LEN + self.tenant_size;
        &self.buf[database_pos..]
    }
}

#[cfg(test)]
impl DeleteVnodeBlock {
    pub fn build(seq: u64, tenant: &str, database: &str, vnode_id: VnodeId) -> Self {
        let mut buf = Vec::new();
        let tenant_bytes = tenant.as_bytes();
        buf.push(MalEntryType::DeleteVnode as u8);
        buf.extend_from_slice(&seq.to_be_bytes());
        buf.extend_from_slice(&vnode_id.to_be_bytes());
        buf.extend_from_slice(&(tenant_bytes.len() as u64).to_be_bytes());
        buf.extend_from_slice(tenant_bytes);
        buf.extend_from_slice(database.as_bytes());

        Self {
            buf,
            tenant_size: tenant_bytes.len(),
        }
    }
}

/// buf:
/// - header: ENTRY_HEADER_LEN
/// - tenant_size: ENTRY_TENANT_SIZE_LEN
/// - database_size: ENTRY_DATABASE_SIZE_LEN
/// - tenant: tenant_size
/// - database: database_size
/// - table: ..
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteTableBlock {
    buf: Vec<u8>,
    tenant_len: usize,
    database_len: usize,
}

impl DeleteTableBlock {
    pub fn new(buf: Vec<u8>) -> DeleteTableBlock {
        let tenant_len =
            decode_be_u64(&buf[ENTRY_HEADER_LEN..ENTRY_HEADER_LEN + ENTRY_TENANT_SIZE_LEN])
                as usize;
        let database_len_pos = ENTRY_HEADER_LEN + ENTRY_TENANT_SIZE_LEN;
        let database_len =
            decode_be_u32(&buf[database_len_pos..database_len_pos + ENTRY_DATABASE_SIZE_LEN])
                as usize;
        Self {
            buf,
            tenant_len,
            database_len,
        }
    }

    pub fn check_buf_size(size: usize) -> bool {
        size >= ENTRY_HEADER_LEN + ENTRY_TENANT_SIZE_LEN + ENTRY_DATABASE_SIZE_LEN
    }

    pub fn tenant(&self) -> &[u8] {
        let tenant_pos = ENTRY_HEADER_LEN + ENTRY_TENANT_SIZE_LEN + ENTRY_DATABASE_SIZE_LEN;
        &self.buf[tenant_pos..tenant_pos + self.tenant_len]
    }

    pub fn database(&self) -> &[u8] {
        let database_pos =
            ENTRY_HEADER_LEN + ENTRY_TENANT_SIZE_LEN + ENTRY_DATABASE_SIZE_LEN + self.tenant_len;
        &self.buf[database_pos..database_pos + self.database_len]
    }

    pub fn table(&self) -> &[u8] {
        let table_pos = ENTRY_HEADER_LEN
            + ENTRY_TENANT_SIZE_LEN
            + ENTRY_DATABASE_SIZE_LEN
            + self.tenant_len
            + self.database_len;
        &self.buf[table_pos..]
    }
}

#[cfg(test)]
impl DeleteTableBlock {
    pub fn build(seq: u64, tenant: &str, database: &str, table: &str) -> Self {
        let mut buf = Vec::new();
        let tenant_bytes = tenant.as_bytes();
        let database_bytes = database.as_bytes();
        let table_bytes = table.as_bytes();
        buf.push(MalEntryType::DeleteTable as u8);
        buf.extend_from_slice(&seq.to_be_bytes());
        buf.extend_from_slice(&(tenant_bytes.len() as u64).to_be_bytes());
        buf.extend_from_slice(&(database_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(tenant_bytes);
        buf.extend_from_slice(database_bytes);
        buf.extend_from_slice(table_bytes);

        Self {
            buf,
            tenant_len: tenant_bytes.len(),
            database_len: database_bytes.len(),
        }
    }
}

pub struct MalReader {
    inner: record_file::Reader,
}

impl MalReader {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let reader = record_file::Reader::open(&path).await?;

        Ok(Self { inner: reader })
    }

    pub async fn next_mal_entry(&mut self) -> Result<Option<MalEntryBlock>> {
        let data = match self.inner.read_record().await {
            Ok(r) => r.data,
            Err(Error::Eof) => {
                return Ok(None);
            }
            Err(e) => {
                trace::error!("Error reading wal: {:?}", e);
                return Err(Error::WalTruncated);
            }
        };
        Ok(Some(MalEntryBlock::new(data)))
    }

    pub fn path(&self) -> PathBuf {
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

pub struct MalWriter {
    id: u64,
    inner: record_file::Writer,
    path: PathBuf,
    config: Arc<WalOptions>,

    buf: Vec<u8>,
}

impl MalWriter {
    /// Opens a wal file at path, returns a MalWriter with id and config.
    /// If wal file doesn't exist, create new wal file and set it's min_log_sequence(default 0).
    pub async fn open(config: Arc<WalOptions>, id: u64, path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let writer = record_file::Writer::open(path, RecordDataType::Mal).await?;
        let size = writer.file_size();

        Ok(Self {
            id,
            inner: writer,
            path: PathBuf::from(path),
            config,
            buf: Vec::new(),
        })
    }

    pub async fn delete_vnode(
        &mut self,
        seq: u64,
        tenant: String,
        database: String,
        vnode_id: VnodeId,
    ) -> Result<usize> {
        let tenant_len = tenant.len() as u64;

        let written_size = self
            .inner
            .write_record(
                RecordDataVersion::V1 as u8,
                RecordDataType::Wal as u8,
                [
                    &[MalEntryType::DeleteVnode as u8][..],
                    &seq.to_be_bytes(),
                    &vnode_id.to_be_bytes(),
                    &tenant_len.to_be_bytes(),
                    tenant.as_bytes(),
                    database.as_bytes(),
                ]
                .as_slice(),
            )
            .await?;

        if self.config.sync {
            self.inner.sync().await?;
        }
        Ok(written_size)
    }

    pub async fn delete_table(
        &mut self,
        seq: u64,
        tenant: String,
        database: String,
        table: String,
    ) -> Result<usize> {
        let tenant_len = tenant.len() as u64;
        let database_len = database.len() as u32;

        let written_size = self
            .inner
            .write_record(
                RecordDataVersion::V1 as u8,
                RecordDataType::Wal as u8,
                [
                    &[MalEntryType::DeleteTable as u8][..],
                    &seq.to_be_bytes(),
                    &tenant_len.to_be_bytes(),
                    &database_len.to_be_bytes(),
                    tenant.as_bytes(),
                    database.as_bytes(),
                    table.as_bytes(),
                ]
                .as_slice(),
            )
            .await?;

        if self.config.sync {
            self.inner.sync().await?;
        }
        Ok(written_size)
    }

    pub async fn sync(&self) -> Result<()> {
        self.inner.sync().await
    }

    pub async fn close(mut self) -> Result<()> {
        self.inner.close().await
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}
