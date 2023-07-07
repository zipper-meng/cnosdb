use std::path::{Path, PathBuf};

use models::codec::Encoding;
use models::meta_data::VnodeId;
use models::schema::Precision;
use protos::models_helper::print_points;

use crate::byte_utils::{decode_be_u32, decode_be_u64};
use crate::error::{Error, Result};
use crate::file_system::file_manager;
use crate::record_file;
use crate::tsm::codec::get_str_codec;
use crate::wal::{
    mal, wal_path_to_mal_path, WalEntryType, ENTRY_HEADER_LEN, ENTRY_PRECISION_LEN,
    ENTRY_TENANT_SIZE_LEN, ENTRY_VNODE_ID_LEN, FOOTER_MAGIC_NUMBER,
};

/// Reads a wal file and parse footer, returns sequence range
pub async fn read_footer(path: impl AsRef<Path>) -> Result<Option<(u64, u64)>> {
    if file_manager::try_exists(&path) {
        let reader = WalReader::open(path).await?;
        Ok(Some((reader.min_sequence, reader.max_sequence)))
    } else {
        Ok(None)
    }
}

pub struct WalEntryBlock {
    pub typ: WalEntryType,
    pub seq: u64,
    pub entry: WalEntry,
}

impl WalEntryBlock {
    pub fn new(buf: Vec<u8>) -> WalEntryBlock {
        if buf.len() < ENTRY_HEADER_LEN {
            return Self {
                typ: WalEntryType::Unknown,
                seq: 0,
                entry: WalEntry::Unknown,
            };
        }
        let seq = decode_be_u64(&buf[1..9]);
        let entry_type: WalEntryType = buf[0].into();
        let entry: WalEntry = match entry_type {
            WalEntryType::Write => WalEntry::Write(WriteBlock::new(buf)),
            WalEntryType::DeleteTable => WalEntry::Unknown,
            WalEntryType::DeleteVnode => WalEntry::Unknown,
            WalEntryType::Unknown => WalEntry::Unknown,
        };
        Self {
            typ: entry_type,
            seq,
            entry,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum WalEntry {
    Write(WriteBlock),
    Unknown,
}

/// buf:
/// - header: ENTRY_HEADER_LEN
/// - vnode_id: ENTRY_VNODE_ID_LEN
/// - precision: ENTRY_PRECISION_LEN
/// - tenant_size: ENTRY_TENANT_SIZE_LEN
/// - tenant: tenant_size
/// - data: ..
#[derive(Debug, Clone, PartialEq)]
pub struct WriteBlock {
    buf: Vec<u8>,
    tenant_size: usize,
}

impl WriteBlock {
    pub fn new(buf: Vec<u8>) -> WriteBlock {
        let tenatn_size_pos = ENTRY_HEADER_LEN + ENTRY_VNODE_ID_LEN + ENTRY_PRECISION_LEN;
        let tenant_size =
            decode_be_u64(&buf[tenatn_size_pos..tenatn_size_pos + ENTRY_TENANT_SIZE_LEN]) as usize;
        Self { buf, tenant_size }
    }

    pub fn check_buf_size(size: usize) -> bool {
        size >= ENTRY_HEADER_LEN + ENTRY_VNODE_ID_LEN + ENTRY_PRECISION_LEN + ENTRY_TENANT_SIZE_LEN
    }

    pub fn vnode_id(&self) -> VnodeId {
        decode_be_u32(&self.buf[ENTRY_HEADER_LEN..ENTRY_HEADER_LEN + ENTRY_VNODE_ID_LEN])
    }

    pub fn precision(&self) -> Precision {
        Precision::from(self.buf[ENTRY_HEADER_LEN + ENTRY_VNODE_ID_LEN])
    }

    pub fn tenant(&self) -> &[u8] {
        let tenant_pos =
            ENTRY_HEADER_LEN + ENTRY_VNODE_ID_LEN + ENTRY_PRECISION_LEN + ENTRY_TENANT_SIZE_LEN;
        &self.buf[tenant_pos..tenant_pos + self.tenant_size]
    }

    pub fn points(&self) -> &[u8] {
        let points_pos = ENTRY_HEADER_LEN
            + ENTRY_VNODE_ID_LEN
            + ENTRY_PRECISION_LEN
            + ENTRY_TENANT_SIZE_LEN
            + self.tenant_size;
        &self.buf[points_pos..]
    }
}

#[cfg(test)]
impl WriteBlock {
    pub fn build(
        seq: u64,
        tenant: &str,
        vnode_id: VnodeId,
        precision: Precision,
        points: Vec<u8>,
    ) -> Self {
        let mut buf = Vec::new();
        buf.push(WalEntryType::Write as u8);
        buf.extend_from_slice(&seq.to_be_bytes());
        buf.extend_from_slice(&vnode_id.to_be_bytes());
        buf.push(precision as u8);
        buf.extend_from_slice(&(tenant.len() as u64).to_be_bytes());
        buf.extend_from_slice(tenant.as_bytes());
        buf.extend_from_slice(&points);

        Self {
            buf,
            tenant_size: tenant.len(),
        }
    }
}

pub struct WalReader {
    /// A reader of WAL file.
    inner: record_file::Reader,
    /// A reader of MAL file.
    mal_reader: Option<mal::MalReader>,

    /// Min write sequence in the wal file, may be 0 if wal file is new or
    /// CnosDB was crushed or force-killed.
    min_sequence: u64,
    /// Max write sequence in the wal file, may be 0 if wal file is new or
    /// CnosDB was crushed or force-killed.
    max_sequence: u64,
}

impl WalReader {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let wal_path = path.as_ref();

        let reader = record_file::Reader::open(&path).await?;
        let (min_sequence, max_sequence) = match reader.footer() {
            Some(footer) => Self::parse_footer(footer).unwrap_or((0_u64, 0_u64)),
            None => (0_u64, 0_u64),
        };

        let mal_path = wal_path_to_mal_path(wal_path)?;
        let mal_reader = if file_manager::try_exists(&mal_path) {
            Some(mal::MalReader::open(mal_path).await?)
        } else {
            None
        };

        Ok(Self {
            inner: reader,
            mal_reader,
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

    pub async fn next_wal_entry(&mut self) -> Result<Option<WalEntryBlock>> {
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
        Ok(Some(WalEntryBlock::new(data)))
    }

    pub fn min_sequence(&self) -> u64 {
        self.min_sequence
    }

    pub fn max_sequence(&self) -> u64 {
        self.max_sequence
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

pub async fn print_wal_statistics(path: impl AsRef<Path>) {
    use protos::models as fb_models;

    let mut reader = WalReader::open(path).await.unwrap();
    let decoder = get_str_codec(Encoding::Zstd);
    loop {
        match reader.next_wal_entry().await {
            Ok(Some(entry_block)) => {
                println!("============================================================");
                println!("Seq: {}, Typ: {}", entry_block.seq, entry_block.typ);
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
                    WalEntry::Unknown => {
                        println!("Unknown WAL entry type.");
                    }
                }
            }
            Ok(None) => {
                println!("============================================================");
                break;
            }
            Err(Error::WalTruncated) => {
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
