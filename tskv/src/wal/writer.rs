use std::path::{Path, PathBuf};
use std::sync::Arc;

use trace::info;

use super::reader::WalReader;
use super::{WalEntryType, FOOTER_MAGIC_NUMBER};
use crate::error::Result;
use crate::file_system::file_manager;
use crate::kv_option::WalOptions;
use crate::record_file::{self, RecordDataType, RecordDataVersion};
use crate::TseriesFamilyId;

pub struct WalWriter {
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
    pub async fn open(
        config: Arc<WalOptions>,
        id: u64,
        path: impl AsRef<Path>,
        min_seq: u64,
    ) -> Result<Self> {
        let path = path.as_ref();

        // Use min_sequence existing in file, otherwise in parameter
        let (writer, min_sequence, max_sequence) = if file_manager::try_exists(path) {
            let writer = record_file::Writer::open(path, RecordDataType::Wal).await?;
            let (min_sequence, max_sequence) = match writer.footer() {
                Some(footer) => WalReader::parse_footer(footer).unwrap_or((min_seq, min_seq)),
                None => (min_seq, min_seq),
            };
            (writer, min_sequence, max_sequence)
        } else {
            (
                record_file::Writer::open(path, RecordDataType::Wal).await?,
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
        typ: WalEntryType,
        data: Arc<Vec<u8>>,
        id: TseriesFamilyId,
        tenant: Arc<Vec<u8>>,
    ) -> Result<(u64, usize)> {
        let mut seq = self.max_sequence;
        let tenant_len = tenant.len() as u64;

        let written_size = self
            .inner
            .write_record(
                RecordDataVersion::V1 as u8,
                RecordDataType::Wal as u8,
                [
                    &[typ as u8][..],
                    &seq.to_be_bytes(),
                    &id.to_be_bytes(),
                    &tenant_len.to_be_bytes(),
                    &tenant,
                    &data,
                ]
                .as_slice(),
            )
            .await?;

        if self.config.sync {
            self.inner.sync().await?;
        }
        // write & fsync succeed
        self.max_sequence += 1;
        self.size += written_size as u64;
        Ok((seq, written_size))
    }

    pub async fn sync(&self) -> Result<()> {
        self.inner.sync().await
    }

    pub async fn close(mut self) -> Result<()> {
        info!(
            "Closing wal with sequence: [{}, {})",
            self.min_sequence, self.max_sequence
        );
        let footer = build_footer(self.min_sequence, self.max_sequence);
        self.inner.write_footer(footer).await?;
        self.inner.close().await
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn min_sequence(&self) -> u64 {
        self.min_sequence
    }

    pub fn max_sequence(&self) -> u64 {
        self.max_sequence
    }

    pub fn set_max_sequence(&mut self, max_sequence: u64) {
        self.max_sequence = max_sequence;
    }
}

fn build_footer(min_sequence: u64, max_sequence: u64) -> [u8; record_file::FILE_FOOTER_LEN] {
    let mut footer = [0_u8; record_file::FILE_FOOTER_LEN];
    footer[0..4].copy_from_slice(&FOOTER_MAGIC_NUMBER.to_be_bytes());
    footer[16..24].copy_from_slice(&min_sequence.to_be_bytes());
    footer[24..32].copy_from_slice(&max_sequence.to_be_bytes());
    footer
}
