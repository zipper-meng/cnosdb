use std::cmp::Ordering;
use std::path::{Path, PathBuf};

use async_recursion::async_recursion;
use num_traits::ToPrimitive;

use super::{
    file_crc_source_len, Record, FILE_FOOTER_CRC32_NUMBER_LEN, FILE_FOOTER_LEN,
    FILE_FOOTER_MAGIC_NUMBER_LEN, FILE_MAGIC_NUMBER_LEN, READER_BUF_SIZE, RECORD_CRC32_NUMBER_LEN,
    RECORD_DATA_SIZE_LEN, RECORD_DATA_TYPE_LEN, RECORD_DATA_VERSION_LEN, RECORD_HEADER_LEN,
    RECORD_MAGIC_NUMBER, RECORD_MAGIC_NUMBER_LEN,
};
use crate::byte_utils::decode_be_u32;
use crate::error::{Error, Result};
use crate::file_system::file::async_file::AsyncFile;
use crate::file_system::file::IFile;
use crate::file_system::file_manager;

pub struct Reader {
    path: PathBuf,
    file: AsyncFile,
    pos: usize,
    buf: Vec<u8>,
    buf_len: usize,
    buf_use: usize,
    footer: Option<Vec<u8>>, //FILE_FOOTER_LEN
    footer_pos: u64,
}

impl Reader {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = file_manager::open_file(path).await?;
        let (footer_pos, footer) = match Self::read_footer(&path).await {
            Ok((p, f)) => (p, Some(f)),
            Err(Error::NoFooter) => (file.len(), None),
            Err(e) => {
                trace::error!(
                    "Failed to read footer of record_file '{}': {e}",
                    path.display(),
                );
                return Err(e);
            }
        };
        let records_len = if footer_pos == file.len() {
            // If there is no footer
            file.len() - FILE_MAGIC_NUMBER_LEN as u64
        } else {
            file.len() - FILE_FOOTER_LEN as u64 - FILE_MAGIC_NUMBER_LEN as u64
        };
        let buf_size = records_len.min(READER_BUF_SIZE as u64) as usize;
        Ok(Reader {
            path: path.to_path_buf(),
            file,
            pos: FILE_MAGIC_NUMBER_LEN,
            buf: vec![0_u8; buf_size],
            buf_len: 0,
            buf_use: 0,
            footer,
            footer_pos,
        })
    }

    async fn set_pos(&mut self, pos: usize) -> Result<()> {
        if self.pos - self.buf_use == pos {
            self.pos = pos;
            self.buf_use = 0;
            return Ok(());
        }
        if pos as u64 > self.file.len() {
            return Err(Error::InvalidParam {
                reason: format!("pos ({}) is too large (> {})", pos, self.file.len()),
            });
        }

        match self.pos.cmp(&pos) {
            Ordering::Greater => {
                let size = self.pos - pos;
                self.pos = pos;
                match self.buf_use.cmp(&size) {
                    Ordering::Greater => {
                        self.buf_use -= size;
                        Ok(())
                    }
                    _ => self.load_buf().await,
                }
            }
            Ordering::Less => {
                let size = pos - self.pos;
                self.pos = pos;
                match (self.buf_len - self.buf_use).cmp(&size) {
                    Ordering::Greater => {
                        self.buf_use += size;
                        Ok(())
                    }
                    _ => self.load_buf().await,
                }
            }
            Ordering::Equal => Ok(()),
        }
    }

    async fn find_record_header(&mut self) -> Result<(usize, &[u8])> {
        loop {
            let origin_pos = self.pos;
            let (_, magic_number_sli) = self.read_buf(RECORD_MAGIC_NUMBER_LEN).await?;
            let magic_number = decode_be_u32(magic_number_sli);
            if magic_number == RECORD_MAGIC_NUMBER {
                self.set_pos(origin_pos).await?;
                return self.read_buf(RECORD_HEADER_LEN).await;
            } else {
                self.set_pos(origin_pos + 1).await?;
            }
        }
    }

    /// Returns Ok(record), it means EOF when returns Err.
    #[async_recursion]
    pub async fn read_record(&mut self) -> Result<Record> {
        if self.pos as u64 >= self.footer_pos {
            return Err(Error::Eof);
        }
        let (origin_pos, header) = self.find_record_header().await?;

        let mut p = RECORD_MAGIC_NUMBER_LEN;
        let data_version = header[p];
        p += RECORD_DATA_VERSION_LEN;
        let data_type = header[p];
        p += RECORD_DATA_TYPE_LEN;
        let data_size = decode_be_u32(&header[p..p + RECORD_DATA_SIZE_LEN]);
        p += RECORD_DATA_SIZE_LEN;
        let data_crc = decode_be_u32(&header[p..p + RECORD_CRC32_NUMBER_LEN]);

        // A hasher for record header and record data.
        let mut hasher = crc32fast::Hasher::new();
        // Hash record header (Exclude magic number and crc32 number)
        hasher
            .update(&header[RECORD_MAGIC_NUMBER_LEN..RECORD_HEADER_LEN - RECORD_CRC32_NUMBER_LEN]);

        // TODO: Check if data_size is too large.
        let data = match self.read_buf(data_size as usize).await {
            Ok((_, d)) => d.to_vec(),
            Err(e) => {
                trace::error!(
                    "Failed to read record data at {origin_pos} for {data_size} bytes: {e}",
                );
                self.set_pos(origin_pos + 1).await?;
                return self.read_record().await;
            }
        };

        // Hash record data
        hasher.update(&data);
        let calc_crc = hasher.finalize();
        // check crc32 number
        if calc_crc != data_crc {
            trace::error!("Data crc check failed at {origin_pos} for {data_size} bytes",);
            self.set_pos(origin_pos + 1).await?;
            return self.read_record().await;
        }

        Ok(Record {
            data_type,
            data_version,
            data,
            pos: origin_pos.to_u64().expect("too big for u64"),
        })
    }

    /// Returns footer position and footer data.
    pub async fn read_footer(path: impl AsRef<Path>) -> Result<(u64, Vec<u8>)> {
        let path = path.as_ref();
        let file = file_manager::open_file(&path).await?;
        if file.len() < (FILE_MAGIC_NUMBER_LEN + FILE_FOOTER_LEN) as u64 {
            return Err(Error::NoFooter);
        }

        let len = file_crc_source_len(file.len(), FILE_FOOTER_LEN);
        let buf = file
            .read_at(FILE_MAGIC_NUMBER_LEN as u64, len)
            .await
            .map_err(|e| Error::ReadFile {
                path: path.to_path_buf(),
                source: e,
            })?;
        let crc = crc32fast::hash(&buf);

        // Read footer
        let footer_pos = file.len() - FILE_FOOTER_LEN as u64;
        let footer = file
            .read_at(footer_pos, FILE_FOOTER_LEN)
            .await
            .map_err(|e| Error::ReadFile {
                path: path.to_path_buf(),
                source: e,
            })?;
        // Check file crc
        let footer_crc = decode_be_u32(
            &footer[FILE_FOOTER_MAGIC_NUMBER_LEN
                ..FILE_FOOTER_MAGIC_NUMBER_LEN + FILE_FOOTER_CRC32_NUMBER_LEN],
        );

        // If crc doesn't match, this file may not contain a footer.
        if crc != footer_crc {
            Err(Error::NoFooter)
        } else {
            Ok((footer_pos, footer))
        }
    }

    /// Returns a clone of file footer.
    pub fn footer(&self) -> Option<Vec<u8>> {
        self.footer.clone()
    }

    async fn load_buf(&mut self) -> Result<()> {
        if (self.pos + self.buf_len) as u64 > self.footer_pos {
            self.buf
                .truncate((self.footer_pos - self.pos as u64) as usize);
        }
        trace::trace!(
            "Trying load buf at {} for {} bytes",
            self.pos,
            self.buf.len()
        );
        let buf = self
            .file
            .read_at(self.pos as u64, self.buf.len())
            .await
            .map_err(|e| Error::ReadFile {
                path: self.path.clone(),
                source: e,
            })?;
        self.buf_len = buf.len();
        self.buf = buf;
        self.buf_use = 0;
        Ok(())
    }

    /// Returns a position where to read a slice and that slice.
    async fn read_buf(&mut self, size: usize) -> Result<(usize, &[u8])> {
        if self.buf_len - self.buf_use < size {
            self.load_buf().await?;
            // TODO: If size may be greater than READER_BUF_SIZE,
            // this would be a wrong logic.
            if self.buf_len - self.buf_use < size {
                return Err(Error::Eof);
            }
        }

        let origin_pos = self.pos;
        let buf_sli = &self.buf[self.buf_use..self.buf_use + size];
        self.pos += size;
        self.buf_use += size;
        Ok((origin_pos, buf_sli))
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn len(&self) -> u64 {
        self.file.len()
    }

    pub fn is_empty(&self) -> bool {
        self.file.is_empty()
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::path::Path;

    use super::Reader;
    use crate::byte_utils::decode_be_u32;
    use crate::error::{Error, Result};
    use crate::file_system::file::IFile;
    use crate::record_file::{
        Record, RECORD_CRC32_NUMBER_LEN, RECORD_DATA_SIZE_LEN, RECORD_DATA_TYPE_LEN,
        RECORD_DATA_VERSION_LEN, RECORD_HEADER_LEN, RECORD_MAGIC_NUMBER, RECORD_MAGIC_NUMBER_LEN,
    };

    impl Reader {
        pub(crate) async fn read_at(&mut self, pos: usize) -> Result<Record> {
            let record_header_buf = match self.file.read_at(pos as u64, RECORD_HEADER_LEN).await {
                Ok(buf) => buf,
                Err(e) => {
                    return Err(Error::ReadFile {
                        path: self.path.clone(),
                        source: e,
                    });
                }
            };
            if record_header_buf.len() != RECORD_HEADER_LEN {
                return Err(Error::RecordFileIo {
                    reason: format!("invalid record header (pos is {})", pos),
                });
            }
            let mut p = 0_usize;
            let magic_number = decode_be_u32(&record_header_buf[p..p + RECORD_MAGIC_NUMBER_LEN]);
            if magic_number != RECORD_MAGIC_NUMBER {
                return Err(Error::RecordFileIo {
                    reason: format!("invalid magic number (pos is {})", pos),
                });
            }
            p += RECORD_MAGIC_NUMBER_LEN;
            let data_version = record_header_buf[p];
            p += RECORD_DATA_VERSION_LEN;
            let data_type = record_header_buf[p];
            p += RECORD_DATA_TYPE_LEN;
            let data_size = decode_be_u32(&record_header_buf[p..p + RECORD_DATA_SIZE_LEN]);
            p += RECORD_DATA_SIZE_LEN;
            let _data_crc = decode_be_u32(&record_header_buf[p..p + RECORD_CRC32_NUMBER_LEN]);
            p += RECORD_CRC32_NUMBER_LEN;

            // TODO: Reuse data vector.
            // TODO: Check if data_size is too large.
            let data = match self
                .file
                .read_at((pos + p) as u64, data_size as usize)
                .await
            {
                Ok(len) => len,
                Err(e) => {
                    return Err(Error::ReadFile {
                        path: self.path.clone(),
                        source: e,
                    });
                }
            };
            if data.len() != data_size as usize {
                return Err(Error::RecordFileIo {
                    reason: format!(
                        "data truncated to {} (pos is {}, len is {})",
                        data.len(),
                        pos,
                        data_size
                    ),
                });
            }

            Ok(Record {
                pos: pos as u64,
                data_type,
                data_version,
                data,
            })
        }
    }

    pub(crate) async fn test_reader_read_one(path: impl AsRef<Path>, pos: usize, data: &[u8]) {
        let mut r = Reader::open(path).await.unwrap();
        let record = r.read_at(pos).await.unwrap();
        assert_eq!(record.data, data);
    }

    pub(crate) async fn test_reader(path: impl AsRef<Path>, data: &[Vec<u8>]) {
        let mut r = Reader::open(path).await.unwrap();

        for d in data {
            let record = match r.read_record().await {
                Ok(r) => r,
                Err(Error::Eof) => break,
                Err(e) => panic!("Error reading record: {:?}", e),
            };
            assert_eq!(record.data, *d);
        }
    }
}
