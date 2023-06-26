use std::io::{IoSlice, SeekFrom};
use std::path::{Path, PathBuf};

use num_traits::ToPrimitive;
use snafu::ResultExt;

use super::{
    file_crc_source_len, Reader, RecordDataType, RecordFileError, RecordFileResult,
    FILE_FOOTER_LEN, FILE_MAGIC_NUMBER, FILE_MAGIC_NUMBER_LEN, RECORD_MAGIC_NUMBER,
};
use crate::file_system::file::cursor::FileCursor;
use crate::file_system::file::IFile;
use crate::file_system::file_manager;

pub struct Writer {
    cursor: FileCursor,
    footer: Option<[u8; FILE_FOOTER_LEN]>,
    file_size: u64,
}

impl Writer {
    pub async fn open(path: impl AsRef<Path>, data_type: RecordDataType) -> RecordFileResult<Self> {
        let path = path.as_ref();
        trace::trace!(
            "Open record file writer (type {data_type}) '{}'",
            path.display()
        );

        let mut cursor: FileCursor = file_manager::open_create_file(path)
            .await
            .with_context(|_| super::CreateFileSnafu { path: path.clone() })?
            .into();
        let mut file_size = 0_u64;
        let footer = if cursor.is_empty() {
            // For new file, write file magic number, next write is at 4.
            file_size += cursor
                .write(&FILE_MAGIC_NUMBER.to_be_bytes())
                .await
                .with_context(|_| super::WriteFileSnafu { path: path.clone() })?
                as u64;
            // Get none as footer data.
            None
        } else {
            let footer_data = match Reader::read_footer(&path).await {
                Ok((_, f)) => Some(f),
                Err(RecordFileError::NoFooter) => None,
                Err(e) => {
                    trace::error!(
                        "Failed to read footer of record_file '{}': {e}",
                        path.display()
                    );
                    return Err(e);
                }
            };

            // For writed file, skip to footer position, next write is at (file.len() - footer_len).
            // Note that footer_len may be zero.
            let seek_pos_end = footer_data.map(|f| f.len()).unwrap_or(0);
            // TODO: truncate this file using seek_pos_end.
            cursor
                .seek(SeekFrom::End(-(seek_pos_end as i64)))
                .with_context(|_| super::SeekFileSnafu { path: path.clone() })?;

            footer_data
        };
        Ok(Writer {
            cursor,
            footer,
            file_size,
        })
    }

    // Writes record data and returns the written data size.
    pub async fn write_record(
        &mut self,
        data_version: u8,
        data_type: u8,
        data: &[&[u8]],
    ) -> RecordFileResult<usize> {
        let data_len: usize = data.iter().map(|d| (*d).len()).sum();
        let data_len = match data_len.to_u32() {
            Some(v) => v,
            None => {
                return Err(RecordFileError::LenOverFlow {
                    path: self.path().clone(),
                    data_type,
                    data_len: data_len as u64,
                });
            }
        };

        // Build record header and hash.
        let mut hasher = crc32fast::Hasher::new();
        let data_meta = [data_version, data_type];
        hasher.update(&data_meta);
        let data_len = data_len.to_be_bytes();
        hasher.update(&data_len);
        for d in data.iter() {
            hasher.update(d);
        }
        let data_crc = hasher.finalize().to_be_bytes();

        let magic_number = RECORD_MAGIC_NUMBER.to_be_bytes();
        let mut write_buf: Vec<IoSlice> = Vec::with_capacity(6);
        write_buf.push(IoSlice::new(&magic_number));
        write_buf.push(IoSlice::new(&data_meta));
        write_buf.push(IoSlice::new(&data_len));
        write_buf.push(IoSlice::new(&data_crc));
        for d in data {
            write_buf.push(IoSlice::new(d));
        }

        // Write record header and record data.
        let written_size = self.cursor.write_vec(&mut write_buf).await.map_err(|e| {
            RecordFileError::WriteFile {
                path: self.path().clone(),
                source: e,
            }
        })?;
        self.file_size += written_size as u64;
        Ok(written_size)
    }

    pub async fn write_footer(
        &mut self,
        mut footer: [u8; FILE_FOOTER_LEN],
    ) -> RecordFileResult<usize> {
        self.sync().await?;

        // Get file crc
        let mut buf = vec![0_u8; file_crc_source_len(self.file_size(), 0_usize)];
        self.cursor
            .read_at(FILE_MAGIC_NUMBER_LEN as u64, &mut buf)
            .await
            .map_err(|e| RecordFileError::ReadFile {
                path: self.path().clone(),
                source: e,
            })?;
        let crc = crc32fast::hash(&buf);

        // Set file crc to footer
        footer[4..8].copy_from_slice(&crc.to_be_bytes());
        self.footer = Some(footer);

        self.cursor
            .write(&footer)
            .await
            .map_err(|e| RecordFileError::WriteFile {
                path: self.path().clone(),
                source: e,
            })
    }

    pub fn footer(&self) -> Option<[u8; FILE_FOOTER_LEN]> {
        self.footer
    }

    pub async fn sync(&self) -> RecordFileResult<()> {
        self.cursor
            .sync_data()
            .await
            .with_context(|_| super::SyncFileSnafu {
                path: self.path().clone(),
            })
    }

    pub async fn close(&mut self) -> RecordFileResult<()> {
        self.sync().await
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn path(&self) -> &PathBuf {
        self.cursor.open_path()
    }
}

#[cfg(test)]
mod test {

    use serial_test::serial;

    use super::Writer;
    use crate::file_system::file_manager;
    use crate::record_file::reader::test::{test_reader, test_reader_read_one};
    use crate::record_file::{RecordDataType, RecordFileResult, FILE_FOOTER_LEN};

    #[tokio::test]
    #[serial]
    async fn test_writer() -> RecordFileResult<()> {
        let path = "/tmp/test/record_file/1/test.log";
        if file_manager::try_exists(path) {
            std::fs::remove_file(path).unwrap();
        }
        let mut w = Writer::open(&path, RecordDataType::Summary).await.unwrap();
        let data_vec = vec![b"hello".to_vec(); 10];
        for d in data_vec.iter() {
            let size = w.write_record(1, 1, &[d.as_slice()]).await?;
            println!("Writed new record(1, 1, {:?}) {} bytes", d, size);
        }
        w.write_footer([0_u8; FILE_FOOTER_LEN]).await?;
        w.close().await?;

        println!("Testing read one record.");
        test_reader_read_one(&path, 23, b"hello").await;
        println!("Testing read all record.");
        test_reader(&path, &data_vec).await;
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_writer_truncated() -> RecordFileResult<()> {
        let path = "/tmp/test/record_file/2/test.log";
        if file_manager::try_exists(path) {
            std::fs::remove_file(path).unwrap();
        }
        let mut w = Writer::open(&path, RecordDataType::Summary).await.unwrap();
        let data_vec = vec![b"hello".to_vec(); 10];
        for d in data_vec.iter() {
            let size = w.write_record(1, 1, &[d.as_slice()]).await?;
            println!("Writed new record(1, 1, {:?}) {} bytes", d, size);
        }
        // Do not write footer.
        w.close().await?;

        println!("Testing read one record.");
        test_reader_read_one(&path, 23, b"hello").await;
        println!("Testing read all record.");
        test_reader(&path, &data_vec).await;
        Ok(())
    }
}
