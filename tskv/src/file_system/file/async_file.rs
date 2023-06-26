use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, IoSlice, Result};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::task::spawn_blocking;

use super::os;
use crate::file_system::file::IFile;

#[derive(Debug)]
struct RawFile {
    open_path: Arc<PathBuf>,
    file: Arc<File>,

    #[cfg(feature = "io_uring")]
    rio: Arc<rio::Rio>,
}

impl RawFile {
    #[cfg(not(feature = "io_uring"))]
    fn new(path: Arc<PathBuf>, file: Arc<File>) -> Self {
        Self {
            open_path: path,
            file,
        }
    }

    #[cfg(feature = "io_uring")]
    fn new(path: Arc<PathBuf>, file: Arc<File>, rio: Arc<Rio>) {
        Self {
            open_path: path,
            file,
            rio,
        }
    }

    fn file_size(&self) -> Result<u64> {
        os::file_size(os::fd(self.file.as_ref()))
    }

    async fn pwrite(&self, pos: u64, data: &[u8]) -> Result<usize> {
        #[cfg(feature = "io_uring")]
        {
            let completion = self.rio.write_at(&self.std_file, &data, pos).await?;
            Ok(data.len())
        }

        #[cfg(not(feature = "io_uring"))]
        {
            let len = data.len();
            let ptr = data.as_ptr() as u64;
            let fd = os::fd(self.file.as_ref());
            asyncify(move || os::pwrite(fd, pos, len, ptr)).await
        }
    }

    async fn pread(&self, pos: u64, data: &mut [u8]) -> Result<usize> {
        #[cfg(feature = "io_uring")]
        {
            let completion = self.rio.read_at(&self.std_file, &data, pos).await?;
            Ok(data.len())
        }
        #[cfg(not(feature = "io_uring"))]
        {
            let len = data.len();
            let ptr = data.as_ptr() as u64;
            let fd = os::fd(self.file.as_ref());
            let len = asyncify(move || os::pread(fd, pos, len, ptr)).await?;
            Ok(len)
        }
    }

    async fn sync_data(&self) -> Result<()> {
        #[cfg(feature = "io_uring")]
        {
            self.rio.fsync(&self.file).await?;
            Ok(())
        }
        #[cfg(not(feature = "io_uring"))]
        {
            let file = self.file.clone();
            asyncify(move || file.sync_data()).await
        }
    }

    async fn truncate(&self, size: u64) -> Result<()> {
        #[cfg(feature = "io_uring")]
        {
            let file = self.file.clone();
            asyncify(move || file.set_len(size)).await
        }
        #[cfg(not(feature = "io_uring"))]
        {
            let file = self.file.clone();
            asyncify(move || file.set_len(size)).await
        }
    }
}

pub(crate) async fn asyncify<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    match spawn_blocking(f).await {
        Ok(res) => res,
        Err(e) => Err(Error::new(
            ErrorKind::Other,
            format!("background task failed: {:?}", e),
        )),
    }
}

pub struct FsRuntime {
    #[cfg(feature = "io_uring")]
    rio: Arc<Rio>,
}

unsafe impl Send for FsRuntime {}

impl FsRuntime {
    pub fn new_runtime() -> Self {
        #[cfg(feature = "io_uring")]
        {
            let rio = Arc::new(rio::new().unwrap());
            FsRuntime { rio }
        }
        #[cfg(not(feature = "io_uring"))]
        {
            FsRuntime {}
        }
    }
}

pub struct AsyncFile {
    inner: RawFile,
    ctx: Arc<FsRuntime>,
    size: u64,
}

#[async_trait::async_trait]
impl IFile for AsyncFile {
    async fn write_vec<'a>(&self, pos: u64, bufs: &'a mut [IoSlice<'a>]) -> Result<usize> {
        let mut p = pos;
        for buf in bufs {
            p += self.write_at(p, buf.deref()).await? as u64;
        }
        Ok((p - pos) as usize)
    }

    async fn write_at(&self, pos: u64, data: &[u8]) -> Result<usize> {
        self.inner.pwrite(pos, data).await
    }

    async fn read_at(&self, pos: u64, data: &mut [u8]) -> Result<usize> {
        self.inner.pread(pos, data).await
    }

    async fn sync_data(&self) -> Result<()> {
        self.inner.sync_data().await
    }

    async fn truncate(&self, size: u64) -> Result<()> {
        self.inner.truncate(size).await
    }

    fn len(&self) -> u64 {
        self.size
    }

    fn is_empty(&self) -> bool {
        self.size == 0
    }
}

impl AsyncFile {
    pub async fn open<P: AsRef<Path>>(
        path: P,
        ctx: Arc<FsRuntime>,
        options: OpenOptions,
    ) -> Result<AsyncFile> {
        let path = Arc::new(path.as_ref().to_path_buf());
        let path_to_open = path.clone();
        let file = asyncify(move || options.open(path_to_open.as_ref())).await?;
        #[cfg(feature = "io_uring")]
        {
            let inner = RawFile::new(path, Arc::new(file), ctx.rio.clone());
            let size = inner.file_size()?;
            Ok(AsyncFile { inner, ctx, size })
        }
        #[cfg(not(feature = "io_uring"))]
        {
            let inner = RawFile::new(path, Arc::new(file));
            let size = inner.file_size()?;
            Ok(AsyncFile { inner, ctx, size })
        }
    }

    pub fn fd(&self) -> usize {
        os::fd(&self.inner.file)
    }

    pub fn open_path(&self) -> &PathBuf {
        &self.inner.open_path
    }
}
