use std::env;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinHandle;
use tskv::file_system::file_manager;
use tskv::index::binlog::BinlogReader;
use tskv::record_file;
use tskv::tsm::TsmReader;

enum CheckingObject {
    Storage,
    Wal,
}

const MAX_CONCURRENT_VNODE_SCAN_TASKS: usize = 4;

#[tokio::main]
async fn main() {
    let mut args = env::args().peekable();
    let _ = args.next();
    if let Some(arg) = args.next() {
        if arg.as_str() == "check" {
            if let Some(arg) = args.next() {
                let checking_object = match arg.as_str() {
                    "storage" => CheckingObject::Storage,
                    "wal" => CheckingObject::Wal,
                    _ => {
                        eprintln!("[E] Unknown arguments: check {arg}");
                        exit(1)
                    }
                };
                if let Some(path) = args.next() {
                    match std::fs::canonicalize(&path) {
                        Ok(p) => {
                            if !p.is_dir() {
                                eprintln!("[E] Target path is not a directory");
                                exit(1);
                            }
                            check(checking_object, &p).await;
                            return;
                        }
                        Err(e) => {
                            eprintln!("[E] Cannot detect target directory: {e}");
                            exit(1)
                        }
                    }
                }
            }
        }
    }
    eprintln!(
        "[E] Unknown arguments: {}",
        env::args().collect::<Vec<String>>().join(" ")
    );
    exit(1);
}

async fn check<P: AsRef<Path>>(checking_object: CheckingObject, path: P) {
    let path = path.as_ref();
    match checking_object {
        CheckingObject::Storage => check_storage_dir(path).await,
        CheckingObject::Wal => check_wal_dir(path).await,
    }
}

#[derive(Clone, Default)]
pub struct StorageContext {
    pub summary_num: Arc<AtomicU64>,
    pub summary_bytes: Arc<AtomicU64>,
    pub wal_num: Arc<AtomicU64>,
    pub wal_bytes: Arc<AtomicU64>,
    pub tsm_num: Arc<AtomicU64>,
    pub tsm_bytes: Arc<AtomicU64>,
    pub delta_num: Arc<AtomicU64>,
    pub delta_bytes: Arc<AtomicU64>,
    pub index_num: Arc<AtomicU64>,
    pub index_bytes: Arc<AtomicU64>,

    pub errors: Arc<Mutex<Vec<String>>>,
}

impl StorageContext {
    pub fn fetch_add_file_len<P: AsRef<Path>>(path: P, len: Arc<AtomicU64>) -> std::io::Result<()> {
        let l = std::fs::metadata(path).map(|m| m.len())?;
        len.fetch_add(l, Ordering::SeqCst);
        Ok(())
    }

    pub async fn print(&self) {
        println!(
            "# Summary\n- num: {}\n- size: {}\n",
            self.summary_num.load(Ordering::SeqCst),
            self.summary_bytes.load(Ordering::SeqCst),
        );
        println!(
            "# WAL\n- num: {}\n- size: {}\n",
            self.wal_num.load(Ordering::SeqCst),
            self.wal_bytes.load(Ordering::SeqCst),
        );
        println!(
            "# TSM\n- num: {}\n- size: {}\n",
            self.tsm_num.load(Ordering::SeqCst),
            self.tsm_bytes.load(Ordering::SeqCst),
        );
        println!(
            "# Delta\n- num: {}\n- size: {}\n",
            self.delta_num.load(Ordering::SeqCst),
            self.delta_bytes.load(Ordering::SeqCst),
        );
        println!(
            "# Index\n- num: {}\n- size: {}\n",
            self.index_num.load(Ordering::SeqCst),
            self.index_bytes.load(Ordering::SeqCst),
        );
        {
            let errors = self.errors.lock().await;
            println!("# Errors");
            if errors.is_empty() {
                println!("None");
            } else {
                for err in errors.iter() {
                    println!("- {err}");
                }
            }
        }
    }
}

// struct DirectoryChecker {
//     pub dir: PathBuf,
//     pub file_type: String,
//     pub ctx: Arc<StorageContext>,

//     pub filter_fn: Box<dyn Fn(Option<&OsStr>) -> bool>,
//     pub dir_entry_tx: mpsc::Sender<PathBuf>,
//     pub handler: Pin<Box<dyn Future<Output = ()> + Send>>,
// }

// impl DirectoryChecker {
//     pub async fn handle(self) {
//         let DirectoryChecker {
//             dir,
//             file_type,
//             ctx,
//             filter_fn,
//             dir_entry_tx,
//             handler,
//         } = self;

//         let jh = tokio::spawn(handler);
//         println!("Into {file_type} dir: {dir:?}");
//         match dir.read_dir() {
//             Ok(read_dir) => {
//                 for read_dir_result in read_dir {
//                     match read_dir_result {
//                         Ok(dir_entry) => {
//                             let p = dir_entry.path();
//                             if filter_fn(p.extension()) {
//                                 if dir_entry_tx.send((p)).await.is_err() {
//                                     eprintln!("[E] Handler of {file_type} was cancelled.");
//                                     break;
//                                 }
//                             }
//                         }
//                         Err(e) => {}
//                     }
//                 }
//             }
//             Err(e) => {
//                 eprintln!("[E] Failed to open '{dir:?}': {e}");
//                 ctx.print().await;
//                 exit(1);
//             }
//         }

//         drop(dir_entry_tx);
//         let _ = jh.await;
//     }
// }

async fn check_storage_dir(path: &Path) {
    if !path.exists() {
        eprintln!("[W] Storage path '{path:?}' not exists");
        return;
    }

    let ctx = Arc::new(StorageContext::default());

    // Check summary file.
    let summary_path = path.join("summary/summary-000000");
    check_summary_file(&summary_path, ctx.clone(), 0).await;

    // Check databases files: .delta, .tsm, .binlog
    let databases_dir = path.join("data");
    println!("Into databases dir: {databases_dir:?}");
    const DB_INDENT: usize = 1;
    const VNODE_INDENT: usize = 2;

    match databases_dir.read_dir() {
        Ok(read_databases_dir) => {
            for read_databases_result in read_databases_dir {
                match read_databases_result {
                    Ok(database_dir) => {
                        let database_dir = database_dir.path();
                        println!("{:DB_INDENT$}Into database dir: {database_dir:?}", "");

                        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_VNODE_SCAN_TASKS));
                        let mut tasks = vec![];
                        match database_dir.read_dir() {
                            Ok(read_vnode_dir) => {
                                for read_vnodes_result in read_vnode_dir {
                                    match read_vnodes_result {
                                        Ok(vnode_dir) => {
                                            let vnode_dir = vnode_dir.path();
                                            match check_vnode_dir(
                                                &vnode_dir,
                                                ctx.clone(),
                                                semaphore.clone(),
                                                VNODE_INDENT,
                                            )
                                            .await
                                            {
                                                Ok(jh) => tasks.push(jh),
                                                Err(_) => {
                                                    ctx.print().await;
                                                    exit(1);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!("{:VNODE_INDENT$}[E] Failed to read '{database_dir:?}: {e}", "");
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!(
                                    "{:DB_INDENT$}[E] Failed to open '{database_dir:?}: {e}",
                                    ""
                                );
                                ctx.print().await;
                                exit(1);
                            }
                        }
                        for t in tasks {
                            let _ = t.await;
                        }
                    }
                    Err(e) => {
                        eprintln!("[E] Failed to read '{databases_dir:?}: {e}");
                        ctx.print().await;
                        exit(1);
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("[E] Failed to open '{databases_dir:?}': {e}");
            ctx.print().await;
            exit(1);
        }
    }

    ctx.print().await;
}

async fn check_summary_file(path: &PathBuf, ctx: Arc<StorageContext>, indent: usize) {
    println!("{:indent$}Checking summary file '{path:?}", "");
    let new_indent = indent + 1;
    match record_file::Reader::open(path).await {
        Ok(mut r) => {
            ctx.summary_num.fetch_add(1, Ordering::SeqCst);
            if let Err(e) = StorageContext::fetch_add_file_len(path, ctx.summary_bytes.clone()) {
                eprintln!(
                    "{:new_indent$}[W] Cannot get metadata of summary file '{path:?}': {e}",
                    ""
                );
            }

            let mut next_pos = 0_u64;
            loop {
                match r.read_record().await {
                    Ok(r) => {
                        next_pos = r.pos + r.data.len() as u64;
                    }
                    Err(tskv::Error::Eof) => break,
                    Err(e) => {
                        eprintln!(
                            "{:new_indent$}[E] Invalid summary file '{path:?}: [{next_pos}..), {e}",
                            ""
                        );
                        ctx.errors
                            .lock()
                            .await
                            .push(format!("'{path:?}': [{next_pos}..), {e}"));
                    }
                }
            }
        }
        Err(e) => {
            eprintln!(
                "{:new_indent$}[E] Failed to open summary file '{path:?}: {e}",
                ""
            );
            ctx.errors.lock().await.push(format!("'{path:?}': {e}"));
        }
    }
}

async fn check_vnode_dir(
    path: &PathBuf,
    ctx: Arc<StorageContext>,
    semaphore: Arc<Semaphore>,
    indent: usize,
) -> Result<JoinHandle<()>, ()> {
    println!("{:indent$}Into vnode dir: {path:?}", "");
    let new_indent = indent + 1;
    let semaphore_permit = match semaphore.acquire_owned().await {
        Ok(s) => s,
        Err(_) => {
            eprintln!(
                "{:new_indent$}[E] Failed to acquire semaphore because it was closed",
                ""
            );
            return Err(());
        }
    };
    let delta_path = path.join("delta");
    let tsm_path = path.join("tsm");
    let index_path = path.join("index");
    Ok(tokio::spawn(async move {
        let _ = check_tsm_dir(&delta_path, ctx.clone(), new_indent).await;
        let _ = check_tsm_dir(&tsm_path, ctx.clone(), new_indent).await;
        let _ = check_index_dir(&index_path, ctx.clone(), new_indent).await;
        drop(semaphore_permit);
    }))
}

async fn check_tsm_dir(path: &PathBuf, ctx: Arc<StorageContext>, indent: usize) -> Result<(), ()> {
    if !path.exists() {
        return Ok(());
    }
    println!("{:indent$}Into tsm dir '{path:?}", "");
    let new_indent = indent + 1;
    match path.read_dir() {
        Ok(read_tsm_dir) => {
            for read_tsm_result in read_tsm_dir {
                match read_tsm_result {
                    Ok(tsm_file) => {
                        let tsm_file = tsm_file.path();
                        if let Some(ext) = tsm_file.extension() {
                            if ext == "tsm" {
                                check_tsm_file(&tsm_file, ctx.clone(), new_indent).await;
                            } else if ext == "delta" {
                                check_delta_file(&tsm_file, ctx.clone(), new_indent).await;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("{:new_indent$}[E] Failed to read '{path:?}: {e}", "");
                        return Err(());
                    }
                }
            }
            Ok(())
        }
        Err(e) => {
            eprintln!("{:new_indent$}[E] Failed to open '{path:?}': {e}", "");
            Err(())
        }
    }
}

async fn check_tsm_file(path: &PathBuf, ctx: Arc<StorageContext>, indent: usize) {
    println!("{:indent$}Checking tsm file '{path:?}", "");
    let new_indent = indent + 1;
    match TsmReader::open(path).await {
        Ok(_t) => {
            ctx.tsm_num.fetch_add(1, Ordering::SeqCst);
            if let Err(e) = StorageContext::fetch_add_file_len(path, ctx.tsm_bytes.clone()) {
                eprintln!(
                    "{:new_indent$}[W] Cannot get metadata of tsm file '{path:?}': {e}",
                    ""
                );
            }
        }
        Err(e) => {
            println!("{:new_indent$}Invalid tsm file '{path:?}: {e}", "");
            ctx.errors.lock().await.push(format!("'{path:?}': {e}"));
        }
    }
}

async fn check_delta_file(path: &PathBuf, ctx: Arc<StorageContext>, indent: usize) {
    println!("{:indent$}Checking delta file '{path:?}", "");
    let new_indent = indent + 1;
    match TsmReader::open(path).await {
        Ok(_t) => {
            ctx.delta_num.fetch_add(1, Ordering::SeqCst);
            if let Err(e) = StorageContext::fetch_add_file_len(path, ctx.delta_bytes.clone()) {
                eprintln!(
                    "{:new_indent$}[W] Cannot get metadata of delta file '{path:?}': {e}",
                    ""
                );
            }
        }
        Err(e) => {
            eprintln!("{:new_indent$}[E] Invalid delta file '{path:?}: {e}", "");
            ctx.errors.lock().await.push(format!("'{path:?}': {e}"));
        }
    }
}

async fn check_index_dir(
    path: &PathBuf,
    ctx: Arc<StorageContext>,
    indent: usize,
) -> Result<(), ()> {
    if !path.exists() {
        return Err(());
    }
    println!("{:indent$}Into index dir '{path:?}", "");
    let new_indent = indent + 1;
    match path.read_dir() {
        Ok(read_index_dir) => {
            for read_index_result in read_index_dir {
                match read_index_result {
                    Ok(index_file) => {
                        let index_file = index_file.path();
                        if let Some(ext) = index_file.extension() {
                            if ext == "binlog" {
                                check_index_binlog_file(&index_file, ctx.clone(), new_indent).await;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("{:new_indent$}[E] Failed to read '{path:?}: {e}", "");
                        ctx.print().await;
                        return Err(());
                    }
                }
            }
            Ok(())
        }
        Err(e) => {
            eprintln!("{:new_indent$}[E] Failed to open '{path:?}': {e}", "");
            Err(())
        }
    }
}

async fn check_index_binlog_file(path: &PathBuf, ctx: Arc<StorageContext>, indent: usize) {
    println!("{:indent$}Checking index-binlog file '{path:?}", "");
    let new_indent = indent + 1;
    match file_manager::open_file(path).await {
        Ok(f) => match BinlogReader::new(0, f.into()).await {
            Ok(mut t) => {
                ctx.index_num.fetch_add(1, Ordering::SeqCst);
                if let Err(e) = StorageContext::fetch_add_file_len(path, ctx.index_bytes.clone()) {
                    eprintln!("{:new_indent$}[W] Cannot get metadata of index-binlog file '{path:?}': {e}", "");
                }

                loop {
                    match t.next_block().await {
                        Ok(Some(_b)) => {}
                        Ok(None) => {
                            break;
                        }
                        Err(e) => {
                            eprintln!(
                                "{:new_indent$}[E] Invalid index-binlog file block: '{path:?}: {e}",
                                ""
                            );
                            ctx.errors.lock().await.push(format!("'{path:?}': {e}"));
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "{:new_indent$}[E] Invalid index-binlog file '{path:?}: {e}",
                    ""
                );
                ctx.errors.lock().await.push(format!("'{path:?}': {e}"));
            }
        },
        Err(e) => {
            eprintln!(
                "{:new_indent$}[W] Cannot open index-binlog file '{path:?}': {e}",
                ""
            );
            ctx.errors.lock().await.push(format!("'{path:?}': {e}"));
        }
    }
}

async fn check_wal_dir(path: &Path) {
    if !path.exists() {
        eprintln!("[W] WAL path '{path:?}' not exists");
        return;
    }

    let ctx = Arc::new(StorageContext::default());

    match path.read_dir() {
        Ok(read_wal_dir) => {
            for read_wal_result in read_wal_dir {
                match read_wal_result {
                    Ok(wal_file) => {
                        let wal_file = wal_file.path();
                        if let Some(ext) = wal_file.extension() {
                            if ext == "wal" {
                                check_wal_file(&wal_file, ctx.clone(), 1).await;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!(" [E] Failed to read '{path:?}: {e}");
                        ctx.print().await;
                        exit(1)
                    }
                }
            }
        }
        Err(e) => {
            eprintln!(" [E] Failed to open '{path:?}': {e}");
        }
    }
}

async fn check_wal_file(path: &Path, ctx: Arc<StorageContext>, indent: usize) {
    println!("{:indent$}Checking wal file '{path:?}", "");
    let new_indent = indent + 1;
    match record_file::Reader::open(path).await {
        Ok(mut r) => {
            ctx.wal_num.fetch_add(1, Ordering::SeqCst);
            if let Err(e) = StorageContext::fetch_add_file_len(path, ctx.wal_bytes.clone()) {
                eprintln!(
                    "{:new_indent$}[W] Cannot get metadata of wal file '{path:?}': {e}",
                    ""
                );
            }

            let mut next_pos = 0_u64;
            loop {
                match r.read_record().await {
                    Ok(r) => {
                        next_pos = r.pos + r.data.len() as u64;
                    }
                    Err(tskv::Error::Eof) => break,
                    Err(e) => {
                        eprintln!(
                            "{:new_indent$}[E] Invalid wal file '{path:?}: [{next_pos}..), {e}",
                            ""
                        );
                        ctx.errors
                            .lock()
                            .await
                            .push(format!("'{path:?}': [{next_pos}..), {e}"));
                    }
                }
            }
        }
        Err(e) => {
            eprintln!(
                "{:new_indent$}[E] Failed to open wal file '{path:?}: {e}",
                ""
            );
            ctx.errors.lock().await.push(format!("'{path:?}': {e}"));
        }
    }

    ctx.print().await;
}
