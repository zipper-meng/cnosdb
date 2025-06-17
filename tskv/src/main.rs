use std::env;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::{Mutex, Semaphore};
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
struct StorageContext {
    summary_num: Arc<AtomicU64>,
    summary_bytes: Arc<AtomicU64>,
    wal_num: Arc<AtomicU64>,
    wal_bytes: Arc<AtomicU64>,
    tsm_num: Arc<AtomicU64>,
    tsm_bytes: Arc<AtomicU64>,
    delta_num: Arc<AtomicU64>,
    delta_bytes: Arc<AtomicU64>,
    index_num: Arc<AtomicU64>,
    index_bytes: Arc<AtomicU64>,

    errors: Arc<Mutex<Vec<String>>>,
}

impl StorageContext {
    fn fetch_add_file_len<P: AsRef<Path>>(path: P, len: Arc<AtomicU64>) -> std::io::Result<()> {
        let l = std::fs::metadata(path).map(|m| m.len())?;
        len.fetch_add(l, Ordering::SeqCst);
        Ok(())
    }

    async fn print(&self) {
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

async fn check_storage_dir(path: &Path) {
    if !path.exists() {
        eprintln!("[W] Storage path '{path:?}' not exists");
        return;
    }

    let ctx = Arc::new(StorageContext::default());

    // Check summary file.
    let summary_path = path.join("summary/summary-000000");
    check_summary_file(&summary_path, ctx.clone()).await;

    // Check databases files: .delta, .tsm, .binlog
    let databases_dir = path.join("data");
    println!("Into databases dir: {databases_dir:?}");
    match databases_dir.read_dir() {
        Ok(read_databases_dir) => {
            for read_databases_result in read_databases_dir {
                match read_databases_result {
                    Ok(database_dir) => {
                        let database_dir = database_dir.path();
                        println!(" Into database dir: {database_dir:?}");

                        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_VNODE_SCAN_TASKS));
                        let mut tasks = vec![];
                        match database_dir.read_dir() {
                            Ok(read_vnode_dir) => {
                                for read_vnodes_result in read_vnode_dir {
                                    match read_vnodes_result {
                                        Ok(vnode_dir) => {
                                            let vnode_dir = vnode_dir.path();
                                            println!("  Into vnode dir: {vnode_dir:?}");

                                            let semaphore_permit = match semaphore
                                                .clone()
                                                .acquire_owned()
                                                .await
                                            {
                                                Ok(s) => s,
                                                Err(_) => {
                                                    eprintln!("[E] Failed to acquire semaphore because it was closed");
                                                    exit(1)
                                                }
                                            };
                                            let delta_path = vnode_dir.join("delta");
                                            let tsm_path = vnode_dir.join("tsm");
                                            let index_path = vnode_dir.join("index");
                                            let ctx = ctx.clone();
                                            tasks.push(tokio::spawn(async move {
                                                let _ =
                                                    check_tsm_dir(&delta_path, ctx.clone()).await;
                                                let _ = check_tsm_dir(&tsm_path, ctx.clone()).await;
                                                let _ =
                                                    check_index_dir(&index_path, ctx.clone()).await;
                                                drop(semaphore_permit);
                                            }));
                                        }
                                        Err(e) => {
                                            eprintln!(" [E] Failed to read '{database_dir:?}: {e}");
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!(" [E] Failed to open '{database_dir:?}: {e}");
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

async fn check_summary_file(path: &PathBuf, ctx: Arc<StorageContext>) {
    println!("Checking summary file '{path:?} ...");

    match record_file::Reader::open(path).await {
        Ok(mut r) => {
            ctx.summary_num.fetch_add(1, Ordering::SeqCst);
            if let Err(e) = StorageContext::fetch_add_file_len(path, ctx.summary_bytes.clone()) {
                eprintln!(" [W] Cannot get metadata of summary file '{path:?}': {e}");
            }

            let mut next_pos = 0_u64;
            loop {
                match r.read_record().await {
                    Ok(r) => {
                        next_pos = r.pos + r.data.len() as u64;
                    }
                    Err(tskv::Error::Eof) => break,
                    Err(e) => {
                        eprintln!(" [E] Invalid summary file '{path:?}: [{next_pos}..), {e}");
                        ctx.errors
                            .lock()
                            .await
                            .push(format!("'{path:?}': [{next_pos}..), {e}"));
                    }
                }
            }
        }
        Err(e) => {
            eprintln!(" [E] Failed to open summary file '{path:?}: {e}");
            ctx.errors.lock().await.push(format!("'{path:?}': {e}"));
        }
    }
}

async fn check_tsm_dir(path: &PathBuf, ctx: Arc<StorageContext>) -> Result<(), ()> {
    if !path.exists() {
        return Ok(());
    }
    println!("   Into tsm dir '{path:?}");
    match path.read_dir() {
        Ok(read_tsm_dir) => {
            for read_tsm_result in read_tsm_dir {
                match read_tsm_result {
                    Ok(tsm_file) => {
                        let tsm_file = tsm_file.path();
                        if let Some(ext) = tsm_file.extension() {
                            if ext == "tsm" {
                                check_tsm_file(&tsm_file, ctx.clone()).await;
                            } else if ext == "delta" {
                                check_delta_file(&tsm_file, ctx.clone()).await;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("   [E] Failed to read '{path:?}: {e}");
                        return Err(());
                    }
                }
            }
            Ok(())
        }
        Err(e) => {
            eprintln!("[E] Failed to read '{path:?}': {e}");
            Err(())
        }
    }
}

async fn check_tsm_file(path: &PathBuf, ctx: Arc<StorageContext>) {
    println!("    Checking tsm file '{path:?} ...");
    match TsmReader::open(path).await {
        Ok(_t) => {
            ctx.tsm_num.fetch_add(1, Ordering::SeqCst);
            if let Err(e) = StorageContext::fetch_add_file_len(path, ctx.tsm_bytes.clone()) {
                eprintln!("     [W] Cannot get metadata of tsm file '{path:?}': {e}");
            }
        }
        Err(e) => {
            println!("     Invalid tsm file '{path:?}: {e}");
            ctx.errors.lock().await.push(format!("'{path:?}': {e}"));
        }
    }
}

async fn check_delta_file(path: &PathBuf, ctx: Arc<StorageContext>) {
    println!("    Checking delta file '{path:?} ...");
    match TsmReader::open(path).await {
        Ok(_t) => {
            ctx.delta_num.fetch_add(1, Ordering::SeqCst);
            if let Err(e) = StorageContext::fetch_add_file_len(path, ctx.delta_bytes.clone()) {
                eprintln!("     [W] Cannot get metadata of delta file '{path:?}': {e}");
            }
        }
        Err(e) => {
            eprintln!("     [E] Invalid delta file '{path:?}: {e}");
            ctx.errors.lock().await.push(format!("'{path:?}': {e}"));
        }
    }
}

async fn check_index_dir(path: &PathBuf, ctx: Arc<StorageContext>) -> Result<(), ()> {
    if !path.exists() {
        return Err(());
    }
    println!("   Into index dir '{path:?}");
    match path.read_dir() {
        Ok(read_index_dir) => {
            for read_index_result in read_index_dir {
                match read_index_result {
                    Ok(index_file) => {
                        let index_file = index_file.path();
                        if let Some(ext) = index_file.extension() {
                            if ext == "binlog" {
                                check_index_binlog_file(&index_file, ctx.clone()).await;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("    [E] Failed to read '{path:?}: {e}");
                        ctx.print().await;
                        return Err(());
                    }
                }
            }
            Ok(())
        }
        Err(e) => {
            eprintln!("    [E] Failed to read '{path:?}': {e}");
            Err(())
        }
    }
}

async fn check_index_binlog_file(path: &PathBuf, ctx: Arc<StorageContext>) {
    println!("    Checking index-binlog file '{path:?} ...");
    match file_manager::open_file(path).await {
        Ok(f) => match BinlogReader::new(0, f.into()).await {
            Ok(mut t) => {
                ctx.index_num.fetch_add(1, Ordering::SeqCst);
                if let Err(e) = StorageContext::fetch_add_file_len(path, ctx.index_bytes.clone()) {
                    eprintln!("     [W] Cannot get metadata of index-binlog file '{path:?}': {e}");
                }

                loop {
                    match t.next_block().await {
                        Ok(Some(_b)) => {}
                        Ok(None) => {
                            break;
                        }
                        Err(e) => {
                            eprintln!("     [E] Invalid index-binlog file block: '{path:?}: {e}");
                            ctx.errors.lock().await.push(format!("'{path:?}': {e}"));
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("     [E] Invalid index-binlog file '{path:?}: {e}");
                ctx.errors.lock().await.push(format!("'{path:?}': {e}"));
            }
        },
        Err(e) => {
            eprintln!("     [W] Cannot open index-binlog file '{path:?}': {e}");
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
                                check_wal_file(&wal_file, ctx.clone()).await;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("    [E] Failed to read '{path:?}: {e}");
                        ctx.print().await;
                        exit(1)
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("    [E] Failed to read '{path:?}': {e}");
        }
    }
}

async fn check_wal_file(path: &Path, ctx: Arc<StorageContext>) {
    println!("Checking wal file '{path:?} ...");

    match record_file::Reader::open(path).await {
        Ok(mut r) => {
            ctx.wal_num.fetch_add(1, Ordering::SeqCst);
            if let Err(e) = StorageContext::fetch_add_file_len(path, ctx.wal_bytes.clone()) {
                eprintln!(" [W] Cannot get metadata of wal file '{path:?}': {e}");
            }

            let mut next_pos = 0_u64;
            loop {
                match r.read_record().await {
                    Ok(r) => {
                        next_pos = r.pos + r.data.len() as u64;
                    }
                    Err(tskv::Error::Eof) => break,
                    Err(e) => {
                        eprintln!(" [E] Invalid wal file '{path:?}: [{next_pos}..), {e}");
                        ctx.errors
                            .lock()
                            .await
                            .push(format!("'{path:?}': [{next_pos}..), {e}"));
                    }
                }
            }
        }
        Err(e) => {
            eprintln!(" [E] Failed to open wal file '{path:?}: {e}");
            ctx.errors.lock().await.push(format!("'{path:?}': {e}"));
        }
    }

    ctx.print().await;
}
