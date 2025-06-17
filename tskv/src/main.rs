use std::env;
use std::path::{Path, PathBuf};
use std::process::exit;

use tskv::tsm::{TsmReader, TsmTombstone};

enum CheckingObject {
    Storage,
    Wal,
}

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
        CheckingObject::Storage => check_storage(path).await,
        CheckingObject::Wal => check_wal(path).await,
    }
}

async fn check_storage(path: &Path) {
    let summary_path = path.join("summary");
    check_summary(&summary_path).await;

    let databases_dir = path.join("data");

    println!("Checking databases dir: {databases_dir:?}");
    match databases_dir.read_dir() {
        Ok(read_databases_dir) => {
            for read_databases_result in read_databases_dir {
                match read_databases_result {
                    Ok(database_dir) => {
                        let database_dir = database_dir.path();

                        println!(" Checking database dir: {database_dir:?}");
                        match database_dir.read_dir() {
                            Ok(read_vnode_dir) => {
                                for read_vnodes_result in read_vnode_dir {
                                    match read_vnodes_result {
                                        Ok(vnode_dir) => {
                                            let vnode_dir = vnode_dir.path();

                                            println!("  Checking vnode dir: {vnode_dir:?}");
                                            check_tsm_dir(&vnode_dir.join("delta")).await;
                                            check_tsm_dir(&vnode_dir.join("tsm")).await;
                                        }
                                        Err(e) => {
                                            eprintln!("[E] Failed to read '{database_dir:?}: {e}");
                                            exit(1);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("[E] Failed to read '{database_dir:?}: {e}");
                                exit(1);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[E] Failed to read '{databases_dir:?}: {e}");
                        exit(1);
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("[E] Failed to read '{databases_dir:?}': {e}");
            exit(1);
        }
    }
}

async fn check_summary(path: &PathBuf) {
    println!("Checking summary file '{path:?}");
    println!("Skipped checking summary file");
}

async fn check_tsm_dir(path: &PathBuf) {
    if !path.exists() {
        return;
    }
    println!("   Checking tsm dir '{path:?}");
    match path.read_dir() {
        Ok(read_tsm_dir) => {
            for read_tsm_result in read_tsm_dir {
                match read_tsm_result {
                    Ok(tsm_file) => {
                        let tsm_file = tsm_file.path();
                        if let Some(ext) = tsm_file.extension() {
                            if ext == "tsm" || ext == "delta" {
                                check_tsm(&tsm_file).await;
                            } else if ext == "tombstone" {
                                check_tombstone(&tsm_file).await;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[E] Failed to read '{path:?}: {e}");
                        exit(1);
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("[E] Failed to read '{path:?}': {e}");
            exit(1);
        }
    }
}

async fn check_tsm(path: &PathBuf) {
    print!("    Checking tsm file '{path:?} ...");
    match TsmReader::open(path).await {
        Ok(_t) => {
            println!(" OK");
        }
        Err(e) => {
            println!(" FAIL: {e}");
        }
    }
}

async fn check_tombstone(path: &PathBuf) {
    print!("    Checking tsm-tombstone file '{path:?} ...");
    match TsmTombstone::open(&path, 0).await {
        Ok(_t) => {
            println!(" OK");
        }
        Err(e) => {
            println!(" FAIL: {e}");
        }
    }
}

async fn check_wal(path: &Path) {
    println!("Checking wal file '{path:?}");
    println!("  Skipped checking wal file");
}
