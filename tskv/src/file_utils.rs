use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use lazy_static::lazy_static;
use regex::Regex;
use tokio::fs;

use crate::error::GenericError;
use crate::file_system::file_manager;

const INDEX_BINLOG: &str = "index binlog";
const SUMMARY: &str = "summary";
const WAL: &str = "wal";
const TSM: &str = "tsm";

lazy_static! {
    static ref SUMMARY_FILE_NAME_PATTERN: Regex = Regex::new(r"summary-\d{6}").unwrap();
    static ref WAL_FILE_NAME_PATTERN: Regex = Regex::new(r"_\d{6}\.wal").unwrap();
    static ref TSM_FILE_NAME_PATTERN: Regex = Regex::new(r"_\d{6}\.tsm").unwrap();
    static ref SCHEMA_FILE_NAME_PATTERN: Regex = Regex::new(r"_\d{6}\.schema").unwrap();
    static ref HINTEDOFF_FILE_NAME_PATTERN: Regex = Regex::new(r"_\d{6}\.hh").unwrap();
    static ref INDEX_BINLOG_FILE_NAME_PATTERN: Regex = Regex::new(r"_\d{6}\.binlog").unwrap();
}

fn err_path_terminates_with_double_dot<T>(path: &Path) -> Result<T, GenericError> {
    Err(format!("path '{}' terminates in '..'", path.display()).into())
}

fn err_file_name_is_not_utf8<T>(
    file_type: &'static str,
    file_name: &OsStr,
) -> Result<T, GenericError> {
    Err(format!(
        "{file_type} file's name '{}' is not valid utf8 string",
        file_name.to_string_lossy()
    )
    .into())
}

fn err_file_does_not_have_id<T>(
    file_type: &'static str,
    file_name: &str,
) -> Result<T, GenericError> {
    Err(format!("{file_type} file's name '{file_name}' does not contain an id").into())
}

fn err_file_have_invalid_id<T>(
    file_type: &'static str,
    file_name: &str,
) -> Result<T, GenericError> {
    Err(format!("{file_type} file's name '{file_name}' contains an invalid id").into())
}

// Summary file.
pub fn make_summary_file(path: impl AsRef<Path>, number: u64) -> PathBuf {
    let p = format!("summary-{:06}", number);
    path.as_ref().join(p)
}

pub fn make_summary_file_tmp(path: impl AsRef<Path>) -> PathBuf {
    let p = "summary.tmp".to_string();
    path.as_ref().join(p)
}

pub fn check_summary_file_name(file_name: &str) -> bool {
    SUMMARY_FILE_NAME_PATTERN.is_match(file_name)
}

pub async fn rename(old_name: impl AsRef<Path>, new_name: impl AsRef<Path>) -> std::io::Result<()> {
    fs::create_dir_all(new_name.as_ref().parent().unwrap()).await?;
    fs::rename(old_name, new_name).await
}

pub fn get_summary_file_id(file_name: &str) -> Result<u64, GenericError> {
    if !check_summary_file_name(file_name) {
        return err_file_does_not_have_id(SUMMARY, file_name);
    }
    let (_, file_number) = file_name.split_at(8);
    match file_number.parse::<u64>() {
        Err(_) => err_file_have_invalid_id(SUMMARY, file_name),
        Ok(id) => Ok(id),
    }
}

// index binlog files.

pub fn make_index_binlog_file(path: impl AsRef<Path>, sequence: u64) -> PathBuf {
    let p = format!("_{:06}.binlog", sequence);
    path.as_ref().join(p)
}

pub fn check_index_binlog_file_name(file_name: &str) -> bool {
    INDEX_BINLOG_FILE_NAME_PATTERN.is_match(file_name)
}

pub fn get_index_binlog_file_id(file_name: &str) -> Result<u64, GenericError> {
    if !check_index_binlog_file_name(file_name) {
        return err_file_does_not_have_id(INDEX_BINLOG, file_name);
    }
    let file_number = &file_name[1..7];
    match file_number.parse::<u64>() {
        Err(_) => err_file_have_invalid_id(INDEX_BINLOG, file_name),
        Ok(id) => Ok(id),
    }
}

// WAL (write ahead log) file.
pub fn make_wal_file(path: impl AsRef<Path>, sequence: u64) -> PathBuf {
    let p = format!("_{:06}.wal", sequence);
    path.as_ref().join(p)
}

pub fn check_wal_file_name(file_name: &str) -> bool {
    WAL_FILE_NAME_PATTERN.is_match(file_name)
}

pub fn get_wal_file_id(file_name: &str) -> Result<u64, GenericError> {
    if !check_wal_file_name(file_name) {
        return err_file_does_not_have_id(WAL, file_name);
    }
    let file_number = &file_name[1..7];
    match file_number.parse::<u64>() {
        Err(_) => err_file_have_invalid_id(WAL, file_name),
        Ok(id) => Ok(id),
    }
}

// TSM file
pub fn make_tsm_file_name(path: impl AsRef<Path>, sequence: u64) -> PathBuf {
    let p = format!("_{:06}.tsm", sequence);
    path.as_ref().join(p)
}

pub fn get_tsm_file_id_by_path(tsm_path: impl AsRef<Path>) -> Result<u64, GenericError> {
    let path = tsm_path.as_ref();
    let file_name = match path.file_name() {
        Some(f) => f,
        None => return err_path_terminates_with_double_dot(path),
    };
    let file_name_utf8 = match file_name.to_str() {
        Some(f) => f,
        None => return err_file_name_is_not_utf8(TSM, file_name),
    };
    if file_name_utf8.len() == 1 {
        return err_file_have_invalid_id(TSM, file_name_utf8);
    }
    let start = file_name_utf8.find('_').unwrap_or(0_usize) + 1;
    let end = file_name_utf8.find('.').unwrap_or(file_name_utf8.len());
    let file_number = &file_name_utf8[start..end];
    match file_number.parse::<u64>() {
        Err(_) => err_file_have_invalid_id(TSM, file_name_utf8),
        Ok(id) => Ok(id),
    }
}

// TSM tombstone file
pub fn make_tsm_tombstone_file_name(path: impl AsRef<Path>, sequence: u64) -> PathBuf {
    let p = format!("_{:06}.tombstone", sequence);
    path.as_ref().join(p)
}

// delta file
pub fn make_delta_file_name(path: impl AsRef<Path>, sequence: u64) -> PathBuf {
    let p = format!("_{:06}.delta", sequence);
    path.as_ref().join(p)
}

// Common
pub fn get_max_sequence_file_name<F>(
    dir: impl AsRef<Path>,
    get_sequence: F,
) -> Option<(PathBuf, u64)>
where
    F: Fn(&str) -> Result<u64, GenericError>,
{
    let segments = file_manager::list_file_names(dir);
    if segments.is_empty() {
        return None;
    }

    let mut max_id = 0;
    let mut max_index = 0;
    let mut is_found = false;
    for (i, file_name) in segments.iter().enumerate() {
        match get_sequence(file_name) {
            Ok(id) => {
                is_found = true;
                if max_id < id {
                    max_id = id;
                    max_index = i;
                }
            }
            Err(_) => continue,
        }
    }

    if !is_found {
        return None;
    }

    let max_file_name = segments.get(max_index).unwrap();
    Some((PathBuf::from(max_file_name), max_id))
}

/* -------------------------------------------------------------------------------------- */

pub fn make_file_name(path: impl AsRef<Path>, id: u64, suffix: &str) -> PathBuf {
    let p = format!("_{:06}.{}", id, suffix);
    path.as_ref().join(p)
}

pub fn get_file_id_range(dir: impl AsRef<Path>, suffix: &str) -> Option<(u64, u64)> {
    let file_names = file_manager::list_file_names(dir);
    if file_names.is_empty() {
        return None;
    }

    let pattern = Regex::new(&(format!("_\\d{{6}}\\.{suffix}"))).unwrap();
    let get_file_id = |file_name: &str| -> Result<u64, GenericError> {
        if !pattern.is_match(file_name) {
            return err_file_does_not_have_id(INDEX_BINLOG, file_name);
        }

        let file_number = &file_name[1..7];
        match file_number.parse::<u64>() {
            Err(_) => err_file_have_invalid_id(INDEX_BINLOG, file_name),
            Ok(id) => Ok(id),
        }
    };

    let mut max_id = 0;
    let mut min_id = u64::MAX;
    let mut is_found = false;
    for (_i, file_name) in file_names.iter().enumerate() {
        if let Ok(id) = get_file_id(file_name) {
            is_found = true;
            if max_id < id {
                max_id = id;
            }

            if min_id > id {
                min_id = id;
            }
        }
    }

    if !is_found {
        return None;
    }

    Some((min_id, max_id))
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use super::{check_summary_file_name, make_summary_file};
    use crate::file_utils::{
        check_wal_file_name, get_summary_file_id, get_wal_file_id, make_wal_file,
    };

    #[test]
    fn test_get_file_id() {
        let summary_file_name = "summary-000123";
        let summary_file_id = get_summary_file_id(summary_file_name).unwrap();
        dbg!(summary_file_id);
        assert_eq!(summary_file_id, 123);

        let wal_file_name = "_000123.wal";
        let wal_file_id = get_wal_file_id(wal_file_name).unwrap();
        dbg!(wal_file_id);
        assert_eq!(wal_file_id, 123);
    }

    #[test]
    fn test_make_file() {
        let path = PathBuf::from("/tmp/test".to_string());
        {
            let summary_file_path = make_summary_file(&path, 0);
            let summary_file_name = summary_file_path.file_name().unwrap().to_str().unwrap();
            assert!(check_summary_file_name(summary_file_name));
            let summary_file_id = get_summary_file_id(summary_file_name).unwrap();
            assert_eq!(summary_file_id, 0);
        }
        {
            let wal_file_path = make_wal_file(&path, 0);
            let wal_file_name = wal_file_path.file_name().unwrap().to_str().unwrap();
            assert!(check_wal_file_name(wal_file_name));
            let wal_file_id = get_wal_file_id(wal_file_name).unwrap();
            assert_eq!(wal_file_id, 0);
        }
    }
}
