pub mod check;
mod compact;
mod flush;
mod iterator;
pub mod job;
mod picker;

use std::collections::HashMap;
use std::sync::Arc;

use models::predicate::domain::TimeRange;
use parking_lot::RwLock;
use utils::BloomFilter;

pub use crate::compaction::compact::CompactState;
pub use crate::compaction::flush::run_flush_memtable_job;
pub use crate::compaction::picker::pick_compaction;
use crate::context::GlobalContext;
use crate::memcache::MemCache;
use crate::tseries_family::{ColumnFile, Version};
use crate::{ColumnFileId, LevelId, TseriesFamilyId, VersionEdit};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompactTask {
    /// Compact the files in the in_level into the out_level.
    Normal(TseriesFamilyId),
    /// Compact the files in level-0 to larger files.
    Delta(TseriesFamilyId),
    /// Triggers compaction manually.
    Manual(TseriesFamilyId),
}

impl CompactTask {
    pub fn ts_family_id(&self) -> TseriesFamilyId {
        match self {
            CompactTask::Normal(ts_family_id) => *ts_family_id,
            CompactTask::Delta(ts_family_id) => *ts_family_id,
            CompactTask::Manual(ts_family_id) => *ts_family_id,
        }
    }

    fn priority(&self) -> usize {
        match self {
            CompactTask::Manual(_) => 0,
            CompactTask::Delta(_) => 1,
            CompactTask::Normal(_) => 2,
        }
    }
}

impl Ord for CompactTask {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority().cmp(&other.priority())
    }
}

impl PartialOrd for CompactTask {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Display for CompactTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompactTask::Normal(ts_family_id) => write!(f, "Normal({})", ts_family_id),
            CompactTask::Delta(ts_family_id) => write!(f, "Delta({})", ts_family_id),
            CompactTask::Manual(ts_family_id) => write!(f, "Manual({})", ts_family_id),
        }
    }
}

pub struct CompactReq {
    compact_task: CompactTask,

    version: Arc<Version>,
    files: Vec<Arc<ColumnFile>>,
    in_level: LevelId,
    out_level: LevelId,
    out_time_range: TimeRange,
}

impl CompactReq {
    /// Split the `files` into delta files and an optional level-1~4 file. Only for delta compaction.
    pub fn split_delta_and_level_files(&self) -> (Vec<Arc<ColumnFile>>, Option<Arc<ColumnFile>>) {
        debug_assert!(self.in_level == 0);
        let (mut delta_files, mut level_files) = (vec![], Option::None);
        for f in self.files.iter() {
            if f.is_delta() {
                delta_files.push(f.clone());
            } else if level_files.is_none() {
                level_files = Some(f.clone());
            }
        }
        (delta_files, level_files)
    }
}

impl std::fmt::Display for CompactReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tenant_database: {}, ts_family: {}, in_level: {}, out_level: {:?}, out_time_range: {}, files: [",
            self.version.borrowed_database(),
            self.version.tf_id(),
            self.in_level,
            self.out_level,
            self.out_time_range,
        )?;
        if !self.files.is_empty() {
            write!(
                f,
                "{{ Level-{}, file_id: {}, time_range: {}-{} }}",
                self.files[0].level(),
                self.files[0].file_id(),
                self.files[0].time_range().min_ts,
                self.files[0].time_range().max_ts
            )?;
            for file in self.files.iter().skip(1) {
                write!(
                    f,
                    ", {{ Level-{}, file_id: {}, time_range: {}-{} }}",
                    file.level(),
                    file.file_id(),
                    file.time_range().min_ts,
                    file.time_range().max_ts
                )?;
            }
        }
        write!(f, "]")
    }
}

#[derive(Debug)]
pub struct FlushReq {
    pub ts_family_id: TseriesFamilyId,
    pub mems: Vec<Arc<RwLock<MemCache>>>,
    pub force_flush: bool,
}

pub async fn run_compaction_job(
    request: CompactReq,
    ctx: Arc<GlobalContext>,
) -> crate::Result<Option<(VersionEdit, HashMap<ColumnFileId, Arc<BloomFilter>>)>> {
    let key = models::oid::UuidGenerator::default().next_id();
    let start = std::time::Instant::now();
    trace::info!("compaction begin: {key}, task: {}", request.compact_task);
    let ret = if request.in_level == 0 {
        compact::run_delta_compaction_job(request, ctx).await
    } else {
        compact::run_compaction_job(request, ctx).await
    };
    trace::info!(
        "compaction end: {key}, cost: {} ms",
        start.elapsed().as_millis()
    );

    ret
}

mod metric {
    use std::collections::HashMap;

    use models::utils as model_utils;

    use super::CompactTask;

    pub const NEXT_FIELD: &str = "next_field";
    pub const MERGE_FIELD: &str = "merge_field";
    pub const MERGE_BLOCK: &str = "merge_block";
    pub const READ_BLOCK: &str = "read_block";
    pub const MERGE_BLOCK_BATCH: &str = "merge_block_batch";
    pub const WRITE_BLOCK: &str = "write_block";

    pub trait MetricStore: Send {
        fn add_metric(
            &mut self,
            metric: String,
            time_unit: models::arrow::TimeUnit,
            buf_size: usize,
        );
        fn begin_all(&mut self);
        fn finish_all(&mut self);
        fn begin(&mut self, metric: &str);
        fn finish(&mut self, metric: &str);
    }

    pub struct TimeUnit(models::arrow::TimeUnit);

    impl TimeUnit {
        fn parse_duration(&self, duration: std::time::Duration) -> f64 {
            match self.0 {
                models::arrow::TimeUnit::Second => duration.as_secs() as f64,
                models::arrow::TimeUnit::Millisecond => duration.as_millis() as f64,
                models::arrow::TimeUnit::Microsecond => duration.as_micros() as f64,
                models::arrow::TimeUnit::Nanosecond => duration.as_nanos() as f64,
            }
        }

        fn seconds_f64(&self, duration: f64) -> f64 {
            match self.0 {
                models::arrow::TimeUnit::Second => duration,
                models::arrow::TimeUnit::Millisecond => duration / 1000_f64,
                models::arrow::TimeUnit::Microsecond => duration / 1_000_000_f64,
                models::arrow::TimeUnit::Nanosecond => duration / 1_000_000_000_f64,
            }
        }
    }

    impl std::fmt::Display for TimeUnit {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self.0 {
                models::arrow::TimeUnit::Second => write!(f, "s"),
                models::arrow::TimeUnit::Millisecond => write!(f, "ms"),
                models::arrow::TimeUnit::Microsecond => write!(f, "us"),
                models::arrow::TimeUnit::Nanosecond => write!(f, "ns"),
            }
        }
    }

    pub struct CompactMetrics {
        compact_task: CompactTask,
        total_instant: std::time::Instant,

        metric_name_map: HashMap<String, usize>,
        metric_time_unit_list: Vec<TimeUnit>,
        instant_list: Vec<std::time::Instant>,
        cost_buf_list: Vec<(Vec<f64>, usize)>,
        cost_avg_list: Vec<(bool, f64)>,
        cost_total_min_max_list: Vec<(f64, f64)>,
        cost_min_max_list: Vec<(f64, f64)>,
    }

    impl CompactMetrics {
        const DEFAULT_MIN_MAX: (f64, f64) = (31415926.53_f64, 0_f64);

        #[rustfmt::skip]
        pub fn default(compact_task: CompactTask) -> Self {
            CompactMetrics::new(
                compact_task,
                vec![
                    (NEXT_FIELD.to_string(), models::arrow::TimeUnit::Microsecond, 5000),
                    (MERGE_FIELD.to_string(), models::arrow::TimeUnit::Microsecond, 5000),
                    (MERGE_BLOCK.to_string(), models::arrow::TimeUnit::Microsecond, 5000),
                    (READ_BLOCK.to_string(), models::arrow::TimeUnit::Microsecond, 10000),
                    (MERGE_BLOCK_BATCH.to_string(), models::arrow::TimeUnit::Nanosecond, 10000),
                    (WRITE_BLOCK.to_string(), models::arrow::TimeUnit::Microsecond, 10000),
                ],
            )
        }

        pub fn new(
            compact_task: CompactTask,
            metrics: Vec<(String, models::arrow::TimeUnit, usize)>,
        ) -> Self {
            let mut m = Self {
                compact_task,
                total_instant: std::time::Instant::now(),
                metric_name_map: HashMap::with_capacity(16),
                metric_time_unit_list: Vec::with_capacity(16),
                instant_list: Vec::with_capacity(16),
                cost_buf_list: Vec::with_capacity(16),
                cost_avg_list: Vec::with_capacity(16),
                cost_total_min_max_list: Vec::with_capacity(16),
                cost_min_max_list: Vec::with_capacity(16),
            };
            for (metric, time_unit, buf_size) in metrics {
                m.add_metric(metric, time_unit, buf_size);
            }
            m
        }

        fn flush_metric(&mut self, metric: &str, force_flush: bool) {
            let idx = match self.metric_name_map.get(metric) {
                Some(idx) => *idx,
                None => return,
            };
            let instant = match self.instant_list.get_mut(idx) {
                Some(instant) => instant,
                None => return,
            };
            let unit = match self.metric_time_unit_list.get(idx) {
                Some(unit) => unit,
                None => return,
            };
            let (cost_buf, max_buf_size) = match self.cost_buf_list.get_mut(idx) {
                Some(buf) => buf,
                None => return,
            };
            let (first_avg, cost_avg) = match self.cost_avg_list.get_mut(idx) {
                Some(avg) => avg,
                None => return,
            };
            let (total_cost_min, total_cost_max) = match self.cost_total_min_max_list.get_mut(idx) {
                Some(total_cost) => total_cost,
                None => return,
            };
            let (cost_min, cost_max) = match self.cost_min_max_list.get_mut(idx) {
                Some(cost) => cost,
                None => return,
            };

            let cost_t = unit.parse_duration(instant.elapsed());
            *total_cost_min = total_cost_min.min(cost_t);
            *total_cost_max = total_cost_max.max(cost_t);
            *cost_min = cost_min.min(cost_t);
            *cost_max = cost_max.max(cost_t);
            cost_buf.push(cost_t);
            if force_flush || cost_buf.len() == *max_buf_size {
                let (sum, avg) = if !cost_buf.is_empty() {
                    let sum = cost_buf.iter().sum::<f64>();
                    let avg = sum / cost_buf.len() as f64;
                    cost_buf.clear();
                    (sum, avg)
                } else {
                    (0.0, 0.0)
                };

                if *first_avg {
                    *first_avg = false;
                    *cost_avg = avg;
                } else {
                    *cost_avg = (*cost_avg + avg) / 2_f64;
                }
                let sum_sec = unit.seconds_f64(sum);
                trace::info!(
                    "{metric},compaction={},unit={unit} batch={max_buf_size},sum={sum_sec:.2},avg={avg:.2},total_avg={cost_avg:.2},min={cost_min},max={cost_max},total_min={total_cost_min},total_max={total_cost_max} {}",
                    self.compact_task, model_utils::now_timestamp_nanos()
                );
                (*cost_min, *cost_max) = Self::DEFAULT_MIN_MAX;
            }
        }
    }

    impl MetricStore for CompactMetrics {
        fn add_metric(
            &mut self,
            metric: String,
            time_unit: models::arrow::TimeUnit,
            buf_size: usize,
        ) {
            let idx = self.metric_name_map.len();
            self.metric_name_map.insert(metric, idx);
            self.metric_time_unit_list.push(TimeUnit(time_unit));
            self.instant_list.push(std::time::Instant::now());
            self.cost_buf_list
                .push((Vec::with_capacity(buf_size), buf_size));
            self.cost_avg_list.push((true, 0_f64));
            self.cost_total_min_max_list.push(Self::DEFAULT_MIN_MAX);
            self.cost_min_max_list.push(Self::DEFAULT_MIN_MAX);
        }

        fn begin_all(&mut self) {
            self.total_instant = std::time::Instant::now();
        }

        fn finish_all(&mut self) {
            let total_cost_sec = self.total_instant.elapsed().as_secs() as f64;
            let timestamp = model_utils::now_timestamp_nanos();
            trace::info!(
                "total_cost,compaction={},unit=s total={total_cost_sec} {timestamp}",
                self.compact_task,
            );
            for (metric, idx) in self.metric_name_map.clone() {
                self.flush_metric(&metric, true);

                let unit = match self.metric_time_unit_list.get(idx) {
                    Some(unit) => unit,
                    None => return,
                };
                let (_, cost_avg) = match self.cost_avg_list.get_mut(idx) {
                    Some(cost_avg) => cost_avg,
                    None => return,
                };
                let (cost_min, cost_max) = match self.cost_total_min_max_list.get_mut(idx) {
                    Some(cost_range) => cost_range,
                    None => return,
                };
                trace::info!(
                    "final_{metric},compaction={},unit={unit} avg={cost_avg:.2},min={cost_min},max={cost_max} {timestamp}",
                    self.compact_task
                );
            }
        }

        fn begin(&mut self, metric: &str) {
            let idx = match self.metric_name_map.get(metric) {
                Some(idx) => idx,
                None => return,
            };
            self.instant_list[*idx] = std::time::Instant::now();
        }

        fn finish(&mut self, metric: &str) {
            self.flush_metric(metric, false);
        }
    }

    pub struct FakeMetricStore;

    impl MetricStore for FakeMetricStore {
        fn add_metric(
            &mut self,
            _metric: String,
            _time_unit: models::arrow::TimeUnit,
            _buf_size: usize,
        ) {
        }

        fn begin_all(&mut self) {}

        fn finish_all(&mut self) {}

        fn begin(&mut self, _metric: &str) {}

        fn finish(&mut self, _metric: &str) {}
    }
}

#[cfg(test)]
pub mod test {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use cache::ShardedAsyncCache;
    use models::predicate::domain::TimeRange;

    pub use super::compact::test::{
        check_column_file, create_options, generate_data_block, read_data_blocks_from_column_file,
        write_data_block_desc, write_data_blocks_to_column_file, TsmSchema,
    };
    pub use super::flush::flush_tests::default_table_schema;
    use crate::compaction::{CompactReq, CompactTask};
    use crate::file_utils::{make_delta_file_name, make_tsm_file_name};
    use crate::kv_option::StorageOptions;
    use crate::tseries_family::{ColumnFile, LevelInfo, Version};
    use crate::tsm::test::{write_to_tsm, write_to_tsm_tombstone_v2};
    use crate::tsm::{TsmTombstoneCache, TOMBSTONE_FILE_SUFFIX};
    use crate::ColumnFileId;

    /// The sketch of a version of a vnode.
    #[derive(Debug)]
    pub struct VersionSketch {
        pub id: u32,
        pub dir: PathBuf,
        pub tenant_database: Arc<String>,
        pub levels: [LevelSketch; 5],
        pub tombstone_map: HashMap<u64, TimeRange>,
        pub max_level_ts: i64,
    }

    impl VersionSketch {
        pub fn new<P: AsRef<Path>>(dir: P, tenant_database: Arc<String>, vnode_id: u32) -> Self {
            let levels = [
                LevelSketch(0, (i64::MAX, i64::MIN), vec![]),
                LevelSketch(1, (i64::MAX, i64::MIN), vec![]),
                LevelSketch(2, (i64::MAX, i64::MIN), vec![]),
                LevelSketch(3, (i64::MAX, i64::MIN), vec![]),
                LevelSketch(4, (i64::MAX, i64::MIN), vec![]),
            ];
            Self {
                id: vnode_id,
                dir: dir.as_ref().to_path_buf(),
                tenant_database,
                levels,
                tombstone_map: HashMap::new(),
                max_level_ts: i64::MIN,
            }
        }

        pub fn add(mut self, level: usize, file: FileSketch) -> Self {
            self.max_level_ts = self.max_level_ts.max(file.1 .1);
            let level_sketch = &mut self.levels[level];
            level_sketch.1 .0 = level_sketch.1 .0.min(file.1 .0);
            level_sketch.1 .1 = level_sketch.1 .1.max(file.1 .1);
            level_sketch.2.push(file);
            self
        }

        pub fn add_t(
            mut self,
            level: usize,
            file: FileSketch,
            tomb_all_excluded: (i64, i64),
        ) -> Self {
            self.tombstone_map.insert(file.0, tomb_all_excluded.into());
            self = self.add(level, file);
            self
        }

        async fn to_column_files(
            &self,
            storage_opt: &StorageOptions,
            files: &mut Vec<Arc<ColumnFile>>,
            mut filter: impl FnMut(&LevelSketch, &FileSketch) -> bool,
        ) {
            for level_sketch in self.levels.iter() {
                let level_dir = if level_sketch.0 == 0 {
                    storage_opt.delta_dir(self.tenant_database.as_str(), self.id)
                } else {
                    storage_opt.tsm_dir(self.tenant_database.as_str(), self.id)
                };
                for file_sketch in level_sketch.2.iter() {
                    if filter(level_sketch, file_sketch) {
                        let file = file_sketch.to_column_file(&level_dir, level_sketch.0).await;
                        files.push(Arc::new(file));
                    }
                }
            }
        }

        pub async fn to_version(&self, storage_opt: Arc<StorageOptions>) -> Version {
            let mut level_infos =
                LevelInfo::init_levels(self.tenant_database.clone(), self.id, storage_opt.clone());
            for (level, level_sketch) in self.levels.iter().enumerate() {
                let level_dir = if level == 0 {
                    storage_opt.delta_dir(self.tenant_database.as_str(), self.id)
                } else {
                    storage_opt.tsm_dir(self.tenant_database.as_str(), self.id)
                };
                level_sketch
                    .to_level_info(&mut level_infos[level], &level_dir, level as u32)
                    .await;
            }

            Version::new(
                self.id,
                self.tenant_database.clone(),
                storage_opt,
                1,
                level_infos,
                self.max_level_ts,
                Arc::new(ShardedAsyncCache::create_lru_sharded_cache(1)),
            )
        }

        pub async fn to_version_with_tsm(&self, storage_opt: Arc<StorageOptions>) -> Version {
            let version = self.to_version(storage_opt).await;
            self.make_tsm_files(&version).await;
            version
        }

        async fn make_tsm_files(&self, version: &Version) {
            let tsm_dir = version
                .storage_opt()
                .tsm_dir(self.tenant_database.as_str(), self.id);
            let delta_dir = version
                .storage_opt()
                .delta_dir(self.tenant_database.as_str(), self.id);
            let _ = std::fs::remove_dir_all(&tsm_dir);
            let _ = std::fs::remove_dir_all(&delta_dir);

            let tsm_data = &Vec::new();
            for level_sketch in self.levels.iter() {
                for file_sketch in level_sketch.2.iter() {
                    let tsm_path = if level_sketch.0 == 0 {
                        make_delta_file_name(&delta_dir, file_sketch.0)
                    } else {
                        make_tsm_file_name(&tsm_dir, file_sketch.0)
                    };
                    write_to_tsm(&tsm_path, tsm_data, false).await.unwrap();

                    if let Some(tr) = self.tombstone_map.get(&file_sketch.0) {
                        let tombstone_path = tsm_path.with_extension(TOMBSTONE_FILE_SUFFIX);
                        let tomb = TsmTombstoneCache::with_all_excluded(*tr);
                        write_to_tsm_tombstone_v2(tombstone_path, &tomb).await;
                    }
                }
            }
        }
    }

    /// The sketch of a level, contains a tuple of
    /// `level, (min_ts, max_ts), files`.
    #[derive(Debug, Clone)]
    pub struct LevelSketch(pub u32, pub (i64, i64), pub Vec<FileSketch>);

    impl LevelSketch {
        pub async fn to_level_info(
            &self,
            level_info: &mut LevelInfo,
            level_dir: impl AsRef<Path>,
            level: u32,
        ) {
            let mut level_cur_size = 0_u64;
            let mut files = Vec::with_capacity(self.2.len());
            for file_sketch in self.2.iter() {
                level_cur_size += file_sketch.2;
                let file = file_sketch.to_column_file(&level_dir, level).await;
                files.push(Arc::new(file));
            }
            level_info.files = files;
            level_info.cur_size = level_cur_size;
            level_info.time_range = self.1.into();
        }
    }

    /// The sketch of column file, contains `a tuple of
    /// file_id, (min_ts, max_ts), size, being_compact`.
    #[derive(Debug, Clone)]
    pub struct FileSketch(pub u64, pub (i64, i64), pub u64, pub bool);

    impl FileSketch {
        async fn to_column_file(&self, file_dir: impl AsRef<Path>, level: u32) -> ColumnFile {
            let path = if level == 0 {
                make_delta_file_name(file_dir, self.0)
            } else {
                make_tsm_file_name(file_dir, self.0)
            };
            let col = ColumnFile::new(self.0, level, self.1.into(), self.2, path);
            if self.3 {
                col.mark_compacting().await;
            }
            col
        }
    }

    #[tokio::test]
    async fn test_generate_version() {
        let dir = "/tmp/test/compaction/test_generate_version";
        let storage_opt = create_options(dir.to_string(), 1).storage.clone();
        let vnode_sketch = VersionSketch::new(dir, Arc::new("dba".to_string()), 1)
            .add(0, FileSketch(6, (1, 10), 50, false))
            .add(0, FileSketch(7, (790, 800), 50, false))
            .add(1, FileSketch(1, (701, 800), 100, false))
            .add(1, FileSketch(2, (601, 700), 100, false))
            .add(2, FileSketch(3, (501, 600), 200, false))
            .add(3, FileSketch(4, (301, 500), 300, false))
            .add(4, FileSketch(5, (1, 300), 400, false));
        assert_eq!(vnode_sketch.id, 1);
        assert_eq!(vnode_sketch.tenant_database.as_str(), "dba");
        assert_eq!(vnode_sketch.max_level_ts, 800);
        let levels_sketch = vnode_sketch.levels.clone();
        assert_eq!(levels_sketch[0].0, 0);
        assert_eq!(levels_sketch[0].1, (1, 800));
        assert_eq!(levels_sketch[0].2.len(), 2);
        assert_eq!(levels_sketch[1].0, 1);
        assert_eq!(levels_sketch[1].1, (601, 800));
        assert_eq!(levels_sketch[1].2.len(), 2);
        assert_eq!(levels_sketch[2].0, 2);
        assert_eq!(levels_sketch[2].1, (501, 600));
        assert_eq!(levels_sketch[2].2.len(), 1);
        assert_eq!(levels_sketch[3].0, 3);
        assert_eq!(levels_sketch[3].1, (301, 500));
        assert_eq!(levels_sketch[3].2.len(), 1);
        assert_eq!(levels_sketch[4].0, 4);
        assert_eq!(levels_sketch[4].1, (1, 300));
        assert_eq!(levels_sketch[4].2.len(), 1);

        let version = vnode_sketch.to_version(storage_opt.clone()).await;
        assert_eq!(version.tf_id(), 1);
        assert_eq!(version.database().as_str(), "dba");
        assert_eq!(version.levels_info().len(), levels_sketch.len());
        let tsm_dir = storage_opt.tsm_dir("dba", 1);
        let delta_dir = storage_opt.delta_dir("dba", 1);
        for (version_level, level_sketch) in version.levels_info().iter().zip(levels_sketch.iter())
        {
            assert_eq!(version_level.database.as_str(), "dba");
            assert_eq!(version_level.tsf_id, 1);
            assert_eq!(version_level.level, level_sketch.0);
            assert_eq!(
                version_level.cur_size,
                level_sketch.2.iter().map(|f| f.2).sum::<u64>()
            );
            assert_eq!(version_level.time_range, level_sketch.1.into());
            assert_eq!(version_level.files.len(), level_sketch.2.len());
            for (version_file, file_sketch) in version_level.files.iter().zip(level_sketch.2.iter())
            {
                assert_eq!(version_file.file_id(), file_sketch.0);
                assert_eq!(version_file.level(), level_sketch.0);
                assert_eq!(version_file.time_range(), &(file_sketch.1.into()));
                assert_eq!(version_file.size(), file_sketch.2);
                assert_eq!(version_file.is_compacting().await, file_sketch.3);
                if level_sketch.0 == 0 {
                    assert_eq!(
                        version_file.file_path(),
                        &make_delta_file_name(&delta_dir, file_sketch.0)
                    );
                } else {
                    assert_eq!(
                        version_file.file_path(),
                        &make_tsm_file_name(&tsm_dir, file_sketch.0)
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_compact_req_methods_delta_compaction() {
        // This case doesn't need directory to exist.
        let dir = "/tmp/test/compaction/test_compact_req_methods_delta_compaction";
        let opt = create_options(dir.to_string(), 1);

        {
            // Merge delta files to level-1.
            let version_sketch = VersionSketch::new(dir, Arc::new("t_d".to_string()), 1)
                .add(0, FileSketch(1, (20, 30), 10, false))
                .add(0, FileSketch(2, (30, 40), 10, false))
                .add(1, FileSketch(3, (10, 20), 10, false));
            let version = version_sketch.to_version(opt.storage.clone()).await;
            let files = version.levels_info()[0].files.clone();
            let req = CompactReq {
                compact_task: CompactTask::Delta(1),
                version: Arc::new(version),
                files,
                in_level: 0,
                out_level: 1,
                out_time_range: (1, 20).into(),
            };

            let mut delta_files_exp = vec![];
            version_sketch
                .to_column_files(&opt.storage, &mut delta_files_exp, |l, _| l.0 == 0)
                .await;
            let (delta_files, level_file) = req.split_delta_and_level_files();
            assert_eq!(
                delta_files
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>(),
                delta_files_exp
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>()
            );
            assert!(level_file.is_none());
        }

        {
            // Merge delta files to an empty level: level-3.
            let version_sketch = VersionSketch::new(dir, Arc::new("t_d".to_string()), 1)
                .add(0, FileSketch(1, (1, 9), 10, false))
                .add(0, FileSketch(2, (1, 9), 10, false))
                .add(2, FileSketch(3, (10, 20), 10, false));
            let version = version_sketch.to_version(opt.storage.clone()).await;
            let files = version.levels_info()[0].files.clone();
            let req = CompactReq {
                compact_task: CompactTask::Delta(1),
                version: Arc::new(version),
                files,
                in_level: 0,
                out_level: 3,
                out_time_range: (1, 9).into(),
            };

            let mut delta_files_exp = vec![];
            version_sketch
                .to_column_files(&opt.storage, &mut delta_files_exp, |l, _| l.0 == 0)
                .await;
            let (delta_files, level_file) = req.split_delta_and_level_files();
            assert_eq!(
                delta_files
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>(),
                delta_files_exp
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>()
            );
            assert!(level_file.is_none());
        }

        {
            // Merge delta files to level-2 but level-3 is empty.
            let version_sketch = VersionSketch::new(dir, Arc::new("t_d".to_string()), 1)
                .add(0, FileSketch(1, (9, 20), 10, false))
                .add(0, FileSketch(2, (9, 20), 10, false))
                .add(2, FileSketch(3, (1, 10), 10, false));
            let version = version_sketch.to_version(opt.storage.clone()).await;
            let files = version.levels_info()[0].files.clone();
            let req = CompactReq {
                compact_task: CompactTask::Delta(1),
                version: Arc::new(version),
                files,
                in_level: 0,
                out_level: 2,
                out_time_range: (11, 20).into(),
            };

            let mut delta_files_exp = vec![];
            version_sketch
                .to_column_files(&opt.storage, &mut delta_files_exp, |l, _| l.0 == 0)
                .await;
            let (delta_files, level_file) = req.split_delta_and_level_files();
            assert_eq!(
                delta_files
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>(),
                delta_files_exp
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>()
            );
            assert!(level_file.is_none());
        }

        {
            // Merge delta files with level files.
            let version_sketch = VersionSketch::new(dir, Arc::new("t_d".to_string()), 1)
                .add(0, FileSketch(1, (1, 20), 10, false))
                .add(0, FileSketch(2, (1, 20), 10, false))
                .add(2, FileSketch(3, (1, 10), 10, false));
            let version = version_sketch.to_version(opt.storage.clone()).await;
            let mut req = CompactReq {
                compact_task: CompactTask::Delta(1),
                version: Arc::new(version),
                files: vec![],
                in_level: 0,
                out_level: 2,
                out_time_range: (1, 10).into(),
            };
            version_sketch
                .to_column_files(&opt.storage, &mut req.files, |_, _| true)
                .await;

            let (mut delta_files_exp, mut level_files_exp) = (vec![], vec![]);
            version_sketch
                .to_column_files(&opt.storage, &mut delta_files_exp, |l, _| l.0 == 0)
                .await;
            version_sketch
                .to_column_files(&opt.storage, &mut level_files_exp, |l, _| l.0 != 0)
                .await;
            let (delta_files, level_file) = req.split_delta_and_level_files();
            assert_eq!(
                delta_files
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>(),
                delta_files_exp
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>()
            );
            assert_eq!(
                level_file.map(|f| f.file_id()),
                Some(level_files_exp[0].file_id()),
            );
        }

        {
            // Merge delta files to level files, not existed level files.
            let version_sketch = VersionSketch::new(dir, Arc::new("t_d".to_string()), 1)
                .add(0, FileSketch(1, (11, 15), 10, false))
                .add(0, FileSketch(2, (16, 20), 10, false));
            let version = version_sketch.to_version(opt.storage.clone()).await;
            let mut req = CompactReq {
                compact_task: CompactTask::Delta(1),
                version: Arc::new(version),
                files: vec![],
                in_level: 0,
                out_level: 1,
                out_time_range: (11, 20).into(),
            };
            version_sketch
                .to_column_files(&opt.storage, &mut req.files, |_, _| true)
                .await;

            let mut delta_files_exp = vec![];
            version_sketch
                .to_column_files(&opt.storage, &mut delta_files_exp, |l, _| l.0 == 0)
                .await;
            let (delta_files, level_file) = req.split_delta_and_level_files();
            assert_eq!(
                delta_files
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>(),
                delta_files_exp
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>()
            );
            assert!(level_file.is_none());
        }

        {
            // Merge delta files to level files, target_level has no level files.
            let version_sketch = VersionSketch::new(dir, Arc::new("t_d".to_string()), 1)
                .add(0, FileSketch(1, (11, 15), 10, false))
                .add(0, FileSketch(2, (16, 20), 10, false))
                .add(2, FileSketch(3, (1, 5), 5, false))
                .add(2, FileSketch(4, (6, 10), 5, false));
            let version = version_sketch.to_version(opt.storage.clone()).await;
            let mut req = CompactReq {
                compact_task: CompactTask::Delta(1),
                version: Arc::new(version),
                files: vec![],
                in_level: 0,
                out_level: 1,
                out_time_range: TimeRange::all(),
            };
            version_sketch
                .to_column_files(&opt.storage, &mut req.files, |l, _| l.0 == 0)
                .await;

            let mut delta_files_exp = vec![];
            version_sketch
                .to_column_files(&opt.storage, &mut delta_files_exp, |l, _| l.0 == 0)
                .await;
            let (delta_files, level_file) = req.split_delta_and_level_files();
            assert_eq!(
                delta_files
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>(),
                delta_files_exp
                    .iter()
                    .map(|f| f.file_id())
                    .collect::<Vec<ColumnFileId>>()
            );
            assert!(level_file.is_none());
        }
    }

    #[tokio::test]
    async fn test_compact_req_methods_normal_compaction() {
        // This case doesn't need directory to exist.
        let dir = "/tmp/test/compaction/test_compact_req_methods_normal_compaction";
        let opt = create_options(dir.to_string(), 1);

        {
            // Merge level files to next level.
            let version_sketch = VersionSketch::new(dir, Arc::new("t_d".to_string()), 1)
                .add(1, FileSketch(1, (11, 15), 10, false))
                .add(1, FileSketch(2, (16, 20), 10, false));
            let version = version_sketch.to_version(opt.storage.clone()).await;
            let mut req = CompactReq {
                compact_task: CompactTask::Normal(1),
                version: Arc::new(version),
                files: vec![],
                in_level: 1,
                out_level: 2,
                out_time_range: TimeRange::all(),
            };
            version_sketch
                .to_column_files(&opt.storage, &mut req.files, |_, _| true)
                .await;
        }

        {
            // Merge level files to next level.
            let version_sketch = VersionSketch::new(dir, Arc::new("t_d".to_string()), 1)
                .add(1, FileSketch(1, (11, 15), 10, false))
                .add(1, FileSketch(2, (16, 20), 10, false))
                .add(2, FileSketch(3, (1, 5), 5, false))
                .add(2, FileSketch(4, (6, 10), 5, false));
            let version = version_sketch.to_version(opt.storage.clone()).await;
            let mut req = CompactReq {
                compact_task: CompactTask::Normal(1),
                version: Arc::new(version),
                files: vec![],
                in_level: 1,
                out_level: 2,
                out_time_range: TimeRange::all(),
            };
            version_sketch
                .to_column_files(&opt.storage, &mut req.files, |l, _| l.0 == 1)
                .await;
            assert_eq!(req.out_time_range, TimeRange::all());
        }
    }
}
