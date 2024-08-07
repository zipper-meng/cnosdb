static METRIC_COMPACTION_TOTAL: &str = "wal_file_num";

pub fn register_metric() {
    let metric =
        prometheus::IntCounter::new(METRIC_COMPACTION_TOTAL, "Total number of wal files").unwrap();
    prometheus::register(Box::new(metric)).unwrap();
}
