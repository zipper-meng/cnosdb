use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Receiver;

use crate::background_task::BackgroundTask;
use crate::TseriesFamilyId;

#[derive(Default, Debug)]
pub struct GlobalContext {
    file_id: AtomicU64,
    mem_seq: AtomicU64,
    last_seq: AtomicU64,
}

impl GlobalContext {
    pub fn new() -> Self {
        Self {
            file_id: AtomicU64::new(0),
            mem_seq: AtomicU64::new(0),
            last_seq: AtomicU64::new(0),
        }
    }
}

impl GlobalContext {
    pub fn file_id(&self) -> u64 {
        self.file_id.load(Ordering::Acquire)
    }

    /// # Examples
    ///  ```ignore
    ///  assert_eq!(foo.file_id_next(), 0);
    ///  assert_eq!(foo.file_id(), 1);
    /// ```
    pub fn file_id_next(&self) -> u64 {
        self.file_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn mem_seq_next(&self) -> u64 {
        self.mem_seq.fetch_add(1, Ordering::SeqCst)
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seq.load(Ordering::Acquire)
    }

    pub fn fetch_add_log_seq(&self, n: u64) -> u64 {
        self.file_id.fetch_add(n, Ordering::SeqCst)
    }

    pub fn set_last_seq(&self, v: u64) {
        self.last_seq.store(v, Ordering::Release);
    }
    pub fn set_file_id(&self, v: u64) {
        self.file_id.store(v, Ordering::Release);
    }

    pub fn mark_log_number_used(&self, v: u64) {
        let mut old = self.file_id.load(Ordering::Acquire);
        while old <= v {
            match self
                .file_id
                .compare_exchange(old, v + 1, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(x) => old = x,
            }
        }
    }
}

#[derive(Debug)]
pub struct GlobalSequenceContext {
    min_seq: AtomicU64,
    inner: Arc<RwLock<GlobalSequenceContextInner>>,
}

impl GlobalSequenceContext {
    pub fn new(min_seq: u64, tsf_seq_map: HashMap<TseriesFamilyId, u64>) -> Self {
        Self {
            min_seq: AtomicU64::new(min_seq),
            inner: Arc::new(RwLock::new(GlobalSequenceContextInner {
                min_seq,
                tsf_seq_map,
            })),
        }
    }

    /// Write lock the inner, apply arguments, then update atomic min_seq.
    pub fn next_stage(
        &self,
        del_ts_family: HashSet<TseriesFamilyId>,
        ts_family_min_seq: HashMap<TseriesFamilyId, u64>,
    ) {
        let mut inner = self.inner.write();

        if del_ts_family.is_empty() {
            let mut tsf_min_seq = if inner.tsf_seq_map.is_empty() {
                u64::MAX
            } else {
                inner.min_seq
            };
            for (tsf_id, min_seq) in ts_family_min_seq {
                inner.tsf_seq_map.insert(tsf_id, min_seq);
                tsf_min_seq = tsf_min_seq.min(min_seq);
            }
            inner.min_seq = tsf_min_seq;
        } else {
            for tsf_id in del_ts_family {
                inner.tsf_seq_map.remove(&tsf_id);
            }
            for (tsf_id, min_seq) in ts_family_min_seq {
                inner.tsf_seq_map.insert(tsf_id, min_seq);
            }
            inner.reset_min_seq();
        }
        self.min_seq.store(inner.min_seq, Ordering::Release);
    }

    pub fn min_seq(&self) -> u64 {
        self.min_seq.load(Ordering::Acquire)
    }
}

#[cfg(test)]
impl GlobalSequenceContext {
    pub fn empty() -> Arc<Self> {
        Arc::new(Self {
            min_seq: AtomicU64::new(0),
            inner: Arc::new(RwLock::new(GlobalSequenceContextInner {
                min_seq: 0,
                tsf_seq_map: HashMap::new(),
            })),
        })
    }

    pub fn set_min_seq(&self, min_seq: u64) {
        let mut inner = self.inner.write();
        inner.min_seq = min_seq;
        self.min_seq.store(inner.min_seq, Ordering::Release);
    }
}

#[derive(Debug)]
struct GlobalSequenceContextInner {
    /// Minimum sequence number of all `TseriesFamily`s in all `Database`s
    min_seq: u64,
    /// Maps `TseriesFamily`-ID to it's last sequence number flushed to disk.
    tsf_seq_map: HashMap<TseriesFamilyId, u64>,
}

impl GlobalSequenceContextInner {
    /// Reset min_seq using current tsf_seq_map.
    fn reset_min_seq(&mut self) {
        if self.tsf_seq_map.is_empty() {
            self.min_seq = 0;
        } else {
            let mut min_seq = u64::MAX;
            for (_, v) in self.tsf_seq_map.iter() {
                min_seq = min_seq.min(*v);
            }
            self.min_seq = min_seq;
        }
    }
}

pub struct GlobalSequenceTask {
    pub del_ts_family: HashSet<TseriesFamilyId>,
    pub ts_family_min_seq: HashMap<TseriesFamilyId, u64>,
}

/// Start a async job to maintain GlobalSequenceCOntext.
pub(crate) fn run_global_context_job(
    runtime: Arc<Runtime>,
    mut receiver: Receiver<GlobalSequenceTask>,
    global_sequence_context: Arc<GlobalSequenceContext>,
) -> BackgroundTask<()> {
    let jh = runtime.spawn(async move {
        while let Some(t) = receiver.recv().await {
            global_sequence_context.next_stage(t.del_ts_family, t.ts_family_min_seq);
        }
    });
    BackgroundTask::new("global_sequence".to_string(), jh, false)
}

#[cfg(test)]
mod test_context {
    use std::collections::{HashMap, HashSet};

    use super::GlobalSequenceContext;

    #[test]
    fn test_global_sequence_context() {
        let ctx = GlobalSequenceContext::empty();
        assert_eq!(ctx.min_seq(), 0);

        // Add tsfamily, no delete tsfamily
        ctx.next_stage(HashSet::new(), HashMap::from([(1, 2), (2, 10), (3, 5)]));
        assert_eq!(ctx.min_seq(), 2);

        // Delete tsfamily, no add tsfamily
        ctx.next_stage(HashSet::from([1, 2]), HashMap::new());
        assert_eq!(ctx.min_seq(), 5);

        // Delete tsfamily and add tsfamily
        ctx.next_stage(HashSet::from([3]), HashMap::from([(4, 6), (5, 8), (6, 10)]));
        assert_eq!(ctx.min_seq(), 6);
    }
}
