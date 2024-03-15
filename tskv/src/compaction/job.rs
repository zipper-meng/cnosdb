use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{atomic, Arc};
use std::time::{Duration, Instant};

use flush::run_flush_memtable_job;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{oneshot, RwLock, RwLockWriteGuard, Semaphore};
use trace::{error, info};

use crate::compaction::{flush, CompactTask, FlushReq, LevelCompactionPicker, Picker};
use crate::summary::SummaryTask;
use crate::tseries_family::TseriesFamily;
use crate::{TsKvContext, TseriesFamilyId};

const COMPACT_BATCH_CHECKING_SECONDS: u64 = 1;

struct CompactTaskGroup {
    /// Maps CompactTask to the number of times it inserted.
    compact_tasks: HashMap<CompactTask, usize>,
}

impl CompactTaskGroup {
    fn insert(&mut self, task: CompactTask) {
        let num = self.compact_tasks.entry(task).or_default();
        *num += 1;
    }

    fn extend<T: IntoIterator<Item = CompactTask>>(&mut self, iter: T) {
        for task in iter {
            self.insert(task);
        }
    }

    fn try_take(&mut self) -> Option<HashMap<TseriesFamilyId, Vec<CompactTask>>> {
        if self.compact_tasks.is_empty() {
            return None;
        }
        let compact_tasks = std::mem::replace(&mut self.compact_tasks, HashMap::with_capacity(32));
        let mut grouped_compact_tasks: HashMap<TseriesFamilyId, Vec<CompactTask>> = HashMap::new();
        for task in compact_tasks.into_keys() {
            let vnode_id = task.ts_family_id();
            let tasks = grouped_compact_tasks.entry(vnode_id).or_default();
            tasks.push(task);
        }
        for tasks in grouped_compact_tasks.values_mut() {
            tasks.sort();
        }
        Some(grouped_compact_tasks)
    }

    fn is_empty(&self) -> bool {
        self.compact_tasks.is_empty()
    }
}

impl Default for CompactTaskGroup {
    fn default() -> Self {
        Self {
            compact_tasks: HashMap::with_capacity(32),
        }
    }
}

pub struct CompactJob {
    inner: Arc<RwLock<CompactJobInner>>,
}

impl CompactJob {
    pub fn new(runtime: Arc<Runtime>, ctx: Arc<TsKvContext>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(CompactJobInner::new(runtime, ctx))),
        }
    }

    pub async fn start_merge_compact_task_job(&self, compact_task_receiver: Receiver<CompactTask>) {
        self.inner
            .read()
            .await
            .start_merge_compact_task_job(compact_task_receiver);
    }

    pub async fn start_vnode_compaction_job(&self) {
        self.inner.read().await.start_vnode_compaction_job();
    }

    pub async fn prepare_stop_vnode_compaction_job(&self) -> StartVnodeCompactionGuard {
        info!("StopCompactionGuard(create):prepare stop vnode compaction job");
        let inner = self.inner.write().await;
        inner
            .enable_compaction
            .store(false, atomic::Ordering::SeqCst);
        StartVnodeCompactionGuard { inner }
    }
}

impl std::fmt::Debug for CompactJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactJob").finish()
    }
}

struct CompactJobInner {
    ctx: Arc<TsKvContext>,
    runtime: Arc<Runtime>,
    compact_task_group: Arc<RwLock<CompactTaskGroup>>,
    enable_compaction: Arc<AtomicBool>,
    running_compactions: Arc<AtomicUsize>,
}

impl CompactJobInner {
    fn new(runtime: Arc<Runtime>, ctx: Arc<TsKvContext>) -> Self {
        let compact_processor = Arc::new(RwLock::new(CompactTaskGroup::default()));

        Self {
            ctx,
            runtime,
            compact_task_group: compact_processor,
            enable_compaction: Arc::new(AtomicBool::new(false)),
            running_compactions: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn start_merge_compact_task_job(&self, mut compact_task_receiver: Receiver<CompactTask>) {
        info!("Compaction: start merge compact task job");
        let compact_task_group = self.compact_task_group.clone();
        self.runtime.spawn(async move {
            while let Some(compact_task) = compact_task_receiver.recv().await {
                compact_task_group.write().await.insert(compact_task);
            }
        });
    }

    fn start_vnode_compaction_job(&self) {
        info!("Compaction: start vnode compaction job");
        if self
            .enable_compaction
            .compare_exchange(
                false,
                true,
                atomic::Ordering::Acquire,
                atomic::Ordering::Relaxed,
            )
            .is_err()
        {
            info!("Compaction: failed to change enable_compaction from false to true, compaction is already started");
            return;
        }

        let runtime_inner = self.runtime.clone();
        let compact_task_group = self.compact_task_group.clone();
        let enable_compaction = self.enable_compaction.clone();
        let running_compaction = self.running_compactions.clone();
        let ctx = self.ctx.clone();

        self.runtime.spawn(async move {
            // TODO: Concurrent compactions should not over argument $cpu.
            let compaction_limit = Arc::new(Semaphore::new(
                ctx.options.storage.max_concurrent_compaction as usize,
            ));
            // Maps vnode_id to whether it's compacting.
            let vnode_compacting_map: Arc<RwLock<HashMap<TseriesFamilyId, bool>>> =
                Arc::new(RwLock::new(HashMap::new()));
            let mut check_interval =
                tokio::time::interval(Duration::from_secs(COMPACT_BATCH_CHECKING_SECONDS));

            loop {
                check_interval.tick().await;
                if !enable_compaction.load(atomic::Ordering::SeqCst) {
                    break;
                }
                if compact_task_group.read().await.compact_tasks.is_empty() {
                    continue;
                }

                let vnode_compact_tasks = match Self::group_vnode_compaction_tasks(
                    compact_task_group.clone(),
                    vnode_compacting_map.clone(),
                )
                .await
                {
                    Some(tasks) => tasks,
                    None => continue,
                };

                for (vnode_id, compact_tasks) in vnode_compact_tasks {
                    let vnode = match ctx
                        .version_set
                        .read()
                        .await
                        .get_tsfamily_by_tf_id(vnode_id)
                        .await
                    {
                        Some(vnode) => vnode,
                        None => continue,
                    };
                    // Method acquire_owned() will return AcquireError if the semaphore has been closed.
                    let permit = compaction_limit.clone().acquire_owned().await.unwrap();
                    let ctx = ctx.clone();
                    let vnode_compacting_map = vnode_compacting_map.clone();

                    runtime_inner.spawn()
                }

                let vnode_ids = compact_task_group.write().await.take();
                let vnode_ids_for_debug = vnode_ids.clone();
                let now = Instant::now();
                info!("Compacting on vnode(job start): {:?}", &vnode_ids_for_debug);
                for vnode_id in vnode_ids {
                    let ts_family = ctx
                        .version_set
                        .read()
                        .await
                        .get_tsfamily_by_tf_id(vnode_id)
                        .await;
                    if let Some(tsf) = ts_family {
                        info!("Starting compaction on ts_family {}", vnode_id);
                        let start = Instant::now();
                        if !tsf.read().await.can_compaction() {
                            info!("forbidden compaction on moving vnode {}", vnode_id);
                            return;
                        }
                        let picker = LevelCompactionPicker::new(ctx.options.storage.clone());
                        let version = tsf.read().await.version();
                        let compact_req = picker.pick_compaction(version);
                        if let Some(req) = compact_req {
                            let database = req.database.clone();
                            let compact_ts_family = req.ts_family_id;
                            let out_level = req.out_level;

                            // Method acquire_owned() will return AcquireError if the semaphore has been closed.
                            let permit = compaction_limit.clone().acquire_owned().await.unwrap();
                            let enable_compaction = enable_compaction.clone();
                            let running_compaction = running_compaction.clone();

                            let ctx = ctx.clone();
                            runtime_inner.spawn(async move {
                                // Check enable compaction
                                if !enable_compaction.load(atomic::Ordering::SeqCst) {
                                    return;
                                }
                                // Edit running_compation
                                running_compaction.fetch_add(1, atomic::Ordering::SeqCst);
                                let _sub_running_compaction_guard = DeferGuard(Some(|| {
                                    running_compaction.fetch_sub(1, atomic::Ordering::SeqCst);
                                }));

                                match super::run_compaction_job(req, ctx.global_ctx.clone()).await {
                                    Ok(Some((version_edit, file_metas))) => {
                                        metrics::incr_compaction_success();
                                        let (summary_tx, _summary_rx) = oneshot::channel();
                                        let _ = ctx
                                            .summary_task_sender
                                            .send(SummaryTask::new(
                                                tsf.clone(),
                                                version_edit,
                                                Some(file_metas),
                                                None,
                                                summary_tx,
                                            ))
                                            .await;

                                        metrics::sample_tskv_compaction_duration(
                                            database.as_str(),
                                            compact_ts_family.to_string().as_str(),
                                            out_level.to_string().as_str(),
                                            start.elapsed().as_secs_f64(),
                                        )
                                        // TODO Handle summary result using summary_rx.
                                    }
                                    Ok(None) => {
                                        info!("There is nothing to compact.");
                                    }
                                    Err(e) => {
                                        metrics::incr_compaction_failed();
                                        error!("Compaction job failed: {:?}", e);
                                    }
                                }
                                drop(permit);
                            });
                        }
                    }
                }
                info!(
                    "Compacting on vnode(job start): {:?} costs {} sec",
                    vnode_ids_for_debug,
                    now.elapsed().as_secs()
                );
            }
        });
    }

    /// Get vnode_id maps to compact tasks, and mark vnodes compacting.
    async fn group_vnode_compaction_tasks(
        compact_task_group: Arc<RwLock<CompactTaskGroup>>,
        vnode_compacting_map: Arc<RwLock<HashMap<TseriesFamilyId, bool>>>,
    ) -> Option<HashMap<TseriesFamilyId, Vec<CompactTask>>> {
        let mut task_group = compact_task_group.write().await;
        let mut vnode_compacting = vnode_compacting_map.write().await;
        // Consume the compact tasks group.
        match task_group.try_take() {
            Some(mut vnode_tasks) => {
                let vnode_ids = vnode_tasks.keys().cloned().collect::<Vec<_>>();
                for vnode_id in vnode_ids.iter() {
                    match vnode_compacting.get(vnode_id) {
                        Some(true) => {
                            // If vnode is compacting, put the tasks back to the compact task group.
                            trace::trace!("vnode {vnode_id} is compacting, skip this time");
                            if let Some(tasks) = vnode_tasks.remove(vnode_id) {
                                // Put the tasks back to the compact task group.
                                task_group.extend(tasks)
                            }
                        }
                        _ => {
                            // If vnode is not compacting, mark it as compacting.
                            vnode_compacting.insert(*vnode_id, true);
                        }
                    }
                }
                Some(vnode_tasks)
            }
            None => None,
        }
    }

    async fn run_vnode_compaction_tasks(
        vnode_id: TseriesFamilyId,
        vnode: Arc<RwLock<TseriesFamily>>,
        compact_tasks: Vec<CompactTask>,
    ) {
        for compact_task in compact_tasks {
            info!("Starting compaction: {compact_task}");
            let start = Instant::now();

            let version = {
                let vnode_rlock = vnode.read().await;
                if !vnode_rlock.can_compaction() {
                    info!("forbidden compaction on moving vnode {vnode_id}",);
                    return;
                }
                vnode_rlock.version()
            };
            let compact_req = match picker::pick_compaction(compact_task, version).await {
                Some(req) => req,
                None => {
                    info!("Finished compaction, did nothing");
                    continue;
                }
            };
            let database = compact_req.version.database();
            let in_level = compact_req.in_level;
            let out_level = compact_req.out_level;

            info!("Running compaction job: {compact_task}, sending to summary write.");
            match super::run_compaction_job(compact_req, ctx.clone()).await {
                Ok(Some((version_edit, file_metas))) => {
                    info!("Finished compaction1: {compact_task}, sending to summary write.");
                    metrics::incr_compaction_success();
                    let (summary_tx, summary_rx) = oneshot::channel();
                    let _ = summary_task_sender
                        .send(SummaryTask::new(
                            vec![version_edit.clone()],
                            Some(file_metas),
                            None,
                            summary_tx,
                        ))
                        .await;

                    metrics::sample_tskv_compaction_duration(
                        database.as_str(),
                        vnode_id.to_string().as_str(),
                        in_level.to_string().as_str(),
                        out_level.to_string().as_str(),
                        start.elapsed().as_secs_f64(),
                    );
                    info!("Finished compaction2: {compact_task}, waiting for summary write.");
                    match summary_rx.await {
                        Ok(Ok(())) => {
                            info!("Compaction: summary write success: {version_edit:?}");
                        }
                        Ok(Err(e)) => {
                            error!("Compaction: Failed to write summary: {}", e);
                        }
                        Err(e) => {
                            error!("Compaction: Failed to receive summary write task: {}", e);
                        }
                    }
                    // Send a normal compact request if it's a delta compaction.
                    if let CompactTask::Delta(vnode_id) = &compact_task {
                        let _ = compact_task_sender
                            .send(CompactTask::Normal(*vnode_id))
                            .await;
                    }
                }
                Ok(None) => {
                    info!("Compaction There is nothing to compact.");
                }
                Err(e) => {
                    metrics::incr_compaction_failed();
                    error!("Compaction: job failed: {}", e);
                }
            }
        }
    }

    pub async fn prepare_stop_vnode_compaction_job(&self) {
        self.enable_compaction
            .store(false, atomic::Ordering::SeqCst);
    }

    async fn wait_stop_vnode_compaction_job(&self) {
        let mut check_interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            check_interval.tick().await;
            if self.running_compactions.load(atomic::Ordering::SeqCst) == 0 {
                return;
            }
        }
    }
}

pub struct StartVnodeCompactionGuard<'a> {
    inner: RwLockWriteGuard<'a, CompactJobInner>,
}

impl<'a> StartVnodeCompactionGuard<'a> {
    pub async fn wait(&self) {
        info!("StopCompactionGuard(wait): wait vnode compaction job to stop");
        self.inner.wait_stop_vnode_compaction_job().await
    }
}

impl<'a> Drop for StartVnodeCompactionGuard<'a> {
    fn drop(&mut self) {
        info!("StopCompactionGuard(drop): start vnode compaction job");
        self.inner.start_vnode_compaction_job();
    }
}

pub struct DeferGuard<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> Drop for DeferGuard<F> {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f()
        }
    }
}

pub struct FlushJob {
    ctx: Arc<TsKvContext>,
    runtime: Arc<Runtime>,
}

impl FlushJob {
    pub fn new(runtime: Arc<Runtime>, ctx: Arc<TsKvContext>) -> Self {
        Self { ctx, runtime }
    }

    pub fn start_vnode_flush_job(&self, mut flush_req_receiver: Receiver<FlushReq>) {
        let runtime_inner = self.runtime.clone();
        let ctx = self.ctx.clone();
        self.runtime.spawn(async move {
            while let Some(x) = flush_req_receiver.recv().await {
                // TODO(zipper): this make config `flush_req_channel_cap` wasted
                // Run flush job and trigger compaction.
                runtime_inner.spawn(run_flush_memtable_job(x, ctx.clone(), true));
            }
        });
        info!("Flush task handler started");
    }
}

impl std::fmt::Debug for FlushJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlushJob").finish()
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{self, AtomicI32};
    use std::sync::Arc;

    use super::DeferGuard;
    use crate::compaction::job::CompactTaskGroup;
    use crate::TseriesFamilyId;

    #[test]
    fn test_build_compact_batch() {
        let mut compact_batch_builder = CompactTaskGroup::default();
        compact_batch_builder.insert(1);
        compact_batch_builder.insert(2);
        compact_batch_builder.insert(1);
        compact_batch_builder.insert(3);
        assert_eq!(compact_batch_builder.compact_tasks.len(), 3);

        let mut keys: Vec<TseriesFamilyId> = compact_batch_builder.compact_tasks.clone();
        keys.sort();
        assert_eq!(keys, vec![1, 2, 3]);

        let vnode_ids = compact_batch_builder.take();
        assert_eq!(vnode_ids, vec![1, 2, 3]);
    }

    #[test]
    fn test_defer_guard() {
        let a = Arc::new(AtomicI32::new(0));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        {
            let a = a.clone();
            let jh = runtime.spawn(async move {
                a.fetch_add(1, atomic::Ordering::SeqCst);
                let _guard = DeferGuard(Some(|| {
                    a.fetch_sub(1, atomic::Ordering::SeqCst);
                }));
                a.fetch_add(1, atomic::Ordering::SeqCst);
                a.fetch_add(1, atomic::Ordering::SeqCst);

                assert_eq!(a.load(atomic::Ordering::SeqCst), 3);
            });
            let _ = runtime.block_on(jh);
        }
        assert_eq!(a.load(atomic::Ordering::SeqCst), 2);
    }
}
