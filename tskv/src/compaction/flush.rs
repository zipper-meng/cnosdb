use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::oneshot;
use tokio::time::timeout;
use trace::{error, info, warn};
use utils::BloomFilter;

use crate::compaction::{CompactTask, FlushReq};
use crate::context::GlobalContext;
use crate::error::Result;
use crate::memcache::MemCache;
use crate::summary::{CompactMetaBuilder, SummaryTask, VersionEdit};
use crate::tsm::writer::TsmWriter;
use crate::{ColumnFileId, Error, TsKvContext, TseriesFamilyId};

pub struct FlushTask {
    ts_family_id: TseriesFamilyId,
    mem_caches: Vec<Arc<RwLock<MemCache>>>,
    low_seq_no: u64,
    high_seq_no: u64,
    global_context: Arc<GlobalContext>,
    path_delta: PathBuf,
}

impl FlushTask {
    pub fn new(
        ts_family_id: TseriesFamilyId,
        mem_caches: Vec<Arc<RwLock<MemCache>>>,
        low_seq_no: u64,
        high_seq_no: u64,
        global_context: Arc<GlobalContext>,
        path_delta: impl AsRef<Path>,
    ) -> Self {
        Self {
            ts_family_id,
            mem_caches,
            low_seq_no,
            high_seq_no,
            global_context,
            path_delta: path_delta.as_ref().into(),
        }
    }

    pub async fn run(self, edit: &mut VersionEdit) -> Result<HashMap<u64, Arc<BloomFilter>>> {
        let mut tsm_writer = TsmWriter::open(
            &self.path_delta,
            self.global_context.file_id_next(),
            0,
            true,
        )
        .await?;
        let mut file_metas = HashMap::new();

        for memcache in self.mem_caches {
            let group = memcache.read().to_chunk_group()?;
            tsm_writer.write_data(group).await?;
        }

        let compact_meta_builder = CompactMetaBuilder::new(self.ts_family_id);

        tsm_writer.finish().await?;
        file_metas.insert(
            tsm_writer.file_id(),
            Arc::new(tsm_writer.series_bloom_filter().clone()),
        );
        let tsm_meta = compact_meta_builder.build(
            tsm_writer.file_id(),
            tsm_writer.size() as u64,
            0,
            tsm_writer.min_ts(),
            tsm_writer.max_ts(),
        );
        edit.add_file(tsm_meta);

        Ok(file_metas)
    }
}

pub async fn run_flush_memtable_job(
    req: FlushReq,
    ctx: Arc<TsKvContext>,
    trigger_compact: bool,
) -> Result<()> {
    let req_str = format!("{req}");
    info!("Flush: running: {req_str}");

    let mut file_metas: HashMap<ColumnFileId, Arc<BloomFilter>> = HashMap::new();

    let tsf = ctx
        .version_set
        .read()
        .await
        .get_tsfamily_by_tf_id(req.ts_family_id)
        .await
        .ok_or(Error::VnodeNotFound {
            vnode_id: req.ts_family_id,
        })?;

    // todo: build path by vnode data
    let path_delta = {
        let tsf_rlock = tsf.read().await;
        tsf_rlock.update_last_modified().await;
        tsf_rlock
            .storage_opt()
            .delta_dir(tsf_rlock.tenant_database().as_str(), req.ts_family_id)
    };

    let flush_task = FlushTask::new(
        req.ts_family_id,
        req.mems.clone(),
        req.low_seq_no,
        req.high_seq_no,
        ctx.global_ctx.clone(),
        path_delta,
    );

    let mut version_edit =
        VersionEdit::new_update_vnode(req.ts_family_id, req.owner, req.high_seq_no);
    if let Ok(fm) = flush_task.run(&mut version_edit).await {
        file_metas = fm;
    }

    tsf.read().await.update_last_modified().await;
    if trigger_compact {
        let _ = ctx
            .compact_task_sender
            .send(CompactTask::Delta(req.ts_family_id))
            .await;
    }

    info!(
        "Flush: completed: {req_str}, version edit: {:?}",
        version_edit
    );

    let (task_state_sender, task_state_receiver) = oneshot::channel();
    let task = SummaryTask::new(
        tsf.clone(),
        version_edit,
        Some(file_metas),
        Some(req.mems),
        task_state_sender,
    );

    if let Err(e) = ctx.summary_task_sender.send(task).await {
        warn!("Flush: failed to send summary task for {req_str}: {e}",);
    }

    if timeout(Duration::from_secs(10), task_state_receiver)
        .await
        .is_err()
    {
        error!("Flush: failed to receive summary task result in 10 seconds for {req_str}",);
    }

    Ok(())
}

#[cfg(test)]
pub mod flush_tests {
    use minivec::MiniVec;
    use models::codec::Encoding;
    use models::schema::{ColumnType, TableColumn, TskvTableSchema};
    use models::{ColumnId, ValueType};
    use utils::dedup_front_by_key;

    pub fn default_table_schema(ids: Vec<ColumnId>) -> TskvTableSchema {
        let fields = ids
            .into_iter()
            .map(|id| TableColumn {
                id,
                name: format!("col_{id}"),
                column_type: ColumnType::Field(ValueType::Unknown),
                encoding: Encoding::Default,
            })
            .collect();

        TskvTableSchema::new(
            "cnosdb".to_string(),
            "public".to_string(),
            "".to_string(),
            fields,
        )
    }

    #[test]
    fn test_sort_dedup() {
        {
            let mut data = vec![(1, 11), (1, 12), (2, 21), (3, 3), (2, 22), (4, 41), (4, 42)];
            data.sort_by_key(|a| a.0);
            assert_eq!(
                &data,
                &vec![(1, 11), (1, 12), (2, 21), (2, 22), (3, 3), (4, 41), (4, 42)]
            );
            dedup_front_by_key(&mut data, |a| a.0);
            assert_eq!(&data, &vec![(1, 12), (2, 22), (3, 3), (4, 42)]);
        }
        {
            // Test dedup-front for list with no duplicated key.
            let mut data: Vec<(i32, MiniVec<u8>)> = vec![
                (1, "a1".into()),
                (2, "b2".into()),
                (3, "c3".into()),
                (4, "d4".into()),
            ];
            data.sort_by_key(|a| a.0);
            dedup_front_by_key(&mut data, |a| a.0);
            assert_eq!(
                &data,
                &vec![
                    (1, "a1".into()),
                    (2, "b2".into()),
                    (3, "c3".into()),
                    (4, "d4".into()),
                ]
            );
        }
        {
            // Test dedup-front for list with only one key.
            let mut data: Vec<(i32, MiniVec<u8>)> = vec![
                (1, "a1".into()),
                (1, "a2".into()),
                (1, "a3".into()),
                (1, "a4".into()),
            ];
            dedup_front_by_key(&mut data, |a| a.0);
            assert_eq!(&data, &vec![(1, "a4".into()),]);
        }
        {
            // Test dedup-front for list with shuffled multiply duplicated key.
            let mut data: Vec<(i32, MiniVec<u8>)> = vec![
                (1, "a1".into()),
                (1, "b1".into()),
                (2, "c2".into()),
                (3, "d3".into()),
                (2, "e2".into()),
                (4, "e4".into()),
                (4, "f4".into()),
            ];
            data.sort_by_key(|a| a.0);
            dedup_front_by_key(&mut data, |a| a.0);
            assert_eq!(
                &data,
                &vec![
                    (1, "b1".into()),
                    (2, "e2".into()),
                    (3, "d3".into()),
                    (4, "f4".into()),
                ]
            );
        }
    }

    #[tokio::test]
    async fn test_flush_tsm2() {
        // todo!("test_flush");
    }
}
