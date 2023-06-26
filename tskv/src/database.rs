use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;

use lru_cache::asynchronous::ShardedCache;
use memory_pool::MemoryPoolRef;
use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
use models::meta_data::VnodeId;
use models::predicate::domain::TimeRange;
use models::schema::{DatabaseSchema, Precision, TskvTableSchema};
use models::utils::unite_id;
use models::{ColumnId, SchemaId, SeriesId, SeriesKey, Timestamp};
use protos::models::{FieldType, Point, Table};
use protos::{fb_table_name, schema_field_name, schema_field_type, schema_tag_name};
use snafu::ResultExt;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Sender;
use tokio::sync::{oneshot, RwLock};
use trace::{debug, error, info};
use utils::BloomFilter;

use crate::compaction::{CompactTask, FlushReq};
use crate::context::GlobalContext;
use crate::error::{self, Result, SchemaSnafu};
use crate::index::{self, IndexResult};
use crate::kv_option::{Options, INDEX_PATH};
use crate::memcache::{MemCache, RowData, RowGroup};
use crate::schema::schemas::DBschemas;
use crate::summary::{SummaryTask, VersionEdit};
use crate::tseries_family::{LevelInfo, TseriesFamily, Version};
use crate::version_set::VersionSet;
use crate::Error::{self, InvalidPoint};
use crate::{file_utils, ColumnFileId};

pub type FlatBufferPoint<'a> = flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Point<'a>>>;
pub type FlatBufferTable<'a> = flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Table<'a>>>;

#[derive(Debug)]
pub struct Database {
    //tenant_name.database_name => owner
    owner: Arc<String>,
    opt: Arc<Options>,

    schemas: Arc<DBschemas>,
    ts_indexes: HashMap<VnodeId, Arc<index::ts_index::TSIndex>>,
    ts_families: HashMap<VnodeId, Arc<RwLock<TseriesFamily>>>,
    runtime: Arc<Runtime>,
    memory_pool: MemoryPoolRef,
    metrics_register: Arc<MetricsRegister>,
}

impl Database {
    pub async fn new(
        schema: DatabaseSchema,
        opt: Arc<Options>,
        runtime: Arc<Runtime>,
        meta: MetaRef,
        memory_pool: MemoryPoolRef,
        metrics_register: Arc<MetricsRegister>,
    ) -> Result<Self> {
        let db = Self {
            opt,
            owner: Arc::new(schema.owner()),
            schemas: Arc::new(DBschemas::new(schema, meta).await.context(SchemaSnafu)?),
            ts_indexes: HashMap::new(),
            ts_families: HashMap::new(),
            runtime,
            memory_pool,
            metrics_register,
        };

        Ok(db)
    }

    pub fn open_tsfamily(
        &mut self,
        ver: Arc<Version>,
        flush_task_sender: Sender<FlushReq>,
        compact_task_sender: Sender<CompactTask>,
    ) {
        let tf = TseriesFamily::new(
            ver.tf_id(),
            ver.database(),
            MemCache::new(
                ver.tf_id(),
                self.opt.cache.max_buffer_size,
                ver.last_seq,
                &self.memory_pool,
            ),
            ver.clone(),
            self.opt.cache.clone(),
            self.opt.storage.clone(),
            flush_task_sender,
            compact_task_sender,
            self.memory_pool.clone(),
            &self.metrics_register,
        );
        tf.schedule_compaction(self.runtime.clone());

        self.ts_families
            .insert(ver.tf_id(), Arc::new(RwLock::new(tf)));
    }

    // todo: Maybe TseriesFamily::new() should be refactored.
    #[allow(clippy::too_many_arguments)]
    pub async fn add_tsfamily(
        &mut self,
        tsf_id: u32,
        seq_no: u64,
        version_edit: Option<VersionEdit>,
        summary_task_sender: Sender<SummaryTask>,
        flush_task_sender: Sender<FlushReq>,
        compact_task_sender: Sender<CompactTask>,
        global_ctx: Arc<GlobalContext>,
    ) -> Result<Arc<RwLock<TseriesFamily>>> {
        let (seq_no, version_edits, file_metas) = match version_edit {
            Some(mut ve) => {
                ve.tsf_id = tsf_id;
                ve.has_seq_no = true;
                ve.seq_no = seq_no;
                let mut file_metas = HashMap::with_capacity(ve.add_files.len());
                for f in ve.add_files.iter_mut() {
                    let new_file_id = global_ctx.file_id_next();
                    f.tsf_id = tsf_id;
                    let file_path = f
                        .rename_file(&self.opt.storage, &self.owner, f.tsf_id, new_file_id)
                        .await
                        .context(error::FileSystemSnafu)?;
                    let file_reader = crate::tsm::TsmReader::open(file_path)
                        .await
                        .context(error::TsmSnafu)?;
                    file_metas.insert(new_file_id, file_reader.bloom_filter());
                }
                for f in ve.del_files.iter_mut() {
                    let new_file_id = global_ctx.file_id_next();
                    f.tsf_id = tsf_id;
                    f.rename_file(&self.opt.storage, &self.owner, f.tsf_id, new_file_id)
                        .await
                        .context(error::FileSystemSnafu)?;
                }
                //move index
                let origin_index = self
                    .opt
                    .storage
                    .move_dir(&self.owner, tsf_id)
                    .join(INDEX_PATH);
                let new_index = self.opt.storage.index_dir(&self.owner, tsf_id);
                info!("move index from {:?} to {:?}", &origin_index, &new_index);
                file_utils::rename(origin_index, new_index).await?;
                tokio::fs::remove_dir_all(self.opt.storage.move_dir(&self.owner, tsf_id)).await?;
                (ve.seq_no, vec![ve], Some(file_metas))
            }
            None => (
                seq_no,
                vec![VersionEdit::new_add_vnode(
                    tsf_id,
                    self.owner.as_ref().clone(),
                    seq_no,
                )],
                None,
            ),
        };

        let ver = Arc::new(Version::new(
            tsf_id,
            self.owner.clone(),
            self.opt.storage.clone(),
            seq_no,
            LevelInfo::init_levels(self.owner.clone(), tsf_id, self.opt.storage.clone()),
            i64::MIN,
            Arc::new(ShardedCache::with_capacity(
                self.opt.storage.max_cached_readers,
            )),
        ));
        let tf = TseriesFamily::new(
            tsf_id,
            self.owner.clone(),
            MemCache::new(
                tsf_id,
                self.opt.cache.max_buffer_size,
                seq_no,
                &self.memory_pool,
            ),
            ver,
            self.opt.cache.clone(),
            self.opt.storage.clone(),
            flush_task_sender,
            compact_task_sender,
            self.memory_pool.clone(),
            &self.metrics_register,
        );

        let tf = Arc::new(RwLock::new(tf));
        self.ts_families.insert(tsf_id, tf.clone());

        let (task_state_sender, _task_state_receiver) = oneshot::channel();
        let task = SummaryTask::new(version_edits, file_metas, None, task_state_sender);
        if let Err(e) = summary_task_sender.send(task).await {
            error!("failed to send Summary task, {:?}", e);
        }

        Ok(tf)
    }

    pub async fn del_tsfamily(&mut self, tf_id: u32, summary_task_sender: Sender<SummaryTask>) {
        if let Some(tf) = self.ts_families.remove(&tf_id) {
            tf.read().await.close();
        }

        let edits = vec![VersionEdit::new_del_vnode(tf_id)];
        let (task_state_sender, _task_state_receiver) = oneshot::channel();
        let task = SummaryTask::new(edits, None, None, task_state_sender);
        if let Err(e) = summary_task_sender.send(task).await {
            error!("failed to send Summary task, {:?}", e);
        }
    }

    pub async fn build_write_group(
        &self,
        db_name: &str,
        precision: Precision,
        tables: FlatBufferTable<'_>,
        ts_index: Arc<index::ts_index::TSIndex>,
    ) -> Result<HashMap<(SeriesId, SchemaId), RowGroup>> {
        if self.opt.storage.strict_write {
            self.build_write_group_strict_mode(db_name, precision, tables, ts_index)
                .await
        } else {
            self.build_write_group_loose_mode(db_name, precision, tables, ts_index)
                .await
        }
    }

    pub async fn build_write_group_strict_mode(
        &self,
        db_name: &str,
        precision: Precision,
        tables: FlatBufferTable<'_>,
        ts_index: Arc<index::ts_index::TSIndex>,
    ) -> Result<HashMap<(SeriesId, SchemaId), RowGroup>> {
        // (series id, schema id) -> RowGroup
        let mut map = HashMap::new();
        for table in tables {
            let table_name = fb_table_name(&table)?;

            let points = table.points().ok_or(Error::CommonError {
                reason: "table missing points".to_string(),
            })?;

            for point in points {
                let schema = table.schema().ok_or(Error::CommonError {
                    reason: "table missing schema in point".to_string(),
                })?;
                let field_type = schema_field_type(&schema)?;
                let field_names = schema_field_name(&schema)?;
                let tag_names = schema_tag_name(&schema)?;

                let sid =
                    Self::build_index(db_name, &table_name, &point, &tag_names, ts_index.clone())
                        .await?;
                self.build_row_data(
                    &mut map,
                    &table_name,
                    precision,
                    point,
                    &field_names,
                    &field_type,
                    sid,
                )?
            }
        }
        Ok(map)
    }

    pub async fn build_write_group_loose_mode(
        &self,
        db_name: &str,
        precision: Precision,
        tables: FlatBufferTable<'_>,
        ts_index: Arc<index::ts_index::TSIndex>,
    ) -> Result<HashMap<(SeriesId, SchemaId), RowGroup>> {
        let mut map = HashMap::new();
        for table in tables {
            let table_name = fb_table_name(&table)?;

            let points = table.points().ok_or(Error::CommonError {
                reason: "table missing points".to_string(),
            })?;

            for point in points {
                let schema = table.schema().ok_or(Error::CommonError {
                    reason: "table missing schema in point".to_string(),
                })?;
                let tag_names = schema_tag_name(&schema)?;
                let field_names = schema_field_name(&schema)?;
                let field_type = schema_field_type(&schema)?;

                let sid =
                    Self::build_index(db_name, &table_name, &point, &tag_names, ts_index.clone())
                        .await?;
                if self
                    .schemas
                    .check_field_type_from_cache(&table_name, &tag_names, &field_names, &field_type)
                    .is_err()
                {
                    self.schemas
                        .check_field_type_or_else_add(
                            db_name,
                            &table_name,
                            &tag_names,
                            &field_names,
                            &field_type,
                        )
                        .await?;
                }

                self.build_row_data(
                    &mut map,
                    &table_name,
                    precision,
                    point,
                    &field_names,
                    &field_type,
                    sid,
                )?
            }
        }
        Ok(map)
    }

    fn build_row_data(
        &self,
        map: &mut HashMap<(SeriesId, SchemaId), RowGroup>,
        table_name: &str,
        precision: Precision,
        point: Point,
        field_names: &[&str],
        field_type: &[FieldType],
        sid: u32,
    ) -> Result<()> {
        let table_schema = self.schemas.get_table_schema(table_name)?;
        let table_schema = match table_schema {
            Some(v) => v,
            None => return Ok(()),
        };

        let row =
            RowData::point_to_row_data(point, &table_schema, precision, field_names, field_type)?;
        let schema_size = table_schema.size();
        let schema_id = table_schema.schema_id;
        let entry = map.entry((sid, schema_id)).or_insert(RowGroup {
            schema: Arc::new(TskvTableSchema::default()),
            rows: vec![],
            range: TimeRange {
                min_ts: i64::MAX,
                max_ts: i64::MIN,
            },
            size: size_of::<RowGroup>(),
        });
        entry.schema = table_schema;
        entry.size += schema_size;
        entry.range.merge(&TimeRange {
            min_ts: row.ts,
            max_ts: row.ts,
        });
        entry.size += row.size();
        //todo: remove this copy
        entry.rows.push(row);
        Ok(())
    }

    async fn build_index(
        db_name: &str,
        tab_name: &str,
        info: &Point<'_>,
        tag_names: &[&str],
        ts_index: Arc<index::ts_index::TSIndex>,
    ) -> Result<u32> {
        let nullbits = info.fields_nullbit().ok_or(InvalidPoint)?.bytes();
        if !nullbits.iter().any(|x| *x != 0) {
            return Err(InvalidPoint);
        }

        let series_key =
            SeriesKey::build_series_key(db_name, tab_name, tag_names, info).map_err(|e| {
                Error::CommonError {
                    reason: e.to_string(),
                }
            })?;

        if let Some(id) = ts_index.get_series_id(&series_key).await? {
            return Ok(id);
        }

        let id = ts_index.add_series_if_not_exists(&series_key).await?;

        Ok(id)
    }

    /// Snashots last version before `last_seq` of this database's all vnodes
    /// or specified vnode by `vnode_id`.
    ///
    /// Generated version data will be inserted into `version_edits` and `file_metas`.
    ///
    /// - `version_edits` are for all vnodes and db-files,
    /// - `file_metas` is for index data
    /// (field-id filter) of db-files.
    pub async fn snapshot(
        &self,
        vnode_id: Option<VnodeId>,
        version_edits: &mut Vec<VersionEdit>,
        file_metas: &mut HashMap<ColumnFileId, Arc<BloomFilter>>,
    ) {
        if let Some(tsf_id) = vnode_id.as_ref() {
            if let Some(tsf) = self.ts_families.get(tsf_id) {
                let ve = tsf.read().await.snapshot(self.owner.clone(), file_metas);
                version_edits.push(ve);
            }
        } else {
            for tsf in self.ts_families.values() {
                let ve = tsf.read().await.snapshot(self.owner.clone(), file_metas);
                version_edits.push(ve);
            }
        }
    }

    pub async fn get_series_key(&self, vnode_id: u32, sid: u32) -> IndexResult<Option<SeriesKey>> {
        if let Some(idx) = self.get_ts_index(vnode_id) {
            return idx.get_series_key(sid).await;
        }

        Ok(None)
    }

    pub fn get_table_schema(&self, table_name: &str) -> Result<Option<Arc<TskvTableSchema>>> {
        Ok(self.schemas.get_table_schema(table_name)?)
    }

    pub fn get_tsfamily(&self, id: u32) -> Option<Arc<RwLock<TseriesFamily>>> {
        if let Some(v) = self.ts_families.get(&id) {
            return Some(v.clone());
        }

        None
    }

    pub fn ts_families(&self) -> &HashMap<VnodeId, Arc<RwLock<TseriesFamily>>> {
        &self.ts_families
    }

    pub fn for_each_ts_family<F>(&self, func: F)
    where
        F: FnMut((&VnodeId, &Arc<RwLock<TseriesFamily>>)),
    {
        self.ts_families.iter().for_each(func);
    }

    pub fn del_ts_index(&mut self, id: u32) {
        self.ts_indexes.remove(&id);
    }

    pub fn get_ts_index(&self, id: u32) -> Option<Arc<index::ts_index::TSIndex>> {
        if let Some(v) = self.ts_indexes.get(&id) {
            return Some(v.clone());
        }

        None
    }

    pub fn ts_indexes(&self) -> HashMap<VnodeId, Arc<index::ts_index::TSIndex>> {
        self.ts_indexes.clone()
    }

    pub async fn get_table_sids(&self, table: &str) -> IndexResult<Vec<SeriesId>> {
        let mut res = vec![];
        for (_, ts_index) in self.ts_indexes.iter() {
            let mut list = ts_index.get_series_id_list(table, &[]).await?;
            res.append(&mut list);
        }
        Ok(res)
    }

    pub async fn get_ts_index_or_add(&mut self, id: u32) -> Result<Arc<index::ts_index::TSIndex>> {
        if let Some(v) = self.ts_indexes.get(&id) {
            return Ok(v.clone());
        }

        let path = self.opt.storage.index_dir(&self.owner, id);

        let idx = index::ts_index::TSIndex::new(path).await?;
        let idx = Arc::new(idx);

        self.ts_indexes.insert(id, idx.clone());

        Ok(idx)
    }

    pub fn get_schemas(&self) -> Arc<DBschemas> {
        self.schemas.clone()
    }

    pub fn get_schema(&self) -> Result<DatabaseSchema> {
        Ok(self.schemas.db_schema()?)
    }

    pub fn owner(&self) -> Arc<String> {
        self.owner.clone()
    }
}

#[cfg(test)]
impl Database {
    pub fn tsf_num(&self) -> usize {
        self.ts_families.len()
    }
}

pub(crate) async fn delete_table_async(
    tenant: String,
    database: String,
    table: String,
    version_set: Arc<RwLock<VersionSet>>,
) -> Result<()> {
    info!("Drop table: '{}.{}'", &database, &table);
    let version_set_rlock = version_set.read().await;
    let db_instance = version_set_rlock.get_db(&tenant, &database);
    drop(version_set_rlock);

    if let Some(db) = db_instance {
        let schemas = db.read().await.get_schemas();
        let field_infos = schemas.get_table_schema(&table)?;
        //schemas.del_table_schema(&table).await?;

        let mut sids = vec![];
        for (_id, index) in db.read().await.ts_indexes().iter() {
            let mut ids = index
                .get_series_id_list(&table, &[])
                .await
                .context(error::IndexErrSnafu)?;

            for sid in ids.iter() {
                index
                    .del_series_info(*sid)
                    .await
                    .context(error::IndexErrSnafu)?;
            }
            index.flush().await.context(error::IndexErrSnafu)?;

            sids.append(&mut ids);
        }

        if let Some(fields) = field_infos {
            debug!(
                "Drop table: deleting series in table: {}.{}",
                &database, &table
            );
            let fids: Vec<ColumnId> = fields.columns().iter().map(|f| f.id).collect();
            let storage_fids: Vec<u64> = sids
                .iter()
                .flat_map(|sid| fids.iter().map(|fid| unite_id(*fid, *sid)))
                .collect();
            let time_range = &TimeRange {
                min_ts: Timestamp::MIN,
                max_ts: Timestamp::MAX,
            };

            for (_ts_family_id, ts_family) in db.read().await.ts_families().iter() {
                // TODO: Concurrent delete on ts_family.
                // TODO: Limit parallel delete to 1.
                ts_family.write().await.delete_series(&sids, time_range);
                let version = ts_family.read().await.super_version();
                for column_file in version.version.column_files(&storage_fids, time_range) {
                    column_file.add_tombstone(&storage_fids, time_range).await?;
                }
            }
        }
    }
    Ok(())
}
