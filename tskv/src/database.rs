use std::collections::{HashMap, LinkedList};
use std::mem::size_of;
use std::sync::Arc;

use flatbuffers::{ForwardsUOffset, Vector};
use lru_cache::asynchronous::ShardedCache;
use memory_pool::MemoryPoolRef;
use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
use models::predicate::domain::TimeRange;
use models::schema::{DatabaseSchema, Precision, TskvTableSchema, TskvTableSchemaRef};
use models::{SchemaId, SeriesId, SeriesKey};
use protos::models::{Column, ColumnType, FieldType, Table};
use snafu::ResultExt;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Sender;
use tokio::sync::{oneshot, RwLock};
use trace::{error, info};
use utils::BloomFilter;

use crate::compaction::{CompactTask, FlushReq};
use crate::context::GlobalContext;
use crate::error::{Result, SchemaSnafu};
use crate::index::{self, IndexResult};
use crate::kv_option::{Options, INDEX_PATH};
use crate::memcache::{MemCache, RowData, RowGroup};
use crate::schema::schemas::DBschemas;
use crate::summary::{SummaryTask, VersionEdit};
use crate::tseries_family::{LevelInfo, TseriesFamily, Version};
use crate::Error::{self};
use crate::{file_utils, ColumnFileId, TseriesFamilyId};

pub type FlatBufferTable<'a> = flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Table<'a>>>;

#[derive(Debug)]
pub struct Database {
    //tenant_name.database_name => owner
    owner: Arc<String>,
    opt: Arc<Options>,

    schemas: Arc<DBschemas>,
    ts_indexes: HashMap<TseriesFamilyId, Arc<index::ts_index::TSIndex>>,
    ts_families: HashMap<TseriesFamilyId, Arc<RwLock<TseriesFamily>>>,
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
            ver.tenant_database(),
            MemCache::new(
                ver.tf_id(),
                self.opt.cache.max_buffer_size,
                self.opt.cache.partition,
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
        version_edit: Option<VersionEdit>,
        summary_task_sender: Sender<SummaryTask>,
        flush_task_sender: Sender<FlushReq>,
        compact_task_sender: Sender<CompactTask>,
        global_ctx: Arc<GlobalContext>,
    ) -> Result<Arc<RwLock<TseriesFamily>>> {
        let new_version_edit_seq_no = 0;
        let (seq_no, version_edits, file_metas) = match version_edit {
            Some(mut ve) => {
                ve.tsf_id = tsf_id;
                ve.has_seq_no = true;
                ve.seq_no = new_version_edit_seq_no;
                let mut file_metas = HashMap::with_capacity(ve.add_files.len());
                for f in ve.add_files.iter_mut() {
                    let new_file_id = global_ctx.file_id_next();
                    f.tsf_id = tsf_id;
                    let file_path = f
                        .rename_file(&self.opt.storage, &self.owner, f.tsf_id, new_file_id)
                        .await?;
                    let file_reader = crate::tsm::TsmReader::open(file_path).await?;
                    file_metas.insert(new_file_id, file_reader.bloom_filter());
                }
                for f in ve.del_files.iter_mut() {
                    let new_file_id = global_ctx.file_id_next();
                    f.tsf_id = tsf_id;
                    f.rename_file(&self.opt.storage, &self.owner, f.tsf_id, new_file_id)
                        .await?;
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
                new_version_edit_seq_no,
                vec![VersionEdit::new_add_vnode(
                    tsf_id,
                    self.owner.as_ref().clone(),
                    new_version_edit_seq_no,
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
                self.opt.cache.partition,
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
        if let Some(tsf) = self.ts_families.get(&tsf_id) {
            return Ok(tsf.clone());
        }
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

        // TODO(zipper): If no ts_family recovered from summary, do not write summary.
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
            let table_name = table.tab_ext()?;
            let columns = table.columns().ok_or(Error::CommonError {
                reason: "table missing columns".to_string(),
            })?;
            let fb_schema = FbSchema::from_fb_column(columns)?;
            let num_rows = table.num_rows() as usize;
            let sids = Self::build_index(
                db_name,
                table_name,
                &columns,
                &fb_schema.tag_indexes,
                num_rows,
                ts_index.clone(),
            )
            .await?;

            for i in 0..num_rows {
                self.build_row_data(
                    &mut map,
                    table_name,
                    precision,
                    &columns,
                    &fb_schema.field_indexes,
                    fb_schema.time_index,
                    i,
                    &sids,
                    false,
                )
                .await?;
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
            let table_name = table.tab_ext()?;
            let columns = table.columns().ok_or(Error::CommonError {
                reason: "table missing columns".to_string(),
            })?;
            let fb_schema = FbSchema::from_fb_column(columns)?;
            let num_rows = table.num_rows() as usize;
            let sids = Self::build_index(
                db_name,
                table_name,
                &columns,
                &fb_schema.tag_indexes,
                num_rows,
                ts_index.clone(),
            )
            .await?;

            for i in 0..num_rows {
                let mut schema_change_or_create = false;
                if self
                    .schemas
                    .check_field_type_from_cache(
                        table_name,
                        &fb_schema.tag_names,
                        &fb_schema.field_names,
                        &fb_schema.field_types,
                    )
                    .is_err()
                {
                    schema_change_or_create = self
                        .schemas
                        .check_field_type_or_else_add(
                            db_name,
                            table_name,
                            &fb_schema.tag_names,
                            &fb_schema.field_names,
                            &fb_schema.field_types,
                        )
                        .await?;
                }
                self.build_row_data(
                    &mut map,
                    table_name,
                    precision,
                    &columns,
                    &fb_schema.field_indexes,
                    fb_schema.time_index,
                    i,
                    &sids,
                    schema_change_or_create,
                )
                .await?;
            }
        }
        Ok(map)
    }

    async fn build_row_data(
        &self,
        map: &mut HashMap<(SeriesId, SchemaId), RowGroup>,
        table_name: &str,
        precision: Precision,
        columns: &Vector<'_, ForwardsUOffset<Column<'_>>>,
        fields_idx: &[usize],
        ts_idx: usize,
        row_count: usize,
        sids: &[u32],
        schema_change_or_create: bool,
    ) -> Result<()> {
        let table_schema = if schema_change_or_create {
            self.schemas.get_table_schema_by_meta(table_name).await?
        } else {
            self.schemas.get_table_schema(table_name)?
        };
        let table_schema = match table_schema {
            Some(v) => v,
            None => return Ok(()),
        };

        let row = RowData::point_to_row_data(
            &table_schema,
            precision,
            columns,
            fields_idx,
            ts_idx,
            row_count,
        )?;
        let schema_size = table_schema.size();
        let schema_id = table_schema.schema_id;
        let entry = map.entry((sids[row_count], schema_id)).or_insert(RowGroup {
            schema: Arc::new(TskvTableSchema::default()),
            rows: LinkedList::new(),
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
        entry.rows.push_back(row);
        Ok(())
    }

    async fn build_index(
        db_name: &str,
        tab_name: &str,
        columns: &Vector<'_, ForwardsUOffset<Column<'_>>>,
        tag_idx: &[usize],
        row_num: usize,
        ts_index: Arc<index::ts_index::TSIndex>,
    ) -> Result<Vec<u32>> {
        let mut res_sids = Vec::with_capacity(row_num);
        let mut series_keys = Vec::with_capacity(row_num);
        for row_count in 0..row_num {
            let series_key =
                SeriesKey::build_series_key(db_name, tab_name, columns, tag_idx, row_count)
                    .map_err(|e| Error::CommonError {
                        reason: e.to_string(),
                    })?;
            if let Some(id) = ts_index.get_series_id(&series_key).await? {
                res_sids.push(Some(id));
            } else {
                res_sids.push(None);
                series_keys.push(series_key);
            }
        }

        let mut ids = ts_index
            .add_series_if_not_exists(series_keys)
            .await?
            .into_iter();
        for item in res_sids.iter_mut() {
            if item.is_none() {
                *item = Some(ids.next().ok_or(Error::CommonError {
                    reason: "add series failed, new series id is missing".to_string(),
                })?);
            }
        }
        let res_sids = res_sids.into_iter().flatten().collect::<Vec<_>>();

        Ok(res_sids)
    }

    /// Snapshots last version before `last_seq` of this database's all vnodes
    /// or specified vnode by `vnode_id`.
    ///
    /// Generated version data will be inserted into `version_edits` and `file_metas`.
    ///
    /// - `version_edits` are for all vnodes and db-files,
    /// - `file_metas` is for index data
    /// (field-id filter) of db-files.
    pub async fn snapshot(
        &self,
        vnode_id: Option<TseriesFamilyId>,
        version_edits: &mut Vec<VersionEdit>,
        file_metas: &mut HashMap<ColumnFileId, Arc<BloomFilter>>,
    ) {
        if let Some(tsf_id) = vnode_id.as_ref() {
            if let Some(tsf) = self.ts_families.get(tsf_id) {
                let ve = tsf.read().await.build_version_edit(file_metas);
                version_edits.push(ve);
            }
        } else {
            for tsf in self.ts_families.values() {
                let ve = tsf.read().await.build_version_edit(file_metas);
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

    pub fn get_table_schema(&self, table_name: &str) -> Result<Option<TskvTableSchemaRef>> {
        Ok(self.schemas.get_table_schema(table_name)?)
    }

    pub fn get_tsfamily(&self, id: u32) -> Option<Arc<RwLock<TseriesFamily>>> {
        if let Some(v) = self.ts_families.get(&id) {
            return Some(v.clone());
        }

        None
    }

    pub fn ts_families(&self) -> &HashMap<TseriesFamilyId, Arc<RwLock<TseriesFamily>>> {
        &self.ts_families
    }

    pub fn for_each_ts_family<F>(&self, func: F)
    where
        F: FnMut((&TseriesFamilyId, &Arc<RwLock<TseriesFamily>>)),
    {
        self.ts_families.iter().for_each(func);
    }

    pub fn del_ts_index(&mut self, id: TseriesFamilyId) {
        self.ts_indexes.remove(&id);
    }

    pub fn get_ts_index(&self, id: TseriesFamilyId) -> Option<Arc<index::ts_index::TSIndex>> {
        if let Some(v) = self.ts_indexes.get(&id) {
            return Some(v.clone());
        }

        None
    }

    pub fn ts_indexes(&self) -> HashMap<TseriesFamilyId, Arc<index::ts_index::TSIndex>> {
        self.ts_indexes.clone()
    }

    pub async fn get_ts_index_or_add(&mut self, id: u32) -> Result<Arc<index::ts_index::TSIndex>> {
        if let Some(v) = self.ts_indexes.get(&id) {
            return Ok(v.clone());
        }

        let path = self.opt.storage.index_dir(&self.owner, id);

        let idx = index::ts_index::TSIndex::new(path).await?;

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

struct FbSchema<'a> {
    time_index: usize,
    tag_indexes: Vec<usize>,
    tag_names: Vec<&'a str>,
    field_indexes: Vec<usize>,
    field_names: Vec<&'a str>,
    field_types: Vec<FieldType>,
}

impl<'a> FbSchema<'a> {
    pub fn from_fb_column(
        columns: Vector<'a, ForwardsUOffset<Column<'a>>>,
    ) -> Result<FbSchema<'a>> {
        let mut time_index = usize::MAX;
        let mut tag_indexes = vec![];
        let mut tag_names = vec![];
        let mut field_indexes = vec![];
        let mut field_names = vec![];
        let mut field_types = vec![];

        for (index, column) in columns.iter().enumerate() {
            match column.column_type() {
                ColumnType::Time => {
                    time_index = index;
                }
                ColumnType::Tag => {
                    tag_indexes.push(index);
                    tag_names.push(column.name().ok_or(Error::CommonError {
                        reason: "Tag column name not found in flatbuffer columns".to_string(),
                    })?);
                }
                ColumnType::Field => {
                    field_indexes.push(index);
                    field_names.push(column.name().ok_or(Error::CommonError {
                        reason: "Field column name not found in flatbuffer columns".to_string(),
                    })?);
                    field_types.push(column.field_type());
                }
                _ => {}
            }
        }

        if time_index == usize::MAX {
            return Err(Error::CommonError {
                reason: "Time column not found in flatbuffer columns".to_string(),
            });
        }

        if field_indexes.is_empty() {
            return Err(Error::CommonError {
                reason: "Field column not found in flatbuffer columns".to_string(),
            });
        }

        Ok(Self {
            time_index,
            tag_indexes,
            tag_names,
            field_indexes,
            field_names,
            field_types,
        })
    }
}
