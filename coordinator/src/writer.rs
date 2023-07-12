use std::collections::HashMap;
use std::time::Duration;

use flatbuffers::{FlatBufferBuilder, WIPOffset};
use meta::error::MetaError;
use meta::model::{MetaClientRef, MetaRef};
use models::meta_data::*;
use models::schema::{timestamp_convert, Precision};
use models::utils::{now_timestamp_millis, now_timestamp_nanos};
use models::Timestamp;
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::{Meta, WritePointsRequest, WriteVnodeRequest};
use protos::models as fb_models;
use protos::models::{
    FieldBuilder, Point, PointBuilder, Points, PointsArgs, Schema, SchemaBuilder, TableBuilder,
    TagBuilder,
};
use snafu::ResultExt;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tonic::transport::Channel;
use tonic::Code;
use tower::timeout::Timeout;
use trace::{debug, SpanContext, SpanExt, SpanRecorder};
use trace_http::ctx::append_trace_context;
use tskv::EngineRef;

use crate::errors::*;
use crate::hh_queue::{HintedOffBlock, HintedOffWriteReq};
use crate::{status_response_to_result, WriteRequest};

pub struct VnodePoints<'a> {
    db: String,
    fbb: FlatBufferBuilder<'a>,
    offset: HashMap<String, Vec<WIPOffset<Point<'a>>>>,
    schema: HashMap<String, WIPOffset<Schema<'a>>>,

    pub data: Vec<u8>,
    pub repl_set: ReplicationSet,
}

impl<'a> VnodePoints<'a> {
    pub fn new(db: String, repl_set: ReplicationSet) -> Self {
        Self {
            db,
            repl_set,
            fbb: FlatBufferBuilder::new(),
            offset: HashMap::new(),
            schema: HashMap::new(),
            data: vec![],
        }
    }

    pub fn add_schema(&mut self, table_name: &str, schema: Schema) {
        if self.schema.get(table_name).is_none() {
            let tag_names_off = schema
                .tag_name()
                .unwrap_or_default()
                .iter()
                .map(|item| self.fbb.create_string(item))
                .collect::<Vec<_>>();
            let tag_names = self.fbb.create_vector(&tag_names_off);

            let field_name_off = schema
                .field_name()
                .unwrap_or_default()
                .iter()
                .map(|item| self.fbb.create_string(item))
                .collect::<Vec<_>>();
            let field_names = self.fbb.create_vector(&field_name_off);
            let field_type = self.fbb.create_vector(
                &schema
                    .field_type()
                    .unwrap_or_default()
                    .iter()
                    .collect::<Vec<_>>(),
            );

            let mut schema_builder = SchemaBuilder::new(&mut self.fbb);
            schema_builder.add_tag_name(tag_names);
            schema_builder.add_field_name(field_names);
            schema_builder.add_field_type(field_type);

            let schema = schema_builder.finish();
            self.schema.insert(table_name.to_string(), schema);
        }
    }

    pub fn add_point(&mut self, table_name: &str, point: Point) {
        let mut tags = Vec::with_capacity(point.tags().unwrap_or_default().len());
        for tag in point.tags().unwrap_or_default().iter() {
            let tags_value = self
                .fbb
                .create_vector(tag.value().unwrap_or_default().bytes());

            let mut tag_builder = TagBuilder::new(&mut self.fbb);
            tag_builder.add_value(tags_value);
            tags.push(tag_builder.finish());
        }

        let tags = self.fbb.create_vector(&tags);
        let tags_nullbit = self
            .fbb
            .create_vector(point.tags_nullbit().unwrap_or_default().bytes());

        let mut fields = Vec::with_capacity(point.fields().unwrap_or_default().len());
        for field in point.fields().unwrap_or_default().iter() {
            let field_value = self
                .fbb
                .create_vector(field.value().unwrap_or_default().bytes());

            let mut field_builder = FieldBuilder::new(&mut self.fbb);
            field_builder.add_value(field_value);
            fields.push(field_builder.finish());
        }

        let fields = self.fbb.create_vector(&fields);
        let fields_nullbit = self
            .fbb
            .create_vector(point.fields_nullbit().unwrap_or_default().bytes());

        let mut point_builder = PointBuilder::new(&mut self.fbb);
        point_builder.add_tags(tags);
        point_builder.add_tags_nullbit(tags_nullbit);
        point_builder.add_fields(fields);
        point_builder.add_fields_nullbit(fields_nullbit);
        point_builder.add_timestamp(point.timestamp());

        let point = point_builder.finish();

        match self.offset.get_mut(table_name) {
            None => {
                self.offset.insert(table_name.to_string(), vec![point]);
            }
            Some(points) => {
                points.push(point);
            }
        }
    }

    pub fn finish(&mut self) -> CoordinatorResult<()> {
        let fbb_db = self.fbb.create_vector(self.db.as_bytes());
        let mut fbb_tables = Vec::with_capacity(self.offset.len());
        let table_names = self.offset.iter().map(|item| item.0.as_str());

        for table_name in table_names {
            let table_points = self
                .offset
                .get(table_name)
                .ok_or(CoordinatorError::Points {
                    msg: format!("can not found points for {}", table_name),
                })?;
            let schema = self
                .schema
                .get(table_name)
                .ok_or(CoordinatorError::Points {
                    msg: format!("can not found schema for {}", table_name),
                })?;
            let num_rows = table_points.len();
            let table_points = self.fbb.create_vector(table_points);
            let table_name = self.fbb.create_vector(table_name.as_bytes());

            let mut table_builder = TableBuilder::new(&mut self.fbb);
            table_builder.add_tab(table_name);
            table_builder.add_schema(*schema);
            table_builder.add_points(table_points);
            table_builder.add_num_rows(num_rows as u64);
            fbb_tables.push(table_builder.finish())
        }

        let tables = self.fbb.create_vector(&fbb_tables);

        let points = Points::create(
            &mut self.fbb,
            &PointsArgs {
                db: Some(fbb_db),
                tables: Some(tables),
            },
        );
        self.fbb.finish(points, None);
        self.data = self.fbb.finished_data().to_vec();
        Ok(())
    }
}

pub struct VnodeMapping<'a> {
    /// Maps replication id to VnodePoints.
    pub points: HashMap<u32, VnodePoints<'a>>,
}

impl<'a> VnodeMapping<'a> {
    pub fn new() -> Self {
        Self {
            points: HashMap::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn map_point(
        &mut self,
        meta_client: MetaClientRef,
        database: &str,
        db_precision: Precision,
        db_min_ts: Option<Timestamp>,
        fb_schema: Schema<'_>,
        table: &str,
        fb_point: Point<'_>,
        point_precision: Precision,
    ) -> CoordinatorResult<()> {
        let point_ts: i64 = timestamp_convert(point_precision, db_precision, fb_point.timestamp())
            .ok_or_else(|| CoordinatorError::NormalizeTimestamp {
                from: point_precision,
                to: db_precision,
                ts: fb_point.timestamp(),
            })?;

        if let Some(database_min_ts) = db_min_ts {
            if point_ts < database_min_ts {
                return Err(CoordinatorError::PointTimestampExpired {
                    database: database.to_string(),
                    database_min_ts,
                    point_ts,
                });
            }
        }

        let hash_id = fb_point.hash_id(table, &fb_schema)?;
        // TODO(zipper): use Arc<ReplicationSet>
        let repl_set = meta_client
            .locate_replication_set_for_write(database, hash_id, point_ts)
            .await?;
        let vnode_pts = self
            .points
            .entry(repl_set.id)
            .or_insert_with(|| VnodePoints::new(database.to_string(), repl_set));
        vnode_pts.add_point(table, fb_point);
        vnode_pts.add_schema(table, fb_schema);

        Ok(())
    }
}

impl<'a> Default for VnodeMapping<'a> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct PointWriter {
    node_id: u64,
    timeout_ms: u64,
    kv_inst: Option<EngineRef>,
    meta_manager: MetaRef,
    hh_sender: Sender<HintedOffWriteReq>,
}

impl PointWriter {
    pub fn new(
        node_id: u64,
        timeout_ms: u64,
        kv_inst: Option<EngineRef>,
        meta_manager: MetaRef,
        hh_sender: Sender<HintedOffWriteReq>,
    ) -> Self {
        Self {
            node_id,
            kv_inst,
            timeout_ms,
            meta_manager,
            hh_sender,
        }
    }

    pub async fn write_points(
        &self,
        req: &WriteRequest,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        let meta_client = self.meta_manager.tenant_meta(&req.tenant).await.ok_or(
            CoordinatorError::TenantNotFound {
                name: req.tenant.clone(),
            },
        )?;

        let mut mapping = VnodeMapping::new();
        {
            let _span_recorder = SpanRecorder::new(span_ctx.child_span("map point"));
            let fb_points = flatbuffers::root::<fb_models::Points>(&req.request.points)
                .context(InvalidFlatbufferSnafu)?;
            let database_name = fb_points.db_ext()?.to_string();
            let db_schema = meta_client.get_db_schema(&database_name)?.ok_or_else(|| {
                MetaError::DatabaseNotFound {
                    database: database_name.clone(),
                }
            })?;
            let db_min_ts = meta_client.database_min_ts(&database_name);
            let db_precision = *db_schema.config.precision_or_default();
            for fb_table in fb_points.tables_iter_ext()? {
                let table_name = fb_table.tab_ext()?.to_string();
                let fb_schema = fb_table.schema_ext()?;

                for fb_point in fb_table.points_iter_ext()? {
                    mapping
                        .map_point(
                            meta_client.clone(),
                            &database_name,
                            db_precision,
                            db_min_ts,
                            fb_schema,
                            &table_name,
                            fb_point,
                            req.precision,
                        )
                        .await?;
                }
            }
        }

        let now = tokio::time::Instant::now();
        let mut requests = vec![];
        {
            let _span_recorder = SpanRecorder::new(span_ctx.child_span("build requests"));
            for (_id, points) in mapping.points.iter_mut() {
                points.finish()?;
                if points.repl_set.vnodes.is_empty() {
                    return Err(CoordinatorError::CommonError {
                        msg: "no available vnode in replication set".to_string(),
                    });
                }
                for vnode in points.repl_set.vnodes.iter() {
                    debug!("Preparing write points on {:?} at {:?}", vnode, now);
                    if vnode.status == VnodeStatus::Copying {
                        debug!(
                            "Preparing write points prevented because state is Copying on {:?} at {:?}",
                            vnode, now
                        );
                        return Err(CoordinatorError::CommonError {
                            msg: "vnode is moving write forbidden ".to_string(),
                        });
                    }
                    let request = self.write_to_node(
                        vnode.id,
                        &req.tenant,
                        vnode.node_id,
                        req.precision,
                        points.data.clone(),
                        SpanRecorder::new(span_ctx.child_span(format!(
                            "write to vnode {} on node {}",
                            vnode.id, vnode.node_id
                        ))),
                    );
                    debug!("Writing points on {:?} at {:?}", vnode, now);
                    requests.push(request);
                }
            }
        }

        for res in futures::future::join_all(requests).await {
            debug!(
                "parallel write points on vnode over, start at: {:?}, elapsed: {} millis, result: {:?}",
                now,
                now.elapsed().as_millis(),
                res
            );
            res?
        }

        Ok(())
    }

    async fn write_to_node(
        &self,
        vnode_id: u32,
        tenant: &str,
        node_id: u64,
        precision: Precision,
        data: Vec<u8>,
        span_recorder: SpanRecorder,
    ) -> CoordinatorResult<()> {
        if node_id == self.node_id && self.kv_inst.is_some() {
            let span_recorder = span_recorder.child("write to local node");

            let result = self
                .write_to_local_node(span_recorder.span_ctx(), vnode_id, tenant, precision, data)
                .await;
            debug!("write data to local {}({}) {:?}", node_id, vnode_id, result);

            return result;
        }

        let mut span_recorder = span_recorder.child("write to remote node");

        let result = self
            .write_to_remote_node(
                vnode_id,
                node_id,
                tenant,
                precision,
                data.clone(),
                span_recorder.span_ctx(),
            )
            .await;
        if let Err(err @ CoordinatorError::FailoverNode { .. }) = result {
            debug!(
                "write data to remote {}({}) failed; write to hinted handoff!",
                node_id, vnode_id
            );

            span_recorder.error(err.to_string());

            return self
                .write_to_handoff(vnode_id, node_id, tenant, precision, data)
                .await;
        }

        debug!(
            "write data to remote {}({}) , inst exist: {}, {:?}!",
            node_id,
            vnode_id,
            self.kv_inst.is_some(),
            result
        );

        result
    }

    async fn write_to_handoff(
        &self,
        vnode_id: u32,
        node_id: u64,
        tenant: &str,
        precision: Precision,
        data: Vec<u8>,
    ) -> CoordinatorResult<()> {
        let (sender, receiver) = oneshot::channel();
        let block = HintedOffBlock::new(
            now_timestamp_nanos(),
            vnode_id,
            tenant.to_string(),
            precision,
            data,
        );
        let request = HintedOffWriteReq {
            node_id,
            sender,
            block,
        };

        self.hh_sender.send(request).await?;

        receiver.await?
    }

    pub async fn write_to_remote_node(
        &self,
        vnode_id: u32,
        node_id: u64,
        tenant: &str,
        precision: Precision,
        data: Vec<u8>,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        let channel = self
            .meta_manager
            .get_node_conn(node_id)
            .await
            .map_err(|error| CoordinatorError::FailoverNode {
                id: node_id,
                error: error.to_string(),
            })?;
        let timeout_channel = Timeout::new(channel, Duration::from_millis(self.timeout_ms));
        let mut client = TskvServiceClient::<Timeout<Channel>>::new(timeout_channel);

        let mut cmd = tonic::Request::new(WriteVnodeRequest {
            vnode_id,
            precision: precision as u32,
            tenant: tenant.to_string(),
            data,
        });

        // 将当前的trace span信息写入到请求的metadata中
        append_trace_context(span_ctx, cmd.metadata_mut()).map_err(|_| {
            CoordinatorError::CommonError {
                msg: "Parse trace_id, this maybe a bug".to_string(),
            }
        })?;

        let begin_time = now_timestamp_millis();
        let response = client
            .write_vnode_points(cmd)
            .await
            .map_err(|err| match err.code() {
                Code::Internal => CoordinatorError::TskvError { source: err.into() },
                _ => CoordinatorError::FailoverNode {
                    id: node_id,
                    error: format!("{err:?}"),
                },
            })?
            .into_inner();

        let use_time = now_timestamp_millis() - begin_time;
        if use_time > 200 {
            debug!(
                "write points to node:{}, use time too long {}",
                node_id, use_time
            )
        }
        status_response_to_result(&response)
    }

    async fn write_to_local_node(
        &self,
        span_ctx: Option<&SpanContext>,
        vnode_id: u32,
        tenant: &str,
        precision: Precision,
        data: Vec<u8>,
    ) -> CoordinatorResult<()> {
        let req = WritePointsRequest {
            version: 1,
            meta: Some(Meta {
                tenant: tenant.to_string(),
                user: None,
                password: None,
            }),
            points: data.clone(),
        };

        if let Some(kv_inst) = self.kv_inst.clone() {
            let _ = kv_inst.write(span_ctx, vnode_id, precision, req).await?;
            Ok(())
        } else {
            Err(CoordinatorError::KvInstanceNotFound { node_id: 0 })
        }
    }
}

#[cfg(test)]
pub mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::TimeUnit;
    use meta::model::meta_tenant::TenantMeta;
    use models::codec::Encoding;
    use models::consistency_level::ConsistencyLevel;
    use models::meta_data::{ReplicationSet, VnodeInfo};
    use models::schema::{
        ColumnType, DatabaseSchema, Duration, Precision, TableColumn, TableSchema, TskvTableSchema,
    };
    use models::utils::DAY_NANOS;
    use models::{utils as model_utils, ValueType};
    use protos::kv_service::WritePointsRequest;
    use protos::models::{Point, Points};
    use protos::test_helper::{PointsDescription, TableDescription};

    use super::VnodeMapping;
    use crate::WriteRequest;

    #[tokio::test]
    #[allow(unused_variables, unused_mut)]
    async fn test_map_points() {
        let tenant = "test_tenant".to_string();
        let database = "test_db".to_string();
        let tables = ["test_table_1".to_string(), "test_table_2".to_string()];
        let nodes = vec![1, 2];
        #[rustfmt::skip]
        let _replication_sets = vec![
            ReplicationSet {
                id: 1, vnodes: vec![VnodeInfo { id: 2, node_id: 1, ..Default::default() }, VnodeInfo { id: 3, node_id: 2, ..Default::default() }],
            },
            ReplicationSet {
                id: 4, vnodes: vec![VnodeInfo { id: 5, node_id: 1, ..Default::default() }, VnodeInfo { id: 6, node_id: 2, ..Default::default() }],
            },
        ];
        let nanos_since_unix_epoch = model_utils::now_timestamp_nanos();
        let days_since_unix_epoch = (nanos_since_unix_epoch / DAY_NANOS) as u64;
        let point_timestamps: Vec<i64> =
            (nanos_since_unix_epoch..nanos_since_unix_epoch + 10).collect();

        // Init mock meta client.
        let tenant_meta = Arc::new(TenantMeta::mock());
        #[rustfmt::skip]
        let _ = {
            let mut db_schema = DatabaseSchema::new(&tenant, &database);
            db_schema.config.with_ttl(Duration::new_with_day(days_since_unix_epoch));
            db_schema.config.with_vnode_duration(Duration::new_with_day(days_since_unix_epoch / 10));
            db_schema.config.with_shard_num(2);
            db_schema.config.with_replica(2);
            tenant_meta.create_db(db_schema).await.unwrap();
            "ok"
        };

        // Init table schema and points data to write, send table schema to meta.
        let mut points_desc = PointsDescription::new(&database);
        for table in tables.iter() {
            let mut table_desc = TableDescription::new(table);
            table_desc.reset_points(&point_timestamps);
            #[rustfmt::skip]
            let table_columns = {
                let mut col_id = 1_u32;
                let mut table_columns = vec![TableColumn::new_time_column(col_id, TimeUnit::Nanosecond)];
                for tag_key in table_desc.schema_tags.iter() {
                    col_id += 1;
                    table_columns.push(TableColumn::new_tag_column(col_id, tag_key.clone()));
                }
                for (field_name, field_type) in table_desc.schema_field_iter() {
                    col_id += 1;
                    table_columns.push(TableColumn::new(col_id, field_name.clone(), ColumnType::Field(ValueType::from(*field_type)), Encoding::Default));
                }
                table_columns
            };
            points_desc.table_descs.push(table_desc);

            tenant_meta
                .create_table(&TableSchema::TsKvTableSchema(Arc::new(
                    TskvTableSchema::new(
                        tenant.clone(),
                        database.clone(),
                        table.clone(),
                        table_columns,
                    ),
                )))
                .await
                .unwrap();
        }

        let point_bytes = points_desc.as_fb_bytes();
        let fb_points = flatbuffers::root::<Points>(&point_bytes).unwrap();
        let write_request = WriteRequest {
            tenant: tenant.clone(),
            level: ConsistencyLevel::All,
            precision: Precision::NS,
            request: WritePointsRequest {
                version: 1,
                meta: None,
                points: point_bytes.clone(),
            },
        };

        let db_info = tenant_meta.get_db_info(&database).unwrap().unwrap();
        let db_precision = db_info.schema.config.precision_or_default();
        let db_min_ts = tenant_meta.database_min_ts(&database);
        let mut vnode_mapping = VnodeMapping::new();
        for fb_table in fb_points.tables_iter_ext().unwrap() {
            let table = fb_table.tab_ext().unwrap().to_string();
            let fb_schema = fb_table.schema_ext().unwrap();
            for fb_point in fb_table.points_iter_ext().unwrap() {
                vnode_mapping
                    .map_point(
                        tenant_meta.clone(),
                        &points_desc.database,
                        *db_precision,
                        db_min_ts,
                        fb_schema,
                        &table,
                        fb_point,
                        write_request.precision,
                    )
                    .await
                    .unwrap();
            }
        }

        let mut vnode_fb_points = Vec::with_capacity(vnode_mapping.points.len());
        for vnode_points in vnode_mapping.points.values_mut() {
            vnode_points.finish().unwrap();
            let fb_points = flatbuffers::root::<Points>(&vnode_points.data).unwrap();
            vnode_fb_points.push(fb_points);
        }
        assert_eq!(2, vnode_fb_points.len());
        let mut tables: HashMap<String, Vec<Point>> = HashMap::new();
        for fb_points in vnode_fb_points.iter() {
            for fb_table in fb_points.tables_iter_ext().unwrap() {
                let tab = fb_table.tab_ext().unwrap().to_string();
                for fb_point in fb_table.points_iter_ext().unwrap() {}
            }
        }

        println!("Finished");
    }
}
// Database: 2, HashId: test_db, ReplicationSet: 2
// Vnodes: [ {node: 1, vnode: 3}, {node: 2, vnode: 4} ]
// ==============================
// Database: test_db
// ------------------------------
// Table: test_table_1
// Timestamp: 1683869767684377001
// Tags[3]: { ta: tac }, { tb: tbc }, { tc: tcc }
// Fields[5]: { f1: 5, Float }, { f2: 5, Integer }, { f3: 3, Unsigned }, { f4: true, Boolean }, { f5: outrageous, String }
// Timestamp: 1683869767684377004
// Tags[3]: { ta: taa }, { tb: tba }, { tc: tca }
// Fields[5]: { f1: 13, Float }, { f2: 13, Integer }, { f3: 6, Unsigned }, { f4: true, Boolean }, { f5: sea, String }
// Timestamp: 1683869767684377006
// Tags[3]: { ta: tac }, { tb: tbc }, { tc: tcc }
// Fields[5]: { f1: 19, Float }, { f2: 19, Integer }, { f3: 8, Unsigned }, { f4: true, Boolean }, { f5: opposing, String }
// Timestamp: 1683869767684377009
// Tags[3]: { ta: taa }, { tb: tba }, { tc: tca }
// Fields[5]: { f1: 31, Float }, { f2: 31, Integer }, { f3: 11, Unsigned }, { f4: false, Boolean }, { f5: outrageous, String }
// ------------------------------
// Table: test_table_2
// Timestamp: 1683869767684377000
// Tags[3]: { ta: tab }, { tb: tbb }, { tc: tcb }
// Fields[5]: { f1: 3, Float }, { f2: 3, Integer }, { f3: 2, Unsigned }, { f4: false, Boolean }, { f5: arrows, String }
// Timestamp: 1683869767684377002
// Tags[3]: { ta: tad }, { tb: tbd }, { tc: tcd }
// Fields[5]: { f1: 7, Float }, { f2: 7, Integer }, { f3: 4, Unsigned }, { f4: true, Boolean }, { f5: fortune, String }
// Timestamp: 1683869767684377003
// Tags[3]: { ta: tae }, { tb: tbe }, { tc: tcd }
// Fields[5]: { f1: 11, Float }, { f2: 11, Integer }, { f3: 5, Unsigned }, { f4: false, Boolean }, { f5: arms, String }
// Timestamp: 1683869767684377005
// Tags[3]: { ta: tab }, { tb: tbb }, { tc: tcb }
// Fields[5]: { f1: 17, Float }, { f2: 17, Integer }, { f3: 7, Unsigned }, { f4: false, Boolean }, { f5: troubles, String }
// Timestamp: 1683869767684377007
// Tags[3]: { ta: tad }, { tb: tbd }, { tc: tcd }
// Fields[5]: { f1: 23, Float }, { f2: 23, Integer }, { f3: 9, Unsigned }, { f4: false, Boolean }, { f5: slings, String }
// Timestamp: 1683869767684377008
// Tags[3]: { ta: tae }, { tb: tbe }, { tc: tcd }
// Fields[5]: { f1: 29, Float }, { f2: 29, Integer }, { f3: 10, Unsigned }, { f4: false, Boolean }, { f5: arrows, String }
// ------------------------------
//
// Database: 5, HashId: test_db, ReplicationSet: 5
// Vnodes: [ {node: 1, vnode: 6}, {node: 2, vnode: 7} ]
// ==============================
// Database: test_db
// ------------------------------
// Table: test_table_2
// Timestamp: 1683869767684377001
// Tags[3]: { ta: tac }, { tb: tbc }, { tc: tcc }
// Fields[5]: { f1: 5, Float }, { f2: 5, Integer }, { f3: 3, Unsigned }, { f4: true, Boolean }, { f5: outrageous, String }
// Timestamp: 1683869767684377004
// Tags[3]: { ta: taa }, { tb: tba }, { tc: tca }
// Fields[5]: { f1: 13, Float }, { f2: 13, Integer }, { f3: 6, Unsigned }, { f4: true, Boolean }, { f5: sea, String }
// Timestamp: 1683869767684377006
// Tags[3]: { ta: tac }, { tb: tbc }, { tc: tcc }
// Fields[5]: { f1: 19, Float }, { f2: 19, Integer }, { f3: 8, Unsigned }, { f4: true, Boolean }, { f5: opposing, String }
// Timestamp: 1683869767684377009
// Tags[3]: { ta: taa }, { tb: tba }, { tc: tca }
// Fields[5]: { f1: 31, Float }, { f2: 31, Integer }, { f3: 11, Unsigned }, { f4: false, Boolean }, { f5: outrageous, String }
// ------------------------------
// Table: test_table_1
// Timestamp: 1683869767684377000
// Tags[3]: { ta: tab }, { tb: tbb }, { tc: tcb }
// Fields[5]: { f1: 3, Float }, { f2: 3, Integer }, { f3: 2, Unsigned }, { f4: false, Boolean }, { f5: arrows, String }
// Timestamp: 1683869767684377002
// Tags[3]: { ta: tad }, { tb: tbd }, { tc: tcd }
// Fields[5]: { f1: 7, Float }, { f2: 7, Integer }, { f3: 4, Unsigned }, { f4: true, Boolean }, { f5: fortune, String }
// Timestamp: 1683869767684377003
// Tags[3]: { ta: tae }, { tb: tbe }, { tc: tcd }
// Fields[5]: { f1: 11, Float }, { f2: 11, Integer }, { f3: 5, Unsigned }, { f4: false, Boolean }, { f5: arms, String }
// Timestamp: 1683869767684377005
// Tags[3]: { ta: tab }, { tb: tbb }, { tc: tcb }
// Fields[5]: { f1: 17, Float }, { f2: 17, Integer }, { f3: 7, Unsigned }, { f4: false, Boolean }, { f5: troubles, String }
// Timestamp: 1683869767684377007
// Tags[3]: { ta: tad }, { tb: tbd }, { tc: tcd }
// Fields[5]: { f1: 23, Float }, { f2: 23, Integer }, { f3: 9, Unsigned }, { f4: false, Boolean }, { f5: slings, String }
// Timestamp: 1683869767684377008
// Tags[3]: { ta: tae }, { tb: tbe }, { tc: tcd }
// Fields[5]: { f1: 29, Float }, { f2: 29, Integer }, { f3: 10, Unsigned }, { f4: false, Boolean }, { f5: arrows, String }
// ------------------------------
//
