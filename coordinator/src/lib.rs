#![feature(stmt_expr_attributes)]
#![feature(arc_unwrap_or_clone)]
#![feature(allocator_api)]

use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use errors::CoordinatorError;
use futures::Stream;
use meta::model::{MetaClientRef, MetaRef};
use models::meta_data::{ReplicaAllInfo, ReplicationSet, ReplicationSetId, VnodeAllInfo};
use models::object_reference::ResolvedTable;
use models::predicate::domain::ResolvedPredicateRef;
use models::schema::{Precision, TskvTableSchemaRef};
use protocol_parser::Line;
use protos::kv_service::AdminCommandRequest;
use raft::manager::RaftNodesManager;
use trace::SpanContext;
use tskv::reader::QueryOption;
use tskv::EngineRef;

use crate::errors::CoordinatorResult;
use crate::service::CoordServiceMetrics;

pub mod errors;
pub mod file_info;
pub mod hh_queue;
pub mod metrics;
pub mod raft;
pub mod reader;
pub mod service;
pub mod service_mock;
pub mod vnode_mgr;
pub mod writer;

pub const FAILED_RESPONSE_CODE: i32 = -1;
pub const FINISH_RESPONSE_CODE: i32 = 0;
pub const SUCCESS_RESPONSE_CODE: i32 = 1;

pub type SendableCoordinatorRecordBatchStream =
    Pin<Box<dyn Stream<Item = CoordinatorResult<RecordBatch>> + Send>>;

#[derive(Debug)]
pub struct WriteRequest {
    pub tenant: String,
    pub level: models::consistency_level::ConsistencyLevel,
    pub precision: Precision,
    pub request: protos::kv_service::WritePointsRequest,
}

#[derive(Debug, Clone)]
pub enum VnodeManagerCmdType {
    /// vnode id, dst node id
    Copy(u32, u64),
    /// vnode id, dst node id
    Move(u32, u64),
    /// vnode id
    Drop(u32),
    /// vnode id list
    Compact(Vec<u32>),

    /// replica set id, dst nod id
    AddRaftFollower(u32, u64),
    /// vnode id
    RemoveRaftNode(u32),
    /// replica set id
    DestoryRaftGroup(u32),
}

#[derive(Debug, Clone)]
pub enum VnodeSummarizerCmdType {
    /// replication set id
    Checksum(u32),
}

#[async_trait::async_trait]
pub trait Coordinator: Send + Sync {
    fn node_id(&self) -> u64;
    fn meta_manager(&self) -> MetaRef;
    fn store_engine(&self) -> Option<EngineRef>;
    fn raft_manager(&self) -> Arc<RaftNodesManager>;
    async fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef>;

    /// get all vnodes of a table to quering
    async fn table_vnodes(
        &self,
        table: &ResolvedTable,
        predicate: ResolvedPredicateRef,
    ) -> CoordinatorResult<Vec<ReplicationSet>>;

    async fn exec_write_replica_points(
        &self,
        tenant: &str,
        db_name: &str,
        data: Arc<Vec<u8>>,
        precision: Precision,
        replica: ReplicationSet,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()>;

    async fn write_lines<'a>(
        &self,
        tenant: &str,
        db: &str,
        precision: Precision,
        lines: Vec<Line<'a>>,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<usize>;

    async fn write_record_batch<'a>(
        &self,
        table_schema: TskvTableSchemaRef,
        record_batch: RecordBatch,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<usize>;

    fn table_scan(
        &self,
        option: QueryOption,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream>;

    fn tag_scan(
        &self,
        option: QueryOption,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream>;

    async fn broadcast_command(&self, req: AdminCommandRequest) -> CoordinatorResult<()>;

    /// A manager to manage vnode.
    async fn vnode_manager(
        &self,
        tenant: &str,
        cmd_type: VnodeManagerCmdType,
    ) -> CoordinatorResult<()>;

    /// A summarizer to summarize vnode info.
    async fn vnode_summarizer(
        &self,
        tenant: &str,
        cmd_type: VnodeSummarizerCmdType,
    ) -> CoordinatorResult<Vec<RecordBatch>>;

    fn metrics(&self) -> &Arc<CoordServiceMetrics>;

    fn using_raft_replication(&self) -> bool;
}

pub fn status_response_to_result(
    status: &protos::kv_service::StatusResponse,
) -> errors::CoordinatorResult<()> {
    if status.code == SUCCESS_RESPONSE_CODE {
        Ok(())
    } else {
        Err(errors::CoordinatorError::GRPCRequest {
            msg: status.data.clone(),
        })
    }
}

pub async fn get_vnode_all_info(
    meta: MetaRef,
    tenant: &str,
    vnode_id: u32,
) -> CoordinatorResult<VnodeAllInfo> {
    match meta.tenant_meta(tenant).await {
        Some(meta_client) => match meta_client.get_vnode_all_info(vnode_id) {
            Some(all_info) => Ok(all_info),
            None => Err(CoordinatorError::VnodeNotFound { id: vnode_id }),
        },

        None => Err(CoordinatorError::TenantNotFound {
            name: tenant.to_string(),
        }),
    }
}

pub async fn get_replica_all_info(
    meta: MetaRef,
    tenant: &str,
    replica_id: ReplicationSetId,
) -> CoordinatorResult<ReplicaAllInfo> {
    let replica = meta
        .tenant_meta(tenant)
        .await
        .ok_or(CoordinatorError::TenantNotFound {
            name: tenant.to_owned(),
        })?
        .get_replica_all_info(replica_id)
        .ok_or(CoordinatorError::ReplicationSetNotFound { id: replica_id })?;

    Ok(replica)
}

pub async fn update_replication_set(
    meta: MetaRef,
    tenant: &str,
    db_name: &str,
    bucket_id: u32,
    replica_id: u32,
    del_info: &[models::meta_data::VnodeInfo],
    add_info: &[models::meta_data::VnodeInfo],
) -> CoordinatorResult<()> {
    meta.tenant_meta(tenant)
        .await
        .ok_or(CoordinatorError::TenantNotFound {
            name: tenant.to_owned(),
        })?
        .update_replication_set(db_name, bucket_id, replica_id, del_info, add_info)
        .await?;

    Ok(())
}
