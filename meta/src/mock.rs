#![allow(dead_code, unused_variables)]

use std::collections::{hash_map, HashMap};
use std::sync::atomic::AtomicU32;
use std::sync::{atomic, Arc};

use models::auth::privilege::DatabasePrivilege;
use models::auth::role::{CustomTenantRole, SystemTenantRole, TenantRoleIdentifier};
use models::meta_data::{
    self, BucketInfo, DatabaseInfo, ExpiredBucketInfo, NodeAttribute, NodeId, NodeInfo,
    ReplicationSet, VnodeAllInfo, VnodeInfo,
};
use models::oid::Oid;
use models::schema::{
    DatabaseSchema, ExternalTableSchema, TableSchema, Tenant, TenantOptions, TskvTableSchema,
    DEFAULT_CATALOG, DEFAULT_DATABASE,
};
use parking_lot::RwLock;
use tonic::transport::Channel;

use crate::error::{MetaError, MetaResult};
use crate::limiter::RequestLimiter;
use crate::model::user_manager_mock::UserManagerMock;
use crate::model::{
    AdminMeta, AdminMetaRef, MetaClient, MetaClientRef, MetaManager, TenantManager,
    TenantManagerRef, UserManagerRef,
};
use crate::store::command::EntryLog;

#[derive(Default, Debug)]
pub struct MockAdminMeta {}
#[async_trait::async_trait]
impl AdminMeta for MockAdminMeta {
    async fn sync_all(&self) -> MetaResult<u64> {
        Ok(0)
    }

    async fn add_data_node(&self) -> MetaResult<()> {
        Ok(())
    }

    async fn data_nodes(&self) -> Vec<NodeInfo> {
        vec![]
    }

    async fn node_info_by_id(&self, id: u64) -> MetaResult<NodeInfo> {
        Ok(NodeInfo::default())
    }

    async fn get_node_conn(&self, node_id: u64) -> MetaResult<Channel> {
        todo!()
    }

    async fn retain_id(&self, count: u32) -> MetaResult<u32> {
        Ok(0)
    }

    async fn process_watch_log(&self, entry: &EntryLog) -> MetaResult<()> {
        Ok(())
    }

    async fn report_node_metrics(&self) -> MetaResult<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct MockMetaClient {
    tenant: Tenant,
    inner: Arc<RwLock<MockMetaClientInner>>,
    next_id: AtomicU32,

    always_fail: bool,
}

impl MockMetaClient {
    pub fn with_nodes(node_ids: &[NodeId]) -> Self {
        let inner = MockMetaClientInner {
            nodes: node_ids
                .iter()
                .map(|nid| NodeInfo {
                    id: *nid,
                    grpc_addr: format!("grpc://mock_addr_{}", nid),
                    http_addr: format!("http://mock_addr_{}", nid),
                    attribute: NodeAttribute::Hot,
                })
                .collect(),
            ..Default::default()
        };

        Self {
            tenant: Tenant::new(
                0_u128,
                DEFAULT_CATALOG.to_string(),
                TenantOptions::default(),
            ),
            inner: Arc::new(RwLock::new(inner)),
            next_id: AtomicU32::new(1),
            always_fail: false,
        }
    }
}

impl Default for MockMetaClient {
    fn default() -> Self {
        Self {
            tenant: Tenant::new(
                0_u128,
                DEFAULT_CATALOG.to_string(),
                TenantOptions::default(),
            ),
            inner: Arc::new(RwLock::new(MockMetaClientInner::default())),
            next_id: AtomicU32::new(1),
            always_fail: false,
        }
    }
}

#[async_trait::async_trait]
impl MetaClient for MockMetaClient {
    fn tenant(&self) -> &Tenant {
        &self.tenant
    }

    async fn create_db(&self, database_schema: DatabaseSchema) -> MetaResult<()> {
        let mut inner = self.inner.write();
        let database = database_schema.database_name().to_string();
        if let hash_map::Entry::Vacant(entry) = inner.data.entry(database.clone()) {
            entry.insert(DatabaseInfo {
                schema: database_schema,
                buckets: vec![],
                tables: HashMap::new(),
            });
            Ok(())
        } else {
            Err(MetaError::DatabaseAlreadyExists { database })
        }
    }
    async fn alter_db_schema(&self, database_schema: &DatabaseSchema) -> MetaResult<()> {
        let mut inner = self.inner.write();
        let database = database_schema.database_name();
        if let Some(db_info) = inner.data.get_mut(database) {
            db_info.schema = database_schema.clone();
            Ok(())
        } else {
            Err(MetaError::DatabaseNotFound {
                database: database.to_string(),
            })
        }
    }

    fn get_db_schema(&self, database: &str) -> MetaResult<Option<DatabaseSchema>> {
        let inner = self.inner.read();
        Ok(inner.data.get(database).map(|d| d.schema.clone()))
    }

    fn list_databases(&self) -> MetaResult<Vec<String>> {
        let inner = self.inner.read();
        Ok(inner.data.keys().cloned().collect::<Vec<String>>())
    }

    async fn drop_db(&self, database: &str) -> MetaResult<bool> {
        let mut inner = self.inner.write();
        Ok(inner.data.remove(database).is_some())
    }

    async fn create_table(&self, table_schema: &TableSchema) -> MetaResult<()> {
        let mut inner = self.inner.write();
        let database = table_schema.db();
        if let Some(db_info) = inner.data.get_mut(&database) {
            let table_name = table_schema.name();
            if let hash_map::Entry::Vacant(entry) = db_info.tables.entry(table_name) {
                entry.insert(table_schema.clone());
                Ok(())
            } else {
                Err(MetaError::DatabaseAlreadyExists { database })
            }
        } else {
            Err(MetaError::DatabaseNotFound { database })
        }
    }

    async fn update_table(&self, table_schema: &TableSchema) -> MetaResult<()> {
        let mut inner = self.inner.write();
        let database = table_schema.db();
        if let Some(db_info) = inner.data.get_mut(&database) {
            let table = table_schema.name();
            if let Some(tab) = db_info.tables.remove(&table) {
                match (tab, table_schema) {
                    (TableSchema::TsKvTableSchema(a), TableSchema::TsKvTableSchema(b)) => {
                        db_info.tables.insert(table, table_schema.clone());
                        Ok(())
                    }
                    _ => Err(MetaError::CommonError {
                        msg: "not supported".to_string(),
                    }),
                }
            } else {
                Err(MetaError::TableNotFound { table })
            }
        } else {
            Err(MetaError::DatabaseNotFound { database })
        }
    }

    fn get_table_schema(&self, db: &str, table: &str) -> MetaResult<Option<TableSchema>> {
        let inner = self.inner.read();
        if let Some(db_info) = inner.data.get(db) {
            Ok(db_info.tables.get(table).cloned())
        } else {
            Err(MetaError::DatabaseNotFound {
                database: db.to_string(),
            })
        }
    }

    fn get_tskv_table_schema(
        &self,
        db: &str,
        table: &str,
    ) -> MetaResult<Option<Arc<TskvTableSchema>>> {
        // Ok(Some(Arc::new(TskvTableSchema::new_test())))
        let inner = self.inner.read();
        if let Some(db_info) = inner.data.get(db) {
            if let Some(TableSchema::TsKvTableSchema(s)) = db_info.tables.get(table) {
                Ok(Some(s.clone()))
            } else {
                Ok(None)
            }
        } else {
            Err(MetaError::DatabaseNotFound {
                database: db.to_string(),
            })
        }
    }

    fn get_external_table_schema(
        &self,
        db: &str,
        table: &str,
    ) -> MetaResult<Option<Arc<ExternalTableSchema>>> {
        let inner = self.inner.read();
        if let Some(db_info) = inner.data.get(db) {
            if let Some(TableSchema::ExternalTableSchema(s)) = db_info.tables.get(table) {
                Ok(Some(s.clone()))
            } else {
                Ok(None)
            }
        } else {
            Err(MetaError::DatabaseNotFound {
                database: db.to_string(),
            })
        }
    }

    fn list_tables(&self, db: &str) -> MetaResult<Vec<String>> {
        let inner = self.inner.read();
        if let Some(db_info) = inner.data.get(db) {
            Ok(db_info.tables.keys().cloned().collect::<Vec<String>>())
        } else {
            Err(MetaError::DatabaseNotFound {
                database: db.to_string(),
            })
        }
    }

    async fn drop_table(&self, db: &str, table: &str) -> MetaResult<()> {
        let mut inner = self.inner.write();
        if let Some(db_info) = inner.data.get_mut(db) {
            if db_info.tables.remove(table).is_some() {
                Ok(())
            } else {
                Err(MetaError::TableNotFound {
                    table: table.to_string(),
                })
            }
        } else {
            Err(MetaError::DatabaseNotFound {
                database: db.to_string(),
            })
        }
    }

    async fn create_bucket(&self, db: &str, ts: i64) -> MetaResult<BucketInfo> {
        let mut inner = self.inner.write();
        let nodes = inner.nodes.clone();
        if let Some(db_info) = inner.data.get_mut(db) {
            if ts < db_info.schema.time_to_expired() {
                return Err(MetaError::CommonError {
                    msg: "Failed to crated bucket: expired".to_string(),
                });
            }
            let bucket = match db_info
                .buckets
                .iter()
                .find(|bucket| (ts >= bucket.start_time) && (ts < bucket.end_time))
            {
                Some(bucket) => bucket.clone(),
                None => {
                    let duration = db_info.schema.config.vnode_duration_or_default();
                    let (start_time, end_time) =
                        meta_data::get_time_range(ts, duration.to_nanoseconds());
                    let next_id = self.next_id.fetch_add(1, atomic::Ordering::SeqCst);
                    let (shard_group, used) = meta_data::allocation_replication_set(
                        &nodes,
                        db_info.schema.config.shard_num_or_default() as u32,
                        db_info.schema.config.replica_or_default() as u32,
                        next_id + 1,
                    );
                    self.next_id.fetch_add(used, atomic::Ordering::SeqCst);
                    let bucket = BucketInfo {
                        id: next_id,
                        start_time,
                        end_time,
                        shard_group,
                    };
                    db_info.buckets.push(bucket.clone());
                    bucket
                }
            };
            Ok(bucket)
        } else {
            Err(MetaError::DatabaseNotFound {
                database: db.to_string(),
            })
        }
    }

    async fn delete_bucket(&self, db: &str, id: u32) -> MetaResult<()> {
        Ok(())
    }

    fn database_min_ts(&self, db: &str) -> Option<i64> {
        let inner = self.inner.read();
        inner.data.get(db).map(|db_info| db_info.time_to_expired())
    }

    #[allow(clippy::await_holding_lock)]
    async fn locate_replication_set_for_write(
        &self,
        db: &str,
        hash_id: u64,
        ts: i64,
    ) -> MetaResult<ReplicationSet> {
        // Ok(ReplicationSet::default())
        let db_info = self.inner.read().data.get(db).cloned();
        if let Some(db_info) = db_info {
            let bucket = match db_info
                .buckets
                .iter()
                .find(|bucket| (ts >= bucket.start_time) && (ts < bucket.end_time))
            {
                Some(bucket) => bucket.vnode_for(hash_id),
                None => {
                    let bucket = self.create_bucket(db, ts).await?;
                    bucket.vnode_for(hash_id)
                }
            };
            Ok(bucket)
        } else {
            Err(MetaError::DatabaseNotFound {
                database: db.to_string(),
            })
        }
    }

    fn mapping_bucket(&self, db_name: &str, start: i64, end: i64) -> MetaResult<Vec<BucketInfo>> {
        Ok(vec![])
    }

    fn print_data(&self) -> String {
        "".to_string()
    }

    async fn add_member_with_role(
        &self,
        user_id: Oid,
        role: TenantRoleIdentifier,
    ) -> MetaResult<()> {
        todo!()
    }

    async fn member_role(&self, user_id: &Oid) -> MetaResult<Option<TenantRoleIdentifier>> {
        todo!()
    }

    async fn members(&self) -> MetaResult<HashMap<String, TenantRoleIdentifier>> {
        todo!()
    }

    async fn reassign_member_role(
        &self,
        user_id: Oid,
        role: TenantRoleIdentifier,
    ) -> MetaResult<()> {
        todo!()
    }

    async fn remove_member(&self, user_id: Oid) -> MetaResult<()> {
        todo!()
    }

    async fn create_custom_role(
        &self,
        role_name: String,
        system_role: SystemTenantRole,
        additiona_privileges: HashMap<String, DatabasePrivilege>,
    ) -> MetaResult<()> {
        todo!()
    }

    async fn custom_role(&self, role_name: &str) -> MetaResult<Option<CustomTenantRole<Oid>>> {
        todo!()
    }

    async fn custom_roles(&self) -> MetaResult<Vec<CustomTenantRole<Oid>>> {
        todo!()
    }

    async fn grant_privilege_to_custom_role(
        &self,
        database_privileges: Vec<(DatabasePrivilege, String)>,
        role_name: &str,
    ) -> MetaResult<()> {
        todo!()
    }

    async fn revoke_privilege_from_custom_role(
        &self,
        database_privileges: Vec<(DatabasePrivilege, String)>,
        role_name: &str,
    ) -> MetaResult<()> {
        todo!()
    }

    async fn drop_custom_role(&self, role_name: &str) -> MetaResult<bool> {
        todo!()
    }

    fn expired_bucket(&self) -> Vec<ExpiredBucketInfo> {
        vec![]
    }

    fn get_replication_set(&self, repl_id: u32) -> Option<ReplicationSet> {
        todo!()
    }

    async fn update_replication_set(
        &self,
        db: &str,
        bucket_id: u32,
        repl_id: u32,
        del_info: &[VnodeInfo],
        add_info: &[VnodeInfo],
    ) -> MetaResult<()> {
        Ok(())
    }

    async fn process_watch_log(&self, entry: &EntryLog) -> MetaResult<()> {
        Ok(())
    }

    async fn version(&self) -> u64 {
        0
    }

    async fn update_vnode(&self, info: &VnodeAllInfo) -> MetaResult<()> {
        Ok(())
    }

    fn get_vnode_all_info(&self, id: u32) -> Option<VnodeAllInfo> {
        None
    }

    fn get_vnode_repl_set(&self, id: u32) -> Option<ReplicationSet> {
        None
    }

    fn get_db_info(&self, name: &str) -> MetaResult<Option<DatabaseInfo>> {
        #[allow(clippy::inconsistent_digit_grouping)]
        let db_info = if name.eq("with_nonempty_database") {
            DatabaseInfo {
                buckets: vec![
                    BucketInfo {
                        id: 1,
                        // 2023-01-01 00:00:00.000000000
                        start_time: 1672502400_000_000_000_i64,
                        // 2023-07-01 00:00:00.000000000
                        end_time: 1688140800_000_000_000_i64,
                        ..Default::default()
                    },
                    BucketInfo {
                        id: 2,
                        // 2023-07-01 00:00:00.000000000
                        start_time: 1688140800_000_000_000_i64,
                        // 2024-01-01 00:00:00.000000000
                        end_time: 1704038400_000_000_000_i64,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }
        } else {
            DatabaseInfo::default()
        };
        Ok(Some(db_info))
    }
}

#[derive(Debug)]
struct MockMetaClientInner {
    nodes: Vec<NodeInfo>,
    data: HashMap<String, DatabaseInfo>,
}

impl Default for MockMetaClientInner {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
            data: HashMap::from([(
                DEFAULT_DATABASE.to_string(),
                DatabaseInfo {
                    schema: DatabaseSchema::new(DEFAULT_CATALOG, DEFAULT_DATABASE),
                    buckets: vec![],
                    tables: HashMap::new(),
                },
            )]),
        }
    }
}

#[derive(Default, Debug)]
pub struct MockMetaManager {}

#[async_trait::async_trait]
impl MetaManager for MockMetaManager {
    fn node_id(&self) -> u64 {
        0
    }

    async fn use_tenant(&self, val: &str) -> MetaResult<()> {
        Ok(())
    }

    async fn expired_bucket(&self) -> Vec<ExpiredBucketInfo> {
        vec![]
    }

    fn admin_meta(&self) -> AdminMetaRef {
        Arc::new(MockAdminMeta::default())
    }

    fn user_manager(&self) -> UserManagerRef {
        Arc::new(UserManagerMock::default())
    }

    fn tenant_manager(&self) -> TenantManagerRef {
        Arc::new(TenantManagerMock::default())
    }

    async fn user_with_privileges(
        &self,
        user_name: &str,
        tenant_name: Option<&str>,
    ) -> MetaResult<models::auth::user::User> {
        todo!()
    }
}

#[derive(Debug, Default)]
pub struct TenantManagerMock {}

#[async_trait::async_trait]
impl TenantManager for TenantManagerMock {
    async fn clear(&self) {}
    async fn create_tenant(
        &self,
        name: String,
        options: TenantOptions,
    ) -> MetaResult<MetaClientRef> {
        todo!()
    }

    async fn tenant(&self, name: &str) -> MetaResult<Option<Tenant>> {
        todo!()
    }

    async fn alter_tenant(&self, name: &str, options: TenantOptions) -> MetaResult<()> {
        todo!()
    }

    async fn drop_tenant(&self, name: &str) -> MetaResult<bool> {
        todo!()
    }

    async fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef> {
        todo!()
    }

    async fn expired_bucket(&self) -> Vec<ExpiredBucketInfo> {
        vec![]
    }

    async fn tenants(&self) -> MetaResult<Vec<Tenant>> {
        todo!()
    }

    async fn get_tenant_meta(&self, tenant: &str) -> Option<MetaClientRef> {
        todo!()
    }

    async fn limiter(&self, tenant: &str) -> Arc<dyn RequestLimiter> {
        todo!()
    }
}