use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{InstallSnapshotError, NetworkError, RemoteError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::*;
use openraft::MessageSummary;
use parking_lot::RwLock;
use protos::raft_service::*;
use protos::{raft_service_time_out_client, DEFAULT_GRPC_SERVER_MESSAGE_LEN};
use tonic::transport::{Channel, Endpoint};
use trace::debug;

use crate::errors::{GRPCRequestSnafu, ReplicationResult};
use crate::{RaftNodeId, RaftNodeInfo, ReplicationConfig, TypeConfig};

// ------------------------------------------------------------------------- //
#[derive(Clone)]
pub struct NetworkConn {
    config: ReplicationConfig,
    conn_map: Arc<RwLock<HashMap<String, Channel>>>,
}

impl NetworkConn {
    pub fn new(config: ReplicationConfig) -> Self {
        Self {
            config,
            conn_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    async fn get_conn(&self, addr: &str) -> ReplicationResult<Channel> {
        if let Some(val) = self.conn_map.read().get(addr) {
            return Ok(val.clone());
        }

        let connector = Endpoint::from_shared(format!("http://{}", addr)).map_err(|err| {
            GRPCRequestSnafu {
                msg: format!("Connect to({}) error: {}", addr, err),
            }
            .build()
        })?;

        let channel = connector.connect().await.map_err(|err| {
            GRPCRequestSnafu {
                msg: format!("Connect to({}) error: {}", addr, err),
            }
            .build()
        })?;

        self.conn_map
            .write()
            .insert(addr.to_string(), channel.clone());

        Ok(channel)
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkConn {
    type Network = TargetClient;

    async fn new_client(&mut self, target: RaftNodeId, node: &RaftNodeInfo) -> Self::Network {
        TargetClient {
            target,
            conn: self.clone(),
            target_node: node.clone(),
            config: self.config.clone(),
        }
    }
}

// ------------------------------------------------------------------------- //
type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<RaftNodeId, E>;
type RPCError<E = openraft::error::Infallible> =
    openraft::error::RPCError<RaftNodeId, RaftNodeInfo, RaftError<E>>;

pub struct TargetClient {
    conn: NetworkConn,
    target: RaftNodeId,
    target_node: RaftNodeInfo,

    config: ReplicationConfig,
}

impl RaftNetwork<TypeConfig> for TargetClient {
    async fn vote(
        &mut self,
        req: VoteRequest<RaftNodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<RaftNodeId>, RPCError> {
        debug!(
            "Network callback send_vote target:{}, req: {:?}",
            self.target, req
        );

        let channel = self
            .conn
            .get_conn(&self.target_node.address)
            .await
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;

        let mut client = raft_service_time_out_client(
            channel,
            Duration::from_millis(self.config.send_append_entries_timeout),
            DEFAULT_GRPC_SERVER_MESSAGE_LEN,
            self.config.grpc_enable_gzip,
        );

        let data = serde_json::to_string(&req)
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;
        let cmd = tonic::Request::new(RaftVoteReq {
            data,
            group_id: self.target_node.group_id,
        });

        let rsp = client
            .raft_vote(cmd)
            .await
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?
            .into_inner();

        let res: Result<VoteResponse<u64>, RaftError> = serde_json::from_str(&rsp.data)
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;

        res.map_err(|e| openraft::error::RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<RaftNodeId>, RPCError> {
        // let begin = req.entries.first().map_or(0, |ent| ent.log_id.index);
        // let end = req.entries.last().map_or(0, |ent| ent.log_id.index);
        // debug!(
        //     "Network callback send_append_entries target:{}, entries: [{}-{}]",
        //     self.target, begin, end
        // );

        let channel = self
            .conn
            .get_conn(&self.target_node.address)
            .await
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;

        let mut client = raft_service_time_out_client(
            channel,
            Duration::from_millis(self.config.send_append_entries_timeout),
            DEFAULT_GRPC_SERVER_MESSAGE_LEN,
            self.config.grpc_enable_gzip,
        );

        let data = bincode::serialize(&req)
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;
        let cmd = tonic::Request::new(RaftAppendEntriesReq {
            data,
            group_id: self.target_node.group_id,
        });

        let rsp = client
            .raft_append_entries(cmd)
            .await
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?
            .into_inner();

        let res: Result<AppendEntriesResponse<u64>, RaftError> = serde_json::from_str(&rsp.data)
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;

        res.map_err(|e| openraft::error::RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<RaftNodeId>, RPCError<InstallSnapshotError>> {
        debug!(
            "Network callback send_install_snapshot target:{}, req: {:?}",
            self.target,
            req.summary()
        );

        let channel = self
            .conn
            .get_conn(&self.target_node.address)
            .await
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;

        let mut client = raft_service_time_out_client(
            channel,
            Duration::from_millis(self.config.install_snapshot_timeout),
            DEFAULT_GRPC_SERVER_MESSAGE_LEN,
            self.config.grpc_enable_gzip,
        );

        let data = bincode::serialize(&req)
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;
        let cmd = tonic::Request::new(RaftSnapshotReq {
            data,
            group_id: self.target_node.group_id,
        });

        let rsp = client
            .raft_snapshot(cmd)
            .await
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?
            .into_inner();

        let res: Result<InstallSnapshotResponse<u64>, RaftError<InstallSnapshotError>> =
            serde_json::from_str(&rsp.data)
                .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;

        res.map_err(|e| openraft::error::RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}
