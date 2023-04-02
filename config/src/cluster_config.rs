use std::net::ToSocketAddrs;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};
use crate::environment::OverrideByEnv;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterConfig {
    #[serde(default = "ClusterConfig::default_node_id")]
    pub node_id: u64,
    #[serde(default = "ClusterConfig::default_name")]
    pub name: String,
    #[serde(default = "ClusterConfig::default_meta_service_addr")]
    pub meta_service_addr: String,

    #[serde(default = "ClusterConfig::default_http_listen_addr")]
    pub http_listen_addr: String,
    #[serde(default = "ClusterConfig::default_grpc_listen_addr")]
    pub grpc_listen_addr: String,
    #[serde(default = "ClusterConfig::default_flight_rpc_listen_addr")]
    pub flight_rpc_listen_addr: String,
    #[serde(default = "ClusterConfig::default_store_metrics")]
    pub store_metrics: bool,

    #[serde(default = "ClusterConfig::default_cold_data_server")]
    pub cold_data_server: bool,
}

impl ClusterConfig {
    fn default_node_id() -> u64 {
        100
    }

    fn default_name() -> String {
        "cluster_xxx".to_string()
    }

    fn default_meta_service_addr() -> String {
        "127.0.0.1:21001".to_string()
    }

    fn default_http_listen_addr() -> String {
        "127.0.0.1:31007".to_string()
    }

    fn default_grpc_listen_addr() -> String {
        "127.0.0.1:31008".to_string()
    }

    fn default_flight_rpc_listen_addr() -> String {
        "127.0.0.1:31006".to_string()
    }

    fn default_store_metrics() -> bool {
        true
    }

    fn default_cold_data_server() -> bool {
        false
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_id: Self::default_node_id(),
            name: Self::default_name(),
            meta_service_addr: Self::default_meta_service_addr(),
            http_listen_addr: Self::default_http_listen_addr(),
            grpc_listen_addr: Self::default_grpc_listen_addr(),
            flight_rpc_listen_addr: Self::default_flight_rpc_listen_addr(),
            store_metrics: Self::default_store_metrics(),
            cold_data_server: Self::default_cold_data_server(),
        }
    }
}

impl OverrideByEnv for ClusterConfig {
    fn override_by_env(&mut self) {
        if let Ok(id) = std::env::var("CNOSDB_CLUSTER_NODE_ID") {
            self.node_id = id.parse::<u64>().unwrap();
        }
        if let Ok(name) = std::env::var("CNOSDB_CLUSTER_NAME") {
            self.name = name;
        }
        if let Ok(addr) = std::env::var("CNOSDB_CLUSTER_META_SERVICE_ADDR") {
            self.meta_service_addr = addr;
        }

        if let Ok(addr) = std::env::var("CNOSDB_CLUSTER_HTTP_LISTEN_ADDR") {
            self.http_listen_addr = addr;
        }
        if let Ok(addr) = std::env::var("CNOSDB_CLUSTER_GRPC_LISTEN_ADDR") {
            self.grpc_listen_addr = addr;
        }
        if let Ok(addr) = std::env::var("CNOSDB_CLUSTER_FLIGHT_RPC_LISTEN_ADDR") {
            self.flight_rpc_listen_addr = addr;
        }
    }
}

impl CheckConfig for ClusterConfig {
    fn check(&self, _: &crate::Config) -> Option<CheckConfigResult> {
        let config_name = Arc::new("cluster".to_string());
        let mut ret = CheckConfigResult::default();

        if self.name.is_empty() {
            ret.add_warn(CheckConfigItemResult {
                config: config_name.clone(),
                item: "name".to_string(),
                message: "'name' is empty".to_string(),
            });
        }

        if let Err(e) = self.meta_service_addr.to_socket_addrs() {
            ret.add_error(CheckConfigItemResult {
                config: config_name.clone(),
                item: "meta_service_addr".to_string(),
                message: format!("Cannot resolve 'meta_service_addr': {}", e),
            });
        }
        if let Err(e) = self.http_listen_addr.to_socket_addrs() {
            ret.add_error(CheckConfigItemResult {
                config: config_name.clone(),
                item: "http_listen_addr".to_string(),
                message: format!("Cannot resolve 'http_listen_addr': {}", e),
            });
        }
        if let Err(e) = self.grpc_listen_addr.to_socket_addrs() {
            ret.add_error(CheckConfigItemResult {
                config: config_name.clone(),
                item: "grpc_listen_addr".to_string(),
                message: format!("Cannot resolve 'grpc_listen_addr': {}", e),
            });
        }
        if let Err(e) = self.flight_rpc_listen_addr.to_socket_addrs() {
            ret.add_error(CheckConfigItemResult {
                config: config_name,
                item: "flight_rpc_listen_addr".to_string(),
                message: format!("Cannot resolve 'flight_rpc_listen_addr': {}", e),
            });
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
