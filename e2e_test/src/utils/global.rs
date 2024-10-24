#![allow(dead_code)]

use std::collections::{BTreeMap, HashSet};
use std::io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::ops::RangeInclusive;
use std::sync::{Arc, Once};

use parking_lot::Mutex;
use tokio::runtime::Runtime;

use crate::case::E2eExecutor;
use crate::cluster_def::{CnosdbClusterDefinition, DataNodeDefinition};
use crate::utils::{FnMutCnosdbConfig, FnMutMetaStoreConfig};

pub const LOOPBACK_IP: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 1);
pub const WILDCARD_IP: Ipv4Addr = Ipv4Addr::new(0_u8, 0_u8, 0_u8, 0_u8);
pub const MIN_PORT: u16 = 17000;
pub const MAX_PORT: u16 = 18000;

pub fn init_test(case_group: &str, case_name: &str) -> E2eContext {
    static mut GLOBAL_CONTEXT: Option<Arc<E2eGlobalContext>> = None;

    static INIT_TEST: Once = Once::new();
    INIT_TEST.call_once(|| unsafe {
        GLOBAL_CONTEXT = Some(Arc::new(E2eGlobalContext::default()));
    });

    let global_context = unsafe {
        GLOBAL_CONTEXT
            .clone()
            .expect("initialized by INIT_TEST.call_once")
    };

    E2eContext {
        case_group: Arc::new(case_group.to_string()),
        case_name: Arc::new(case_name.to_string()),
        cluster_definition: None,
        global_context,
    }
}

pub struct E2eContext {
    case_group: Arc<String>,
    case_name: Arc<String>,
    cluster_definition: Option<Arc<CnosdbClusterDefinition>>,
    global_context: Arc<E2eGlobalContext>,
}

impl E2eContext {
    pub fn next_wildcard_addr(&self) -> IoResult<SocketAddrV4> {
        self.global_context.port_picker.lock().next_wildcard_addr()
    }

    pub fn next_addr(&self, host: Ipv4Addr) -> IoResult<SocketAddrV4> {
        self.global_context.port_picker.lock().next_addr(host)
    }

    pub fn build_executor_for_cluster(
        &mut self,
        cluster_definition: CnosdbClusterDefinition,
    ) -> E2eExecutor {
        self.build_executor_for_cluster_with_customized_config(cluster_definition, vec![], vec![])
    }

    pub fn build_executor_for_cluster_with_customized_config(
        &mut self,
        mut cluster_definition: CnosdbClusterDefinition,
        update_meta_config_fn: Vec<Option<FnMutMetaStoreConfig>>,
        update_data_config_fn: Vec<Option<FnMutCnosdbConfig>>,
    ) -> E2eExecutor {
        for def in cluster_definition.meta_cluster_def.iter_mut() {
            def.reset_ports(self);
        }
        for def in cluster_definition.data_cluster_def.iter_mut() {
            def.reset_ports(self);
        }
        let cluster_definition = Arc::new(cluster_definition);
        self.cluster_definition = Some(cluster_definition.clone());
        if update_data_config_fn.is_empty() && update_data_config_fn.is_empty() {
            E2eExecutor::new_cluster(self)
        } else {
            E2eExecutor::new_cluster_with_customized_config(
                self,
                update_meta_config_fn,
                update_data_config_fn,
            )
        }
    }

    pub fn build_executor_for_singleton(
        &mut self,
        data_node_def: DataNodeDefinition,
    ) -> E2eExecutor {
        self.build_executor_for_cluster(CnosdbClusterDefinition {
            meta_cluster_def: vec![],
            data_cluster_def: vec![data_node_def],
        })
    }

    pub fn build_executor_for_singleton_with_customized_config(
        &mut self,
        data_node_def: DataNodeDefinition,
        update_data_config_fn: Option<FnMutCnosdbConfig>,
    ) -> E2eExecutor {
        self.build_executor_for_cluster_with_customized_config(
            CnosdbClusterDefinition {
                meta_cluster_def: vec![],
                data_cluster_def: vec![data_node_def],
            },
            vec![],
            vec![update_data_config_fn],
        )
    }

    pub fn case_group(&self) -> Arc<String> {
        self.case_group.clone()
    }

    pub fn case_name(&self) -> Arc<String> {
        self.case_name.clone()
    }

    pub fn cluster_definition(&self) -> Option<Arc<CnosdbClusterDefinition>> {
        self.cluster_definition.clone()
    }

    pub fn runtime(&self) -> Arc<Runtime> {
        self.global_context.runtime.clone()
    }
}

pub struct E2eGlobalContext {
    runtime: Arc<Runtime>,
    port_picker: Arc<Mutex<Ipv4PortPicker>>,
}

impl Default for E2eGlobalContext {
    fn default() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .build()
            .unwrap();
        Self {
            runtime: Arc::new(runtime),
            port_picker: Arc::new(Mutex::new(Ipv4PortPicker::default())),
        }
    }
}

pub struct Ipv4PortPicker {
    range: RangeInclusive<u16>,

    /// Maps a host IP to the next port number and the set of ports that have been used.
    using: BTreeMap<Ipv4Addr, (u16, HashSet<u16>)>,
}

impl Default for Ipv4PortPicker {
    fn default() -> Self {
        Self {
            range: MIN_PORT..=MAX_PORT,

            using: BTreeMap::new(),
        }
    }
}

impl Ipv4PortPicker {
    pub fn new(range: RangeInclusive<u16>) -> Self {
        Self {
            range,

            ..Default::default()
        }
    }

    /// Get the next port for IP 0.0.0.0 .
    pub fn next_wildcard_addr(&mut self) -> IoResult<SocketAddrV4> {
        self.next_addr(WILDCARD_IP)
    }

    /// Get the next port for the given host IP.
    pub fn next_addr(&mut self, host: Ipv4Addr) -> IoResult<SocketAddrV4> {
        let (port_cur, ports_using) = self
            .using
            .entry(host)
            .or_insert((*self.range.start(), HashSet::new()));

        for port in *port_cur..=*self.range.end() {
            if ports_using.contains(&port) {
                continue;
            }
            if Self::try_bind(host, port)? {
                ports_using.insert(port);
                *port_cur = port + 1;
                return Ok(SocketAddrV4::new(host, port));
            }
        }
        for port in *self.range.start()..=*port_cur {
            if ports_using.contains(&port) {
                continue;
            }
            if Self::try_bind(host, port)? {
                ports_using.insert(port);
                *port_cur = port + 1;
                return Ok(SocketAddrV4::new(host, port));
            }
        }
        Err(IoError::new(
            IoErrorKind::Other,
            format!("No available ports in {:?}", self.range),
        ))
    }

    fn try_bind(host: Ipv4Addr, port: u16) -> IoResult<bool> {
        let socket_addr = SocketAddrV4::new(host, port);
        match TcpListener::bind(socket_addr) {
            Ok(_) => Ok(true),
            Err(e) => {
                println!("Bind error: {e:?}");
                match e.kind() {
                    // code=49, message="Can't bind to the address"
                    IoErrorKind::AddrNotAvailable => Err(e),
                    // Could not resolve to any addresses
                    IoErrorKind::InvalidInput => Err(e),
                    // code=48, message="Address already in use"
                    IoErrorKind::AddrInUse => Ok(false),
                    // code=13, message="Permission denied"
                    // code=47, kind=Uncategorized, message="Address family not supported by protocol family"
                    _ => Err(e),
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test() {
        let mut picker = Ipv4PortPicker::default();
        {
            let mut listeners = Vec::new();
            let mut used_addrs = Vec::new();
            for _ in 0..5 {
                let addr = picker.next_wildcard_addr().unwrap();
                println!("Listening on: {addr:?}");
                let listener = TcpListener::bind(addr).unwrap();
                println!("Listened on: {:?}", listener.local_addr().unwrap());
                listeners.push(listener);
                used_addrs.push(addr);
            }
            for addr in used_addrs {
                println!("Trying bind twice on: {:?}", addr);
                let bind_result = TcpListener::bind(addr);
                assert!(bind_result.is_err());
                println!("Expected binding error: {:?}", bind_result.unwrap_err());
            }
        }
        {
            // Try to bind invalid address.
            let addr = picker.next_addr(Ipv4Addr::new(1, 0, 0, 1));
            assert!(addr.is_err());
            let addr = picker.next_addr(Ipv4Addr::new(255, 255, 255, 255));
            assert!(addr.is_err());
        }
    }
}
