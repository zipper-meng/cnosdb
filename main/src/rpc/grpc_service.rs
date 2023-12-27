use std::net::SocketAddr;
use std::sync::Arc;

use config::TLSConfig;
use coordinator::service::CoordinatorRef;
use metrics::metric_register::MetricsRegister;
use protos::kv_service::tskv_service_server::TskvServiceServer;
use protos::raft_service::raft_service_server::RaftServiceServer;
use protos::DEFAULT_GRPC_SERVER_MESSAGE_LEN;
use replication::network_grpc::RaftCBServer;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tonic::codec::CompressionEncoding;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use trace_http::ctx::SpanContextExtractor;
use trace_http::tower_layer::TraceLayer;
use tskv::EngineRef;

use crate::server;
use crate::rpc::tskv::TskvServiceImpl;
use crate::server::ServiceHandle;
use crate::spi::service::Service;

pub struct GrpcService {
    addr: SocketAddr,
    runtime: Arc<Runtime>,
    kv_inst: EngineRef,
    coord: CoordinatorRef,
    tls_config: Option<TLSConfig>,
    metrics_register: Arc<MetricsRegister>,
    span_context_extractor: Arc<SpanContextExtractor>,
    handle: Option<ServiceHandle<Result<(), tonic::transport::Error>>>,
    enable_gzip: bool,
}

impl GrpcService {
    pub fn new(
        runtime: Arc<Runtime>,
        kv_inst: EngineRef,
        coord: CoordinatorRef,
        addr: SocketAddr,
        tls_config: Option<TLSConfig>,
        metrics_register: Arc<MetricsRegister>,
        span_context_extractor: Arc<SpanContextExtractor>,
        enable_gzip: bool,
    ) -> Self {
        Self {
            addr,
            runtime,
            kv_inst,
            coord,
            tls_config,
            metrics_register,
            span_context_extractor,
            handle: None,
            enable_gzip,
        }
    }
}

macro_rules! build_grpc_server {
    ($tls_config:expr, $trace_collector:expr) => {{
        let trace_layer = TraceLayer::new($trace_collector, "grpc");
        let mut server = Server::builder().layer(trace_layer);

        if let Some(TLSConfig {
            certificate,
            private_key,
        }) = $tls_config
        {
            let cert = std::fs::read(certificate)?;
            let key = std::fs::read(private_key)?;
            let identity = Identity::from_pem(cert, key);
            server = server.tls_config(ServerTlsConfig::new().identity(identity))?;
        }

        server
    }};
}

#[async_trait::async_trait]
impl Service for GrpcService {
    fn start(&mut self) -> server::Result<()> {
        let (shutdown, rx) = oneshot::channel();
        let mut tskv_grpc_service = TskvServiceServer::new(TskvServiceImpl {
            runtime: self.runtime.clone(),
            kv_inst: self.kv_inst.clone(),
            coord: self.coord.clone(),
            metrics_register: self.metrics_register.clone(),
            grpc_enable_gzip: self.enable_gzip,
        })
        .max_decoding_message_size(DEFAULT_GRPC_SERVER_MESSAGE_LEN);

        let multi_raft = self.coord.raft_manager().multi_raft();
        let mut raft_grpc_service = RaftServiceServer::new(RaftCBServer::new(multi_raft))
            .max_decoding_message_size(DEFAULT_GRPC_SERVER_MESSAGE_LEN);

        if self.enable_gzip {
            tskv_grpc_service = tskv_grpc_service
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip);

            raft_grpc_service = raft_grpc_service
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip);
        }

        let mut grpc_builder =
            build_grpc_server!(&self.tls_config, self.span_context_extractor.clone());
        let grpc_router = grpc_builder
            .add_service(tskv_grpc_service)
            .add_service(raft_grpc_service);
        let server = grpc_router.serve_with_shutdown(self.addr, async {
            rx.await.ok();
            trace::info!("grpc server graceful shutdown!");
        });
        trace::info!("grpc server start addr: {}", self.addr);
        let grpc_handle = tokio::spawn(server);
        self.handle = Some(ServiceHandle::new(
            "grpc service".to_string(),
            grpc_handle,
            shutdown,
        ));
        Ok(())
    }

    async fn stop(&mut self, force: bool) {
        if let Some(stop) = self.handle.take() {
            stop.shutdown(force).await
        };
    }
}
