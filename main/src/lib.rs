mod flight_sql;
mod http;
mod meta_single;
mod report;
mod rpc;
pub mod server;
mod spi;
mod tcp;
mod vector;

use std::sync::Arc;

use clap::ValueEnum;
use config::Config;
use memory_pool::GreedyMemoryPool;
use metrics::metric_register::MetricsRegister;
use once_cell::sync::Lazy;
use server::{Server, ServiceBuilder};
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use trace::jaeger::jaeger_exporter;
use trace::log::{CombinationTraceCollector, LogTraceCollector};
use trace::TraceExporter;
use trace_http::ctx::{SpanContextExtractor, TraceHeaderParser};
use tskv::Engine;

use crate::report::ReportService;

pub static VERSION: Lazy<String> = Lazy::new(|| {
    format!(
        "{}, revision {}",
        option_env!("CARGO_PKG_VERSION").unwrap_or("UNKNOWN"),
        option_env!("GIT_HASH").unwrap_or("UNKNOWN")
    )
});

pub struct Cnosdb {
    config: Arc<Config>,
    deployment_mode: DeploymentMode,

    runtime: Arc<Runtime>,
    service_builder: Arc<ServiceBuilder>,
    server: Arc<Mutex<Server>>,
    storage: Option<Arc<dyn Engine>>,
}

impl Cnosdb {
    pub fn new(config: Config) -> std::io::Result<Self> {
        let config = Arc::new(config);
        let mem_bytes = config.deployment.memory * 1024 * 1024 * 1024;
        let deployment_mode = config
            .deployment
            .mode
            .parse::<DeploymentMode>()
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("parsing deployment_mode failed: {e}"),
                )
            })?;

        let runtime = Arc::new(init_runtime(Some(config.deployment.cpu))?);
        let memory_pool = Arc::new(GreedyMemoryPool::new(mem_bytes));
        let service_builder = server::ServiceBuilder {
            cpu: config.deployment.cpu,
            config: config.as_ref().to_owned(),
            runtime: runtime.clone(),
            memory_pool,
            metrics_register: Arc::new(MetricsRegister::new([(
                "node_id",
                config.node_basic.node_id.to_string(),
            )])),
            span_context_extractor: build_span_context_extractor(&config),
        };

        let mut server = server::Server::default();
        if !config.reporting_disabled {
            server.add_service(Box::new(ReportService::new()));
        }

        Ok(Self {
            config,
            deployment_mode,

            runtime,
            service_builder: Arc::new(service_builder),
            server: Arc::new(Mutex::new(server)),
            storage: None,
        })
    }

    pub fn start(&mut self) -> std::io::Result<()> {
        verify_license(self.deployment_mode, &self.config)?;

        let deployment_mode = self.deployment_mode;
        let builder = self.service_builder.clone();
        let server = self.server.clone();
        self.storage = self.runtime.block_on(async move {
            let mut server = server.lock().await;
            match deployment_mode {
                DeploymentMode::QueryTskv => builder.build_query_storage(&mut server).await,
                DeploymentMode::Tskv => builder.build_storage_server(&mut server).await,
                DeploymentMode::Query => builder.build_query_server(&mut server).await,
                DeploymentMode::Singleton => builder.build_singleton(&mut server).await,
            }
        });

        trace::info!("CnosDB server start as {} mode", deployment_mode);
        let server = self.server.clone();
        self.runtime
            .block_on(async move {
                match server.lock().await.start() {
                    Ok(_) => {
                        trace::info!("CnosDB server started");
                        Ok(())
                    }
                    Err(e) => {
                        trace::error!("CnosDB server failed to start: {e}");
                        Err(e)
                    }
                }
            })
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("starting server failed: {e}"),
                )
            })
    }
}

impl Drop for Cnosdb {
    fn drop(&mut self) {
        let server = self.server.clone();
        let storage = self.storage.clone();
        self.runtime.block_on(async move {
            server.lock().await.stop(true).await;
            if let Some(tskv) = storage {
                tskv.close().await;
            }
            trace::info!("CnosDB server stopped");
        });
    }
}

#[derive(Debug, Copy, Clone, ValueEnum, PartialEq, Eq)]
#[clap(rename_all = "snake_case")]
pub enum DeploymentMode {
    /// Default, Run query and tskv engines.
    QueryTskv,
    /// Only run the tskv engine.
    Tskv,
    /// Only run the query engine.
    Query,
    /// Stand-alone deployment.
    Singleton,
}

impl std::str::FromStr for DeploymentMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mode = match s.to_ascii_lowercase().as_str() {
            "query_tskv" => Self::QueryTskv,
            "tskv" => Self::Tskv,
            "query" => Self::Query,
            "singleton" => Self::Singleton,
            _ => {
                return Err(
                    "deployment must be one of [query_tskv, tskv, query, singleton]".to_string(),
                )
            }
        };
        Ok(mode)
    }
}

impl std::fmt::Display for DeploymentMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeploymentMode::QueryTskv => write!(f, "query_tskv"),
            DeploymentMode::Tskv => write!(f, "tskv"),
            DeploymentMode::Query => write!(f, "query"),
            DeploymentMode::Singleton => write!(f, "singleton"),
        }
    }
}

fn init_runtime(cores: Option<usize>) -> std::io::Result<Runtime> {
    use tokio::runtime::Builder;
    match cores {
        None => Runtime::new(),
        Some(cores) => match cores {
            0 => Builder::new_multi_thread()
                .enable_all()
                .thread_stack_size(4 * 1024 * 1024)
                .build(),
            _ => Builder::new_multi_thread()
                .enable_all()
                .worker_threads(cores)
                .thread_stack_size(4 * 1024 * 1024)
                .build(),
        },
    }
}

fn build_span_context_extractor(config: &Config) -> Arc<SpanContextExtractor> {
    let mut res: Vec<Arc<dyn TraceExporter>> = Vec::new();
    let mode = &config.deployment.mode;
    let node_id = config.node_basic.node_id;
    let service_name = format!("cnosdb_{mode}_{node_id}");

    if let Some(trace_log_collector_config) = &config.trace.log {
        trace::info!(
            "Log trace collector created, path: {}",
            trace_log_collector_config.path.display()
        );
        res.push(Arc::new(LogTraceCollector::new(trace_log_collector_config)))
    }

    if let Some(trace_config) = &config.trace.jaeger {
        let exporter =
            jaeger_exporter(trace_config, service_name).expect("build jaeger trace exporter");
        trace::info!("Jaeger trace exporter created");
        res.push(exporter);
    }

    // TODO HttpCollector
    let collector: Option<Arc<dyn TraceExporter>> = if res.is_empty() {
        None
    } else if res.len() == 1 {
        res.pop()
    } else {
        Some(Arc::new(CombinationTraceCollector::new(res)))
    };

    let parser = TraceHeaderParser::new(config.trace.auto_generate_span);

    Arc::new(SpanContextExtractor::new(parser, collector))
}

pub(crate) fn verify_license(
    deployment_mode: DeploymentMode,
    config: &Config,
) -> std::io::Result<()> {
    let license_file_path = &config.license_file;
    let license = license::LicenseConfig::verify(license_file_path).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("license verification failed: path: '{license_file_path}'; error: {e}"),
        )
    })?;

    let mut err_message = String::new();
    match deployment_mode {
        DeploymentMode::QueryTskv | DeploymentMode::Tskv | DeploymentMode::Query => {
            if license.deployment_mode != license::DEPLOYMENT_MODE_CLUSTER {
                err_message.push_str(format!(
                    "license verification failed(deployment_mode): the configured is '{deployment_mode}' but the license is '{}'", license.deployment_mode
                ).as_str());
            }
        }
        DeploymentMode::Singleton => {
            if license.deployment_mode != license::DEPLOYMENT_MODE_SINGLETON {
                err_message.push_str(format!(
                    "license verification failed(deployment_mode): the configured is '{deployment_mode}' but the license is '{}'", license.deployment_mode
                ).as_str());
            }

            if config.deployment.cpu > license.query_tskv_instance_cores {
                if !err_message.is_empty() {
                    err_message.push_str("; ");
                }
                err_message.push_str(format!(
                    "license verification failed(runtime CPU), the cpu is '{}' but the license is '{}'",
                    config.deployment.cpu, license.query_tskv_instance_cores
                ).as_str());
            }
        }
    };
    if err_message.is_empty() {
        Ok(())
    } else {
        Err(std::io::Error::new(std::io::ErrorKind::Other, err_message))
    }
}

#[cfg(test)]
mod test {
    use crate::Cnosdb;

    async fn execute_sql(sql: impl Into<reqwest::Body>) -> Result<String, (u16, String)> {
        let client_builder = reqwest::Client::builder();
        let client = client_builder.build().unwrap();

        let request = client
            .post("http://127.0.0.1:8902/api/v1/sql")
            .basic_auth::<&str, &str>("root", Some("root"))
            .header("accept", "application/csv")
            .query(&[("db", "public")])
            .body(sql)
            .build()
            .unwrap();
        let response = client.execute(request).await.unwrap();
        match response.status() {
            reqwest::StatusCode::OK => {
                let body = response.text().await.unwrap();
                Ok(body)
            }
            other => {
                let body = response.text().await.unwrap();
                Err((other.as_u16(), body))
            }
        }
    }

    #[test]
    fn test() {
        let dir = "/tmp/test/main/start";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();

        let mut config = config::Config::default();
        config.deployment.mode = "singleton".to_string();
        config.license_file = "./config/license_singleton.json".to_string();
        config.hinted_off.path = format!("{dir}/hh");
        config.log.path = format!("{dir}/log");
        config.storage.path = format!("{dir}/storage");
        config.wal.path = format!("{dir}/wal");

        let mut cnosdb = Cnosdb::new(config).unwrap();
        cnosdb.start().unwrap();

        let databases = cnosdb
            .runtime
            .block_on(execute_sql("show databases"))
            .unwrap();
        assert_eq!(databases, "database_name\npublic\nusage_schema\n");
    }
}
