#![allow(dead_code)]

use core::panic;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use config::Config as CnosdbConfig;
use meta::client::MetaHttpClient;
use meta::store::config::Opt as MetaStoreConfig;
use reqwest::blocking::{ClientBuilder, Request, RequestBuilder, Response};
use reqwest::{Certificate, IntoUrl, Method, StatusCode};
use sysinfo::{ProcessExt, ProcessRefreshKind, RefreshKind, System, SystemExt};
use tokio::runtime::Runtime;

use crate::cluster_def::{
    CnosdbClusterDefinition, DataNodeDefinition, DeploymentMode, MetaNodeDefinition,
};
use crate::{E2eError, E2eResult};

pub const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");

pub fn get_workspace_dir() -> PathBuf {
    let crate_dir = std::path::PathBuf::from(CRATE_DIR);
    crate_dir
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or(crate_dir)
}

#[macro_export]
macro_rules! headers {
    ($($k:expr => $v:expr),*) => {
        {
            let mut m = reqwest::header::HeaderMap::new();
            $(
                m.insert(
                    reqwest::header::HeaderName::from_static($k),
                    reqwest::header::HeaderValue::from_static($v),
                );
            )*
            m
        }
    };
}

#[macro_export]
macro_rules! assert_response_is_ok {
    ($resp:expr) => {
        assert_eq!(
            reqwest::StatusCode::OK,
            $resp.status(),
            "{}",
            $resp
                .text()
                .unwrap_or_else(|e| format!("failed to fetch response: {e}"))
        );
    };
}

#[derive(Debug, Clone)]
pub struct Client {
    inner: reqwest::blocking::Client,
    user: String,
    password: Option<String>,
}

impl Client {
    pub fn new() -> Self {
        let inner = ClientBuilder::new().build().unwrap_or_else(|e| {
            panic!("Failed to build http client: {}", e);
        });
        Self {
            inner,
            user: String::new(),
            password: None,
        }
    }

    pub fn with_auth(user: String, password: Option<String>) -> Self {
        let inner = ClientBuilder::new().build().unwrap_or_else(|e| {
            panic!("Failed to build http client: {}", e);
        });
        Self {
            inner,
            user,
            password,
        }
    }

    pub fn with_auth_and_tls(
        user: String,
        password: Option<String>,
        crt_path: impl AsRef<Path>,
    ) -> Self {
        let cert_bytes = std::fs::read(crt_path).expect("fail to read crt file");
        let cert = Certificate::from_pem(&cert_bytes).expect("fail to load crt file");
        let inner = ClientBuilder::new()
            .add_root_certificate(cert)
            .build()
            .unwrap_or_else(|e| {
                panic!("Failed to build http client with tls: {}", e);
            });
        Self {
            inner,
            user,
            password,
        }
    }

    pub fn user(&self) -> &str {
        self.user.as_str()
    }

    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    pub fn auth(&self) -> String {
        format!("{}:{}", self.user, self.password.as_deref().unwrap_or(""))
    }

    /// Returns request builder with method and url.
    pub fn request(&self, method: Method, url: impl IntoUrl) -> RequestBuilder {
        self.inner.request(method, url)
    }

    /// Returns request builder with method and url, and basic_auth header if user is not empty.
    pub fn request_with_auth(&self, method: Method, url: impl IntoUrl) -> RequestBuilder {
        let mut req_builder = self.inner.request(method, url);
        if !self.user.is_empty() {
            req_builder = req_builder.basic_auth(&self.user, self.password.as_ref());
        }
        req_builder
    }

    pub fn execute(&self, request: Request) -> E2eResult<Response> {
        self.inner
            .execute(request)
            .map_err(|e| E2eError::Connect(format!("HTTP execute failed: {e}")))
    }

    pub fn send<U: IntoUrl + std::fmt::Display>(
        &self,
        method: Method,
        url: U,
        body: &str,
        content_encoding: Option<&str>,
        accept_encoding: Option<&str>,
    ) -> E2eResult<Response> {
        let url_str = format!("{url}");
        let mut req_builder = self.request_with_auth(method, url);
        if let Some(encoding) = content_encoding {
            req_builder = req_builder.header(reqwest::header::CONTENT_ENCODING, encoding);
        }
        if let Some(encoding) = accept_encoding {
            req_builder = req_builder.header(reqwest::header::CONTENT_ENCODING, encoding);
        }
        if !body.is_empty() {
            req_builder = req_builder.body(body.to_string());
        }

        match req_builder.send() {
            Ok(r) => Ok(r),
            Err(e) => Err(Self::map_reqwest_err(e, url_str.as_str(), body)),
        }
    }

    pub fn map_reqwest_err(e: reqwest::Error, url: &str, req: &str) -> E2eError {
        let msg = format!("HTTP request failed: url: '{url}', req: '{req}', error: {e}");
        E2eError::Connect(msg)
    }

    pub fn map_reqwest_resp_err(resp: Response, url: &str, req: &str) -> E2eError {
        let status = resp.status();
        match resp.text() {
            Ok(resp_msg) => E2eError::Api {
                status,
                url: Some(url.to_string()),
                req: Some(req.to_string()),
                resp: Some(resp_msg),
            },
            Err(e) => E2eError::Http {
                status,
                url: Some(url.to_string()),
                req: Some(req.to_string()),
                err: Some(e.to_string()),
            },
        }
    }

    pub fn get<U: IntoUrl + std::fmt::Display>(&self, url: U, body: &str) -> E2eResult<Response> {
        self.send(Method::GET, url, body, None, None)
    }

    pub fn post<U: IntoUrl + std::fmt::Display>(&self, url: U, body: &str) -> E2eResult<Response> {
        self.send(Method::POST, url, body, None, None)
    }

    pub fn post_json<U: IntoUrl + std::fmt::Display>(
        &self,
        url: U,
        body: &str,
    ) -> E2eResult<Response> {
        self.send(Method::POST, url, body, Some("application/json"), None)
    }

    pub fn put<U: IntoUrl + std::fmt::Display>(&self, url: U, body: &str) -> E2eResult<Response> {
        self.send(Method::PUT, url, body, None, None)
    }

    pub fn delete<U: IntoUrl + std::fmt::Display>(
        &self,
        url: U,
        body: &str,
    ) -> E2eResult<Response> {
        self.send(Method::DELETE, url, body, None, None)
    }

    pub fn head<U: IntoUrl + std::fmt::Display>(&self, url: U, body: &str) -> E2eResult<Response> {
        self.send(Method::HEAD, url, body, None, None)
    }

    pub fn options<U: IntoUrl + std::fmt::Display>(
        &self,
        url: U,
        body: &str,
    ) -> E2eResult<Response> {
        self.send(Method::OPTIONS, url, body, None, None)
    }

    pub fn connect<U: IntoUrl + std::fmt::Display>(
        &self,
        url: U,
        body: &str,
    ) -> E2eResult<Response> {
        self.send(Method::CONNECT, url, body, None, None)
    }

    pub fn patch<U: IntoUrl + std::fmt::Display>(&self, url: U, body: &str) -> E2eResult<Response> {
        self.send(Method::PATCH, url, body, None, None)
    }

    pub fn trace<U: IntoUrl + std::fmt::Display>(&self, url: U, body: &str) -> E2eResult<Response> {
        self.send(Method::TRACE, url, body, None, None)
    }

    pub fn api_v1_sql<U: IntoUrl + std::fmt::Display>(
        &self,
        url: U,
        sql: &str,
    ) -> E2eResult<Vec<String>> {
        let url_str = format!("{url}");
        let resp = self.post(url, sql)?;
        if resp.status() != StatusCode::OK {
            return Err(Client::map_reqwest_resp_err(resp, &url_str, sql));
        }
        Ok(resp
            .text()
            .map_err(|e| Client::map_reqwest_err(e, &url_str, sql))?
            .trim()
            .split_terminator('\n')
            .map(|s| s.to_owned())
            .collect::<Vec<_>>())
    }

    pub fn api_v1_write<U: IntoUrl + std::fmt::Display>(
        &self,
        url: U,
        req: &str,
    ) -> E2eResult<Vec<String>> {
        let url_str = format!("{url}");
        let resp = self.post(url, req)?;
        if resp.status() != StatusCode::OK {
            return Err(Client::map_reqwest_resp_err(resp, &url_str, req));
        }
        Ok(resp
            .text()
            .map_err(|e| Client::map_reqwest_err(e, &url_str, req))?
            .trim()
            .split_terminator('\n')
            .map(|s| s.to_owned())
            .collect::<Vec<_>>())
    }
}

#[test]
#[ignore]
fn test_reqwest_https() {
    let workspace_dir = get_workspace_dir();
    let ca_crt_path = workspace_dir.join("config").join("tls").join("ca.crt");
    let cert_bytes = std::fs::read(ca_crt_path).expect("fail to read crt file");
    let cert = Certificate::from_pem(&cert_bytes).expect("fail to load crt file");
    let client = ClientBuilder::new()
        .add_root_certificate(cert)
        .build()
        .unwrap_or_else(|e| {
            panic!("Failed to build http client with tls: {}", e);
        });

    let resp = client
        .get("https://127.0.0.1:8902/api/v1/ping")
        .send()
        .unwrap();
    assert_response_is_ok!(resp);
    println!("{}", resp.text().unwrap());
}

#[test]
#[ignore]
fn test_reqwest_http() {
    let client = ClientBuilder::new()
        .timeout(Duration::from_secs(70))
        .build()
        .unwrap_or_else(|e| {
            panic!("Failed to build http client: {}", e);
        });

    let resp = client
        .get("http://127.0.0.1:8902/api/v1/ping")
        .send()
        .unwrap();
    assert_response_is_ok!(resp);
    println!("{}", resp.text().unwrap());

    let resp = client
        .post("http://127.0.0.1:8902/api/v1/sql?db=public")
        .basic_auth("root", Option::<&str>::None)
        .body("show databases")
        .send()
        .unwrap();
    assert_response_is_ok!(resp);
    println!("{}", resp.text().unwrap());
}

#[tokio::test]
#[ignore]
async fn test_http_protocol_client() {
    let client =
        http_protocol::http_client::HttpClient::new("127.0.0.1", 8902, false, false, &[]).unwrap();
    let resp = client
        .post("/api/v1/sql")
        .query(&[("db", "public")])
        .basic_auth("root", Option::<&str>::None)
        .header("Accept-Encoding", "*")
        .body("show databases")
        .send()
        .await
        .unwrap();
    println!("{}", resp.text().await.unwrap());
}

fn cargo_build_cnosdb_meta(workspace_dir: impl AsRef<Path>) {
    let workspace_dir = workspace_dir.as_ref();
    println!("- Building 'meta' at '{}'", workspace_dir.display());
    let mut cargo_build = Command::new("cargo");
    let output = cargo_build
        .current_dir(workspace_dir)
        .args(["build", "--package", "meta", "--bin", "cnosdb-meta"])
        .output()
        .expect("failed to execute cargo build");
    if !output.status.success() {
        let message = format!(
            "Failed to build cnosdb-meta: stdout: {}, stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        panic!("Failed to build cnosdb-meta: {message}");
    }
    println!("- Build 'meta' at '{}' completed", workspace_dir.display());
}

fn cargo_build_cnosdb_data(workspace_dir: impl AsRef<Path>) {
    let workspace_dir = workspace_dir.as_ref();
    println!("Building 'main' at '{}'", workspace_dir.display());
    let mut cargo_build = Command::new("cargo");
    let output = cargo_build
        .current_dir(workspace_dir)
        .args(["build", "--package", "main", "--bin", "cnosdb"])
        .output()
        .expect("failed to execute cargo build");
    if !output.status.success() {
        let message = format!(
            "Failed to build cnosdb: stdout: {}, stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        panic!("Failed to build cnosdb-meta: {message}");
    }
    println!("Build 'main' at '{}' completed", workspace_dir.display());
}

pub struct CnosdbMetaTestHelper {
    pub runtime: Arc<Runtime>,
    pub workspace_dir: PathBuf,
    /// The meta test dir, usually /e2e_test/$mod/$test/meta
    pub test_dir: PathBuf,
    pub meta_node_definitions: Vec<MetaNodeDefinition>,
    pub exe_path: PathBuf,

    pub client: Arc<Client>,
    pub meta_client: Arc<MetaHttpClient>,
    pub sub_processes: HashMap<String, Child>,
}

impl CnosdbMetaTestHelper {
    pub fn new(
        runtime: Arc<Runtime>,
        workspace_dir: impl AsRef<Path>,
        test_base_dir: impl AsRef<Path>,
        meta_node_definitions: Vec<MetaNodeDefinition>,
    ) -> Self {
        let workspace_dir = workspace_dir.as_ref().to_path_buf();
        Self {
            runtime,
            workspace_dir: workspace_dir.clone(),
            test_dir: test_base_dir.as_ref().to_path_buf(),
            meta_node_definitions,
            exe_path: workspace_dir
                .join("target")
                .join("debug")
                .join("cnosdb-meta"),
            client: Arc::new(Client::new()),
            meta_client: Arc::new(MetaHttpClient::new("127.0.0.1:8901")),
            sub_processes: HashMap::with_capacity(3),
        }
    }

    pub fn run_single_meta(&mut self) {
        println!("Running cnosdb-meta at '{}'", self.workspace_dir.display());
        if self.meta_node_definitions.is_empty() {
            panic!("At least 1 meta configs are needed to run singleton");
        }
        let node_def = self.meta_node_definitions[0].clone();
        println!(
            "- Running cnosdb-meta with config '{}'",
            &node_def.config_file_name
        );
        let proc = self.execute(&node_def);
        self.sub_processes.insert(node_def.config_file_name, proc);
        thread::sleep(Duration::from_secs(5));

        println!("- Init cnosdb-meta ...");
        let master_host = format!("http://{}", &self.meta_node_definitions[0].host_port);
        self.client
            .post_json(format!("{master_host}/init").as_str(), "{}")
            .unwrap();
        thread::sleep(Duration::from_secs(1));
        self.client
            .post_json(format!("{master_host}/change-membership").as_str(), "[1]")
            .unwrap();
        thread::sleep(Duration::from_secs(1));
    }

    pub fn run_cluster(&mut self) {
        println!(
            "Running cnosdb-meta cluster at '{}'",
            self.workspace_dir.display()
        );
        if self.meta_node_definitions.len() < 2 {
            panic!("At least 2 meta configs are needed to run cluster");
        }

        let master_host = format!("http://{}", &self.meta_node_definitions[0].host_port);

        let mut wait_startup_threads = Vec::with_capacity(self.meta_node_definitions.len());
        for meta_node_def in self.meta_node_definitions.iter() {
            println!(
                "- Running cnosdb-meta with config '{}', host '{}",
                meta_node_def.config_file_name, meta_node_def.host_port
            );
            let proc = self.execute(meta_node_def);
            self.sub_processes
                .insert(meta_node_def.config_file_name.clone(), proc);
            wait_startup_threads.push(self.wait_startup(&meta_node_def.host_port));
        }
        thread::sleep(Duration::from_secs(3));
        for jh in wait_startup_threads {
            jh.join().unwrap();
        }

        println!("- Installing cnosdb-meta cluster...");
        // Call $host/init for master node
        self.client
            .post_json(format!("{master_host}/init").as_str(), "{}")
            .unwrap();
        thread::sleep(Duration::from_secs(1));

        // Call $host/add-learner for all follower nodes
        let mut all_node_ids = "[1".to_string();
        for meta_node_def in self.meta_node_definitions.iter().skip(1) {
            self.client
                .post_json(
                    format!("{master_host}/add-learner").as_str(),
                    format!("[{}, \"{}\"]", meta_node_def.id, meta_node_def.host_port).as_str(),
                )
                .unwrap();
            all_node_ids.push_str(format!(", {}", meta_node_def.id).as_str());
            thread::sleep(Duration::from_secs(1));
        }
        all_node_ids.push(']');

        // Call $host/change-membership
        self.client
            .post_json(
                format!("{master_host}/change-membership").as_str(),
                all_node_ids.as_str(),
            )
            .unwrap();
        thread::sleep(Duration::from_secs(1));
    }

    /// Wait cnosdb-meta startup by checking ping api in loop
    pub fn wait_startup(&self, host: &str) -> thread::JoinHandle<()> {
        let host = host.to_owned();
        let test_api = format!("http://{host}/debug");
        let startup_time = std::time::Instant::now();
        let client = self.client.clone();
        thread::spawn(move || {
            let mut counter = 0;
            loop {
                thread::sleep(Duration::from_secs(3));
                if let Err(e) = client.get(&test_api, "") {
                    println!(
                        "HTTP get '{test_api}' failed after {} seconds: {}",
                        startup_time.elapsed().as_secs(),
                        e
                    );
                } else {
                    break;
                }
                counter += 1;
                if counter == 30 {
                    panic!("Test case failed, waiting too long for {host} to startup");
                }
            }
        })
    }

    pub fn query(&self) -> String {
        self.client
            .get("http://127.0.0.1:8901/debug", "")
            .unwrap()
            .text()
            .unwrap()
    }

    fn execute(&self, node_def: &MetaNodeDefinition) -> Child {
        let config_file_path = node_def.to_config_path(&self.test_dir);
        println!(
            "Executing {} -c {}",
            self.exe_path.display(),
            config_file_path.display()
        );
        Command::new(&self.exe_path)
            .args([OsStr::new("-c"), config_file_path.as_os_str()])
            .stderr(Stdio::inherit())
            .stdout(Stdio::inherit())
            .spawn()
            .expect("failed to execute cnosdb-meta")
    }
}

impl Drop for CnosdbMetaTestHelper {
    fn drop(&mut self) {
        for (k, p) in self.sub_processes.drain() {
            println!("Killing cnosdb-meta ({k}) sub_processes: {}", p.id());
            kill_child_process(p, true);
        }
    }
}

pub struct CnosdbDataTestHelper {
    pub workspace_dir: PathBuf,
    /// The data test dir, usually /e2e_test/$mod/$test/data
    pub test_dir: PathBuf,
    pub data_node_definitions: Vec<DataNodeDefinition>,
    pub exe_path: PathBuf,
    pub enable_tls: bool,

    pub client: Arc<Client>,
    pub sub_processes: HashMap<String, (Child, DeploymentMode)>,
}

impl CnosdbDataTestHelper {
    pub fn new(
        workspace_dir: impl AsRef<Path>,
        test_dir: impl AsRef<Path>,
        data_node_definitions: Vec<DataNodeDefinition>,
        enable_tls: bool,
    ) -> Self {
        let workspace_dir = workspace_dir.as_ref().to_path_buf();
        let client = if enable_tls {
            let ca_crt_path = workspace_dir.join("config").join("tls").join("ca.crt");
            Arc::new(Client::with_auth_and_tls(
                "root".to_string(),
                Some(String::new()),
                ca_crt_path,
            ))
        } else {
            Arc::new(Client::with_auth("root".to_string(), Some(String::new())))
        };

        Self {
            workspace_dir: workspace_dir.clone(),
            test_dir: test_dir.as_ref().to_path_buf(),
            data_node_definitions,
            exe_path: workspace_dir.join("target").join("debug").join("cnosdb"),
            enable_tls,
            client,
            sub_processes: HashMap::with_capacity(2),
        }
    }

    pub fn run(&mut self) {
        println!("Running cnosdb at '{}'", self.workspace_dir.display());
        let node_definitions = self.data_node_definitions.clone();
        let mut wait_startup_threads = Vec::with_capacity(self.data_node_definitions.len());
        for data_node_def in node_definitions {
            println!(" - cnosdb '{}' starting", &data_node_def.http_host_port);
            let proc = self.execute(&data_node_def);
            self.sub_processes.insert(
                data_node_def.config_file_name.clone(),
                (proc, data_node_def.mode),
            );

            let jh = self.wait_startup(&data_node_def.http_host_port);
            wait_startup_threads.push((jh, data_node_def.http_host_port.clone()));
        }
        thread::sleep(Duration::from_secs(5));
        for (jh, addr) in wait_startup_threads {
            jh.join().unwrap();
            println!(" - cnosdb '{addr}' started");
        }
        thread::sleep(Duration::from_secs(1));
    }

    /// Wait cnosdb startup by checking ping api in loop
    pub fn wait_startup(&self, host_port: &str) -> thread::JoinHandle<()> {
        let host = host_port.to_owned();
        let ping_api = format!(
            "{}://{host}/api/v1/ping",
            if self.enable_tls { "https" } else { "http" }
        );
        let startup_time = std::time::Instant::now();
        let client = self.client.clone();
        thread::spawn(move || {
            let mut counter = 0;
            loop {
                thread::sleep(Duration::from_secs(3));
                if let Err(e) = client.get(&ping_api, "") {
                    println!(
                        "HTTP get '{ping_api}' failed after {} seconds: {}",
                        startup_time.elapsed().as_secs(),
                        e
                    );
                } else {
                    break;
                }
                counter += 1;
                if counter == 30 {
                    panic!("Test case failed, waiting too long for {host} to startup");
                }
            }
        })
    }

    pub fn restart_one_node(&mut self, node_def: &DataNodeDefinition) {
        let (proc, _) = self
            .sub_processes
            .remove(&node_def.config_file_name)
            .unwrap_or_else(|| panic!("No data node created with {}", &node_def.config_file_name));
        kill_child_process(proc, false);

        let new_proc = self.execute(node_def);
        self.sub_processes
            .insert(node_def.config_file_name.clone(), (new_proc, node_def.mode));

        let jh = self.wait_startup(&node_def.http_host_port);
        jh.join().unwrap();
    }

    pub fn stop_one_node(&mut self, config_file_name: &str, force: bool) {
        let (proc, _) = self
            .sub_processes
            .remove(config_file_name)
            .unwrap_or_else(|| panic!("No data node created with {}", config_file_name));
        kill_child_process(proc, force);
    }

    pub fn start_one_node(&mut self, node_def: &DataNodeDefinition) {
        let config_path = node_def.to_config_path(&self.test_dir);
        let new_proc = Command::new(&self.exe_path)
            .args([
                OsStr::new("run"),
                OsStr::new("--config"),
                config_path.as_os_str(),
                OsStr::new("-M"),
                &OsString::from(node_def.mode.to_string()),
            ])
            .stderr(Stdio::inherit())
            .stdout(Stdio::inherit())
            .spawn()
            .expect("failed to execute cnosdb");
        self.sub_processes.insert(
            node_def.config_file_name.to_string(),
            (new_proc, node_def.mode),
        );
        let jh = self.wait_startup(&node_def.http_host_port);
        jh.join().unwrap();
    }

    fn execute(&self, node_def: &DataNodeDefinition) -> Child {
        let config_path = node_def.to_config_path(&self.test_dir);
        println!(
            "Executing {} run --config {} -M {}",
            self.exe_path.display(),
            config_path.display(),
            node_def.mode,
        );
        Command::new(&self.exe_path)
            .args([
                OsStr::new("run"),
                OsStr::new("--config"),
                config_path.as_os_str(),
                OsStr::new("-M"),
                OsString::from(node_def.mode.to_string()).as_os_str(),
            ])
            .stderr(Stdio::inherit())
            .stdout(Stdio::inherit())
            .spawn()
            .expect("failed to execute cnosdb")
    }
}

impl Drop for CnosdbDataTestHelper {
    fn drop(&mut self) {
        for (k, (p, _)) in self.sub_processes.drain() {
            println!("Killing cnosdb ({k}) sub_processes: {}", p.id());
            kill_child_process(p, true);
        }
    }
}

/// Run CnosDB cluster.
///
/// - Meta server directory: $test_dir/meta
/// - Data server directory: $test_dir/data
pub fn run_cluster(
    test_dir: impl AsRef<Path>,
    runtime: Arc<Runtime>,
    cluster_def: &CnosdbClusterDefinition,
    generate_meta_config: bool,
    generate_data_config: bool,
) -> (Option<CnosdbMetaTestHelper>, Option<CnosdbDataTestHelper>) {
    let test_dir = test_dir.as_ref().to_path_buf();
    let workspace_dir = get_workspace_dir();
    cargo_build_cnosdb_meta(&workspace_dir);
    cargo_build_cnosdb_data(&workspace_dir);

    let (mut meta_test_helper, mut data_test_helper) = (
        Option::<CnosdbMetaTestHelper>::None,
        Option::<CnosdbDataTestHelper>::None,
    );

    if !cluster_def.meta_cluster_def.is_empty() {
        // If need to run `cnosdb-meta`
        let meta_test_dir = test_dir.join("meta");
        if generate_meta_config {
            write_meta_node_config_files(&test_dir, &cluster_def.meta_cluster_def);
        }
        let mut meta = CnosdbMetaTestHelper::new(
            runtime,
            &workspace_dir,
            meta_test_dir,
            cluster_def.meta_cluster_def.clone(),
        );
        if cluster_def.meta_cluster_def.len() == 1 {
            meta.run_single_meta();
        } else {
            meta.run_cluster();
        }
        meta_test_helper = Some(meta);
    }

    thread::sleep(Duration::from_secs(1));

    if !cluster_def.data_cluster_def.is_empty() {
        // If need to run `cnosdb run`
        let data_test_dir = test_dir.join("data");
        if generate_data_config {
            write_data_node_config_files(&test_dir, &cluster_def.data_cluster_def);
        }
        let mut data = CnosdbDataTestHelper::new(
            workspace_dir,
            data_test_dir,
            cluster_def.data_cluster_def.clone(),
            false,
        );
        data.run();
        data_test_helper = Some(data);
    }

    (meta_test_helper, data_test_helper)
}

/// Run CnosDB singleton.
///
/// - Data server directory: $test_dir
pub fn run_singleton(
    test_dir: impl AsRef<Path>,
    data_node_definition: &DataNodeDefinition,
    enable_tls: bool,
    generate_data_config: bool,
) -> CnosdbDataTestHelper {
    let test_dir = test_dir.as_ref().to_path_buf();
    let workspace_dir = get_workspace_dir();
    cargo_build_cnosdb_data(&workspace_dir);

    let data_test_dir = test_dir.join("data");

    let mut data_node_definition = data_node_definition.clone();
    data_node_definition.mode = DeploymentMode::Singleton;
    let data_node_definitions = vec![data_node_definition];
    if generate_data_config {
        write_data_node_config_files(&test_dir, &data_node_definitions);
    }
    let mut data = CnosdbDataTestHelper::new(
        workspace_dir,
        data_test_dir,
        data_node_definitions,
        enable_tls,
    );
    data.run();
    data
}

/// Kill all 'cnosdb' and 'cnosdb-meta' process with signal 'KILL(9)'.
pub fn kill_all() {
    println!("Killing all test processes...");
    kill_process("cnosdb");
    kill_process("cnosdb-meta");
    println!("Killed all test processes.");
}

/// Kill all processes with specified process name with signal 'KILL(9)'.
pub fn kill_process(process_name: &str) {
    println!("- Killing processes {process_name}...");
    let system =
        System::new_with_specifics(RefreshKind::new().with_processes(ProcessRefreshKind::new()));
    for (pid, process) in system.processes() {
        if process.name() == process_name {
            match process.kill_with(sysinfo::Signal::Kill) {
                Some(true) => println!("- Killed process {pid} ('{}')", process.name()),
                Some(false) => println!("- Failed killing process {pid} ('{}')", process.name()),
                None => println!("- Kill with signal 'Kill' isn't supported on this platform"),
            }
        }
    }
}

#[cfg(unix)]
fn kill_child_process(mut proc: Child, force: bool) {
    let pid = proc.id().to_string();

    // Kill process
    let mut kill = Command::new("kill");
    let mut killing_thread = if force {
        println!("- Force killing child process {pid}...");
        kill.args(["-s", "KILL", &pid])
            .spawn()
            .expect("failed to run 'kill -s KILL {pid}'")
    } else {
        println!("- Killing child process {pid}...");
        kill.args(["-s", "TERM", &pid])
            .spawn()
            .expect("failed to run 'kill -s TERM {pid}'")
    };
    match killing_thread.wait() {
        Ok(kill_exit_code) => println!("- Killed process {pid}, exit status: {kill_exit_code}"),
        Err(e) => println!("- Process {pid} not running: {e}"),
    }

    // Remove defunct process
    if let Err(e) = proc.wait() {
        println!("- Process {pid} not running: {e}");
    }
    drop(proc);

    // Wait CnosDB shutdown.
    loop {
        let display_process = Command::new("kill")
            .args(["-0", &pid])
            .output()
            .expect("failed to run 'kill -0 {pid}'");
        if display_process.status.success() {
            println!("- Waiting for process {pid} to exit...");
            thread::sleep(Duration::from_secs(1));
        } else {
            println!("- Process {pid} exited");
            break;
        }
    }
}

#[cfg(windows)]
fn kill_child_process(mut proc: Child, force: bool) {
    use std::io::{BufRead, BufReader};

    let pid = proc.id();

    // TODO(zipper) find a way to pass a signal to cnosdb process to let it shutdown gracefully.

    if force {
        let mut kill = Command::new("taskkill.exe");
        println!("- Force killing child process {pid}...");
        let mut killing_thread = kill
            .args(["/PID", &pid.to_string(), "/F"])
            .spawn()
            .expect("failed to run 'taskkill.exe /PID {pid} /F'");
        match killing_thread.wait() {
            Ok(kill_exit_code) => {
                if kill_exit_code.success() {
                    println!("- Killed process {pid}, exit status: {kill_exit_code}");
                } else {
                    println!("- Killing process {pid} failed, exit status: {kill_exit_code}");
                }
            }
            Err(e) => println!("- Process {pid} not running: {e}"),
        }
    } else {
        println!("- Killing child process {pid}...");
        unsafe {
            // SAFETY: TODO
            let proc_handle = winapi::um::processthreadsapi::OpenProcess(
                winapi::um::winnt::PROCESS_TERMINATE,
                winapi::shared::minwindef::FALSE,
                pid,
            );
            if proc_handle.is_null() {
                println!("Failed to open process.");
                return;
            }
            winapi::um::processthreadsapi::TerminateProcess(proc_handle, 0);
            winapi::um::handleapi::CloseHandle(proc_handle);
        }
    };

    // Remove defunct process
    if let Err(e) = proc.wait() {
        println!("- Process {pid} not running: {e}");
    }
    drop(proc);

    // Wait CnosDB shutdown.
    // let wait_for_thread_milliseconds =
    //     winapi::shared::minwindef::DWORD::try_from(1000_u32).expect("One of type: i32, u32, u64, u128");
    loop {
        // unsafe {
        //     let ret = winapi::um::synchapi::WaitForSingleObject(
        //         proc_handle,
        //         wait_for_thread_milliseconds,
        //     );
        //     if ret == winapi::um::winbase::WAIT_ABANDONED {
        //         break;
        //     } else if ret == winapi::um::winbase::WAIT_OBJECT_0 {
        //         println!("- Process {pid} exited");
        //         break;
        //     } else if ret == winapi::shared::winerror::WAIT_TIMEOUT {
        //         continue;
        //     }
        //     if ret == winapi::um::winbase::WAIT_FAILED {
        //         let err = std::io::Error::last_os_error();
        //         println!("Failed waitting process exit: {err}");
        //         break;
        //     }
        // }

        let display_process = Command::new("tasklist.exe")
            .args(["/FI", format!("PID eq {pid}").as_str()])
            .output()
            .expect("failed to run 'tasklist.exe /FI \"PID eq {pid}\"'");
        let lines_count = BufReader::new(&display_process.stdout[..]).lines().count();
        if lines_count > 1 {
            // Multiple lines message to tell us that the process matches the filter "PID eq {pid}" is found.
            println!("- Waiting for process {pid} to exit...");
            thread::sleep(Duration::from_secs(1));
        } else {
            // One line message to tell us that the process matches the filter "PID eq {pid}" is not found.
            println!("- Process {pid} exited");
            break;
        }
    }
}

/// Build meta store config with paths:
///
/// - snapshot_path: $test_dir/meta/$meta_dir_name/snapshot
/// - journal_path: $test_dir/meta/$meta_dir_name/journal
/// - log.level: INFO
/// - log.path: $test_dir/meta/$meta_dir_name/log
pub fn build_meta_node_config(test_dir: impl AsRef<Path>, meta_dir_name: &str) -> MetaStoreConfig {
    let mut config = MetaStoreConfig::default();
    let test_dir = test_dir.as_ref().display();
    config.snapshot_path = format!("{test_dir}/meta/{meta_dir_name}/snapshot");
    config.journal_path = format!("{test_dir}/meta/{meta_dir_name}/journal");
    config.log.level = "INFO".to_string();
    config.log.path = format!("{test_dir}/meta/{meta_dir_name}/log");

    config
}

/// Build meta store config with paths and write to test_dir.
/// Will be write to $test_dir/meta/config/$config_file_name.
pub fn write_meta_node_config_files(
    test_dir: impl AsRef<Path>,
    meta_node_definitions: &[MetaNodeDefinition],
) {
    let meta_config_dir = test_dir.as_ref().join("meta").join("config");
    std::fs::create_dir_all(&meta_config_dir).unwrap();
    for meta_node_def in meta_node_definitions {
        let mut meta_config = build_meta_node_config(&test_dir, &meta_node_def.config_file_name);
        meta_node_def.update_config(&mut meta_config);
        let config_path = meta_config_dir.join(&meta_node_def.config_file_name);
        std::fs::write(&config_path, meta_config.to_string_pretty()).unwrap();
    }
}

/// Build cnosdb config with paths:
///
/// - deployment_mode: singleton
/// - hh file: $test_dir/data/$data_dir_name/hh
/// - log.level: INFO
/// - log.path: $test_dir/data/$data_dir_name/log
/// - storage.path: $test_dir/data/$data_dir_name/storage
/// - wal.path: $test_dir/data/$data_dir_name/wal
pub fn build_data_node_config(test_dir: impl AsRef<Path>, data_dir_name: &str) -> CnosdbConfig {
    let mut config = CnosdbConfig::default();
    config.deployment.mode = "singleton".to_string();
    let test_dir = test_dir.as_ref().display();
    let data_path = format!("{test_dir}/data/{data_dir_name}");
    config.hinted_off.path = format!("{data_path}/hh");
    config.log.level = "INFO".to_string();
    config.log.path = format!("{data_path}/log");
    config.storage.path = format!("{data_path}/storage");
    config.wal.path = format!("{data_path}/wal");
    config.cluster.http_listen_port = Some(8902);
    config.cluster.grpc_listen_port = Some(8903);
    config.cluster.flight_rpc_listen_port = Some(8904);
    config.cluster.tcp_listen_port = Some(8905);
    config.cluster.vector_listen_port = Some(8906);

    config
}

/// Build cnosdb config with paths and write to test_dir.
/// Will be write to $test_dir/data/config/$config_file_name.
pub fn write_data_node_config_files(
    test_dir: impl AsRef<Path>,
    data_node_definitions: &[DataNodeDefinition],
) {
    let cnosdb_config_dir = test_dir.as_ref().join("data").join("config");
    std::fs::create_dir_all(&cnosdb_config_dir).unwrap();
    for data_node_def in data_node_definitions {
        let mut cnosdb_config = build_data_node_config(&test_dir, &data_node_def.config_file_name);
        data_node_def.update_config(&mut cnosdb_config);
        let config_path = cnosdb_config_dir.join(&data_node_def.config_file_name);
        std::fs::write(&config_path, cnosdb_config.to_string_pretty()).unwrap();

        // If we do not make directory $storage.path, the data node seems to be sick by the meta node.
        // TODO(zipper): I think it's the data node who should do this job.
        if let Err(e) = std::fs::create_dir_all(&cnosdb_config.storage.path) {
            println!("Failed to pre-create $storage.path for data node: {e}");
        }
    }
}

/// Copy TLS certificates:
///
/// - $workspace_dir/config/tls/server.crt to $dest_dir/server.crt
/// - $workspace_dir/config/tls/server.key to $dest_dir/server.key
pub fn copy_cnosdb_server_certificate(workspace_dir: impl AsRef<Path>, dest_dir: impl AsRef<Path>) {
    let src_dir = workspace_dir.as_ref().join("config").join("tls");
    let files_to_copy = [src_dir.join("server.crt"), src_dir.join("server.key")];
    let dest_dir = dest_dir.as_ref();
    let _ = std::fs::create_dir_all(dest_dir);
    for src_file in files_to_copy {
        if std::fs::metadata(&src_file).is_err() {
            panic!("certificate file '{}' not found", src_file.display());
        }
        let dst_file = dest_dir.join(src_file.file_name().unwrap());
        println!(
            "- coping certificate file: '{}' to '{}'",
            src_file.display(),
            dst_file.display(),
        );
        std::fs::copy(&src_file, &dst_file).unwrap();
        println!(
            "- copy certificate file completed: '{}' to '{}'",
            src_file.display(),
            dst_file.display(),
        );
    }
}

type TestFnInCluster = fn(Option<&CnosdbMetaTestHelper>, Option<&CnosdbDataTestHelper>);

pub fn test_in_cnosdb_cluster(
    test_dir: &str,
    cluster_def: &CnosdbClusterDefinition,
    test_case_name: &str,
    test_fn: TestFnInCluster,
    generate_meta_config: bool,
    generate_data_config: bool,
) {
    println!("Test begin '{}'", test_case_name);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .unwrap();
    let runtime = Arc::new(runtime);
    kill_all();

    let (meta, data) = run_cluster(
        test_dir,
        runtime,
        cluster_def,
        generate_meta_config,
        generate_data_config,
    );

    (test_fn)(meta.as_ref(), data.as_ref());
    println!("Test complete '{}'", test_case_name);
}

pub fn test_in_cnosdb_clusters(
    cluster_definitions_with_path: &[(PathBuf, CnosdbClusterDefinition)],
    test_case_name: &str,
    test_fn: fn(),
    generate_meta_config: bool,
    generate_data_config: bool,
) {
    println!("Test begin '{}'", test_case_name);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .unwrap();
    let runtime = Arc::new(runtime);
    kill_all();

    let mut context_meta_data: Vec<(Option<CnosdbMetaTestHelper>, Option<CnosdbDataTestHelper>)> =
        Vec::with_capacity(cluster_definitions_with_path.len());
    for (cluster_dir, cluster_def) in cluster_definitions_with_path {
        let (meta, data) = run_cluster(
            cluster_dir,
            runtime.clone(),
            cluster_def,
            generate_meta_config,
            generate_data_config,
        );
        context_meta_data.push((meta, data));
    }

    (test_fn)();
    println!("Test complete '{}'", test_case_name);
}

pub fn ls(dir: impl AsRef<Path>) -> Result<Vec<String>, String> {
    let out = Command::new("ls")
        .arg(dir.as_ref())
        .stdout(Stdio::piped())
        .output()
        .unwrap();
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout)
            .split_terminator('\n')
            .map(|s| s.to_string())
            .collect())
    } else {
        Err(format!(
            "cmd 'ls {}' failed: {}",
            dir.as_ref().display(),
            String::from_utf8_lossy(&out.stderr)
        ))
    }
}

pub(crate) struct DeferGuard<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> DeferGuard<F> {
    pub(crate) fn new(f: F) -> Self {
        Self(Some(f))
    }
}

impl<F: FnOnce()> Drop for DeferGuard<F> {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}

#[macro_export]
macro_rules! defer {
    ($($code:tt)*) => {
        let _defer_guard = $crate::utils::DeferGuard::new(|| {
            $($code)*
        });
    };
}
