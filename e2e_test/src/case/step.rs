use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::{CaseContext, CaseFlowControl, CnosdbAuth};
use crate::utils::{kill_all, run_cluster, Client};
use crate::E2eResult;

pub type StepPtr = Box<dyn Step>;
pub type StepResult = E2eResult<Vec<String>>;
pub type FnStepResult = Box<dyn for<'a> Fn(&'a mut CaseContext) -> CaseFlowControl>;
pub type FnString = Box<dyn for<'a> Fn(&'a CaseContext) -> String>;
pub type FnStringVec = Box<dyn for<'a> Fn(&'a CaseContext) -> Vec<String>>;
pub type FnCommand = Box<dyn for<'a> Fn(&'a CaseContext) -> Vec<Command>>;
pub type FnGeneric<T> = Box<dyn for<'a> Fn(&'a CaseContext) -> T>;
pub type FnVoid = Box<dyn for<'a> Fn(&'a CaseContext)>;
pub type FnAfterRequestSucceed = Box<dyn for<'a> Fn(&'a mut CaseContext, &Vec<String>)>;

pub trait Step: std::fmt::Display {
    fn id(&self) -> usize;

    fn set_id(&self, id: usize);

    fn name(&self) -> &str;

    fn execute(&self, context: &mut CaseContext) -> CaseFlowControl;

    fn build_fail_message(&self, context: &CaseContext, result: &StepResult) -> String {
        format!(
            "test[{}.{}] steps[{}-{}] [{self}], result: {result:?}",
            context.case_group(),
            context.case_name(),
            self.id(),
            self.name(),
        )
    }
}

pub enum StrValue {
    Constant(String),
    Function(FnString),
}

impl StrValue {
    pub fn get(&self, context: &CaseContext) -> String {
        match self {
            StrValue::Constant(s) => s.clone(),
            StrValue::Function(f) => f(context),
        }
    }
}

impl std::fmt::Display for StrValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StrValue::Constant(c) => write!(f, "{c}"),
            StrValue::Function(_) => write!(f, "fn(&c) -> String"),
        }
    }
}

pub enum StrVecValue {
    Constant(Vec<String>),
    Function(FnStringVec),
}

impl StrVecValue {
    pub fn get(&self, context: &CaseContext) -> Vec<String> {
        match self {
            StrVecValue::Constant(s) => s.clone(),
            StrVecValue::Function(f) => f(context),
        }
    }
}

impl std::fmt::Debug for StrVecValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Constant(c) => f.debug_tuple("Constant").field(c).finish(),
            Self::Function(_) => f.write_str("fn(&c) -> Vec<String>"),
        }
    }
}

pub enum GenericValue<T> {
    Constant(T),
    Function(FnGeneric<T>),
}

impl<T: Clone> GenericValue<T> {
    pub fn get(&self, context: &CaseContext) -> T {
        match self {
            Self::Constant(t) => t.clone(),
            Self::Function(f) => f(context),
        }
    }
}

pub struct WrappedStep {
    pub inner: Box<dyn Step>,
    pub before_execute: Option<FnVoid>,
    pub after_execute: Option<FnVoid>,
}

impl WrappedStep {
    pub fn new_boxed(
        inner: Box<dyn Step>,
        before_execute: Option<FnVoid>,
        after_execute: Option<FnVoid>,
    ) -> Box<Self> {
        Box::new(Self {
            inner,
            before_execute,
            after_execute,
        })
    }
}

impl Step for WrappedStep {
    fn id(&self) -> usize {
        self.inner.id()
    }

    fn set_id(&self, id: usize) {
        self.inner.set_id(id);
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn execute(&self, context: &mut CaseContext) -> CaseFlowControl {
        if let Some(f) = &self.before_execute {
            f(context);
        }
        let result = self.inner.execute(context);
        if let Some(f) = &self.after_execute {
            f(context);
        }
        result
    }
}

impl std::fmt::Display for WrappedStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

pub struct FunctionStep {
    id: AtomicUsize,
    name: String,
    function: FnStepResult,
}

impl FunctionStep {
    pub fn new_boxed<S: ToString>(name: S, function: FnStepResult) -> Box<Self> {
        Box::new(Self {
            id: AtomicUsize::new(0),
            name: name.to_string(),
            function,
        })
    }
}

impl Step for FunctionStep {
    fn id(&self) -> usize {
        self.id.load(Ordering::Relaxed)
    }

    fn set_id(&self, id: usize) {
        self.id.store(id, Ordering::Relaxed);
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn execute(&self, context: &mut CaseContext) -> CaseFlowControl {
        (self.function)(context)
    }
}

impl std::fmt::Display for FunctionStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Step(function)[{}-{}]", self.id(), self.name())
    }
}

pub struct LogStep {
    inner: Box<FunctionStep>,
}

impl LogStep {
    pub fn new_boxed<S: ToString>(msg: S) -> Box<Self> {
        let msg = msg.to_string();
        Box::new(Self {
            inner: FunctionStep::new_boxed(
                "Log".to_string(),
                Box::new(move |_| {
                    println!("LOG: {}", msg);
                    CaseFlowControl::Continue
                }),
            ),
        })
    }

    pub fn new_boxed_with_fn(msg_fn: FnString) -> Self {
        Self {
            inner: FunctionStep::new_boxed(
                "Log".to_string(),
                Box::new(move |context| {
                    println!("LOG: {}", msg_fn(context));
                    CaseFlowControl::Continue
                }),
            ),
        }
    }
}

impl Step for LogStep {
    fn id(&self) -> usize {
        self.inner.id()
    }

    fn set_id(&self, id: usize) {
        self.inner.set_id(id);
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn execute(&self, context: &mut CaseContext) -> CaseFlowControl {
        self.inner.execute(context)
    }
}

impl std::fmt::Display for LogStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Step(log)[{}-{}]", self.id(), self.name())
    }
}

pub struct RequestStep {
    id: AtomicUsize,
    name: String,
    req: CnosdbRequest,
    auth: Option<CnosdbAuth>,
    after_request_succeed: Option<FnAfterRequestSucceed>,
}

impl RequestStep {
    pub fn new_boxed<Name: ToString>(
        name: Name,
        req: CnosdbRequest,
        auth: Option<CnosdbAuth>,
        after_request_succeed: Option<FnAfterRequestSucceed>,
    ) -> Box<Self> {
        Box::new(Self {
            id: AtomicUsize::new(0),
            name: name.to_string(),
            req,
            auth,
            after_request_succeed,
        })
    }
}

impl Step for RequestStep {
    fn id(&self) -> usize {
        self.id.load(Ordering::Relaxed)
    }

    fn set_id(&self, id: usize) {
        self.id.store(id, Ordering::Relaxed);
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn execute(&self, context: &mut CaseContext) -> CaseFlowControl {
        let client = match &self.auth {
            Some(a) => Arc::new(Client::with_auth(a.username.clone(), a.password.clone())),
            None => context.data_client(),
        };
        match &self.req {
            CnosdbRequest::Sql(s) => s.do_request(context, self, client.as_ref()),
            CnosdbRequest::SqlNoResult(s) => s.do_request(context, self, client.as_ref()),
            CnosdbRequest::LineProtocol(l) => l.do_request(context, self, client.as_ref()),
            CnosdbRequest::EsBulk(s) => s.do_request(context, self, client.as_ref()),
        }
    }
}

impl std::fmt::Display for RequestStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Step(request)[{}-{}]:", self.id(), self.name())?;
        if let Some(auth) = &self.auth {
            write!(f, " auth:({auth}),")?;
        } else {
            write!(f, " auth(None),")?;
        }
        write!(f, " {}", self.req)
    }
}

pub enum CnosdbRequest {
    /// Execute SQL and check the response text.
    Sql(Sql),
    /// Execute SQL and do not check the result if the response is Ok..
    SqlNoResult(SqlNoResult),
    /// Write Line Protocol.
    LineProtocol(LineProtocol),
    /// Write Elasticsearch Bulk.
    EsBulk(EsBulk),
}

impl std::fmt::Display for CnosdbRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CnosdbRequest::Sql(s) => write!(f, "{s}"),
            CnosdbRequest::SqlNoResult(s) => write!(f, "{s}"),
            CnosdbRequest::LineProtocol(s) => write!(f, "{s}"),
            CnosdbRequest::EsBulk(s) => write!(f, "{s}"),
        }
    }
}

pub struct Sql {
    pub url: StrValue,
    pub sql: StrValue,
    /// The expected response, and result lines if server returns ok.
    pub resp: E2eResult<StrVecValue>,
    pub sorted: bool,
    /// If resp is checked by regex.
    regex: bool,
}

impl Sql {
    pub fn build_request_with_str<Url: ToString, Sql: ToString, Line: ToString>(
        url: Url,
        sql: Sql,
        resp: E2eResult<Vec<Line>>,
        sorted: bool,
        regex: bool,
    ) -> CnosdbRequest {
        CnosdbRequest::Sql(Self {
            url: StrValue::Constant(url.to_string()),
            sql: StrValue::Constant(sql.to_string()),
            resp: resp.map(|v| StrVecValue::Constant(v.iter().map(|l| l.to_string()).collect())),
            sorted,
            regex,
        })
    }

    pub fn build_request_with_fn(
        url_fn: FnString,
        sql_fn: FnString,
        resp: E2eResult<FnStringVec>,
        sorted: bool,
        regex: bool,
    ) -> CnosdbRequest {
        CnosdbRequest::Sql(Self {
            url: StrValue::Function(url_fn),
            sql: StrValue::Function(sql_fn),
            resp: resp.map(|v| StrVecValue::Function(v)),
            sorted,
            regex,
        })
    }

    fn do_request(
        &self,
        context: &mut CaseContext,
        request: &RequestStep,
        client: &Client,
    ) -> CaseFlowControl {
        let url = self.url.get(context);
        let sql = self.sql.get(context);
        let resp = self.resp.as_ref().map(|v| v.get(context));
        let result_resp = client.api_v1_sql(url, &sql);
        let fail_message = request.build_fail_message(context, &result_resp);
        match resp {
            Ok(exp_lines) => {
                assert!(result_resp.is_ok(), "{fail_message}");
                if let Ok(mut resp_lines) = result_resp {
                    if self.sorted {
                        resp_lines.sort_unstable();
                    }
                    assert_eq!(resp_lines, exp_lines.to_vec(), "{fail_message}");
                    if let Some(f) = &request.after_request_succeed {
                        f(context, &resp_lines);
                    }
                } else {
                    assert!(result_resp.is_err(), "{fail_message}");
                }
            }
            Err(exp_err) => {
                assert!(
                    result_resp.is_err(),
                    "{}",
                    request.build_fail_message(context, &result_resp)
                );
                assert_eq!(exp_err, &result_resp.unwrap_err(), "{fail_message}");
            }
        }
        CaseFlowControl::Continue
    }
}

impl std::fmt::Display for Sql {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SQL: url: {}, sql: {} => {} {} {:?}",
            self.url,
            self.sql,
            if self.sorted { "sorted" } else { "" },
            if self.regex { "regex" } else { "" },
            self.resp,
        )
    }
}

pub struct SqlNoResult {
    pub url: StrValue,
    pub sql: StrValue,
    pub resp: E2eResult<()>,
}

impl SqlNoResult {
    pub fn build_request_with_str<Url: ToString, Sql: ToString>(
        url: Url,
        sql: Sql,
        resp: E2eResult<()>,
    ) -> CnosdbRequest {
        CnosdbRequest::SqlNoResult(Self {
            url: StrValue::Constant(url.to_string()),
            sql: StrValue::Constant(sql.to_string()),
            resp,
        })
    }

    pub fn build_request_with_fn(
        url_fn: FnString,
        sql_fn: FnString,
        resp: E2eResult<()>,
    ) -> CnosdbRequest {
        CnosdbRequest::SqlNoResult(Self {
            url: StrValue::Function(url_fn),
            sql: StrValue::Function(sql_fn),
            resp,
        })
    }

    fn do_request(
        &self,
        context: &mut CaseContext,
        request: &RequestStep,
        client: &Client,
    ) -> CaseFlowControl {
        let url = self.url.get(context);
        let sql = self.sql.get(context);
        let result_resp = client.api_v1_write(url, &sql);
        let fail_message = request.build_fail_message(context, &result_resp);
        match &self.resp {
            Ok(_) => {
                assert!(result_resp.is_ok(), "{fail_message}");
                if let Some(f) = &request.after_request_succeed {
                    f(context, &result_resp.unwrap());
                }
            }
            Err(exp_err) => {
                assert!(result_resp.is_err(), "{fail_message}");
                assert_eq!(exp_err, &result_resp.unwrap_err(), "{fail_message}");
            }
        }
        CaseFlowControl::Continue
    }
}

impl std::fmt::Display for SqlNoResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SQL(no result): url: {}, sql: {}", self.url, self.sql)
    }
}

pub struct LineProtocol {
    pub url: StrValue,
    pub req: StrValue,
    pub resp: E2eResult<()>,
}

impl LineProtocol {
    pub fn build_request_with_str<Url: ToString, Req: ToString>(
        url: Url,
        req: Req,
        resp: E2eResult<()>,
    ) -> CnosdbRequest {
        CnosdbRequest::LineProtocol(Self {
            url: StrValue::Constant(url.to_string()),
            req: StrValue::Constant(req.to_string()),
            resp,
        })
    }

    pub fn build_request_with_fn(
        url_fn: FnString,
        req_fn: FnString,
        resp: E2eResult<()>,
    ) -> CnosdbRequest {
        CnosdbRequest::LineProtocol(Self {
            url: StrValue::Function(url_fn),
            req: StrValue::Function(req_fn),
            resp,
        })
    }

    fn do_request(
        &self,
        context: &mut CaseContext,
        request: &RequestStep,
        client: &Client,
    ) -> CaseFlowControl {
        let url = self.url.get(context);
        let req = self.req.get(context);
        let result_resp = client.api_v1_write(url, &req);
        let fail_message = request.build_fail_message(context, &result_resp);
        match &self.resp {
            Ok(_) => {
                assert!(result_resp.is_ok(), "{fail_message}");
                if let Some(f) = &request.after_request_succeed {
                    f(context, &result_resp.unwrap());
                }
            }
            Err(exp_err) => {
                assert!(result_resp.is_err(), "{fail_message}");
                assert_eq!(exp_err, &result_resp.unwrap_err(), "{fail_message}");
            }
        }
        CaseFlowControl::Continue
    }
}

impl std::fmt::Display for LineProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Line Protocol: url: {}, req: {}", self.url, self.req)
    }
}

pub struct EsBulk {
    pub url: StrValue,
    pub req: StrValue,
    pub resp: E2eResult<String>,
}

impl EsBulk {
    pub fn build_request_with_str<Url: ToString, Req: ToString>(
        url: Url,
        req: Req,
        resp: E2eResult<String>,
    ) -> CnosdbRequest {
        CnosdbRequest::EsBulk(Self {
            url: StrValue::Constant(url.to_string()),
            req: StrValue::Constant(req.to_string()),
            resp,
        })
    }

    pub fn build_request_with_fn(
        url_fn: FnString,
        req_fn: FnString,
        resp: E2eResult<String>,
    ) -> CnosdbRequest {
        CnosdbRequest::EsBulk(Self {
            url: StrValue::Function(url_fn),
            req: StrValue::Function(req_fn),
            resp,
        })
    }

    fn do_request(
        &self,
        context: &mut CaseContext,
        request: &RequestStep,
        client: &Client,
    ) -> CaseFlowControl {
        let url = self.url.get(context);
        let req = self.req.get(context);
        let result_resp = client.api_v1_write(url, &req);
        let fail_message = request.build_fail_message(context, &result_resp);
        match &self.resp {
            Ok(_) => {
                assert!(result_resp.is_ok(), "{fail_message}");
                if let Some(f) = &request.after_request_succeed {
                    f(context, &result_resp.unwrap());
                }
            }
            Err(exp_err) => {
                assert!(result_resp.is_err(), "{fail_message}");
                assert_eq!(exp_err, &result_resp.unwrap_err(), "{fail_message}");
            }
        }
        CaseFlowControl::Continue
    }
}

impl std::fmt::Display for EsBulk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Elasticsearch Bulk: url: {}, req: {}",
            self.url, self.req
        )
    }
}

pub struct ControlStep {
    id: AtomicUsize,
    name: String,
    control: Control,
}

impl ControlStep {
    pub fn new_boxed<Name: ToString>(name: Name, control: Control) -> Box<Self> {
        Box::new(Self {
            id: AtomicUsize::new(0),
            name: name.to_string(),
            control,
        })
    }

    pub fn new_boxed_restart_data_node<Name: ToString>(name: Name, node_index: usize) -> Box<Self> {
        Self::new_boxed(name, Control::RestartDataNode(node_index))
    }

    pub fn new_boxed_start_data_node<Name: ToString>(name: Name, node_index: usize) -> Box<Self> {
        Self::new_boxed(name, Control::StartDataNode(node_index))
    }

    pub fn new_boxed_stop_data_node<Name: ToString>(name: Name, node_index: usize) -> Box<Self> {
        Self::new_boxed(name, Control::StopDataNode(node_index))
    }

    pub fn new_boxed_stop_meta_node<Name: ToString>(name: Name, node_index: usize) -> Box<Self> {
        Self::new_boxed(name, Control::StopMetaNode(node_index))
    }

    pub fn new_boxed_restart_cluster<Name: ToString>(name: Name) -> Box<Self> {
        Self::new_boxed(name, Control::RestartCluster)
    }

    pub fn new_boxed_sleep<Name: ToString>(name: Name, seconds: u64) -> Box<Self> {
        Self::new_boxed(name, Control::Sleep(seconds))
    }
}

impl Step for ControlStep {
    fn id(&self) -> usize {
        self.id.load(Ordering::Relaxed)
    }

    fn set_id(&self, id: usize) {
        self.id.store(id, Ordering::Relaxed);
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn execute(&self, context: &mut CaseContext) -> CaseFlowControl {
        self.control.do_control(context);
        CaseFlowControl::Continue
    }
}

impl std::fmt::Display for ControlStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Step(Control)[{}-{}]: {}",
            self.id(),
            self.name(),
            self.control
        )
    }
}

pub enum Control {
    RestartDataNode(usize),
    StartDataNode(usize),
    StopDataNode(usize),
    StopMetaNode(usize),
    RestartCluster,
    /// Sleep current thread for a while(in seconds)
    Sleep(u64),
}

impl Control {
    const WAIT_BEFORE_RESTART_SECONDS: u64 = 1;

    fn do_control(&self, context: &mut CaseContext) {
        let data = context.data_mut();
        match self {
            Control::RestartDataNode(data_node_index) => {
                if Self::WAIT_BEFORE_RESTART_SECONDS > 0 {
                    // TODO(zipper): The test sometimes fail if we restart just after a DDL or Insert, why?
                    std::thread::sleep(Duration::from_secs(Self::WAIT_BEFORE_RESTART_SECONDS));
                }
                let data_node_def = data.data_node_definitions[*data_node_index].clone();
                data.restart_one_node(&data_node_def);
            }
            Control::StartDataNode(data_node_index) => {
                let data_node_def = data.data_node_definitions[*data_node_index].clone();
                data.start_one_node(&data_node_def);
            }
            Control::StopDataNode(data_node_index) => {
                let data_node_def = data.data_node_definitions[*data_node_index].clone();
                data.stop_one_node(&data_node_def.config_file_name, false);
            }
            Control::StopMetaNode(meta_node_index) => {
                let meta: &mut crate::utils::CnosdbMetaTestHelper = context.meta_mut();
                let meta_node_def = meta.meta_node_definitions[*meta_node_index].clone();
                meta.stop_one_node(&meta_node_def.config_file_name, false);
            }
            Control::RestartCluster => {
                kill_all();
                run_cluster(
                    context.test_dir(),
                    context.runtime(),
                    context.cluster_definition(),
                    false,
                    false,
                );
            }
            Control::Sleep(seconds) => {
                std::thread::sleep(Duration::from_secs(*seconds));
            }
        }
    }
}

impl std::fmt::Display for Control {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Control::RestartDataNode(id) => write!(f, "restart data node: {id}"),
            Control::StartDataNode(id) => write!(f, "start data node: {id}"),
            Control::StopDataNode(id) => write!(f, "stop data node: {id}"),
            Control::StopMetaNode(id) => write!(f, "stop meta node: {id}"),
            Control::RestartCluster => write!(f, "restart cluster"),
            Control::Sleep(sec) => write!(f, "sleep {sec} seconds"),
        }
    }
}
pub struct ShellStep {
    id: AtomicUsize,
    name: String,
    command: FnGeneric<Command>,
    terminate_on_fail: bool,
    before_execution: Option<FnVoid>,
    after_execute_succeed: Option<FnAfterRequestSucceed>,
}

impl ShellStep {
    pub fn new_boxed_with_fn<Name: ToString>(
        name: Name,
        command: FnGeneric<Command>,
        terminate_on_fail: bool,
        before_execution: Option<FnVoid>,
        after_execute_succeed: Option<FnAfterRequestSucceed>,
    ) -> Box<Self> {
        Box::new(Self {
            id: AtomicUsize::new(0),
            name: name.to_string(),
            command,
            terminate_on_fail,
            before_execution,
            after_execute_succeed,
        })
    }

    fn command_str(&self, context: &CaseContext) -> String {
        let command = (self.command)(context);
        Self::command_to_string(&command)
    }

    fn command_to_string(command: &Command) -> String {
        let mut buf = command.get_program().to_string_lossy().to_string();
        for arg in command.get_args() {
            buf.push(' ');
            buf.push_str(arg.to_string_lossy().as_ref());
        }
        buf
    }
}

impl Step for ShellStep {
    fn id(&self) -> usize {
        self.id.load(Ordering::Relaxed)
    }

    fn set_id(&self, id: usize) {
        self.id.store(id, Ordering::Relaxed);
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn execute(&self, context: &mut CaseContext) -> CaseFlowControl {
        if let Some(f) = &self.before_execution {
            f(context);
        }
        let mut command = (self.command)(context);
        let command_string = Self::command_to_string(&command);
        let output = command
            .output()
            .unwrap_or_else(|e| panic!("failed to execute process '{command_string}': {e}"));
        if output.status.success() {
            if let Some(f) = &self.after_execute_succeed {
                let stdout_utf8 = String::from_utf8_lossy(&output.stdout);
                let lines: Vec<String> = stdout_utf8.lines().map(|l| l.to_string()).collect();
                f(context, &lines);
            }
        } else if self.terminate_on_fail {
            panic!("Execute shell command failed: {command_string}");
        }
        CaseFlowControl::Continue
    }
}

impl std::fmt::Display for ShellStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Step(shell)[{}-{}]: fn(&c) -> Command",
            self.id(),
            self.name(),
        )
    }
}
