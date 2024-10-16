use core::panic;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::runtime::Runtime;

use super::step::Step;
use super::CaseFlowControl;
use crate::cluster_def::{CnosdbClusterDefinition, DataNodeDefinition};
use crate::utils::{
    kill_all, run_cluster, run_singleton, Client, CnosdbDataTestHelper, CnosdbMetaTestHelper,
};

const WAIT_BEFORE_RESTART_SECONDS: u64 = 1;

// TODO(zipper): This module also needs test.

pub struct E2eExecutor {
    case_group: String,
    case_name: String,
    runtime: Arc<Runtime>,
    cluster_definition: CnosdbClusterDefinition,

    test_dir: PathBuf,
    is_singleton: bool,
}

impl E2eExecutor {
    pub fn new_cluster(
        case_group: &str,
        case_name: &str,
        cluster_definition: CnosdbClusterDefinition,
    ) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .build()
            .unwrap();
        let runtime = Arc::new(runtime);

        let test_dir = PathBuf::from(format!("/tmp/e2e_test/{case_group}/{case_name}"));
        let is_singleton = cluster_definition.meta_cluster_def.is_empty()
            && cluster_definition.data_cluster_def.len() == 1;

        Self {
            case_group: case_group.to_string(),
            case_name: case_name.to_string(),
            runtime,
            cluster_definition,

            test_dir,
            is_singleton,
        }
    }

    pub fn new_singleton(
        case_group: &str,
        case_name: &str,
        data_node_def: DataNodeDefinition,
    ) -> Self {
        Self::new_cluster(
            case_group,
            case_name,
            CnosdbClusterDefinition {
                meta_cluster_def: vec![],
                data_cluster_def: vec![data_node_def],
            },
        )
    }

    fn install_singleton(&self) -> CnosdbDataTestHelper {
        let _ = std::fs::remove_dir_all(&self.test_dir);
        std::fs::create_dir_all(&self.test_dir).unwrap();

        kill_all();

        run_singleton(
            &self.test_dir,
            &self.cluster_definition.data_cluster_def[0],
            false,
            true,
        )
    }

    fn install_cluster(&self) -> (Option<CnosdbMetaTestHelper>, Option<CnosdbDataTestHelper>) {
        let _ = std::fs::remove_dir_all(&self.test_dir);
        std::fs::create_dir_all(&self.test_dir).unwrap();

        kill_all();

        run_cluster(
            &self.test_dir,
            self.runtime.clone(),
            &self.cluster_definition,
            true,
            true,
        )
    }

    pub fn execute_steps(&self, steps: &[Box<dyn Step>]) {
        println!("Test begin: {}_{}", self.case_group, self.case_name);

        for (i, step) in steps.iter().enumerate() {
            step.set_id(i);
        }

        let mut context = CaseContext::from(self);
        if self.is_singleton {
            let data = self.install_singleton();
            context.with_data(Some(data));
        } else {
            let (meta, data) = self.install_cluster();
            context.with_meta(meta);
            context.with_data(data);
        }

        for step in steps {
            println!("- Executing step: [{}]-{}", step.id(), step.name());
            match step.execute(&mut context) {
                CaseFlowControl::Continue => continue,
                CaseFlowControl::Break => break,
            }
        }

        println!("Test complete: {}.{}", self.case_group, self.case_name);
    }
}

pub struct CaseContext {
    case_group: String,
    case_name: String,
    runtime: Arc<Runtime>,
    cluster_definition: CnosdbClusterDefinition,

    test_dir: PathBuf,
    meta: Option<CnosdbMetaTestHelper>,
    data: Option<CnosdbDataTestHelper>,
    global_variables: HashMap<String, String>,
    context_variables: HashMap<String, String>,
    last_step_variables: HashMap<String, String>,
}

impl CaseContext {
    pub fn case_group(&self) -> &str {
        &self.case_group
    }

    pub fn case_name(&self) -> &str {
        &self.case_name
    }

    pub fn runtime(&self) -> Arc<Runtime> {
        self.runtime.clone()
    }

    pub fn cluster_definition(&self) -> &CnosdbClusterDefinition {
        &self.cluster_definition
    }

    pub fn test_dir(&self) -> &PathBuf {
        &self.test_dir
    }

    pub fn with_meta(&mut self, meta: Option<CnosdbMetaTestHelper>) {
        self.meta = meta;
    }

    pub fn meta_opt(&self) -> Option<&CnosdbMetaTestHelper> {
        self.meta.as_ref()
    }

    pub fn meta(&self) -> &CnosdbMetaTestHelper {
        match self.meta_opt() {
            Some(d) => d,
            None => panic!("Cnosdb meta server is not in the e2e context"),
        }
    }

    pub fn meta_mut(&mut self) -> &mut CnosdbMetaTestHelper {
        match self.meta.as_mut() {
            Some(d) => d,
            None => panic!("Cnosdb meta server is not in the e2e context"),
        }
    }

    pub fn with_data(&mut self, data: Option<CnosdbDataTestHelper>) {
        self.data = data;
    }

    pub fn data_opt(&self) -> Option<&CnosdbDataTestHelper> {
        self.data.as_ref()
    }

    pub fn data(&self) -> &CnosdbDataTestHelper {
        match self.data_opt() {
            Some(d) => d,
            None => panic!("Cnosdb data server is not in the e2e context"),
        }
    }

    pub fn data_mut(&mut self) -> &mut CnosdbDataTestHelper {
        match self.data.as_mut() {
            Some(d) => d,
            None => panic!("Cnosdb data server is not in the e2e context"),
        }
    }

    pub fn data_dir(&self, i: usize) -> PathBuf {
        let config = match self.data().data_node_configs.get(i) {
            Some(c) => c,
            None => panic!("Data node config of index {i} not found"),
        };
        self.test_dir.join(&config.storage.path)
    }

    pub fn meta_client(&self) -> Arc<Client> {
        self.meta().client.clone()
    }

    pub fn data_client(&self) -> Arc<Client> {
        self.data().client.clone()
    }

    pub fn global_variables(&self) -> &HashMap<String, String> {
        &self.global_variables
    }

    pub fn global_variables_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.global_variables
    }

    pub fn context_variables(&self) -> &HashMap<String, String> {
        &self.context_variables
    }

    pub fn context_variables_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.context_variables
    }

    pub fn last_step_variables(&self) -> &HashMap<String, String> {
        &self.last_step_variables
    }

    pub fn last_step_variables_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.last_step_variables
    }

    pub fn add_context_variable(&mut self, key: &str, value: &str) {
        self.last_step_variables
            .insert(key.to_string(), value.to_string());
        self.context_variables
            .insert(key.to_string(), value.to_string());
    }
}

impl From<&E2eExecutor> for CaseContext {
    fn from(executor: &E2eExecutor) -> Self {
        CaseContext {
            case_group: executor.case_group.clone(),
            case_name: executor.case_name.clone(),
            runtime: executor.runtime.clone(),
            cluster_definition: executor.cluster_definition.clone(),
            test_dir: executor.test_dir.clone(),
            data: None,
            meta: None,
            global_variables: HashMap::new(),
            context_variables: HashMap::new(),
            last_step_variables: HashMap::new(),
        }
    }
}
