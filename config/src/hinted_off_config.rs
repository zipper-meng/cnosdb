use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};
use crate::environment::OverrideByEnv;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HintedOffConfig {
    #[serde(default = "HintedOffConfig::default_enable")]
    pub enable: bool,
    #[serde(default = "HintedOffConfig::default_path")]
    pub path: String,
}

impl HintedOffConfig {
    fn default_enable() -> bool {
        true
    }

    fn default_path() -> String {
        "/tmp/cnosdb/hh".to_string()
    }
}

impl Default for HintedOffConfig {
    fn default() -> Self {
        Self {
            enable: Self::default_enable(),
            path: Self::default_path(),
        }
    }
}

impl OverrideByEnv for HintedOffConfig {
    fn override_by_env(&mut self) {
        if let Ok(enable) = std::env::var("CNOSDB_HINTEDOFF_ENABLE") {
            self.enable = enable.parse::<bool>().unwrap();
        }
        if let Ok(path) = std::env::var("CNOSDB_HINTEDOFF_PATH") {
            self.path = path;
        }
    }
}

impl CheckConfig for HintedOffConfig {
    fn check(&self, _: &crate::Config) -> Option<CheckConfigResult> {
        let config_name = Arc::new("hinted_off".to_string());
        let mut ret = CheckConfigResult::default();

        if self.enable && self.path.is_empty() {
            ret.add_warn(CheckConfigItemResult {
                config: config_name,
                item: "path".to_string(),
                message: "'path' is empty".to_string(),
            });
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
