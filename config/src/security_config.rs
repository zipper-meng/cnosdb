use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};
use crate::environment::OverrideByEnv;

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SecurityConfig {
    pub tls_config: Option<TLSConfig>,
}

impl OverrideByEnv for SecurityConfig {
    fn override_by_env(&mut self) {
        if let Some(ref mut tls) = self.tls_config {
            tls.override_by_env();
        }
    }
}

impl CheckConfig for SecurityConfig {
    fn check(&self, all_config: &crate::Config) -> Option<CheckConfigResult> {
        let mut ret = CheckConfigResult::default();

        if let Some(ref tls_config) = self.tls_config {
            if let Some(r) = tls_config.check(all_config) {
                ret.add_all(r);
            }
        }

        if ret.is_empty() {
            Some(ret)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TLSConfig {
    #[serde(default = "TLSConfig::default_certificate")]
    pub certificate: String,
    #[serde(default = "TLSConfig::default_private_key")]
    pub private_key: String,
}

impl TLSConfig {
    fn default_certificate() -> String {
        "./config/tls/server.crt".to_string()
    }

    fn default_private_key() -> String {
        "./config/tls/server.key".to_string()
    }
}

impl Default for TLSConfig {
    fn default() -> Self {
        Self {
            certificate: Self::default_certificate(),
            private_key: Self::default_private_key(),
        }
    }
}

impl OverrideByEnv for TLSConfig {
    fn override_by_env(&mut self) {
        if let Ok(cer) = std::env::var("CNOSDB_SECURITY_TLS_CERTIFICATE") {
            self.certificate = cer;
        }
        if let Ok(key) = std::env::var("CNOSDB_SECURITY_TLS_PRIVATE_KEY") {
            self.private_key = key;
        }
    }
}

impl CheckConfig for TLSConfig {
    fn check(&self, _: &crate::Config) -> Option<CheckConfigResult> {
        let config_name = Arc::new("security.tls".to_string());
        let mut ret = CheckConfigResult::default();

        if self.certificate.is_empty() {
            ret.add_error(CheckConfigItemResult {
                config: config_name.clone(),
                item: "certificate".to_string(),
                message: "'certificate' is empty".to_string(),
            });
        }
        if self.private_key.is_empty() {
            ret.add_error(CheckConfigItemResult {
                config: config_name,
                item: "private_key".to_string(),
                message: "'private_key' is empty".to_string(),
            });
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
