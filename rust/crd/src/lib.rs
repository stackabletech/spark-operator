//! This module provides all required CRD definitions and additional helper methods.
pub mod commands;

use serde::{Deserialize, Serialize};
use stackable_operator::{
    kube::{runtime::reflector::ObjectRef, CustomResource},
    product_config_utils::{ConfigError, Configuration},
    role_utils::{CommonConfiguration, Role},
    schemars::{self, JsonSchema},
};
use stackable_spark_common::constants::{
    SPARK_DEFAULTS_AUTHENTICATE, SPARK_DEFAULTS_AUTHENTICATE_SECRET, SPARK_DEFAULTS_EVENT_LOG_DIR,
    SPARK_DEFAULTS_HISTORY_FS_LOG_DIRECTORY, SPARK_DEFAULTS_HISTORY_STORE_PATH,
    SPARK_DEFAULTS_HISTORY_WEBUI_PORT, SPARK_DEFAULTS_PORT_MAX_RETRIES, SPARK_ENV_MASTER_PORT,
    SPARK_ENV_MASTER_WEBUI_PORT, SPARK_ENV_WORKER_CORES, SPARK_ENV_WORKER_MEMORY,
    SPARK_ENV_WORKER_PORT, SPARK_ENV_WORKER_WEBUI_PORT,
};
use std::collections::BTreeMap;
use std::hash::Hash;
use strum_macros::EnumIter;

const DEFAULT_LOG_DIR: &str = "/tmp";
const SPARK_DEFAULTS_CONF: &str = "spark-defaults.conf";
const SPARK_ENV_SH: &str = "spark-env.sh";

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "spark.stackable.tech",
    version = "v1alpha1",
    kind = "SparkCluster",
    shortname = "sc",
    status = "SparkClusterStatus",
    namespaced,
    kube_core = "stackable_operator::kube::core",
    k8s_openapi = "stackable_operator::k8s_openapi",
    schemars = "stackable_operator::schemars"
)]
#[serde(rename_all = "camelCase")]
pub struct SparkClusterSpec {
    pub version: Option<String>,
    pub masters: Role<MasterConfig>,
    pub workers: Role<WorkerConfig>,
    pub history_servers: Option<Role<HistoryServerConfig>>,
    #[serde(flatten)]
    pub config: Option<CommonConfiguration<CommonConfig>>,
}

impl SparkClusterSpec {
    pub fn monitoring_enabled(&self) -> bool {
        if let Some(CommonConfiguration {
            config:
                Some(CommonConfig {
                    enable_monitoring: Some(enabled),
                    ..
                }),
            ..
        }) = &self.config
        {
            return *enabled;
        }
        false
    }
}
#[derive(Clone, Default, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SparkClusterStatus {
    /// An opaque value that changes every time a discovery detail does
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery_hash: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommonConfig {
    pub secret: Option<String>,
    pub log_dir: Option<String>,
    pub max_port_retries: Option<usize>,
    pub enable_monitoring: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MasterConfig {
    pub master_port: Option<u16>,
    pub master_web_ui_port: Option<u16>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerConfig {
    pub cores: Option<usize>,
    pub memory: Option<String>,
    pub worker_port: Option<u16>,
    pub worker_web_ui_port: Option<u16>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryServerConfig {
    pub store_path: Option<String>,
    pub history_web_ui_port: Option<u16>,
}

impl Configuration for MasterConfig {
    type Configurable = SparkCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        resource: &Self::Configurable,
        role_name: &str,
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut config = BTreeMap::new();

        match file {
            SPARK_ENV_SH => {
                if let Some(port) = &self.master_port {
                    config.insert(SPARK_ENV_MASTER_PORT.to_string(), Some(port.to_string()));
                }
                if let Some(web_ui_port) = &self.master_web_ui_port {
                    config.insert(
                        SPARK_ENV_MASTER_WEBUI_PORT.to_string(),
                        Some(web_ui_port.to_string()),
                    );
                }
            }
            SPARK_DEFAULTS_CONF => {
                add_common_spark_defaults(role_name, &mut config, &resource.spec)
            }
            _ => {}
        }

        Ok(config)
    }
}

impl Configuration for WorkerConfig {
    type Configurable = SparkCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        resource: &Self::Configurable,
        role_name: &str,
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut config = BTreeMap::new();

        match file {
            SPARK_ENV_SH => {
                if let Some(cores) = &self.cores {
                    config.insert(SPARK_ENV_WORKER_CORES.to_string(), Some(cores.to_string()));
                }
                if let Some(memory) = &self.memory {
                    config.insert(
                        SPARK_ENV_WORKER_MEMORY.to_string(),
                        Some(memory.to_string()),
                    );
                }
                if let Some(port) = &self.worker_port {
                    config.insert(SPARK_ENV_WORKER_PORT.to_string(), Some(port.to_string()));
                }
                if let Some(web_ui_port) = &self.worker_web_ui_port {
                    config.insert(
                        SPARK_ENV_WORKER_WEBUI_PORT.to_string(),
                        Some(web_ui_port.to_string()),
                    );
                }
            }
            SPARK_DEFAULTS_CONF => {
                add_common_spark_defaults(role_name, &mut config, &resource.spec)
            }
            _ => {}
        }

        Ok(config)
    }
}

impl Configuration for HistoryServerConfig {
    type Configurable = SparkCluster;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        resource: &Self::Configurable,
        role_name: &str,
        file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut config = BTreeMap::new();

        match file {
            SPARK_ENV_SH => {}
            SPARK_DEFAULTS_CONF => {
                if let Some(CommonConfiguration {
                    config: Some(common_config),
                    ..
                }) = &resource.spec.config
                {
                    let log_dir = common_config.log_dir.as_deref().unwrap_or(DEFAULT_LOG_DIR);
                    config.insert(
                        SPARK_DEFAULTS_HISTORY_FS_LOG_DIRECTORY.to_string(),
                        Some(log_dir.to_string()),
                    );
                }

                if let Some(store_path) = &self.store_path {
                    config.insert(
                        SPARK_DEFAULTS_HISTORY_STORE_PATH.to_string(),
                        Some(store_path.to_string()),
                    );
                }
                if let Some(port) = &self.history_web_ui_port {
                    config.insert(
                        SPARK_DEFAULTS_HISTORY_WEBUI_PORT.to_string(),
                        Some(port.to_string()),
                    );
                }

                add_common_spark_defaults(role_name, &mut config, &resource.spec)
            }
            _ => {}
        }

        Ok(config)
    }
}

fn add_common_spark_defaults(
    role: &str,
    config: &mut BTreeMap<String, Option<String>>,
    spec: &SparkClusterSpec,
) {
    if let Some(CommonConfiguration {
        config: Some(common_config),
        ..
    }) = &spec.config
    {
        if let Some(secret) = &common_config.secret {
            config.insert(
                SPARK_DEFAULTS_AUTHENTICATE.to_string(),
                Some("true".to_string()),
            );

            config.insert(
                SPARK_DEFAULTS_AUTHENTICATE_SECRET.to_string(),
                Some(secret.to_string()),
            );
        }

        let max_port_retries = &common_config.max_port_retries.unwrap_or(0);
        config.insert(
            SPARK_DEFAULTS_PORT_MAX_RETRIES.to_string(),
            Some(max_port_retries.to_string()),
        );

        let log_dir = common_config.log_dir.as_deref().unwrap_or(DEFAULT_LOG_DIR);
        if role == SparkRole::HistoryServer.to_string() {
            config.insert(
                SPARK_DEFAULTS_HISTORY_FS_LOG_DIRECTORY.to_string(),
                Some(log_dir.to_string()),
            );
        } else {
            config.insert(
                SPARK_DEFAULTS_EVENT_LOG_DIR.to_string(),
                Some(log_dir.to_string()),
            );
        }
    }
}

/// Enum to manage the different Spark roles.
/// `WARNING`: Do not alter the order of the roles because they are iterated over (via strum)
/// and it always has to start with master (then worker, then history server).
#[derive(
    EnumIter,
    Clone,
    Debug,
    Hash,
    Deserialize,
    Eq,
    JsonSchema,
    PartialEq,
    Serialize,
    strum_macros::Display,
    strum_macros::EnumString,
)]
pub enum SparkRole {
    #[serde(rename = "master")]
    #[strum(serialize = "master")]
    Master,
    #[serde(rename = "slave")]
    #[strum(serialize = "slave")]
    Worker,
    #[serde(rename = "history-server")]
    #[strum(serialize = "history-server")]
    HistoryServer,
}

impl SparkRole {
    /// Returns the container start command for a spark node
    /// Right now works only for images using hadoop2.7
    /// # Arguments
    ///
    /// * `version` - Current specified cluster version
    pub fn get_command(&self) -> String {
        format!("./sbin/start-{}.sh", self.to_string())
    }

    /// Returns a tuple of the required container ports for each role.
    /// (protocol_port, web_ui_port / metrics_port)
    ///
    /// # Arguments
    ///
    /// * `config` - The validated product-config containing user defined or default ports
    pub fn container_ports<'a>(
        &self,
        config: &'a BTreeMap<String, String>,
    ) -> (Option<&'a String>, Option<&'a String>) {
        return match &self {
            SparkRole::Master => (
                config.get(SPARK_ENV_MASTER_PORT),
                config.get(SPARK_ENV_MASTER_WEBUI_PORT),
            ),
            SparkRole::Worker => (
                config.get(SPARK_ENV_WORKER_PORT),
                config.get(SPARK_ENV_WORKER_WEBUI_PORT),
            ),
            SparkRole::HistoryServer => (None, config.get(SPARK_DEFAULTS_HISTORY_WEBUI_PORT)),
        };
    }
}

#[derive(Clone, Debug, Hash, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct ConfigOption {
    pub name: String,
    pub value: String,
}
