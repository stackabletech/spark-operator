//! This module provides all required CRD definitions and additional helper methods.
pub mod commands;

pub use commands::{Restart, Start, Stop};
use semver::Version;
use serde::{Deserialize, Serialize};
use stackable_operator::identity::PodToNodeMapping;
use stackable_operator::k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use stackable_operator::kube::CustomResource;
use stackable_operator::product_config_utils::{ConfigError, Configuration};
use stackable_operator::role_utils::{CommonConfiguration, Role};
use stackable_operator::schemars::{self, JsonSchema};
use stackable_operator::status::{Conditions, Status, Versioned};
use stackable_operator::versioning::{ProductVersion, Versioning, VersioningState};
use stackable_spark_common::constants::{
    SPARK_DEFAULTS_AUTHENTICATE, SPARK_DEFAULTS_AUTHENTICATE_SECRET, SPARK_DEFAULTS_EVENT_LOG_DIR,
    SPARK_DEFAULTS_HISTORY_FS_LOG_DIRECTORY, SPARK_DEFAULTS_HISTORY_STORE_PATH,
    SPARK_DEFAULTS_HISTORY_WEBUI_PORT, SPARK_DEFAULTS_PORT_MAX_RETRIES, SPARK_ENV_MASTER_PORT,
    SPARK_ENV_MASTER_WEBUI_PORT, SPARK_ENV_WORKER_CORES, SPARK_ENV_WORKER_MEMORY,
    SPARK_ENV_WORKER_PORT, SPARK_ENV_WORKER_WEBUI_PORT,
};
use std::cmp::Ordering;
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
    pub version: SparkVersion,
    pub masters: Role<MasterConfig>,
    pub workers: Role<WorkerConfig>,
    pub history_servers: Option<Role<HistoryServerConfig>>,
    #[serde(flatten)]
    pub config: Option<CommonConfiguration<CommonConfig>>,
}

impl Status<SparkClusterStatus> for SparkCluster {
    fn status(&self) -> &Option<SparkClusterStatus> {
        &self.status
    }
    fn status_mut(&mut self) -> &mut Option<SparkClusterStatus> {
        &mut self.status
    }
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

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SparkClusterStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<ProductVersion<SparkVersion>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_command: Option<CurrentCommand>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_execution_status: Option<ClusterExecutionStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history: Option<PodToNodeMapping>,
}

impl Versioned<SparkVersion> for SparkClusterStatus {
    fn version(&self) -> &Option<ProductVersion<SparkVersion>> {
        &self.version
    }
    fn version_mut(&mut self) -> &mut Option<ProductVersion<SparkVersion>> {
        &mut self.version
    }
}

impl Conditions for SparkClusterStatus {
    fn conditions(&self) -> &[Condition] {
        self.conditions.as_slice()
    }
    fn conditions_mut(&mut self) -> &mut Vec<Condition> {
        &mut self.conditions
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum ClusterExecutionStatus {
    Stopped,
    Running,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CurrentCommand {
    pub command_ref: String,
    pub command_type: String,
    pub started_at: String,
}

#[allow(non_camel_case_types)]
#[derive(
    Clone,
    Debug,
    Deserialize,
    Eq,
    JsonSchema,
    PartialEq,
    Serialize,
    strum_macros::Display,
    strum_macros::EnumString,
)]
pub enum SparkVersion {
    #[serde(rename = "2.4.7")]
    #[strum(serialize = "2.4.7")]
    v2_4_7,

    #[serde(rename = "3.0.1")]
    #[strum(serialize = "3.0.1")]
    v3_0_1,

    #[serde(rename = "3.0.2")]
    #[strum(serialize = "3.0.2")]
    v3_0_2,

    #[serde(rename = "3.1.1")]
    #[strum(serialize = "3.1.1")]
    v3_1_1,
}

impl SparkVersion {
    pub fn package_name(&self) -> String {
        format!("spark-{}-bin-hadoop2.7", self.to_string())
    }
}

impl Versioning for SparkVersion {
    fn versioning_state(&self, other: &Self) -> VersioningState {
        let from_version = match Version::parse(&self.to_string()) {
            Ok(v) => v,
            Err(e) => {
                return VersioningState::Invalid(format!(
                    "Could not parse [{}] to SemVer: {}",
                    self.to_string(),
                    e.to_string()
                ))
            }
        };

        let to_version = match Version::parse(&other.to_string()) {
            Ok(v) => v,
            Err(e) => {
                return VersioningState::Invalid(format!(
                    "Could not parse [{}] to SemVer: {}",
                    other.to_string(),
                    e.to_string()
                ))
            }
        };

        match to_version.cmp(&from_version) {
            Ordering::Greater => VersioningState::ValidUpgrade,
            Ordering::Less => VersioningState::ValidDowngrade,
            Ordering::Equal => VersioningState::NoOp,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spark_node_type_get_command() {
        assert_eq!(
            SparkRole::Master.get_command(),
            format!("./sbin/start-{}.sh", SparkRole::Master.to_string())
        );
    }

    #[test]
    fn test_spark_version_versioning() {
        assert_eq!(
            SparkVersion::v2_4_7.versioning_state(&SparkVersion::v3_1_1),
            VersioningState::ValidUpgrade
        );
        assert_eq!(
            SparkVersion::v3_1_1.versioning_state(&SparkVersion::v2_4_7),
            VersioningState::ValidDowngrade
        );
        assert_eq!(
            SparkVersion::v2_4_7.versioning_state(&SparkVersion::v2_4_7),
            VersioningState::NoOp
        );
    }
}
