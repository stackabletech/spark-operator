//! This module provides all required CRD definitions and additional helper methods.
pub mod commands;
pub mod error;

pub use crate::error::CrdError;
pub use commands::{Restart, Start, Stop};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, LabelSelector};
use kube::CustomResource;
use schemars::JsonSchema;
use semver::{Error as SemVerError, Version};
use serde::{Deserialize, Serialize};
use stackable_operator::label_selector::schema;
use stackable_operator::labels::{APP_COMPONENT_LABEL, APP_ROLE_GROUP_LABEL};
use stackable_operator::status::Conditions;
use stackable_operator::Crd;
use stackable_spark_common::constants::{
    SPARK_DEFAULTS_AUTHENTICATE_SECRET, SPARK_DEFAULTS_EVENT_LOG_DIR,
    SPARK_DEFAULTS_HISTORY_FS_LOG_DIRECTORY, SPARK_DEFAULTS_HISTORY_STORE_PATH,
    SPARK_DEFAULTS_HISTORY_WEBUI_PORT, SPARK_DEFAULTS_MASTER_PORT, SPARK_DEFAULTS_PORT_MAX_RETRIES,
    SPARK_ENV_MASTER_PORT, SPARK_ENV_MASTER_WEBUI_PORT, SPARK_ENV_WORKER_CORES,
    SPARK_ENV_WORKER_MEMORY, SPARK_ENV_WORKER_PORT, SPARK_ENV_WORKER_WEBUI_PORT,
};
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use strum_macros::EnumIter;

const DEFAULT_LOG_DIR: &str = "/tmp";

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "spark.stackable.tech",
    version = "v1",
    kind = "SparkCluster",
    shortname = "sc",
    namespaced
)]
#[kube(status = "SparkClusterStatus")]
#[serde(rename_all = "camelCase")]
pub struct SparkClusterSpec {
    pub masters: NodeGroup<MasterConfig>,
    pub workers: NodeGroup<WorkerConfig>,
    pub history_servers: Option<NodeGroup<HistoryServerConfig>>,
    pub version: SparkVersion,
    pub secret: Option<String>,
    pub log_dir: Option<String>,
    pub max_port_retries: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeGroup<T> {
    pub selectors: HashMap<String, SelectorAndConfig<T>>,
}

impl SparkClusterSpec {
    pub fn get_config(
        &self,
        node_type: &SparkNodeType,
        role_group: &str,
    ) -> Option<Box<dyn Config>> {
        match node_type {
            SparkNodeType::Master => {
                if let Some(selector) = self.masters.selectors.get(role_group) {
                    return Some(Box::new(selector.config.clone().unwrap_or(MasterConfig {
                        ..MasterConfig::default()
                    })));
                }
            }
            SparkNodeType::Worker => {
                if let Some(selector) = self.workers.selectors.get(role_group) {
                    return Some(Box::new(selector.config.clone().unwrap_or(WorkerConfig {
                        ..WorkerConfig::default()
                    })));
                }
            }
            SparkNodeType::HistoryServer => {
                if let Some(history_servers) = &self.history_servers {
                    {
                        if let Some(selector) = history_servers.selectors.get(role_group) {
                            return Some(Box::new(selector.config.clone().unwrap_or(
                                HistoryServerConfig {
                                    ..HistoryServerConfig::default()
                                },
                            )));
                        }
                    }
                }
            }
        }

        None
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SelectorAndConfig<T> {
    pub instances: u16,
    pub instances_per_node: u8,
    pub config: Option<T>,
    #[schemars(schema_with = "schema")]
    pub selector: Option<LabelSelector>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MasterConfig {
    pub master_port: Option<u16>,
    pub master_web_ui_port: Option<u16>,
    pub spark_defaults: Option<Vec<ConfigOption>>,
    pub spark_env_sh: Option<Vec<ConfigOption>>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerConfig {
    pub cores: Option<usize>,
    pub memory: Option<String>,
    pub worker_port: Option<u16>,
    pub worker_web_ui_port: Option<u16>,
    pub spark_defaults: Option<Vec<ConfigOption>>,
    pub spark_env_sh: Option<Vec<ConfigOption>>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryServerConfig {
    pub store_path: Option<String>,
    pub history_web_ui_port: Option<u16>,
    pub spark_defaults: Option<Vec<ConfigOption>>,
    pub spark_env_sh: Option<Vec<ConfigOption>>,
}

pub trait Config: Send + Sync {
    /// Get all required configuration options for spark-defaults.conf
    /// - from spec
    /// - from selector
    /// - from user config properties
    ///
    /// # Arguments
    /// * `spec` - SparkCluster spec for common properties
    ///
    fn get_spark_defaults_conf(&self, spec: &SparkClusterSpec) -> BTreeMap<String, String>;

    /// Get all required configuration options for spark-env.sh
    /// - from selector
    /// - from user config properties
    ///
    fn get_spark_env_sh(&self) -> BTreeMap<String, String>;
}

/// This is a workaround to properly access the methods of the Config trait
/// TODO: suggestions how to improve?
impl<T: ?Sized> Config for Box<T>
where
    T: Config,
{
    fn get_spark_defaults_conf(&self, spec: &SparkClusterSpec) -> BTreeMap<String, String> {
        (**self).get_spark_defaults_conf(&spec)
    }

    fn get_spark_env_sh(&self) -> BTreeMap<String, String> {
        (**self).get_spark_env_sh()
    }
}

impl Config for MasterConfig {
    fn get_spark_defaults_conf(&self, spec: &SparkClusterSpec) -> BTreeMap<String, String> {
        let mut config = BTreeMap::new();

        let log_dir = spec.log_dir.as_deref().unwrap_or(DEFAULT_LOG_DIR);
        config.insert(
            SPARK_DEFAULTS_EVENT_LOG_DIR.to_string(),
            log_dir.to_string(),
        );

        if let Some(secret) = &spec.secret {
            config.insert(
                SPARK_DEFAULTS_AUTHENTICATE_SECRET.to_string(),
                secret.to_string(),
            );
        }

        add_common_spark_defaults(&mut config, spec);
        add_user_defined_config_properties(&mut config, &self.spark_defaults);
        config
    }

    fn get_spark_env_sh(&self) -> BTreeMap<String, String> {
        let mut config = BTreeMap::new();

        if let Some(port) = &self.master_port {
            config.insert(SPARK_ENV_MASTER_PORT.to_string(), port.to_string());
        }
        if let Some(web_ui_port) = &self.master_web_ui_port {
            config.insert(
                SPARK_ENV_MASTER_WEBUI_PORT.to_string(),
                web_ui_port.to_string(),
            );
        }

        add_user_defined_config_properties(&mut config, &self.spark_env_sh);
        config
    }
}

impl Config for WorkerConfig {
    fn get_spark_defaults_conf(&self, spec: &SparkClusterSpec) -> BTreeMap<String, String> {
        let mut config = BTreeMap::new();

        let log_dir = spec.log_dir.as_deref().unwrap_or(DEFAULT_LOG_DIR);
        config.insert(
            SPARK_DEFAULTS_EVENT_LOG_DIR.to_string(),
            log_dir.to_string(),
        );

        if let Some(secret) = &spec.secret {
            config.insert(
                SPARK_DEFAULTS_AUTHENTICATE_SECRET.to_string(),
                secret.to_string(),
            );
        }

        add_common_spark_defaults(&mut config, spec);
        add_user_defined_config_properties(&mut config, &self.spark_defaults);
        config
    }

    fn get_spark_env_sh(&self) -> BTreeMap<String, String> {
        let mut config = BTreeMap::new();

        if let Some(cores) = &self.cores {
            config.insert(SPARK_ENV_WORKER_CORES.to_string(), cores.to_string());
        }
        if let Some(memory) = &self.memory {
            config.insert(SPARK_ENV_WORKER_MEMORY.to_string(), memory.to_string());
        }
        if let Some(port) = &self.worker_port {
            config.insert(SPARK_ENV_WORKER_PORT.to_string(), port.to_string());
        }
        if let Some(web_ui_port) = &self.worker_web_ui_port {
            config.insert(
                SPARK_ENV_WORKER_WEBUI_PORT.to_string(),
                web_ui_port.to_string(),
            );
        }

        add_user_defined_config_properties(&mut config, &self.spark_env_sh);
        config
    }
}

impl Config for HistoryServerConfig {
    fn get_spark_defaults_conf(&self, spec: &SparkClusterSpec) -> BTreeMap<String, String> {
        let mut config = BTreeMap::new();

        let log_dir = spec.log_dir.as_deref().unwrap_or(DEFAULT_LOG_DIR);
        config.insert(
            SPARK_DEFAULTS_HISTORY_FS_LOG_DIRECTORY.to_string(),
            log_dir.to_string(),
        );

        if let Some(store_path) = &self.store_path {
            config.insert(
                SPARK_DEFAULTS_HISTORY_STORE_PATH.to_string(),
                store_path.to_string(),
            );
        }
        if let Some(port) = &self.history_web_ui_port {
            config.insert(
                SPARK_DEFAULTS_HISTORY_WEBUI_PORT.to_string(),
                port.to_string(),
            );
        }

        add_common_spark_defaults(&mut config, spec);
        add_user_defined_config_properties(&mut config, &self.spark_defaults);
        config
    }

    fn get_spark_env_sh(&self) -> BTreeMap<String, String> {
        let mut config = BTreeMap::new();
        add_user_defined_config_properties(&mut config, &self.spark_env_sh);
        config
    }
}

fn add_common_spark_defaults(config: &mut BTreeMap<String, String>, spec: &SparkClusterSpec) {
    if let Some(secret) = &spec.secret {
        config.insert(
            SPARK_DEFAULTS_AUTHENTICATE_SECRET.to_string(),
            secret.to_string(),
        );
    }

    let max_port_retries = &spec.max_port_retries.unwrap_or(0);
    config.insert(
        SPARK_DEFAULTS_PORT_MAX_RETRIES.to_string(),
        max_port_retries.to_string(),
    );
}

fn add_user_defined_config_properties(
    config: &mut BTreeMap<String, String>,
    config_properties: &Option<Vec<ConfigOption>>,
) {
    if let Some(conf) = config_properties {
        for config_option in conf {
            config.insert(config_option.name.clone(), config_option.value.clone());
        }
    }
}

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
pub enum SparkNodeType {
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

impl SparkNodeType {
    /// Returns the container start command for a spark node
    /// Right now works only for images using hadoop2.7
    /// # Arguments
    /// * `version` - Current specified cluster version
    ///
    pub fn get_command(&self, version: &str) -> String {
        // TODO: remove hardcoded and adapt for versioning
        format!(
            "spark-{}-bin-hadoop2.7/sbin/start-{}.sh",
            version,
            self.to_string()
        )
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
    pub current_version: Option<SparkVersion>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_version: Option<SparkVersion>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(schema_with = "stackable_operator::conditions::schema")]
    pub conditions: Vec<Condition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_command: Option<CurrentCommand>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_execution_status: Option<ClusterExecutionStatus>,
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

impl Conditions for SparkCluster {
    fn conditions(&self) -> Option<&[Condition]> {
        if let Some(status) = &self.status {
            return Some(&status.conditions.as_slice());
        }
        None
    }

    fn conditions_mut(&mut self) -> &mut Vec<Condition> {
        if self.status.is_none() {
            self.status = Some(SparkClusterStatus::default());
            return &mut self.status.as_mut().unwrap().conditions;
        }
        return &mut self.status.as_mut().unwrap().conditions;
    }
}

impl Crd for SparkCluster {
    const RESOURCE_NAME: &'static str = "sparkclusters.spark.stackable.tech";
    const CRD_DEFINITION: &'static str = include_str!("../../deploy/crd/sparkcluster.crd.yaml");
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
    pub fn is_upgrade(&self, to: &Self) -> Result<bool, SemVerError> {
        let from_version = Version::parse(&self.to_string())?;
        let to_version = Version::parse(&to.to_string())?;
        Ok(to_version > from_version)
    }

    pub fn is_downgrade(&self, to: &Self) -> Result<bool, SemVerError> {
        let from_version = Version::parse(&self.to_string())?;
        let to_version = Version::parse(&to.to_string())?;
        Ok(to_version < from_version)
    }
}

/// Filter all existing pods for master node type and retrieve the selector config
/// for the given role_group. Extract the nodeName from the pod and the specified port
/// from the config to create the master urls for each pod.
///
/// # Arguments
/// * `pods` - Slice of all existing pods
/// * `spec` - The spark cluster spec
///
pub fn get_master_urls(pods: &[Pod], spec: &SparkClusterSpec) -> Vec<String> {
    let mut master_urls = Vec::new();

    for pod in pods {
        if let Some(labels) = &pod.metadata.labels {
            if let (Some(component), Some(role_group)) = (
                labels.get(APP_COMPONENT_LABEL),
                labels.get(APP_ROLE_GROUP_LABEL),
            ) {
                if component != &SparkNodeType::Master.to_string() {
                    continue;
                }

                if let (Some(config), Some(pod_spec)) = (
                    spec.get_config(&SparkNodeType::Master, role_group),
                    &pod.spec,
                ) {
                    let port = get_master_port(config, spec);

                    if let Some(node_name) = &pod_spec.node_name {
                        master_urls.push(create_master_url(node_name, &port))
                    }
                }
            }
        }
    }

    master_urls
}

/// Search for the selected master port in the master config
/// Priority is: spark_defaults.conf > spark_env.sh > default port
///
/// # Arguments
/// * `config` - The custom resource config of the specified master
/// * `spec` - The spark cluster spec
///
fn get_master_port(config: Box<dyn Config>, spec: &SparkClusterSpec) -> String {
    return if let Some(spark_defaults_port) = config
        .get_spark_defaults_conf(spec)
        .get(SPARK_DEFAULTS_MASTER_PORT)
    {
        spark_defaults_port.clone()
    } else if let Some(spark_env_port) = config.get_spark_env_sh().get(SPARK_ENV_MASTER_PORT) {
        spark_env_port.clone()
    } else {
        // TODO: extract default / recommended from product config
        "7077".to_string()
    };
}

/// Create master url in format: <node_name>:<port>
///
/// # Arguments
/// * `node_name` - Master node_name / host name
/// * `port` - Port on which the master is running
///
fn create_master_url(node_name: &str, port: &str) -> String {
    format!("{}:{}", node_name, port)
}

#[cfg(test)]
mod tests {
    use super::*;
    use stackable_spark_common::constants::SPARK_DEFAULTS_MASTER_PORT;
    use stackable_spark_test_utils::cluster::{Data, TestSparkCluster};

    #[test]
    fn test_get_spark_defaults_master() {
        let mut spark_cluster: SparkCluster = stackable_spark_test_utils::setup_test_cluster();
        spark_cluster.metadata.uid = Some("12345".to_string());

        let master_1_config = spark_cluster
            .spec
            .get_config(
                &SparkNodeType::Master,
                TestSparkCluster::MASTER_1_ROLE_GROUP,
            )
            .unwrap();

        let spark_defaults = master_1_config.get_spark_defaults_conf(&spark_cluster.spec);

        assert_eq!(
            spark_defaults.get(SPARK_DEFAULTS_MASTER_PORT),
            Some(&TestSparkCluster::MASTER_1_CONFIG_PORT.to_string())
        );

        assert_eq!(
            spark_defaults.get(SPARK_DEFAULTS_EVENT_LOG_DIR),
            Some(&TestSparkCluster::CLUSTER_LOG_DIR.to_string())
        );

        assert_eq!(
            spark_defaults.get(SPARK_DEFAULTS_PORT_MAX_RETRIES),
            Some(&TestSparkCluster::CLUSTER_MAX_PORT_RETRIES.to_string())
        );

        assert_eq!(
            spark_defaults.get(SPARK_DEFAULTS_AUTHENTICATE_SECRET),
            Some(&TestSparkCluster::CLUSTER_SECRET.to_string())
        );
    }

    #[test]
    fn test_get_spark_env_master() {
        let mut spark_cluster: SparkCluster = stackable_spark_test_utils::setup_test_cluster();
        spark_cluster.metadata.uid = Some("12345".to_string());

        let master_1_config = spark_cluster
            .spec
            .get_config(
                &SparkNodeType::Master,
                TestSparkCluster::MASTER_1_ROLE_GROUP,
            )
            .unwrap();

        let spark_env = master_1_config.get_spark_env_sh();

        assert_eq!(
            spark_env.get(SPARK_ENV_MASTER_PORT),
            Some(&TestSparkCluster::MASTER_1_ENV_PORT.to_string())
        );

        assert_eq!(
            spark_env.get(SPARK_ENV_MASTER_WEBUI_PORT),
            Some(&TestSparkCluster::MASTER_1_WEB_UI_PORT.to_string())
        );
    }

    #[test]
    fn test_get_spark_env_worker() {
        let mut spark_cluster: SparkCluster = stackable_spark_test_utils::setup_test_cluster();
        spark_cluster.metadata.uid = Some("12345".to_string());

        let master_1_config = spark_cluster
            .spec
            .get_config(
                &SparkNodeType::Worker,
                TestSparkCluster::WORKER_1_ROLE_GROUP,
            )
            .unwrap();

        let spark_env = master_1_config.get_spark_env_sh();

        assert_eq!(
            spark_env.get(SPARK_ENV_WORKER_PORT),
            Some(&TestSparkCluster::WORKER_1_PORT.to_string())
        );

        assert_eq!(
            spark_env.get(SPARK_ENV_WORKER_WEBUI_PORT),
            Some(&TestSparkCluster::WORKER_1_WEBUI_PORT.to_string())
        );

        assert_eq!(
            spark_env.get(SPARK_ENV_WORKER_MEMORY),
            Some(&TestSparkCluster::WORKER_1_ENV_MEMORY.to_string())
        );

        assert_eq!(
            spark_env.get(SPARK_ENV_WORKER_CORES),
            Some(&TestSparkCluster::WORKER_1_CORES.to_string())
        );
    }

    #[test]
    fn test_spark_node_type_get_command() {
        let mut spark_cluster: SparkCluster = stackable_spark_test_utils::setup_test_cluster();
        spark_cluster.metadata.uid = Some("12345".to_string());
        let version = &spark_cluster.spec.version;

        assert_eq!(
            SparkNodeType::Master.get_command(&version.to_string()),
            format!(
                "spark-{}-bin-hadoop2.7/sbin/start-{}.sh",
                &version.to_string(),
                SparkNodeType::Master.to_string()
            )
        );
    }

    #[test]
    fn test_spark_version_is_upgrade() {
        assert_eq!(
            SparkVersion::v2_4_7
                .is_upgrade(&SparkVersion::v3_0_1)
                .unwrap(),
            true
        );
        assert_eq!(
            SparkVersion::v3_0_1
                .is_upgrade(&SparkVersion::v3_0_1)
                .unwrap(),
            false
        );
    }

    #[test]
    fn test_spark_version_is_downgrade() {
        assert_eq!(
            SparkVersion::v3_0_1
                .is_downgrade(&SparkVersion::v2_4_7)
                .unwrap(),
            true
        );
        assert_eq!(
            SparkVersion::v3_0_1
                .is_downgrade(&SparkVersion::v3_0_1)
                .unwrap(),
            false
        );
    }

    #[test]
    fn test_get_master_urls() {
        let spark_cluster: SparkCluster = stackable_spark_test_utils::setup_test_cluster();

        let master_pods = stackable_spark_test_utils::create_master_pods();
        let master_urls = get_master_urls(master_pods.as_slice(), &spark_cluster.spec);
        assert!(!master_urls.is_empty());
        // For master_1 we expect the config port
        assert!(master_urls.contains(&create_master_url(
            TestSparkCluster::MASTER_1_NODE_NAME,
            &TestSparkCluster::MASTER_1_CONFIG_PORT.to_string()
        )));
        // For master_2 we expect the env port
        assert!(master_urls.contains(&create_master_url(
            TestSparkCluster::MASTER_2_NODE_NAME,
            &TestSparkCluster::MASTER_2_PORT.to_string()
        )));
        // For master_3 we expect the default port
        assert!(master_urls.contains(&create_master_url(
            TestSparkCluster::MASTER_3_NODE_NAME,
            &stackable_spark_test_utils::MASTER_DEFAULT_PORT.to_string()
        )));
    }

    #[test]
    fn test_create_master_url() {
        assert_eq!(
            create_master_url(
                TestSparkCluster::MASTER_1_NODE_NAME,
                &TestSparkCluster::MASTER_1_CONFIG_PORT.to_string()
            ),
            format!(
                "{}:{}",
                TestSparkCluster::MASTER_1_NODE_NAME,
                &TestSparkCluster::MASTER_1_CONFIG_PORT.to_string()
            )
        );
    }
}
