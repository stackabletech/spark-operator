//! This module provides all required CRD definitions and additional helper methods.
pub mod constants;

use constants::*;

use serde::{Deserialize, Serialize};
use snafu::Snafu;
use stackable_operator::role_utils::RoleGroupRef;
use stackable_operator::{
    kube::{runtime::reflector::ObjectRef, CustomResource},
    product_config_utils::{ConfigError, Configuration},
    role_utils::{CommonConfiguration, Role},
    schemars::{self, JsonSchema},
};
use std::collections::BTreeMap;
use std::hash::Hash;
use strum_macros::EnumIter;

#[derive(Clone, CustomResource, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "spark.stackable.tech",
    version = "v1alpha1",
    kind = "SparkCluster",
    shortname = "sc",
    status = "SparkClusterStatus",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[serde(rename_all = "camelCase")]
pub struct SparkClusterSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub masters: Option<Role<MasterConfig>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workers: Option<Role<WorkerConfig>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_servers: Option<Role<HistoryServerConfig>>,
    #[serde(flatten)]
    pub config: Option<CommonConfiguration<CommonConfig>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped: Option<bool>,
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

/// Reference to a single `Pod` that is a component of a [`SparkCluster`]
///
/// Used for service discovery.
pub struct SparkPodRef {
    pub namespace: String,
    pub role_group_service_name: String,
    pub pod_name: String,
}

impl SparkPodRef {
    pub fn fqdn(&self) -> String {
        format!(
            "{}.{}.{}.svc.cluster.local",
            self.pod_name, self.role_group_service_name, self.namespace
        )
    }
}

#[derive(Debug, Snafu)]
#[snafu(display("object has no namespace associated"))]
pub struct NoNamespaceError;
impl SparkCluster {
    /// The name of the role-level load-balanced Kubernetes `Service`
    pub fn server_role_service_name(&self) -> Option<String> {
        self.metadata.name.clone()
    }

    /// The fully-qualified domain name of the role-level load-balanced Kubernetes `Service`
    pub fn server_role_service_fqdn(&self) -> Option<String> {
        Some(format!(
            "{}.{}.svc.cluster.local",
            self.server_role_service_name()?,
            self.metadata.namespace.as_ref()?
        ))
    }

    /// Metadata about a server rolegroup
    pub fn server_rolegroup_ref(
        &self,
        role_name: impl Into<String>,
        group_name: impl Into<String>,
    ) -> RoleGroupRef<SparkCluster> {
        RoleGroupRef {
            cluster: ObjectRef::from_obj(self),
            role: role_name.into(),
            role_group: group_name.into(),
        }
    }

    /// List all pods expected to form the cluster
    ///
    /// We try to predict the pods here rather than looking at the current cluster state in order to
    /// avoid instance churn.
    pub fn pods(&self) -> Result<impl Iterator<Item = SparkPodRef> + '_, NoNamespaceError> {
        let ns = self.metadata.namespace.clone().ok_or(NoNamespaceError)?;
        Ok(self
            .spec
            .masters
            .iter()
            .flat_map(|role| &role.role_groups)
            .map(|(rolegroup_name, rg)| {
                (
                    SparkRole::Master.to_string(),
                    rolegroup_name,
                    rg.replicas.unwrap_or(0),
                )
            })
            .chain(
                self.spec
                    .workers
                    .iter()
                    .flat_map(|role| &role.role_groups)
                    .map(|(rolegroup_name, rg)| {
                        (
                            SparkRole::Worker.to_string(),
                            rolegroup_name,
                            rg.replicas.unwrap_or(0),
                        )
                    }),
            )
            .chain(
                self.spec
                    .history_servers
                    .iter()
                    .flat_map(|role| &role.role_groups)
                    .map(|(rolegroup_name, rg)| {
                        (
                            SparkRole::HistoryServer.to_string(),
                            rolegroup_name,
                            rg.replicas.unwrap_or(0),
                        )
                    }),
            )
            .flat_map(move |(role_name, rolegroup_name, replicas)| {
                let rolegroup_ref = self.server_rolegroup_ref(role_name, rolegroup_name);
                let ns = ns.clone();
                (0..replicas).map(move |i| SparkPodRef {
                    namespace: ns.clone(),
                    role_group_service_name: rolegroup_ref.object_name(),
                    pod_name: format!("{}-{}", rolegroup_ref.object_name(), i),
                })
            }))
    }

    pub fn enable_monitoring(&self) -> Option<bool> {
        self.spec
            .config
            .as_ref()
            .and_then(|common_configuration| common_configuration.config.as_ref())
            .and_then(|common_config| common_config.enable_monitoring)
    }
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
    _role: &str,
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

        // These two are always set together
        let log_dir = common_config.log_dir.as_deref().unwrap_or(DEFAULT_LOG_DIR);
        config.insert(
            SPARK_DEFAULTS_HISTORY_FS_LOG_DIRECTORY.to_string(),
            Some(log_dir.to_string()),
        );
        config.insert(
            SPARK_DEFAULTS_EVENT_LOG_DIR.to_string(),
            Some(log_dir.to_string()),
        );
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
