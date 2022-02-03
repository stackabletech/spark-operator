//! This module provides all required CRD definitions and additional helper methods.
pub mod constants;

use constants::*;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, Snafu};
use stackable_operator::k8s_openapi::api::core::v1::{ContainerPort, ServicePort};
use stackable_operator::role_utils::RoleGroupRef;
use stackable_operator::{
    kube::{runtime::reflector::ObjectRef, CustomResource},
    product_config_utils::{ConfigError, Configuration},
    role_utils::{CommonConfiguration, Role},
    schemars::{self, JsonSchema},
};
use std::collections::BTreeMap;
use std::hash::Hash;
use std::str::FromStr;
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object has no namespace associated"))]
    NoNamespace,
    #[snafu(display("Unknown spark role found {role}. Should be one of {roles:?}"))]
    UnknownSparkRole { role: String, roles: Vec<String> },
    #[snafu(display("spark master fqdn is missing"))]
    SparkMasterFqdnIsMissing,
}

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
pub struct MasterConfig {}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerConfig {
    pub cores: Option<usize>,
    pub memory: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryServerConfig {
    pub store_path: Option<String>,
}

/// Reference to a single `Pod` that is a component of a [`SparkCluster`]
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

impl SparkCluster {
    /// The name of the role-level load-balanced Kubernetes `Service`
    pub fn master_role_service_name(&self) -> Option<String> {
        self.metadata
            .name
            .as_ref()
            .map(|name| format!("{}-{}", name, SparkRole::Master))
    }

    /// The fully-qualified domain name of the role-level load-balanced Kubernetes `Service`
    pub fn master_role_service_fqdn(&self) -> Option<String> {
        Some(format!(
            "{}.{}.svc.cluster.local",
            self.master_role_service_name()?,
            self.metadata.namespace.as_ref()?
        ))
    }

    /// List all master pods expected to form the cluster
    ///
    /// We try to predict the pods here rather than looking at the current cluster state in order to
    /// avoid instance churn.
    pub fn master_pods(&self) -> Result<impl Iterator<Item = SparkPodRef> + '_, Error> {
        let ns = self.metadata.namespace.clone().context(NoNamespaceSnafu)?;
        Ok(self
            .spec
            .masters
            .iter()
            .flat_map(|role| &role.role_groups)
            // Order rolegroups consistently, to avoid spurious downstream rewrites
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .flat_map(move |(rolegroup_name, rolegroup)| {
                let rolegroup_ref = SparkRole::Master.rolegroup_ref(self, rolegroup_name);
                let ns = ns.clone();
                (0..rolegroup.replicas.unwrap_or(0)).map(move |i| SparkPodRef {
                    namespace: ns.clone(),
                    role_group_service_name: rolegroup_ref.object_name(),
                    pod_name: format!("{}-{}", rolegroup_ref.object_name(), i),
                })
            }))
    }

    pub fn enable_monitoring(&self) -> Option<bool> {
        let spec: &SparkClusterSpec = &self.spec;
        spec.config
            .as_ref()
            .map(|common_configuration| &common_configuration.config)
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
        // IMPORTANT: we need to make sure this env var is set explicitly otherwise
        // it would be impossible to create a cluster simply named "spark".
        // In that case, this variable is created by the rolegroup service but set to tcp://<ip>:8080 or similar.
        // which will break the start-master.sh script.
        Ok([(
            "SPARK_MASTER_PORT".to_string(),
            Some(MASTER_RPC_PORT.to_string()),
        )]
        .into())
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

        if file == SPARK_DEFAULTS_CONF {
            add_common_spark_defaults(role_name, &mut config, &resource.spec)
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

        if file == SPARK_DEFAULTS_CONF {
            if let Some(store_path) = &self.store_path {
                config.insert(
                    SPARK_DEFAULTS_HISTORY_STORE_PATH.to_string(),
                    Some(store_path.to_string()),
                );
            }

            add_common_spark_defaults(role_name, &mut config, &resource.spec)
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
        config: common_config,
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
    }
}

/// Enum to manage the different Spark roles.
#[derive(
    Clone,
    Debug,
    Deserialize,
    EnumIter,
    Eq,
    Hash,
    JsonSchema,
    PartialEq,
    Serialize,
    strum_macros::Display,
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
    pub fn http_port(&self) -> u16 {
        match self {
            SparkRole::Master => MASTER_HTTP_PORT,
            SparkRole::Worker => WORKER_HTTP_PORT,
            SparkRole::HistoryServer => HISTORY_SERVER_HTTP_PORT,
        }
    }

    pub fn rpc_port(&self) -> Option<u16> {
        match self {
            SparkRole::Master => Some(MASTER_RPC_PORT),
            _ => None,
        }
    }

    pub fn container_ports(&self) -> Vec<ContainerPort> {
        let mut ports = vec![ContainerPort {
            name: Some(HTTP_PORT_NAME.to_string()),
            container_port: self.http_port().into(),
            protocol: Some("TCP".to_string()),
            ..ContainerPort::default()
        }];

        if let Some(rpc) = self.rpc_port() {
            ports.push(ContainerPort {
                name: Some(RPC_PORT_NAME.to_string()),
                container_port: rpc.into(),
                protocol: Some("TCP".to_string()),
                ..ContainerPort::default()
            })
        }

        ports
    }

    pub fn service_ports(&self) -> Vec<ServicePort> {
        let mut ports = vec![ServicePort {
            name: Some(HTTP_PORT_NAME.to_string()),
            port: self.http_port().into(),
            protocol: Some("TCP".to_string()),
            ..ServicePort::default()
        }];

        if let Some(rpc) = self.rpc_port() {
            ports.push(ServicePort {
                name: Some(RPC_PORT_NAME.to_string()),
                port: rpc.into(),
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            })
        }

        ports
    }

    pub fn command(&self, spark: &SparkCluster) -> Result<Vec<String>, Error> {
        let mut command = vec![format!("sbin/start-{}.sh", self)];

        if *self == Self::Worker {
            command.push(format!(
                "spark://{}:{}",
                spark
                    .master_role_service_fqdn()
                    .ok_or(Error::SparkMasterFqdnIsMissing)?,
                MASTER_RPC_PORT
            ));
        }

        Ok(command)
    }

    pub fn rolegroup_ref(
        &self,
        spark: &SparkCluster,
        group_name: impl Into<String>,
    ) -> RoleGroupRef<SparkCluster> {
        RoleGroupRef {
            cluster: ObjectRef::from_obj(spark),
            role: self.to_string(),
            role_group: group_name.into(),
        }
    }

    pub fn replicas(&self, spark: &SparkCluster, role_group: &str) -> Option<u16> {
        match self {
            SparkRole::Master => spark.spec.masters.as_ref().and_then(|role| {
                role.role_groups
                    .get(role_group)
                    .and_then(|role_group| role_group.replicas)
            }),
            SparkRole::Worker => spark.spec.workers.as_ref().and_then(|role| {
                role.role_groups
                    .get(role_group)
                    .and_then(|role_group| role_group.replicas)
            }),
            SparkRole::HistoryServer => spark.spec.history_servers.as_ref().and_then(|role| {
                role.role_groups
                    .get(role_group)
                    .and_then(|role_group| role_group.replicas)
            }),
        }
    }

    fn roles() -> Vec<String> {
        let mut roles = vec![];
        for role in Self::iter() {
            roles.push(role.to_string())
        }
        roles
    }
}

impl FromStr for SparkRole {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == Self::Master.to_string() {
            Ok(Self::Master)
        } else if s == Self::Worker.to_string() {
            Ok(Self::Worker)
        } else if s == Self::HistoryServer.to_string() {
            Ok(Self::HistoryServer)
        } else {
            Err(Error::UnknownSparkRole {
                role: s.to_string(),
                roles: Self::roles(),
            })
        }
    }
}
