mod error;

pub use crate::error::CrdError;
use derivative::Derivative;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::Crd;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

// TODO: We need to validate the name of the cluster because it is used in pod and configmap names, it can't bee too long
// This probably also means we shouldn't use the node_names in the pod_name...

#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "spark.stackable.de",
    version = "v1",
    kind = "SparkCluster",
    shortname = "sc",
    namespaced
)]
#[kube(status = "SparkClusterStatus")]
pub struct SparkClusterSpec {
    pub master: SparkNode,
    pub worker: SparkNode,
    pub history_server: Option<SparkNode>,
    pub image: String,
    pub secret: Option<String>,
    pub log_dir: Option<String>,
}

impl SparkClusterSpec {
    /// collect hashed selectors from master, worker and history server
    pub fn get_hashed_selectors(
        &self,
    ) -> HashMap<SparkNodeType, HashMap<String, SparkNodeSelector>> {
        let mut hashed_selectors: HashMap<SparkNodeType, HashMap<String, SparkNodeSelector>> =
            HashMap::new();

        hashed_selectors.insert(
            SparkNodeType::Master,
            self.master.get_hashed_selectors(SparkNodeType::Master),
        );

        hashed_selectors.insert(
            SparkNodeType::Worker,
            self.worker.get_hashed_selectors(SparkNodeType::Worker),
        );

        if let Some(history_server) = &self.history_server {
            hashed_selectors.insert(
                SparkNodeType::HistoryServer,
                history_server.get_hashed_selectors(SparkNodeType::HistoryServer),
            );
        }

        hashed_selectors
    }
}

#[derive(Clone, Debug, Hash, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct SparkNode {
    selectors: Vec<SparkNodeSelector>,
    // options
    // master -> use Option<T>

    // worker -> use Option<T>

    // history_server -> use Option<T>
}

#[derive(Derivative, Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[derivative(Hash)]
pub struct SparkNodeSelector {
    // common
    pub node_name: String,
    // we need to ignore instances from hashing -> if we scale up or down, the hash
    // for the selectors changes and the operator removes all nodes and rebuilds them
    #[derivative(Hash = "ignore")]
    pub instances: usize,
    pub config: Option<ConfigOption>,
    pub env: Option<ConfigOption>,
    // master -> use Option<T>

    // worker -> use Option<T>
    pub cores: Option<usize>,
    pub memory: Option<String>,
    // history_server -> use Option<T>
}

impl SparkNode {
    pub fn get_hashed_selectors(
        &self,
        node_type: SparkNodeType,
    ) -> HashMap<String, SparkNodeSelector> {
        let mut hashed_selectors: HashMap<String, SparkNodeSelector> = HashMap::new();
        for selector in &self.selectors {
            let mut hasher = DefaultHasher::new();
            selector.hash(&mut hasher);
            node_type.as_str().hash(&mut hasher);
            hashed_selectors.insert(hasher.finish().to_string(), selector.clone());
        }
        hashed_selectors
    }

    pub fn get_instances(&self) -> usize {
        let mut instances: usize = 0;
        for selector in &self.selectors {
            instances += selector.instances;
        }
        instances
    }
}

/// A spark node consists of a list of selectors and optional common properties that is shared for every node
#[derive(Clone, Debug, Hash, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum SparkNodeType {
    Master,
    Worker,
    HistoryServer,
}

const MASTER: &'static str = "master";
const WORKER: &'static str = "slave";
const HISTORY_SERVER: &'static str = "history-server";

impl SparkNodeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            SparkNodeType::Master { .. } => MASTER,
            SparkNodeType::Worker { .. } => WORKER,
            SparkNodeType::HistoryServer { .. } => HISTORY_SERVER,
        }
    }

    pub fn get_command(&self) -> String {
        // TODO: remove hardcoded and adapt for versioning
        format!("spark-3.0.1-bin-hadoop2.7/sbin/start-{}.sh", self.as_str())
    }
}

impl FromStr for SparkNodeType {
    type Err = CrdError;

    fn from_str(input: &str) -> Result<SparkNodeType, Self::Err> {
        match input {
            MASTER => Ok(SparkNodeType::Master),
            WORKER => Ok(SparkNodeType::Worker),
            HISTORY_SERVER => Ok(SparkNodeType::HistoryServer),
            _ => Err(CrdError::InvalidNodeType {
                node_type: input.to_string(),
            }),
        }
    }
}

impl fmt::Display for SparkNodeType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Clone, Debug, Hash, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct ConfigOption {
    pub name: String,
    pub value: String,
}

/// status
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct SparkClusterStatus {
    pub image: SparkClusterImage,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct SparkClusterImage {
    pub name: String,
    pub version: String,
    pub timestamp: String,
}

impl Crd for SparkCluster {
    const RESOURCE_NAME: &'static str = "sparkclusters.spark.stackable.de";
    const CRD_DEFINITION: &'static str = include_str!("../sparkcluster.crd.yaml");
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum SparkVersion {
    #[serde(rename = "2.4.7")]
    v2_4_7,

    #[serde(rename = "3.0.1")]
    v3_0_1,
}
