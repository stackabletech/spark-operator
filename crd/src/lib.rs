mod error;

pub use crate::error::CrdError;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::CRD;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
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
    pub nodes: SparkNode,
    pub image: String,
    pub secret: Option<String>,
    pub log_dir: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct SparkNode {
    pub selectors: Vec<SparkNodeSelector>,
}

impl SparkNode {
    pub fn get_hashed_selectors(
        &self,
        node_type: Option<SparkNodeType>,
    ) -> HashMap<String, &SparkNodeSelector> {
        let mut hashed_selectors = HashMap::new();

        for selector in &self.selectors {
            if node_type == None {
                hashed_selectors.insert(selector.str_hash(), selector);
            } else if node_type == Some(selector.node_type) {
                hashed_selectors.insert(selector.str_hash(), selector);
            }
        }

        hashed_selectors
    }

    pub fn get_instances(&self, node_type: Option<SparkNodeType>) -> usize {
        let mut instances: usize = 0;
        for selector in &self.selectors {
            if node_type == None {
                instances += selector.instances;
            } else if node_type == Some(selector.node_type) {
                instances += selector.instances;
            }
        }

        instances
    }
}

const MASTER: &str = "master";
const WORKER: &str = "worker";
const HISTORY_SERVER: &str = "history-server";

#[derive(Clone, Debug, Hash, Deserialize, Eq, JsonSchema, PartialEq, Serialize, Copy)]
#[serde(rename_all = "lowercase")]
pub enum SparkNodeType {
    Master,
    Worker,
    HistoryServer,
}

impl SparkNodeType {
    pub fn as_str(&self) -> &'static str {
        match *self {
            SparkNodeType::Master => MASTER,
            SparkNodeType::Worker => WORKER,
            SparkNodeType::HistoryServer => HISTORY_SERVER,
        }
    }

    pub fn get_command(&self) -> String {
        format!("sbin/start-{}.sh", self.as_str())
    }
}

impl Display for SparkNodeType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
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

#[derive(Clone, Debug, Hash, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct SparkNodeSelector {
    pub node_type: SparkNodeType,
    pub node_name: String,
    pub instances: usize,
    pub cores: Option<String>,
    pub memory: Option<String>,
    pub spark_config: Option<Vec<SparkConfigOption>>,
    pub spark_env: Option<Vec<SparkConfigOption>>,
}

impl SparkNodeSelector {
    pub fn str_hash(&self) -> String {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish().to_string()
    }
}

#[derive(Clone, Debug, Hash, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct SparkConfigOption {
    pub name: String,
    pub value: String,
}

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

impl CRD for SparkCluster {
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
