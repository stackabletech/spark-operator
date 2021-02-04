use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::CRD;

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

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct SparkNode {
    pub selectors: Vec<SparkNodeSelector>,
}

impl SparkNode {
    pub fn get_instances(&self) -> usize {
        let mut instances: usize = 0;
        for selector in &self.selectors {
            instances += selector.instances;
        }

        instances
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct SparkNodeSelector {
    pub node_name: String,
    pub instances: usize,
    pub cores: Option<String>,
    pub memory: Option<String>,
    pub spark_config: Option<Vec<SparkConfigOption>>,
    pub spark_env: Option<Vec<SparkConfigOption>>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
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
