use kube::CustomResource;
use serde::{Deserialize, Serialize};
use stackable_operator::CRD;
use std::hash::Hash;

// TODO: We need to validate the name of the cluster because it is used in pod and configmap names, it can't bee too long
// This probably also means we shouldn't use the node_names in the pod_name...

#[derive(Clone, CustomResource, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[kube(
    group = "spark.stackable.de",
    version = "v1",
    kind = "SparkCluster",
    shortname = "sc",
    namespaced
)]
#[kube(status = "SparkClusterStatus")]
pub struct SparkClusterSpec {
    pub master: String,
    //pub master: Vec<SparkClusterSelector>,
    //pub image: String,
    //pub secret: String,
    //pub log_dir: String,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct SparkClusterSelector {
    pub node_name: String,
    pub instances: u32,
    pub cores: u32,
    pub memory: String,
    pub spark_config: Vec<SparkConfigOption>,
    pub spark_env: Vec<SparkConfigOption>,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct SparkConfigOption {
    pub name: String,
    pub value: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct SparkClusterStatus {
    pub image: SparkClusterImage,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct SparkClusterImage {
    pub name: String,
    pub version: String,
    pub timestamp: String,
}

impl CRD for SparkCluster {
    const RESOURCE_NAME: &'static str = "sparkclusters.spark.stackable.de";
    const CRD_DEFINITION: &'static str = r#"
    apiVersion: apiextensions.k8s.io/v1
    kind: CustomResourceDefinition
    metadata:
      name: sparkclusters.spark.stackable.de
    spec:
      group: spark.stackable.de
      names:
        kind: SparkCluster
        singular: sparkcluster
        plural: sparkclusters
        listKind: SparkClusterList
        shortNames:
          - sc
      scope: Namespaced
      versions:
        - name: v1
          served: true
          storage: true
          subresources:
            status: {}
          schema:
            openAPIV3Schema:
              type: object
              properties:
                spec:
                  type: object
                  properties:
                    # master properties
                    master:
                      type: string"#;
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum SparkVersion {
    #[serde(rename = "2.4.7")]
    v2_4_7,

    #[serde(rename = "3.0.1")]
    v3_0_1,
}
