use crate::SparkCluster;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::Crd;

#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "command.spark.stackable.tech",
    version = "v1",
    kind = "Restart",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct SparkClusterCommandRestartSpec {
    pub name: String,
}

impl stackable_operator::command_controller::CommandCrd for Restart {
    type Parent = SparkCluster;
    fn get_name(&self) -> String {
        self.spec.name.clone()
    }
}

impl Crd for Restart {
    const RESOURCE_NAME: &'static str = "restarts.command.spark.stackable.tech";
    const CRD_DEFINITION: &'static str = include_str!("../sparkcluster.command.restart.crd.yaml");
}
