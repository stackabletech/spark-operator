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
#[kube(status = "RestartCommandStatus")]
#[serde(rename_all = "camelCase")]
pub struct RestartCommandSpec {
    pub name: String,
}

impl stackable_operator::command_controller::CommandCrd for Restart {
    type Owner = SparkCluster;
    fn get_name(&self) -> String {
        self.spec.name.clone()
    }
}

impl Crd for Restart {
    const RESOURCE_NAME: &'static str = "restarts.command.spark.stackable.tech";
    const CRD_DEFINITION: &'static str = include_str!("../restart.command.crd.yaml");
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RestartCommandStatus {
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub message: Option<String>,
}
