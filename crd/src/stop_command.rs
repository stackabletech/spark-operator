use crate::SparkCluster;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::Crd;

#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "command.spark.stackable.tech",
    version = "v1",
    kind = "Stop",
    namespaced
)]
#[kube(status = "StopCommandStatus")]
#[serde(rename_all = "camelCase")]
pub struct StopCommandSpec {
    pub name: String,
}

impl stackable_operator::command_controller::CommandCrd for Stop {
    type Owner = SparkCluster;
    fn get_name(&self) -> String {
        self.spec.name.clone()
    }
}

impl Crd for Stop {
    const RESOURCE_NAME: &'static str = "stops.command.spark.stackable.tech";
    const CRD_DEFINITION: &'static str = include_str!("../stop.command.crd.yaml");
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StopCommandStatus {
    started_at: Option<String>,
    finished_at: Option<String>,
    message: Option<String>,
}
