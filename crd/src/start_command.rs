use crate::SparkCluster;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::Crd;

#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "command.spark.stackable.tech",
    version = "v1",
    kind = "Start",
    namespaced
)]
#[kube(status = "StartCommandStatus")]
#[serde(rename_all = "camelCase")]
pub struct StartCommandSpec {
    pub name: String,
}

impl stackable_operator::command_controller::CommandCrd for Start {
    type Parent = SparkCluster;
    fn get_name(&self) -> String {
        self.spec.name.clone()
    }
}

impl Crd for Start {
    const RESOURCE_NAME: &'static str = "starts.command.spark.stackable.tech";
    const CRD_DEFINITION: &'static str = include_str!("../start.command.crd.yaml");
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StartCommandStatus {}
