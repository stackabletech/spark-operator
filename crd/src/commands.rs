use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::command_controller::Command;
use stackable_operator::Crd;

#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "command.spark.stackable.tech",
    version = "v1",
    kind = "Restart",
    namespaced
)]
#[kube(status = "CommandStatus")]
#[serde(rename_all = "camelCase")]
pub struct RestartCommandSpec {
    pub name: String,
}

impl Crd for Restart {
    const RESOURCE_NAME: &'static str = "restarts.command.spark.stackable.tech";
    const CRD_DEFINITION: &'static str = include_str!("../../deploy/crd/restart.command.crd.yaml");
}

impl Command for Restart {
    fn get_owner_name(&self) -> String {
        self.spec.name.clone()
    }
}

#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "command.spark.stackable.tech",
    version = "v1",
    kind = "Start",
    namespaced
)]
#[kube(status = "CommandStatus")]
#[serde(rename_all = "camelCase")]
pub struct StartCommandSpec {
    pub name: String,
}

impl Crd for Start {
    const RESOURCE_NAME: &'static str = "starts.command.spark.stackable.tech";
    const CRD_DEFINITION: &'static str = include_str!("../../deploy/crd/start.command.crd.yaml");
}

impl Command for Start {
    fn get_owner_name(&self) -> String {
        self.spec.name.clone()
    }
}

#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "command.spark.stackable.tech",
    version = "v1",
    kind = "Stop",
    namespaced
)]
#[kube(status = "CommandStatus")]
#[serde(rename_all = "camelCase")]
pub struct StopCommandSpec {
    pub name: String,
}

impl Crd for Stop {
    const RESOURCE_NAME: &'static str = "stops.command.spark.stackable.tech";
    const CRD_DEFINITION: &'static str = include_str!("../../deploy/crd/stop.command.crd.yaml");
}

impl Command for Stop {
    fn get_owner_name(&self) -> String {
        self.spec.name.clone()
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CommandStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<CommandStatusMessage>,
}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Serialize,
    JsonSchema,
    PartialEq,
    strum_macros::Display,
    strum_macros::EnumString,
)]
pub enum CommandStatusMessage {
    Enqueued,
    Started,
    Running,
    Finished,
    Error,
}
