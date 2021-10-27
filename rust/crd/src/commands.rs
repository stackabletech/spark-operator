use serde::{Deserialize, Serialize};
use serde_json::json;
use stackable_operator::command_controller::Command;
use stackable_operator::k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use stackable_operator::k8s_openapi::chrono::Utc;
use stackable_operator::kube::CustomResource;
use stackable_operator::schemars::{self, JsonSchema};

#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "command.spark.stackable.tech",
    version = "v1alpha1",
    kind = "Restart",
    status = "CommandStatus",
    namespaced,
    kube_core = "stackable_operator::kube::core",
    k8s_openapi = "stackable_operator::k8s_openapi",
    schemars = "stackable_operator::schemars"
)]
#[serde(rename_all = "camelCase")]
pub struct RestartCommandSpec {
    pub name: String,
}

impl Command for Restart {
    fn owner_name(&self) -> String {
        self.spec.name.clone()
    }

    fn start_time(&self) -> Option<&Time> {
        self.status.as_ref()?.started_at.as_ref()
    }

    fn start_patch(&mut self) -> serde_json::Value {
        let time = Time(Utc::now());
        self.status
            .get_or_insert_with(CommandStatus::default)
            .started_at = Some(time.clone());
        json!({ "startedAt": time })
    }

    fn finish_time(&self) -> Option<&Time> {
        self.status.as_ref()?.finished_at.as_ref()
    }

    fn finish_patch(&mut self) -> serde_json::Value {
        let time = Time(Utc::now());
        self.status
            .get_or_insert_with(CommandStatus::default)
            .finished_at = Some(time.clone());
        json!({ "finishedAt": time })
    }
}

#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "command.spark.stackable.tech",
    version = "v1alpha1",
    kind = "Start",
    status = "CommandStatus",
    namespaced,
    kube_core = "stackable_operator::kube::core",
    k8s_openapi = "stackable_operator::k8s_openapi",
    schemars = "stackable_operator::schemars"
)]
#[serde(rename_all = "camelCase")]
pub struct StartCommandSpec {
    pub name: String,
}

impl Command for Start {
    fn owner_name(&self) -> String {
        self.spec.name.clone()
    }

    fn start_time(&self) -> Option<&Time> {
        self.status.as_ref()?.started_at.as_ref()
    }

    fn start_patch(&mut self) -> serde_json::Value {
        let time = Time(Utc::now());
        self.status
            .get_or_insert_with(CommandStatus::default)
            .started_at = Some(time.clone());
        json!({ "startedAt": time })
    }

    fn finish_time(&self) -> Option<&Time> {
        self.status.as_ref()?.finished_at.as_ref()
    }

    fn finish_patch(&mut self) -> serde_json::Value {
        let time = Time(Utc::now());
        self.status
            .get_or_insert_with(CommandStatus::default)
            .finished_at = Some(time.clone());
        json!({ "finishedAt": time })
    }
}

#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "command.spark.stackable.tech",
    version = "v1alpha1",
    kind = "Stop",
    status = "CommandStatus",
    namespaced,
    kube_core = "stackable_operator::kube::core",
    k8s_openapi = "stackable_operator::k8s_openapi",
    schemars = "stackable_operator::schemars"
)]
#[serde(rename_all = "camelCase")]
pub struct StopCommandSpec {
    pub name: String,
}

impl Command for Stop {
    fn owner_name(&self) -> String {
        self.spec.name.clone()
    }

    fn start_time(&self) -> Option<&Time> {
        self.status.as_ref()?.started_at.as_ref()
    }

    fn start_patch(&mut self) -> serde_json::Value {
        let time = Time(Utc::now());
        self.status
            .get_or_insert_with(CommandStatus::default)
            .started_at = Some(time.clone());
        json!({ "startedAt": time })
    }

    fn finish_time(&self) -> Option<&Time> {
        self.status.as_ref()?.finished_at.as_ref()
    }

    fn finish_patch(&mut self) -> serde_json::Value {
        let time = Time(Utc::now());
        self.status
            .get_or_insert_with(CommandStatus::default)
            .finished_at = Some(time.clone());
        json!({ "finishedAt": time })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CommandStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Time>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<Time>,
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
