use crate::SparkRole;
use k8s_openapi::chrono::{DateTime, Utc};
use kube::api::ApiResource;
use kube::{CustomResource, CustomResourceExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::command::{CanBeRolling, HasRoles};
use stackable_operator::command_controller::Command;

#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "command.spark.stackable.tech",
    version = "v1alpha1",
    kind = "Restart",
    namespaced
)]
#[kube(status = "CommandStatus")]
#[serde(rename_all = "camelCase")]
pub struct RestartCommandSpec {
    pub name: String,
    pub rolling: bool,
    pub roles: Option<Vec<SparkRole>>,
}

impl Command for Restart {
    fn get_owner_name(&self) -> String {
        self.spec.name.clone()
    }

    fn start(&mut self) {
        todo!()
    }

    fn done(&mut self) {
        todo!()
    }

    fn start_time(&self) -> Option<DateTime<Utc>> {
        todo!()
    }
}

impl CanBeRolling for Restart {
    fn is_rolling(&self) -> bool {
        self.spec.rolling
    }
}

impl HasRoles for Restart {
    fn get_role_order(&self) -> Option<Vec<String>> {
        self.spec
            .roles
            .clone()
            .map(|roles| roles.into_iter().map(|role| role.to_string()).collect())
    }
}

#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "command.spark.stackable.tech",
    version = "v1alpha1",
    kind = "Start",
    namespaced
)]
#[kube(status = "CommandStatus")]
#[serde(rename_all = "camelCase")]
pub struct StartCommandSpec {
    pub name: String,
}

impl Command for Start {
    fn get_owner_name(&self) -> String {
        self.spec.name.clone()
    }

    fn start(&mut self) {
        todo!()
    }

    fn done(&mut self) {
        todo!()
    }

    fn start_time(&self) -> Option<DateTime<Utc>> {
        todo!()
    }
}

#[derive(Clone, CustomResource, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "command.spark.stackable.tech",
    version = "v1alpha1",
    kind = "Stop",
    namespaced
)]
#[kube(status = "CommandStatus")]
#[serde(rename_all = "camelCase")]
pub struct StopCommandSpec {
    pub name: String,
}

impl Command for Stop {
    fn get_owner_name(&self) -> String {
        self.spec.name.clone()
    }

    fn start(&mut self) {
        todo!()
    }

    fn done(&mut self) {
        todo!()
    }

    fn start_time(&self) -> Option<DateTime<Utc>> {
        todo!()
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
