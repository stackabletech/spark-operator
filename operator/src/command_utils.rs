use crate::Error;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use k8s_openapi::{Metadata, Resource};
use kube::api::{Meta, ObjectMeta};
use serde::de::DeserializeOwned;
use serde_json::json;
use stackable_operator::client::Client;
use stackable_operator::error::OperatorResult;
use stackable_spark_crd::commands::{CommandStatus, CommandStatusMessage};
use stackable_spark_crd::{
    ClusterStatus, CurrentCommand, Restart, SparkCluster, SparkClusterStatus, Start, Stop,
};
use std::fmt::Debug;
use tracing::debug;

/// Collection of all required commands defined in the crd crate.
/// CommandType can easily be stored in a vector to access all available commands.
/// It will result in more duplicated code for implementation but is much easier to
/// access and work with than different traits.
#[derive(Clone, Debug)]
pub enum CommandType {
    Restart(Restart),
    Start(Start),
    Stop(Stop),
}

impl CommandType {
    /// Return the meta name for logging purposes
    pub fn get_name(&self) -> String {
        match self {
            CommandType::Restart(restart) => Meta::name(restart),
            CommandType::Start(start) => Meta::name(start),
            CommandType::Stop(stop) => Meta::name(stop),
        }
    }

    pub fn get_type(&self) -> String {
        match self {
            CommandType::Restart(restart) => restart.kind.clone(),
            CommandType::Start(start) => start.kind.clone(),
            CommandType::Stop(stop) => stop.kind.clone(),
        }
    }

    /// Return the creation timestamp of a command object for sorting purposes
    pub fn get_creation_timestamp(&self) -> Option<Time> {
        match self {
            CommandType::Restart(restart) => restart.meta().creation_timestamp.clone(),
            CommandType::Start(start) => start.meta().creation_timestamp.clone(),
            CommandType::Stop(stop) => stop.meta().creation_timestamp.clone(),
        }
    }

    /// Implementation of command behavior when starting the command
    /// e.g. delete pods for restart
    ///
    /// # Arguments
    /// * `client` - Kubernetes client
    /// * `pods` - All available cluster pods
    ///
    pub async fn process_command_start(
        &self,
        client: &Client,
        cluster: &SparkCluster,
        pods: &[Pod],
    ) -> OperatorResult<()> {
        match self {
            CommandType::Restart(_) => {
                for pod in pods {
                    client.delete(pod).await?;
                }
            }
            CommandType::Start(_) => {
                // remove cluster status stopped
                unimplemented!();
            }
            CommandType::Stop(_) => {
                for pod in pods {
                    client.delete(pod).await?;
                }
                // set cluster status stopped
                update_cluster_status(client, cluster, &ClusterStatus::Stopped).await?;
            }
        }

        Ok(())
    }

    /// Implementation of command behavior when the command is running
    ///
    /// # Arguments
    /// * `client` - Kubernetes client
    /// * `pods` - All available cluster pods
    ///
    pub async fn process_command_running(&self, client: &Client) -> OperatorResult<()> {
        Ok(())
    }

    /// Implementation of behavior when commands are finished (at the end of reconcile)
    ///
    /// # Arguments
    /// * `client` - Kubernetes client
    ///
    pub async fn process_command_finalize(
        &self,
        client: &Client,
        cluster: &SparkCluster,
    ) -> OperatorResult<()> {
        match self {
            CommandType::Restart(_) => {}
            CommandType::Start(_) => {}
            CommandType::Stop(_) => {}
        }

        // delete command from status
        update_current_command_status(client, cluster, &None).await?;
        // set cluster to running
        update_cluster_status(client, cluster, &ClusterStatus::Running).await?;
        // TODO: set label "done" to command to avoid retrieving it via list
        // (for now label selector is not available in list_commands so we need to check
        // the label when retrieving all commands ourselves)
        Ok(())
    }
}

async fn update_cluster_status(
    client: &Client,
    cluster: &SparkCluster,
    cluster_status: &ClusterStatus,
) -> OperatorResult<SparkCluster> {
    client
        .merge_patch_status(cluster, &json!({ "clusterStatus": cluster_status }))
        .await
}

async fn update_current_command_status(
    client: &Client,
    cluster: &SparkCluster,
    current_command: &Option<CurrentCommand>,
) -> OperatorResult<SparkCluster> {
    client
        .merge_patch_status(cluster, &json!({ "currentCommand": current_command }))
        .await
}

async fn update_command_label() {}

pub async fn get_command_from_ref(
    client: &Client,
    command_type: &str,
    command: &str,
    namespace: Option<&str>,
) -> OperatorResult<CommandType> {
    // TODO: adapt ::KIND for kube-rs changes on 0.52
    match command_type {
        stackable_spark_crd::Restart::KIND => Ok(CommandType::Restart(
            client
                .get::<stackable_spark_crd::Restart>(command, namespace)
                .await?,
        )),
        stackable_spark_crd::Start::KIND => Ok(CommandType::Start(
            client
                .get::<stackable_spark_crd::Start>(command, namespace)
                .await?,
        )),
        stackable_spark_crd::Stop::KIND => Ok(CommandType::Stop(
            client
                .get::<stackable_spark_crd::Stop>(command, namespace)
                .await?,
        )),
        _ => Err(stackable_operator::error::Error::MissingCustomResource {
            name: command.to_string(),
        }),
    }
}

/// Collect and sort all available commands and return the first (the one with
/// the oldest creation timestamp) element.
///
/// # Arguments
/// * `client` - Kubernetes client
///
pub async fn get_next_command(client: &Client) -> OperatorResult<Option<CommandType>> {
    let mut all_commands = collect_commands(client).await?;

    all_commands.sort_by_key(|a| a.get_creation_timestamp());

    Ok(all_commands.into_iter().nth(0))
}

/// Collect all different commands in one vector.
///
/// # Arguments
/// * `client` - Kubernetes client
///
async fn collect_commands(client: &Client) -> OperatorResult<Vec<CommandType>> {
    let mut all_commands = vec![];
    let mut restart_commands: Vec<Restart> =
        stackable_operator::command_controller::list_commands::<Restart>(client).await?;

    let mut start_commands: Vec<Start> =
        stackable_operator::command_controller::list_commands::<Start>(client).await?;

    let mut stop_commands: Vec<Stop> =
        stackable_operator::command_controller::list_commands::<Stop>(client).await?;

    while let Some(cmd) = restart_commands.pop() {
        all_commands.push(CommandType::Restart(cmd));
    }

    while let Some(cmd) = start_commands.pop() {
        all_commands.push(CommandType::Start(cmd));
    }

    while let Some(cmd) = stop_commands.pop() {
        all_commands.push(CommandType::Stop(cmd));
    }

    Ok(all_commands)
}

/// Retrieve a timestamp in format: "2021-03-23T16:20:19Z".
/// Required to set command start and finish timestamps.
pub fn get_current_timestamp() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}
