use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use k8s_openapi::Resource;
use kube::api::{Meta, PostParams};
use kube::Api;
use serde::de::DeserializeOwned;
use serde_json::json;
use stackable_operator::client::Client;
use stackable_operator::error::OperatorResult;
use stackable_spark_crd::{ClusterStatus, CurrentCommand, Restart, SparkCluster, Start, Stop};
use std::collections::HashMap;
use std::fmt::Debug;

use stackable_operator::reconcile::ReconcileFunctionAction;
use tracing::info;

const COMMAND_STATUS_LABEL: &str = "status";
const COMMAND_STATUS_VALUE: &str = "done";

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

    /// Return the type/kind
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
    /// * `cluster` - Spark cluster custom resource
    /// * `current_command` - Command that is currently processed
    /// * `pods` - All available cluster pods
    ///
    pub async fn process_command_start(
        &self,
        client: &Client,
        cluster: &SparkCluster,
        current_command: &CurrentCommand,
        pods: &[Pod],
    ) -> OperatorResult<()> {
        // set the current_command in the cluster custom resource status
        let updated_cluster = update_current_command(client, cluster, current_command).await?;

        // apply command specific action
        match self {
            CommandType::Restart(_) => {
                for pod in pods {
                    client.delete(pod).await?;
                }
                update_cluster_status(client, &updated_cluster, &ClusterStatus::Running).await?;
            }
            CommandType::Start(_) => {
                update_cluster_status(client, &updated_cluster, &ClusterStatus::Running).await?;
            }
            CommandType::Stop(_) => {
                for pod in pods {
                    client.delete(pod).await?;
                }
                update_cluster_status(client, &updated_cluster, &ClusterStatus::Stopped).await?;
            }
        }

        info!(
            "Executing command '{}' of type '{}' ...",
            current_command.command_ref, current_command.command_type
        );

        Ok(())
    }

    /// Implementation of command behavior when the command is running
    ///
    /// # Arguments
    /// * `client` - Kubernetes client
    /// * `cluster` - Spark cluster custom resource
    /// * `current_command` - Current running command
    ///
    pub async fn process_command_running(
        &self,
        client: &Client,
        cluster: &mut SparkCluster,
        current_command: &CurrentCommand,
    ) -> OperatorResult<ReconcileFunctionAction> {
        if let CommandType::Stop(stop) = self {
            finalize_current_command(client, cluster, &ClusterStatus::Stopped).await?;
            update_command_label(client, stop).await?;
            return Ok(ReconcileFunctionAction::Done);
        }

        info!(
            "Command '{}' of type '{}' is running since {}...",
            current_command.command_ref, current_command.command_type, current_command.started_at
        );

        Ok(ReconcileFunctionAction::Continue)
    }

    /// Implementation of behavior when commands are finished (at the end of reconcile)
    ///
    /// # Arguments
    /// * `client` - Kubernetes client
    /// * `cluster` - Spark cluster custom resource
    ///
    pub async fn process_command_finalize(
        &self,
        client: &Client,
        cluster: &mut SparkCluster,
    ) -> OperatorResult<SparkCluster> {
        let updated_cluster =
            finalize_current_command(client, cluster, &ClusterStatus::Running).await?;

        // TODO: set label "done" to command to avoid retrieving it via list
        // (for now label selector is not available in list_commands so we need to check
        // the label when retrieving all commands ourselves)
        match self {
            CommandType::Restart(restart) => {
                update_command_label(client, restart).await?;
            }
            CommandType::Start(start) => {
                update_command_label(client, start).await?;
            }
            _ => {}
        }

        Ok(updated_cluster)
    }
}

/// Finalize a finished command. Delete current_command and set the cluster_status.
///
/// # Arguments
/// * `client` - Kubernetes client
/// * `cluster` - Spark cluster custom resource
/// * `cluster_status` - Desired cluster status to be set
///
async fn finalize_current_command(
    client: &Client,
    cluster: &mut SparkCluster,
    cluster_status: &ClusterStatus,
) -> OperatorResult<SparkCluster> {
    // let patch = json_patch::Patch(vec![PatchOperation::Remove(RemoveOperation {
    //     path: "/status/currentCommand".to_string(),
    // })]);
    //
    // client.json_patch(cluster, patch).await

    // TODO: Now we replace the whole status just to remove the current_command.
    // I tried json_patch delete / replace as well (check commented code above), did not work. Any suggestions?
    if let Some(status) = &mut cluster.status {
        status.current_command = None;
        status.cluster_status = Some(cluster_status.clone());

        let api: Api<SparkCluster> = client.get_api(Meta::namespace(cluster).as_deref());

        return Ok(api
            .replace_status(
                &Meta::name(cluster),
                &PostParams {
                    dry_run: false,
                    field_manager: None,
                },
                serde_json::to_vec(&cluster)?,
            )
            .await?);
    }

    Ok(cluster.clone())
}

/// Set/Update the cluster status of the main custom resource.
///
/// # Arguments
/// * `client` - Kubernetes client
/// * `cluster` - Spark cluster custom resource
/// * `cluster_status` - Desired cluster status to be set
///
async fn update_cluster_status(
    client: &Client,
    cluster: &SparkCluster,
    cluster_status: &ClusterStatus,
) -> OperatorResult<SparkCluster> {
    client
        .merge_patch_status(cluster, &json!({ "clusterStatus": cluster_status }))
        .await
}

/// Set/Update the status of the main custom resource with the current command status.
///
/// # Arguments
/// * `client` - Kubernetes client
/// * `cluster` - Spark cluster custom resource
/// * `current_command` - Current command to be set
///
async fn update_current_command(
    client: &Client,
    cluster: &SparkCluster,
    current_command: &CurrentCommand,
) -> OperatorResult<SparkCluster> {
    client
        .merge_patch_status(cluster, &json!({ "currentCommand": current_command }))
        .await
}

/// Update the labels of the command custom resource to finalize and ignore it in the future.
///
/// # Arguments
/// * `client` - Kubernetes client
/// * `command` - Command custom resource
///
async fn update_command_label<T>(client: &Client, command: &T) -> OperatorResult<T>
where
    T: Meta + Clone + DeserializeOwned,
{
    let mut labels = HashMap::new();
    labels.insert(
        COMMAND_STATUS_LABEL.to_string(),
        COMMAND_STATUS_VALUE.to_string(),
    );

    let new_metadata = json!({
        "metadata": {
            "labels": &labels
        }
    });

    client.merge_patch(command, new_metadata).await
}

/// Retrieve the command custom resource depending on the type and name.
///
/// # Arguments
/// * `client` - Kubernetes client
/// * `command_type` - Command type like Restart, Start, Stop etc.
/// * `command` - Reference to meta.name in the command resource
/// * `namespace` - Namespace of the resource
///
pub async fn get_command_from_ref(
    client: &Client,
    command_type: &str,
    command: &str,
    namespace: Option<&str>,
) -> OperatorResult<CommandType> {
    // TODO: adapt ::KIND for kube-rs changes on 0.52
    match command_type {
        Restart::KIND => Ok(CommandType::Restart(
            client
                .get::<stackable_spark_crd::Restart>(command, namespace)
                .await?,
        )),
        Start::KIND => Ok(CommandType::Start(
            client
                .get::<stackable_spark_crd::Start>(command, namespace)
                .await?,
        )),
        Stop::KIND => Ok(CommandType::Stop(
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

    Ok(all_commands.into_iter().next())
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

    // TODO: We check for commands that are done here.
    // will become obsolete with label selector in list_commands
    while let Some(cmd) = restart_commands.pop() {
        if is_command_done(&cmd) {
            continue;
        }
        all_commands.push(CommandType::Restart(cmd));
    }

    while let Some(cmd) = start_commands.pop() {
        if is_command_done(&cmd) {
            continue;
        }
        all_commands.push(CommandType::Start(cmd));
    }

    while let Some(cmd) = stop_commands.pop() {
        if is_command_done(&cmd) {
            continue;
        }
        all_commands.push(CommandType::Stop(cmd));
    }

    Ok(all_commands)
}

/// Check the label of a command custom resource for the "status" set to "done"
/// That means the command was already processed.
/// # Arguments
/// * `command` - Command custom resource
///
// TODO: will be obsolete with label selector in list_commands
fn is_command_done<T>(command: &T) -> bool
where
    T: Meta,
{
    if let Some(labels) = &command.meta().labels {
        if labels.get(COMMAND_STATUS_LABEL) == Some(&COMMAND_STATUS_VALUE.to_string()) {
            return true;
        }
    }

    false
}

/// Retrieve a timestamp in format: "2021-03-23T16:20:19Z".
/// Required to set command start and finish timestamps.
pub fn get_current_timestamp() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}
