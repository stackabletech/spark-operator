use crate::CommandInformation;
use crate::Error;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use k8s_openapi::Metadata;
use kube::api::{Meta, ObjectMeta};
use serde::de::DeserializeOwned;
use stackable_operator::client::Client;
use stackable_operator::error::OperatorResult;
use stackable_spark_crd::commands::{CommandStatus, CommandStatusMessage};
use stackable_spark_crd::{Restart, Start, Stop};
use std::fmt::Debug;
use tracing::{debug, error, info, trace};

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

    /// Return the creation timestamp of a command object for sorting purposes
    pub fn get_creation_timestamp(&self) -> Option<Time> {
        match self {
            CommandType::Restart(restart) => restart.meta().creation_timestamp.clone(),
            CommandType::Start(start) => start.meta().creation_timestamp.clone(),
            CommandType::Stop(stop) => stop.meta().creation_timestamp.clone(),
        }
    }

    /// Return the current command status
    pub fn get_status(&self) -> Option<CommandStatus> {
        match self {
            CommandType::Restart(restart) => restart.status.clone(),
            CommandType::Start(start) => start.status.clone(),
            CommandType::Stop(stop) => stop.status.clone(),
        }
    }

    /// Writes / updates the status in the command object
    ///
    /// # Arguments
    /// * `client` - Kubernetes client
    /// * `new_status` - Desired new status
    ///
    pub async fn write_status(
        &self,
        client: &Client,
        new_status: &CommandStatus,
    ) -> OperatorResult<bool> {
        match self {
            CommandType::Restart(restart) => {
                update_status(client, restart, &self.get_status(), new_status).await
            }
            CommandType::Start(start) => {
                update_status(client, start, &self.get_status(), new_status).await
            }
            CommandType::Stop(stop) => {
                update_status(client, stop, &self.get_status(), new_status).await
            }
        }
    }

    /// Implementation of command behavior when starting the command
    /// e.g. delete pods for restart
    ///
    /// # Arguments
    /// * `client` - Kubernetes client
    /// * `pods` - All available cluster pods
    ///
    pub async fn process_command_execute(
        &self,
        client: &Client,
        pods: &Vec<Pod>,
    ) -> OperatorResult<bool> {
        let new_status = &CommandStatus {
            started_at: get_time(),
            finished_at: None,
            message: Some(CommandStatusMessage::Started),
        };

        match self {
            CommandType::Restart(_) => {
                for pod in pods {
                    client.delete(pod).await?;
                }

                self.write_status(client, new_status).await
            }
            CommandType::Start(_) => {
                unimplemented!();
            }
            CommandType::Stop(_) => {
                unimplemented!();
            }
        }
    }

    /// Implementation of command behavior when the command is running
    ///
    /// # Arguments
    /// * `client` - Kubernetes client
    /// * `pods` - All available cluster pods
    ///
    pub async fn process_command_running(&self, client: &Client) -> OperatorResult<bool> {
        let new_status = &CommandStatus {
            started_at: None,
            finished_at: None,
            message: Some(CommandStatusMessage::Running),
        };

        self.write_status(&client, new_status).await
    }

    /// Implementation of behavior when commands are finished (at the end of reconcile)
    /// e.g. logging
    pub async fn process_command_finalize(&self, client: &Client) -> OperatorResult<bool> {
        let new_status = &CommandStatus {
            started_at: None,
            finished_at: get_time(),
            message: Some(CommandStatusMessage::Finished),
        };

        self.write_status(&client, new_status).await
    }
}

/// Initialize all comments with a status message if no status available yet. Returns true
/// if any status has been updated.
///
/// # Arguments
/// * `client` - Kubernetes client
/// * `commands` - Available commands
///
pub async fn init_commands(client: &Client, commands: &Vec<CommandType>) -> OperatorResult<bool> {
    let mut changed = false;
    for command in commands {
        // patch empty status first
        if command.get_status().is_none() {
            let new_status = CommandStatus {
                started_at: None,
                finished_at: None,
                message: Some(CommandStatusMessage::Enqueued),
            };

            command.write_status(client, &new_status).await?;

            changed = true;
        }
    }
    Ok(changed)
}

/// Collect all different commands in one vector.
///
/// # Arguments
/// * `client` - Kubernetes client
///
pub async fn collect_commands(client: &Client) -> OperatorResult<Vec<CommandType>> {
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

/// Retrieve the current command depending on its status. We search for "enqueued" commands which
/// have no startedAt and finishedAt timestamps, as well as started/running commands that have
/// a startedAt but no finishedAt timestamp. We ignore finished commands (startedAt, finishedAt
/// are set).
///
/// # Arguments
/// * `command_information` - CommandInformation containing a list of all commands
///
pub fn get_current_command(
    command_information: &Option<CommandInformation>,
) -> Result<Option<&CommandType>, Error> {
    // should never happen
    if command_information.is_none() {
        return Err(Error::CommandError("CommandInformation missing, this is a programming error and should never happen. Please report in our issue tracker.".to_string()));
    }

    let mut current_command = None;

    for command in &command_information.as_ref().unwrap().commands {
        // should never happen
        if command.get_status().is_none() {
            return Err(Error::CommandError(format!("Received uninitialized status for [{}] , this is a programming error and should never happen. Please report in our issue tracker.", command.get_name())));
        }

        let status = command.get_status().unwrap();

        match (status.started_at, status.finished_at) {
            (None,Some(_)) => return  Err(Error::CommandError(format!("Received command [{}] with finishedAt but missing startedAt timestamp in status, this is a programming error and should never happen. Please report in our issue tracker.", command.get_name()))),
            (Some(_), Some(_)) => continue,
            _ => {
                current_command = Some(command);
                break;
            }
        }
    }

    Ok(current_command)
}

/// Write / merge the command status.
///
/// # Arguments
/// * `client` - Kubernetes client
/// * `resource` - Resource owning the status
/// * `status` - Status to be written / merged
///
async fn write_status<T>(
    client: &Client,
    resource: &T,
    new_status: &CommandStatus,
) -> OperatorResult<T>
where
    T: Clone + Debug + DeserializeOwned + Meta + Metadata<Ty = ObjectMeta> + Send + Sync,
{
    client
        .merge_patch_status(resource, &serde_json::json!(new_status))
        .await
}

/// Update the status. Checks if current message is already set and only updates the
/// status if a new message (e.g. a transition from started to running) will be written.
///
/// # Arguments
/// * `client` - Kubernetes client
/// * `resource` - Resource owning the status
/// * `current_status` - Current status that will be updated
/// * `new_status` - New status to be written / merged
///
async fn update_status<T>(
    client: &Client,
    resource: &T,
    current_status: &Option<CommandStatus>,
    new_status: &CommandStatus,
) -> OperatorResult<bool>
where
    T: Clone + Debug + DeserializeOwned + Meta + Metadata<Ty = ObjectMeta> + Send + Sync,
{
    let mut changes = false;
    if let Some(status) = current_status {
        if status.message != new_status.message {
            info!(
                "Command [{}] -> {}",
                T::KIND,
                new_status
                    .message
                    .as_ref()
                    .unwrap_or_else(|| &CommandStatusMessage::Enqueued)
            );

            write_status(client, resource, new_status).await?;
            changes = true;
        }
    } else {
        write_status(client, resource, new_status).await?;
        changes = true;
    }

    Ok(changes)
}

/// Retrieve a timestamp in format: "2021-03-23T16:20:19Z".
/// Required to set command start and finish timestamps.
pub fn get_time() -> Option<String> {
    Some(
        chrono::Utc::now()
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
            .to_string(),
    )
}
