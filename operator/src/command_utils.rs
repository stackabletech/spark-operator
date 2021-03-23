use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::api::Meta;
use serde::de::DeserializeOwned;
use stackable_operator::client::Client;
use stackable_operator::error::OperatorResult;
use stackable_spark_crd::commands::CommandStatus;
use stackable_spark_crd::{Restart, Start, Stop};

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

    /// Writes / updates the status in the command object. Additionally updates the local command
    /// to keep local and cluster status in sync.
    ///
    /// # Arguments
    /// * `client` - Kubernetes client
    /// * `new_status` - Desired new status
    ///
    pub async fn write_status(
        &mut self,
        client: &Client,
        new_status: Option<CommandStatus>,
    ) -> OperatorResult<()> {
        match self {
            CommandType::Restart(restart) => {
                write_status(client, restart, &new_status).await?;
                restart.status = merge_status_ignore_none(restart.status.clone(), new_status)
            }
            CommandType::Start(start) => {
                write_status(client, start, &new_status).await?;
                start.status = merge_status_ignore_none(start.status.clone(), new_status)
            }
            CommandType::Stop(stop) => {
                write_status(client, stop, &new_status).await?;
                stop.status = merge_status_ignore_none(stop.status.clone(), new_status)
            }
        }

        Ok(())
    }

    /// Implementation of command behavior when starting the command
    /// e.g. delete pods for restart
    ///
    /// # Arguments
    /// * `client` - Kubernetes client
    /// * `pods` - All available cluster pods
    ///
    pub async fn process_command_start(
        &mut self,
        client: &Client,
        pods: &Vec<Pod>,
    ) -> OperatorResult<()> {
        match self {
            CommandType::Restart(_) => {
                for pod in pods {
                    client.delete(pod).await?;
                }
            }
            CommandType::Start(_) => {}
            CommandType::Stop(_) => {}
        }

        Ok(())
    }

    /// Implementation of behavior when commands are running
    /// e.g. blocking pod reconcile (Stop)
    pub async fn process_command_running(&self) -> OperatorResult<()> {
        match self {
            CommandType::Restart(_) => {}
            CommandType::Start(_) => {}
            CommandType::Stop(_) => {}
        }

        Ok(())
    }

    /// Implementation of behavior when commands are finished (at the end of reconcile)
    /// e.g. logging
    pub async fn process_command_finish(&self) -> OperatorResult<()> {
        match self {
            CommandType::Restart(_) => {}
            CommandType::Start(_) => {}
            CommandType::Stop(_) => {}
        }

        Ok(())
    }
}

/// Merges the current command status with a new one. Mimics the behavior of merge_patch
/// within the command object. Meaning if the new status has fields with None, they do not
/// overwrite the current status fields.
///
/// # Arguments
/// * `current_status` - Current command status
/// * `new_status` - New status to update the current one
///
fn merge_status_ignore_none(
    current_status: Option<CommandStatus>,
    new_status: Option<CommandStatus>,
) -> Option<CommandStatus> {
    if let (Some(current_status), Some(new_status)) = (&current_status, &new_status) {
        Some(CommandStatus {
            started_at: if new_status.started_at.is_none() {
                current_status.started_at.clone()
            } else {
                new_status.started_at.clone()
            },
            finished_at: if new_status.finished_at.is_none() {
                current_status.finished_at.clone()
            } else {
                new_status.finished_at.clone()
            },
            message: if new_status.message.is_none() {
                current_status.message.clone()
            } else {
                new_status.message.clone()
            },
        })
    } else {
        new_status
    }
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

/// Initialize all comments with a status message if no status available yet.
///
/// # Arguments
/// * `client` - Kubernetes client
/// * `commands` - Available commands
///
pub async fn init_commands(client: &Client, commands: &mut Vec<CommandType>) -> OperatorResult<()> {
    for command in commands {
        // patch empty status first
        if command.get_status().is_none() {
            command
                .write_status(
                    client,
                    Some(CommandStatus {
                        started_at: None,
                        finished_at: None,
                        message: Some("Enqueued".to_string()),
                    }),
                )
                .await?
        }
    }
    Ok(())
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
    status: &Option<CommandStatus>,
) -> OperatorResult<()>
where
    T: Clone + DeserializeOwned + Meta + Send + Sync,
{
    client
        .merge_patch_status(resource, &serde_json::json!(status))
        .await?;

    Ok(())
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
