use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::api::Meta;
use serde::de::DeserializeOwned;
use stackable_operator::client::Client;
use stackable_operator::error::OperatorResult;
use stackable_spark_crd::commands::CommandStatus;
use stackable_spark_crd::{Restart, Start, Stop};

#[derive(Clone, Debug)]
pub enum CommandType {
    Restart(Restart),
    Start(Start),
    Stop(Stop),
}

impl CommandType {
    pub fn get_name(&self) -> String {
        match self {
            CommandType::Restart(restart) => Meta::name(restart),
            CommandType::Start(start) => Meta::name(start),
            CommandType::Stop(stop) => Meta::name(stop),
        }
    }

    pub fn get_creation_timestamp(&self) -> Option<Time> {
        match self {
            CommandType::Restart(restart) => restart.meta().creation_timestamp.clone(),
            CommandType::Start(start) => start.meta().creation_timestamp.clone(),
            CommandType::Stop(stop) => stop.meta().creation_timestamp.clone(),
        }
    }

    pub fn get_status(&self) -> Option<CommandStatus> {
        match self {
            CommandType::Restart(restart) => restart.status.clone(),
            CommandType::Start(start) => start.status.clone(),
            CommandType::Stop(stop) => stop.status.clone(),
        }
    }

    fn set_status(&mut self, new_status: Option<CommandStatus>) {
        match self {
            CommandType::Restart(restart) => {
                restart.status = merge_status_ignore_none(restart.status.clone(), new_status)
            }
            CommandType::Start(start) => {
                start.status = merge_status_ignore_none(start.status.clone(), new_status)
            }
            CommandType::Stop(stop) => {
                stop.status = merge_status_ignore_none(stop.status.clone(), new_status)
            }
        }
    }

    pub async fn write_status(
        &mut self,
        client: &Client,
        status: Option<CommandStatus>,
    ) -> OperatorResult<()> {
        match self {
            CommandType::Restart(restart) => {
                write_status(client, restart, &status).await?;
                self.set_status(status.clone());
            }
            CommandType::Start(start) => {
                write_status(client, start, &status).await?;
                self.set_status(status.clone());
            }
            CommandType::Stop(stop) => {
                write_status(client, stop, &status).await?;
                self.set_status(status.clone());
            }
        }

        Ok(())
    }

    pub async fn run(&mut self, client: &Client, pods: &Vec<Pod>) -> OperatorResult<()> {
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
}

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

pub fn get_time() -> Option<String> {
    Some(
        chrono::Utc::now()
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
            .to_string(),
    )
}
