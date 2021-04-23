mod command_utils;
mod config;
mod error;
pub mod pod_utils;

use crate::error::Error;

use crate::config::{create_config_map_name, create_config_map_with_data};
use crate::pod_utils::{filter_pods_for_type, get_master_urls};
use async_trait::async_trait;
use k8s_openapi::api::core::v1::{ConfigMap, Node, Pod};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::api::ListParams;
use kube::Api;
use kube::Resource;
use serde_json::json;
use stackable_operator::client::Client;
use stackable_operator::conditions::ConditionStatus;
use stackable_operator::controller::{Controller, ControllerStrategy, ReconciliationState};
use stackable_operator::error::OperatorResult;
use stackable_operator::k8s_utils;
use stackable_operator::k8s_utils::LabelOptionalValueMap;
use stackable_operator::labels::{
    APP_COMPONENT_LABEL, APP_INSTANCE_LABEL, APP_ROLE_GROUP_LABEL, APP_VERSION_LABEL,
};
use stackable_operator::reconcile::{
    ContinuationStrategy, ReconcileFunctionAction, ReconcileResult, ReconciliationContext,
};
use stackable_operator::role_utils;
use stackable_operator::role_utils::RoleGroup;
use stackable_spark_crd::commands::{Restart, Start, Stop};
use stackable_spark_crd::{
    ClusterStatus, Config, CurrentCommand, NodeGroup, SparkCluster, SparkClusterStatus,
    SparkNodeType, SparkVersion,
};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use strum::IntoEnumIterator;
use tracing::{debug, info, trace, warn};

type SparkReconcileResult = ReconcileResult<error::Error>;

struct SparkState {
    context: ReconciliationContext<SparkCluster>,
    existing_pods: Vec<Pod>,
    eligible_nodes: HashMap<SparkNodeType, HashMap<String, Vec<Node>>>,
}

impl SparkState {
    async fn set_installing_condition(
        &self,
        conditions: &[Condition],
        message: &str,
        reason: &str,
        status: ConditionStatus,
    ) -> OperatorResult<SparkCluster> {
        let resource = self
            .context
            .build_and_set_condition(
                Some(conditions),
                message.to_string(),
                reason.to_string(),
                status,
                "Installing".to_string(),
            )
            .await?;

        Ok(resource)
    }

    async fn set_current_version(
        &self,
        version: Option<&SparkVersion>,
    ) -> OperatorResult<SparkCluster> {
        let resource = self
            .context
            .client
            .merge_patch_status(
                &self.context.resource,
                &json!({ "currentVersion": version }),
            )
            .await?;

        Ok(resource)
    }

    async fn set_target_version(
        &self,
        version: Option<&SparkVersion>,
    ) -> OperatorResult<SparkCluster> {
        let resource = self
            .context
            .client
            .merge_patch_status(&self.context.resource, &json!({ "targetVersion": version }))
            .await?;

        Ok(resource)
    }

    /// Will initialize the status object if it's never been set.
    pub async fn init_status(&mut self) -> SparkReconcileResult {
        // We'll begin by setting an empty status here because later in this method we might
        // update its conditions. To avoid any issues we'll just create it once here.
        if self.context.resource.status.is_none() {
            let status = SparkClusterStatus::default();
            self.context
                .client
                .merge_patch_status(&self.context.resource, &status)
                .await?;
            self.context.resource.status = Some(status);
        }

        // This should always return either the existing one or the one we just created above.
        let status = self.context.resource.status.take().unwrap_or_default();
        let spec_version = self.context.resource.spec.version.clone();

        match (&status.current_version, &status.target_version) {
            (None, None) => {
                // No current_version and no target_version: Must be initial installation.
                // We'll set the Upgrading condition and the target_version to the version from spec.
                info!(
                    "Initial installation, now moving towards version [{}]",
                    spec_version
                );
                self.context.resource.status = self
                    .set_installing_condition(
                        &status.conditions,
                        &format!("Initial installation to version [{:?}]", spec_version),
                        "InitialInstallation",
                        ConditionStatus::True,
                    )
                    .await?
                    .status;
                self.context.resource.status =
                    self.set_target_version(Some(&spec_version)).await?.status;
            }
            (None, Some(target_version)) => {
                // No current_version but a target_version means we're still doing the initial
                // installation. Will continue working towards that goal even if another version
                // was set in the meantime.
                debug!(
                    "Initial installation, still moving towards version [{}]",
                    target_version
                );
                if &spec_version != target_version {
                    info!("A new target version ([{}]) was requested while we still do the initial installation to [{}], finishing running upgrade first", spec_version, target_version)
                }
                // We do this here to update the observedGeneration if needed
                self.context.resource.status = self
                    .set_installing_condition(
                        &status.conditions,
                        &format!("Initial installation to version [{:?}]", spec_version),
                        "InitialInstallation",
                        ConditionStatus::True,
                    )
                    .await?
                    .status;
            }
            (Some(current_version), None) => {
                // We are at a stable version but have no target_version set. This will be the normal state.
                // We'll check if there is a different version in spec and if it is will set it in target_version.
                // TODO: check valid up/downgrades
                let message;
                let reason;
                if current_version.is_upgrade(&spec_version)? {
                    message = format!(
                        "Upgrading from [{:?}] to [{:?}]",
                        current_version, &spec_version
                    );
                    reason = "Upgrading";
                    self.context.resource.status =
                        self.set_target_version(Some(&spec_version)).await?.status;
                } else if current_version.is_downgrade(&spec_version)? {
                    message = format!(
                        "Downgrading from [{:?}] to [{:?}]",
                        current_version, &spec_version
                    );
                    reason = "Downgrading";
                    self.context.resource.status =
                        self.set_target_version(Some(&spec_version)).await?.status;
                } else {
                    message = format!(
                        "No upgrade/downgrade required [{:?}] is still the current_version",
                        current_version
                    );
                    reason = "";
                }

                trace!("{}", message);

                self.context.resource.status = self
                    .set_installing_condition(
                        &status.conditions,
                        &message,
                        &reason,
                        ConditionStatus::True,
                    )
                    .await?
                    .status;
            }
            (Some(current_version), Some(target_version)) => {
                // current_version and target_version are set means we're still in the process
                // of upgrading. We'll only do some logging and checks and will update
                // the condition so observedGeneration can be updated.
                debug!(
                    "Still changing version from [{}] to [{}]",
                    current_version, target_version
                );
                if &self.context.resource.spec.version != target_version {
                    info!("A new target version was requested while we still upgrade from [{}] to [{}], finishing running upgrade/downgrade first", current_version, target_version)
                }
                let message = format!(
                    "Changing version from [{:?}] to [{:?}]",
                    current_version, target_version
                );

                self.context.resource.status = self
                    .set_installing_condition(
                        &status.conditions,
                        &message,
                        "",
                        ConditionStatus::False,
                    )
                    .await?
                    .status;
            }
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    /// Process available / running commands. If current_command in the status is set, we have
    /// a running command. If it is not set, but commands are available, start the oldest command.
    /// If no command is running, no command is waiting and the cluster_status field is "Stopped",
    /// abort the reconcile action.
    pub async fn process_commands(&mut self) -> SparkReconcileResult {
        if let Some(status) = self.context.resource.status.clone() {
            // if a current_command is available we are currently processing that command
            if let Some(current_command) = &status.current_command {
                let running_command = command_utils::get_command_from_ref(
                    &self.context.client,
                    &current_command.command_type,
                    &current_command.command_ref,
                    self.context.resource.namespace().as_deref(),
                )
                .await?;

                return Ok(running_command
                    .process_command_running(
                        &self.context.client,
                        &mut self.context.resource,
                        current_command,
                    )
                    .await?);
            // if no current commands are running, check if any commands are available
            } else if let Some(next_command) =
                command_utils::get_next_command(&self.context.client).await?
            {
                let current_command = CurrentCommand {
                    command_ref: next_command.get_name(),
                    command_type: next_command.get_type(),
                    started_at: command_utils::get_current_timestamp(),
                };

                return Ok(next_command
                    .process_command_execute(
                        &self.context.client,
                        &self.context.resource,
                        &current_command,
                        &self.existing_pods,
                    )
                    .await?);

            // if no next command check if cluster is stopped
            } else if Some(ClusterStatus::Stopped) == status.cluster_status {
                info!("Cluster is stopped. Waiting for next command!");
                return Ok(ReconcileFunctionAction::Done);
            }
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    pub fn get_full_pod_node_map(&self) -> Vec<(Vec<Node>, LabelOptionalValueMap)> {
        let mut eligible_nodes_map = vec![];

        for node_type in SparkNodeType::iter() {
            if let Some(eligible_nodes_for_role) = self.eligible_nodes.get(&node_type) {
                for (group_name, eligible_nodes) in eligible_nodes_for_role {
                    // Create labels to identify eligible nodes
                    trace!(
                        "Adding [{}] nodes to eligible node list for role [{}] and group [{}].",
                        eligible_nodes.len(),
                        node_type,
                        group_name
                    );
                    eligible_nodes_map.push((
                        eligible_nodes.clone(),
                        get_node_and_group_labels(group_name, &node_type),
                    ))
                }
            }
        }
        eligible_nodes_map
    }

    /// Required labels for pods. Pods without any of these will deleted and replaced.
    pub fn get_deletion_labels(&self) -> BTreeMap<String, Option<Vec<String>>> {
        let roles = SparkNodeType::iter()
            .map(|role| role.to_string())
            .collect::<Vec<_>>();
        let mut mandatory_labels = BTreeMap::new();

        mandatory_labels.insert(String::from(APP_COMPONENT_LABEL), Some(roles));
        mandatory_labels.insert(
            String::from(APP_INSTANCE_LABEL),
            Some(vec![self.context.resource.name()]),
        );
        mandatory_labels.insert(
            String::from(APP_VERSION_LABEL),
            Some(vec![self.context.resource.spec.version.to_string()]),
        );
        mandatory_labels
    }

    async fn create_config_map<T>(&self, cm_name: &str, config: Option<T>) -> Result<(), Error>
    where
        T: Config,
    {
        match self
            .context
            .client
            .get::<ConfigMap>(cm_name, Some(&self.context.namespace()))
            .await
        {
            // TODO: check and compare content
            Ok(_) => {
                debug!("ConfigMap [{}] already exists, skipping creation!", cm_name);
                return Ok(());
            }
            Err(e) => {
                // TODO: This is shit, but works for now. If there is an actual error in comms with
                //   K8S, it will most probably also occur further down and be properly handled
                debug!("Error getting ConfigMap [{}]: [{:?}]", cm_name, e);
            }
        }

        let config_map = create_config_map_with_data(&self.context.resource, config, cm_name)?;

        self.context.client.create(&config_map).await?;
        Ok(())
    }

    async fn create_missing_pods(&mut self, node_type: &SparkNodeType) -> SparkReconcileResult {
        let mut changes_applied = false;
        // The iteration happens in two stages here, to accommodate the way our operators think
        // about nodes and roles.
        // The hierarchy is:
        // - Roles (for example master, worker, history-server)
        //   - Node groups for this role (user defined)
        //      - Individual nodes
        if let Some(nodes_for_role) = self.eligible_nodes.get(&node_type) {
            for (role_group, nodes) in nodes_for_role {
                // Create config map for this role group
                let pod_name = pod_utils::create_pod_name(
                    &self.context.name(),
                    role_group,
                    &node_type.to_string(),
                );

                let cm_name = create_config_map_name(&pod_name);
                debug!("pod_name: [{}], cm_name: [{}]", pod_name, cm_name);

                // extract config
                let config = self.context.resource.spec.get_config(node_type, role_group);
                self.create_config_map(&cm_name, config).await?;

                debug!(
                    "Identify missing pods for [{}] role and group [{}]",
                    node_type, role_group
                );
                trace!(
                    "candidate_nodes[{}]: [{:?}]",
                    nodes.len(),
                    nodes
                        .iter()
                        .map(|node| node.metadata.name.as_ref().unwrap())
                        .collect::<Vec<_>>()
                );
                trace!(
                    "existing_pods[{}]: [{:?}]",
                    &self.existing_pods.len(),
                    &self
                        .existing_pods
                        .iter()
                        .map(|pod| pod.metadata.name.as_ref().unwrap())
                        .collect::<Vec<_>>()
                );
                trace!(
                    "labels: [{:?}]",
                    get_node_and_group_labels(role_group, &node_type)
                );
                let nodes_that_need_pods = k8s_utils::find_nodes_that_need_pods(
                    nodes,
                    &self.existing_pods,
                    &get_node_and_group_labels(role_group, &node_type),
                );

                for node in nodes_that_need_pods {
                    let node_name = if let Some(node_name) = &node.metadata.name {
                        node_name
                    } else {
                        warn!("No name found in metadata, this should not happen! Skipping node: [{:?}]", node);
                        continue;
                    };
                    debug!(
                        "Creating pod on node [{}] for [{}] role and group [{}]",
                        node.metadata
                            .name
                            .as_deref()
                            .unwrap_or("<no node name found>"),
                        node_type,
                        role_group
                    );

                    let master_pods =
                        filter_pods_for_type(&self.existing_pods, &SparkNodeType::Master);

                    let master_urls =
                        get_master_urls(master_pods.as_slice(), &self.context.resource.spec);

                    debug!("Found master urls: {:?}", master_urls);

                    let pod = pod_utils::build_pod(
                        &self.context.resource,
                        &node_name,
                        role_group,
                        &node_type,
                        &master_urls,
                    )?;

                    self.context.client.create(&pod).await?;
                    changes_applied = true;
                }
            }
            // we do this here to make sure pods of each role are started after each other
            // roles start in rolling fashion
            // pods of each role start non rolling
            // master > worker > history server
            if changes_applied {
                return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
            }
        }
        Ok(ReconcileFunctionAction::Continue)
    }

    /// In spark stand alone, workers are started via script and require the master urls to connect to.
    /// If masters change (added/deleted), workers need to be updated accordingly to be able
    /// to fall back on other masters, if the primary master fails.
    /// Therefore we always need to keep the workers updated in terms of available master urls.
    /// Available master urls are hashed and stored as label in the worker pod. If the label differs
    /// from the spec, we need to replace (delete and and) the workers in a rolling fashion.
    pub async fn check_worker_master_urls(&self) -> SparkReconcileResult {
        let worker_pods = filter_pods_for_type(&self.existing_pods, &SparkNodeType::Worker);
        let master_pods = filter_pods_for_type(&self.existing_pods, &SparkNodeType::Master);

        for pod in &worker_pods {
            if let Some(labels) = &pod.metadata.labels {
                if let Some(label_hashed_master_urls) =
                    labels.get(pod_utils::MASTER_URLS_HASH_LABEL)
                {
                    let current_hashed_master_urls = pod_utils::get_hashed_master_urls(
                        &get_master_urls(&master_pods, &self.context.resource.spec),
                    );
                    if label_hashed_master_urls != &current_hashed_master_urls {
                        debug!(
                            "Pod [{}] has an outdated '{}' [{}] - required is [{}], deleting it",
                            &pod.name(),
                            pod_utils::MASTER_URLS_HASH_LABEL,
                            label_hashed_master_urls,
                            current_hashed_master_urls,
                        );
                        self.context.client.delete(pod).await?;
                        return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
                    }
                }
            }
        }
        Ok(ReconcileFunctionAction::Continue)
    }

    /// After pod reconcile, if a command has startedAt but no finishedAt timestamp, set finishedAt
    /// timestamp and finalize command.
    pub async fn finalize_commands(&mut self) -> SparkReconcileResult {
        if let Some(status) = &self.context.resource.status {
            // if a current_command is available we are currently
            // processing that command and are finished here.
            if let Some(current_command) = &status.current_command {
                let current_command = command_utils::get_command_from_ref(
                    &self.context.client,
                    &current_command.command_type,
                    &current_command.command_ref,
                    self.context.resource.namespace().as_deref(),
                )
                .await?;

                return Ok(current_command
                    .process_command_finalize(&self.context.client, &mut self.context.resource)
                    .await?);
            }
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    /// Process the cluster status version for upgrades/downgrades.
    /// If we reach here it means all pods must be running on target_version.
    /// We can now set current_version to target_version (if target_version was set),
    /// and target_version to None.
    pub async fn process_version(&mut self) -> SparkReconcileResult {
        if let Some(status) = &self.context.resource.status.clone() {
            if let Some(target_version) = &status.target_version {
                info!(
                    "Finished upgrade/downgrade to [{}]. Cluster ready!",
                    &target_version
                );

                self.context.resource.status = self.set_target_version(None).await?.status;
                self.context.resource.status = self
                    .set_current_version(Some(&target_version))
                    .await?
                    .status;
                self.context.resource.status = self
                    .set_installing_condition(
                        &status.conditions,
                        &format!(
                            "No change required [{:?}] is still the current_version",
                            target_version
                        ),
                        "",
                        ConditionStatus::False,
                    )
                    .await?
                    .status;
            }
        }

        Ok(ReconcileFunctionAction::Continue)
    }
}

impl ReconciliationState for SparkState {
    type Error = error::Error;

    fn reconcile(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<ReconcileFunctionAction, Self::Error>> + Send + '_>>
    {
        info!("========================= Starting reconciliation =========================");
        debug!("Deletion Labels: [{:?}]", &self.get_deletion_labels());

        Box::pin(async move {
            self.init_status()
                .await?
                .then(self.context.delete_illegal_pods(
                    self.existing_pods.as_slice(),
                    &self.get_deletion_labels(),
                    ContinuationStrategy::OneRequeue,
                ))
                .await?
                .then(
                    self.context
                        .wait_for_terminating_pods(self.existing_pods.as_slice()),
                )
                .await?
                .then(
                    self.context
                        .wait_for_running_and_ready_pods(&self.existing_pods),
                )
                .await?
                .then(self.process_commands())
                .await?
                .then(self.context.delete_excess_pods(
                    self.get_full_pod_node_map().as_slice(),
                    &self.existing_pods,
                    ContinuationStrategy::OneRequeue,
                ))
                .await?
                .then(self.create_missing_pods(&SparkNodeType::Master))
                .await?
                .then(self.create_missing_pods(&SparkNodeType::Worker))
                .await?
                .then(self.create_missing_pods(&SparkNodeType::HistoryServer))
                .await?
                .then(self.check_worker_master_urls())
                .await?
                .then(self.finalize_commands())
                .await?
                .then(self.process_version())
                .await
        })
    }
}

#[derive(Debug)]
struct SparkStrategy {}

impl SparkStrategy {
    pub fn new() -> SparkStrategy {
        SparkStrategy {}
    }
}

#[async_trait]
impl ControllerStrategy for SparkStrategy {
    type Item = SparkCluster;
    type State = SparkState;
    type Error = error::Error;

    async fn init_reconcile_state(
        &self,
        context: ReconciliationContext<Self::Item>,
    ) -> Result<Self::State, Self::Error> {
        let existing_pods = context.list_pods().await?;
        trace!("Found [{}] pods", existing_pods.len());

        let cluster_spec = &context.resource.spec;

        let mut eligible_nodes = HashMap::new();

        eligible_nodes.insert(
            SparkNodeType::Master,
            get_nodes_that_fit_selectors(&context.client, &cluster_spec.masters).await?,
        );

        eligible_nodes.insert(
            SparkNodeType::Worker,
            get_nodes_that_fit_selectors(&context.client, &cluster_spec.workers).await?,
        );

        if let Some(history_servers) = &cluster_spec.history_servers {
            eligible_nodes.insert(
                SparkNodeType::HistoryServer,
                get_nodes_that_fit_selectors(&context.client, &history_servers).await?,
            );
        }

        Ok(SparkState {
            context,
            existing_pods,
            eligible_nodes,
        })
    }
}

async fn get_nodes_that_fit_selectors<T>(
    client: &Client,
    group: &NodeGroup<T>,
) -> OperatorResult<HashMap<String, Vec<Node>>> {
    let role_groups: Vec<RoleGroup> = group
        .selectors
        .iter()
        .map(|(group_name, selector_config)| RoleGroup {
            name: group_name.to_string(),
            selector: selector_config.selector.clone().unwrap(),
        })
        .collect();

    role_utils::find_nodes_that_fit_selectors(client, None, role_groups.as_slice()).await
}

fn get_node_and_group_labels(group_name: &str, node_type: &SparkNodeType) -> LabelOptionalValueMap {
    let mut node_labels = BTreeMap::new();
    node_labels.insert(
        String::from(APP_COMPONENT_LABEL),
        Some(node_type.to_string()),
    );
    node_labels.insert(
        String::from(APP_ROLE_GROUP_LABEL),
        Some(String::from(group_name)),
    );
    node_labels
}

/// This creates an instance of a [`Controller`] which waits for incoming events and reconciles them.
///
/// This is an async method and the returned future needs to be consumed to make progress.
pub async fn create_controller(client: Client) {
    let spark_api: Api<SparkCluster> = client.get_all_api();
    let pods_api: Api<Pod> = client.get_all_api();
    let config_maps_api: Api<ConfigMap> = client.get_all_api();
    let cmd_restart_api: Api<Restart> = client.get_all_api();
    let cmd_start_api: Api<Start> = client.get_all_api();
    let cmd_stop_api: Api<Stop> = client.get_all_api();

    let controller = Controller::new(spark_api)
        .owns(pods_api, ListParams::default())
        .owns(config_maps_api, ListParams::default())
        .owns(cmd_restart_api, ListParams::default())
        .owns(cmd_start_api, ListParams::default())
        .owns(cmd_stop_api, ListParams::default());

    let strategy = SparkStrategy::new();

    controller
        .run(client, strategy, Duration::from_secs(10))
        .await;
}
