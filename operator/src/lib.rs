use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{ConfigMap, EnvVar, Node, Pod};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::api::{ListParams, ResourceExt};
use kube::Api;
use product_config::types::PropertyNameKind;
use product_config::ProductConfigManager;
use serde_json::json;
use stackable_operator::client::Client;
use stackable_operator::conditions::ConditionStatus;
use stackable_operator::controller::{Controller, ControllerStrategy, ReconciliationState};
use stackable_operator::error::OperatorResult;
use stackable_operator::labels::{
    APP_COMPONENT_LABEL, APP_INSTANCE_LABEL, APP_ROLE_GROUP_LABEL, APP_VERSION_LABEL,
};
use stackable_operator::product_config_utils::{
    config_for_role_and_group, transform_all_roles_to_config, validate_all_roles_and_groups_config,
    Configuration, ValidatedRoleConfigByPropertyKind,
};
use stackable_operator::reconcile::{
    ContinuationStrategy, ReconcileFunctionAction, ReconcileResult, ReconciliationContext,
};
use stackable_operator::role_utils::{
    find_nodes_that_fit_selectors, get_role_and_group_labels,
    list_eligible_nodes_for_role_and_group, Role,
};
use stackable_operator::{config_map, k8s_utils};
use strum::IntoEnumIterator;
use tracing::{debug, info, trace, warn};

use stackable_spark_common::constants::{SPARK_DEFAULTS_CONF, SPARK_ENV_SH};
use stackable_spark_crd::commands::{Restart, Start, Stop};
use stackable_spark_crd::{
    ClusterExecutionStatus, CurrentCommand, SparkCluster, SparkClusterStatus, SparkRole,
    SparkVersion,
};

use crate::error::Error;
use crate::pod_utils::filter_pods_for_type;

mod command_utils;
mod config;
mod error;
pub mod pod_utils;

type SparkReconcileResult = ReconcileResult<error::Error>;

struct SparkState {
    context: ReconciliationContext<SparkCluster>,
    config: Arc<ProductConfigManager>,
    existing_pods: Vec<Pod>,
    eligible_nodes: HashMap<String, HashMap<String, Vec<Node>>>,
    validated_role_config: ValidatedRoleConfigByPropertyKind,
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
                        ConditionStatus::False,
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
                        ConditionStatus::True,
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
            } else if Some(ClusterExecutionStatus::Stopped) == status.cluster_execution_status {
                info!("Cluster is stopped. Waiting for next command!");
                return Ok(ReconcileFunctionAction::Done);
            }
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    /// Required labels for pods. Pods without any of these will deleted and replaced.
    pub fn get_deletion_labels(&self) -> BTreeMap<String, Option<Vec<String>>> {
        let roles = SparkRole::iter()
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

    /// Create or update a config map.
    /// - Create if no config map of that name exists
    /// - Update if config map exists but the content differs
    /// - Do nothing if the config map exists and the content is identical
    async fn create_config_map<T>(&self, cm_name: &str, config: Option<T>) -> Result<(), Error> {
        // let config_map = create_config_map_with_data(&self.context.resource, config, cm_name)?;
        //
        // match self
        //     .context
        //     .client
        //     .get::<ConfigMap>(cm_name, Some(&self.context.namespace()))
        //     .await
        // {
        //     Ok(ConfigMap {
        //         data: existing_config_map_data,
        //         ..
        //     }) if existing_config_map_data == config_map.data => {
        //         debug!(
        //             "ConfigMap [{}] already exists with identical data, skipping creation!",
        //             cm_name
        //         );
        //     }
        //     Ok(_) => {
        //         debug!(
        //             "ConfigMap [{}] already exists, but differs, updating it!",
        //             cm_name
        //         );
        //         self.context.client.update(&config_map).await?;
        //     }
        //     Err(e) => {
        //         // TODO: This is shit, but works for now. If there is an actual error in comes with
        //         //   K8S, it will most probably also occur further down and be properly handled
        //         debug!("Error getting ConfigMap [{}]: [{:?}]", cm_name, e);
        //         self.context.client.create(&config_map).await?;
        //     }
        // }

        Ok(())
    }

    async fn create_missing_pods(&mut self) -> SparkReconcileResult {
        trace!("Starting `create_missing_pods`");

        let mut changes_applied = false;
        // The iteration happens in two stages here, to accommodate the way our operators think
        // about roles and role groups.
        // The hierarchy is:
        // - Roles
        //   - Role groups for this role (user defined)
        for role in SparkRole::iter() {
            let role_str = &role.to_string();
            if let Some(nodes_for_role) = self.eligible_nodes.get(role_str) {
                for (role_group, nodes) in nodes_for_role {
                    debug!(
                        "Identify missing pods for [{}] role and group [{}]",
                        role_str, role_group
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
                        get_role_and_group_labels(role_str, role_group)
                    );
                    let nodes_that_need_pods = k8s_utils::find_nodes_that_need_pods(
                        nodes,
                        &self.existing_pods,
                        &get_role_and_group_labels(role_str, role_group),
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
                            role_str,
                            role_group
                        );

                        // now we have a node that needs pods -> get validated config
                        // TODO: unwraps cannot fail
                        let validated_config = self
                            .validated_role_config
                            .get(role_str)
                            .unwrap()
                            .get(role_group)
                            .unwrap();

                        let (pod, config_maps) = self
                            .create_pod_and_config_maps(
                                &role,
                                role_group,
                                &node_name,
                                &validated_config,
                            )
                            .await?;

                        self.context.client.create(&pod).await?;

                        for config_map in &config_maps {
                            self.context.client.create(config_map).await?;
                        }

                        changes_applied = true;
                    }

                    // we do this here to make sure pods of each role are started after each other
                    // roles start in rolling fashion
                    // pods of each role start non rolling
                    // master > worker > history server
                    if changes_applied {
                        return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
                    }
                }
            }
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    async fn create_pod_and_config_maps(
        &self,
        role: &SparkRole,
        role_group: &str,
        node_name: &str,
        validated_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    ) -> Result<(Pod, Vec<ConfigMap>), Error> {
        let mut config_maps = vec![];
        let mut cli_command = vec![];
        let mut env_vars = vec![];
        let mut cm_data = BTreeMap::new();

        for (property_name_kind, config) in validated_config {
            // we need to convert to <String, String> to <String, Option<String>> to deal with
            // CLI flags etc. We can not currently represent that via operator-rs / product-config.
            // This is a preparation for that.
            let transformed_config: BTreeMap<String, Option<String>> = config
                .into_iter()
                .map(|(k, v)| (k.to_string(), Some(v.to_string())))
                .collect();

            match property_name_kind {
                PropertyNameKind::File(file_name) => match file_name.as_str() {
                    SPARK_DEFAULTS_CONF => {
                        let config_as_string = config::convert_map_to_string(config, " ");
                        cm_data.insert(SPARK_DEFAULTS_CONF.to_string(), config_as_string);
                    }
                    SPARK_ENV_SH => {
                        let config_as_string = config::convert_map_to_string(config, "=");
                        cm_data.insert(SPARK_ENV_SH.to_string(), config_as_string);
                    }
                    _ => {}
                },
                PropertyNameKind::Env => {
                    for (property_name, property_value) in transformed_config {
                        if property_name.is_empty() {
                            warn!("Received empty property_name for ENV... skipping");
                            continue;
                        }

                        env_vars.push(EnvVar {
                            name: property_name,
                            value: property_value,
                            value_from: None,
                        });
                    }
                }
                PropertyNameKind::Cli => {
                    for (property_name, property_value) in transformed_config {
                        if property_name.is_empty() {
                            warn!("Received empty property_name for CLI... skipping");
                            continue;
                        }
                        // single argument / flag
                        if property_value.is_none() {
                            cli_command.push(property_name);
                        } else {
                            // key value pair
                            cli_command.push(format!(
                                "--{} {}",
                                property_name,
                                property_value.unwrap()
                            ));
                        }
                    }
                }
            }
        }

        let pod_name = pod_utils::create_pod_name(
            &self.context.name(),
            &role.to_string(),
            role_group,
            node_name,
        );

        let cm_name = config::create_config_map_name(&pod_name);

        let master_pods = filter_pods_for_type(&self.existing_pods, &SparkRole::Master);
        let master_urls = &config::get_master_urls(&master_pods, &self.validated_role_config)?;

        // TODO: adapt master urls
        let pod = pod_utils::build_pod(
            &self.context.resource,
            &role,
            role_group,
            node_name,
            &pod_name,
            &cm_name,
            master_urls,
        )?;

        // we only need one config map in spark (spark-defaults.conf and spark-env.sh)
        config_maps.push(config_map::create_config_map(
            &self.context.resource,
            &cm_name,
            cm_data,
        )?);

        Ok((pod, config_maps))
    }

    /// In spark stand alone, workers are started via script and require the master urls to connect to.
    /// If masters change (added/deleted), workers need to be updated accordingly to be able
    /// to fall back on other masters, if the primary master fails.
    /// Therefore we always need to keep the workers updated in terms of available master urls.
    /// Available master urls are hashed and stored as label in the worker pod. If the label differs
    /// from the spec, we need to replace (delete and and) the workers in a rolling fashion.
    pub async fn check_worker_master_urls(&self) -> SparkReconcileResult {
        let worker_pods = filter_pods_for_type(&self.existing_pods, &SparkRole::Worker);
        let master_pods = filter_pods_for_type(&self.existing_pods, &SparkRole::Master);

        for pod in &worker_pods {
            if let (Some(label_hashed_master_urls), Some(role), Some(group)) = (
                pod.metadata.labels.get(pod_utils::MASTER_URLS_HASH_LABEL),
                pod.metadata.labels.get(APP_COMPONENT_LABEL),
                pod.metadata.labels.get(APP_ROLE_GROUP_LABEL),
            ) {
                let current_hashed_master_urls = pod_utils::get_hashed_master_urls(
                    &config::get_master_urls(&master_pods, &self.validated_role_config)?,
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
                    list_eligible_nodes_for_role_and_group(&self.eligible_nodes).as_slice(),
                    &self.existing_pods,
                    ContinuationStrategy::OneRequeue,
                ))
                .await?
                .then(self.create_missing_pods())
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

struct SparkStrategy {
    config: Arc<ProductConfigManager>,
}

impl SparkStrategy {
    pub fn new(config: ProductConfigManager) -> SparkStrategy {
        SparkStrategy {
            config: Arc::new(config),
        }
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
        let existing_pods = context.list_owned().await?;
        trace!(
            "{}: Found [{}] pods",
            context.log_name(),
            existing_pods.len()
        );

        let cluster_spec = &context.resource.spec;

        let mut eligible_nodes = HashMap::new();

        eligible_nodes.insert(
            SparkRole::Master.to_string(),
            find_nodes_that_fit_selectors(&context.client, None, &cluster_spec.masters).await?,
        );

        eligible_nodes.insert(
            SparkRole::Worker.to_string(),
            find_nodes_that_fit_selectors(&context.client, None, &cluster_spec.workers).await?,
        );

        if let Some(history_servers) = &cluster_spec.history_servers {
            eligible_nodes.insert(
                SparkRole::HistoryServer.to_string(),
                find_nodes_that_fit_selectors(&context.client, None, history_servers).await?,
            );
        }

        let mut roles: HashMap<
            String,
            (
                Vec<PropertyNameKind>,
                Role<Box<dyn Configuration<Configurable = SparkCluster>>>,
            ),
        > = HashMap::new();
        roles.insert(
            SparkRole::Master.to_string(),
            (
                vec![
                    PropertyNameKind::File(SPARK_ENV_SH.to_string()),
                    PropertyNameKind::File(SPARK_DEFAULTS_CONF.to_string()),
                    PropertyNameKind::Env,
                ],
                context.resource.spec.masters.clone().into_dyn(),
            ),
        );

        roles.insert(
            SparkRole::Worker.to_string(),
            (
                vec![
                    PropertyNameKind::File(SPARK_ENV_SH.to_string()),
                    PropertyNameKind::File(SPARK_DEFAULTS_CONF.to_string()),
                    PropertyNameKind::Env,
                ],
                context.resource.spec.workers.clone().into_dyn(),
            ),
        );

        if let Some(history_servers) = &context.resource.spec.history_servers {
            roles.insert(
                SparkRole::HistoryServer.to_string(),
                (
                    vec![
                        PropertyNameKind::File(SPARK_ENV_SH.to_string()),
                        PropertyNameKind::File(SPARK_DEFAULTS_CONF.to_string()),
                        PropertyNameKind::Env,
                    ],
                    history_servers.clone().into_dyn(),
                ),
            );
        }

        let role_config = transform_all_roles_to_config(&context.resource, roles);
        let validated_role_config = validate_all_roles_and_groups_config(
            &context.resource.spec.version.to_string(),
            &role_config,
            &self.config,
            false,
            false,
        )?;

        Ok(SparkState {
            context,
            config: self.config.clone(),
            existing_pods,
            eligible_nodes,
            validated_role_config,
        })
    }
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

    let product_config =
        ProductConfigManager::from_yaml_file("deploy/config-spec/properties.yaml").unwrap();
    let strategy = SparkStrategy::new(product_config);

    controller
        .run(client, strategy, Duration::from_secs(10))
        .await;
}
