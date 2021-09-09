use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{ConfigMap, EnvVar, Pod};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::api::{ListParams, ResourceExt};
use kube::Api;
use product_config::types::PropertyNameKind;
use product_config::ProductConfigManager;
use serde_json::json;
use stackable_operator::builder::{
    ContainerBuilder, ContainerPortBuilder, ObjectMetaBuilder, PodBuilder,
};
use stackable_operator::client::Client;
use stackable_operator::conditions::ConditionStatus;
use stackable_operator::controller::{Controller, ControllerStrategy, ReconciliationState};
use stackable_operator::error::OperatorResult;
use stackable_operator::labels::{
    build_common_labels_for_all_managed_resources, get_recommended_labels, APP_COMPONENT_LABEL,
    APP_INSTANCE_LABEL, APP_MANAGED_BY_LABEL, APP_NAME_LABEL, APP_ROLE_GROUP_LABEL,
    APP_VERSION_LABEL,
};
use stackable_operator::product_config_utils::{
    config_for_role_and_group, ValidatedRoleConfigByPropertyKind,
};
use stackable_operator::reconcile::{
    ContinuationStrategy, ReconcileFunctionAction, ReconcileResult, ReconciliationContext,
};
use stackable_operator::role_utils::{
    find_nodes_that_fit_selectors, get_role_and_group_labels,
    list_eligible_nodes_for_role_and_group, EligibleNodesForRoleAndGroup,
};
use stackable_operator::{configmap, k8s_utils, name_utils};
use strum::IntoEnumIterator;
use tracing::{debug, info, trace, warn};

use stackable_spark_common::constants::{
    SPARK_DEFAULTS_CONF, SPARK_ENV_SH, SPARK_METRICS_PROPERTIES,
};
use stackable_spark_crd::commands::{Restart, Start, Stop};
use stackable_spark_crd::{
    ClusterExecutionStatus, CurrentCommand, SparkCluster, SparkClusterStatus, SparkRole,
    SparkVersion,
};

use crate::config::validated_product_config;
use crate::pod_utils::{
    filter_pods_for_type, get_hashed_master_urls, APP_NAME, MANAGED_BY, MASTER_URLS_HASH_LABEL,
};

mod command_utils;
pub mod config;
mod error;
pub mod pod_utils;

const FINALIZER_NAME: &str = "spark.stackable.tech/cleanup";
const SHOULD_BE_SCRAPED: &str = "monitoring.stackable.tech/should_be_scraped";

const CONFIG_MAP_TYPE_CONF: &str = "config";

type SparkReconcileResult = ReconcileResult<error::Error>;

struct SparkState {
    context: ReconciliationContext<SparkCluster>,
    existing_pods: Vec<Pod>,
    eligible_nodes: EligibleNodesForRoleAndGroup,
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
                        reason,
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
    pub fn required_pod_labels(&self) -> BTreeMap<String, Option<Vec<String>>> {
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
        mandatory_labels.insert(
            String::from(APP_NAME_LABEL),
            Some(vec![String::from(APP_NAME)]),
        );
        mandatory_labels.insert(
            String::from(APP_MANAGED_BY_LABEL),
            Some(vec![String::from(MANAGED_BY)]),
        );

        mandatory_labels
    }

    async fn delete_all_pods(&self) -> OperatorResult<ReconcileFunctionAction> {
        for pod in &self.existing_pods {
            self.context.client.delete(pod).await?;
        }
        Ok(ReconcileFunctionAction::Done)
    }

    async fn create_missing_pods(&mut self) -> SparkReconcileResult {
        trace!("Starting `create_missing_pods`");

        let mut changes_applied = false;
        // The iteration happens in two stages here, to accommodate the way our operators think
        // about roles and role groups.
        // The hierarchy is:
        // - Roles (Master, Worker, History-Server)
        //   - Role groups (user defined)
        for role in SparkRole::iter() {
            let role_str = &role.to_string();
            if let Some(nodes_for_role) = self.eligible_nodes.get(role_str) {
                for (role_group, (nodes, replicas)) in nodes_for_role {
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
                        *replicas,
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
                        let validated_config = config_for_role_and_group(
                            role_str,
                            role_group,
                            &self.validated_role_config,
                        )?;

                        let config_maps = self
                            .create_config_maps(role_str, role_group, validated_config)
                            .await?;

                        self.create_pod(
                            &role,
                            role_group,
                            node_name,
                            &config_maps,
                            validated_config,
                        )
                        .await?;

                        changes_applied = true;
                    }

                    // we do this here to make sure pods of each role are started after each other
                    // roles start in rolling fashion
                    // pods of each role group start in parallel
                    // master > worker > history server
                    if changes_applied {
                        return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
                    }
                }
            }
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    /// Creates the config maps required for a spark instance (or role, role_group combination):
    /// * 'spark-defaults.conf'
    /// * 'spark-env.sh'
    /// * 'metrics.properties'
    ///
    /// Labels are automatically adapted from the `recommended_labels` with a type (config for
    /// 'prometheus.yaml'). Names are generated via `name_utils::build_resource_name`.
    ///
    /// Returns a map with a 'type' identifier (e.g. config) as key and the corresponding
    /// ConfigMap as value. This is required to set the volume mounts in the pod later on.
    ///
    /// # Arguments
    ///
    /// - `role` - The spark role.
    /// - `group` - The role group.
    /// - `validated_config` - The validated product config.
    ///
    async fn create_config_maps(
        &self,
        role: &str,
        group: &str,
        validated_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    ) -> Result<HashMap<&'static str, ConfigMap>, error::Error> {
        let mut config_maps = HashMap::new();
        let mut cm_conf_data = BTreeMap::new();

        let mut cm_labels = get_recommended_labels(
            &self.context.resource,
            APP_NAME,
            &self.context.resource.spec.version.to_string(),
            role,
            group,
        );

        cm_labels.insert(
            configmap::CONFIGMAP_TYPE_LABEL.to_string(),
            CONFIG_MAP_TYPE_CONF.to_string(),
        );

        if let Some(config) =
            validated_config.get(&PropertyNameKind::File(SPARK_DEFAULTS_CONF.to_string()))
        {
            let config_as_string = config::convert_map_to_string(config, " ");
            cm_conf_data.insert(SPARK_DEFAULTS_CONF.to_string(), config_as_string);
        }

        if let Some(config) =
            validated_config.get(&PropertyNameKind::File(SPARK_ENV_SH.to_string()))
        {
            let config_as_string = config::convert_map_to_string(config, "=");
            cm_conf_data.insert(SPARK_ENV_SH.to_string(), config_as_string);
        }

        if validated_config
            .get(&PropertyNameKind::File(
                SPARK_METRICS_PROPERTIES.to_string(),
            ))
            .is_some()
        {
            // We need to enable the PrometheusServlet, edit the metrics path to
            // "/metrics" and add the JVM Source for JVM metrics
            if self.context.resource.spec.monitoring_enabled() {
                // TODO: we could specify the role here instead of using the wildcard '*'
                let metrics_properties = "\
                            *.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet\n\
                            *.sink.prometheusServlet.path=/metrics\n\
                            *.source.jvm.class=org.apache.spark.metrics.source.JvmSource";

                cm_conf_data.insert(
                    SPARK_METRICS_PROPERTIES.to_string(),
                    metrics_properties.to_string(),
                );
            }
        }

        let cm_conf_name = name_utils::build_resource_name(
            APP_NAME,
            &self.context.name(),
            role,
            Some(group),
            None,
            Some(CONFIG_MAP_TYPE_CONF),
        )?;

        let cm_config = configmap::build_config_map(
            &self.context.resource,
            &cm_conf_name,
            &self.context.namespace(),
            cm_labels,
            cm_conf_data,
        )?;

        config_maps.insert(
            CONFIG_MAP_TYPE_CONF,
            configmap::create_config_map(&self.context.client, cm_config).await?,
        );

        Ok(config_maps)
    }

    /// Creates the pod required for the spark instance.
    ///
    /// # Arguments
    ///
    /// - `role` - spark role.
    /// - `group` - The role group.
    /// - `node_name` - The node name for this pod.
    /// - `config_maps` - The config maps and respective types required for this pod.
    /// - `validated_config` - The validated product config.
    ///
    async fn create_pod(
        &self,
        role: &SparkRole,
        group: &str,
        node_name: &str,
        config_maps: &HashMap<&'static str, ConfigMap>,
        validated_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    ) -> Result<Pod, error::Error> {
        let mut env_vars = vec![];
        let mut cli_arguments = vec![];
        let mut container_ports = (None, None);
        let role_str = &role.to_string();

        // extract container ports
        if let Some(config) =
            validated_config.get(&PropertyNameKind::File(SPARK_ENV_SH.to_string()))
        {
            container_ports = role.container_ports(config);
        }

        // extract env variables
        if let Some(config) = validated_config.get(&PropertyNameKind::Env) {
            for (property_name, property_value) in config {
                if property_name.is_empty() {
                    warn!("Received empty property_name for ENV... skipping");
                    continue;
                }

                env_vars.push(EnvVar {
                    name: property_name.clone(),
                    value: Some(property_value.to_string()),
                    value_from: None,
                });
            }
        }

        // extract cli
        if let Some(config) = validated_config.get(&PropertyNameKind::Cli) {
            // we need to convert to <String, String> to <String, Option<String>> to deal with
            // CLI flags etc. We can not currently represent that via operator-rs / product-config.
            // This is a preparation for that.
            let transformed_config: BTreeMap<String, Option<String>> = config
                .iter()
                .map(|(k, v)| (k.to_string(), Some(v.to_string())))
                .collect();

            for (property_name, property_value) in transformed_config {
                if property_name.is_empty() {
                    warn!("Received empty property_name for CLI... skipping");
                    continue;
                }
                // single argument / flag
                if property_value.is_none() {
                    cli_arguments.push(property_name.clone());
                } else {
                    // key value pair (safe unwrap)
                    cli_arguments.push(format!("--{} {}", property_name, property_value.unwrap()));
                }
            }
        }

        let pod_name = name_utils::build_resource_name(
            APP_NAME,
            &self.context.name(),
            role_str,
            Some(group),
            Some(node_name),
            None,
        )?;

        let version = &self.context.resource.spec.version;
        let mut recommended_labels = get_recommended_labels(
            &self.context.resource,
            APP_NAME,
            &version.to_string(),
            role_str,
            group,
        );

        let master_pods = filter_pods_for_type(&self.existing_pods, &SparkRole::Master);
        let master_urls = &config::get_master_urls(&master_pods, &self.validated_role_config)?;

        let mut args = vec![];
        // add hashed master url label to workers and adapt start command and
        // adapt worker command with master url(s)
        if role == &SparkRole::Worker {
            recommended_labels.insert(
                MASTER_URLS_HASH_LABEL.to_string(),
                get_hashed_master_urls(master_urls),
            );

            if let Some(master_urls) = config::adapt_worker_command(role, master_urls) {
                args.push(master_urls);
            }
        }

        // add cli arguments from product config / user config
        args.append(&mut cli_arguments);

        let mut cb = ContainerBuilder::new("spark");
        cb.image(format!("spark:{}", version.to_string()));
        cb.command(vec![role.get_command(version)]);
        cb.args(args);
        cb.add_env_vars(env_vars);

        if let Some(config_map_data) = config_maps.get(CONFIG_MAP_TYPE_CONF) {
            if let Some(name) = config_map_data.metadata.name.as_ref() {
                cb.add_configmapvolume(name, "conf".to_string());
            } else {
                return Err(error::Error::MissingConfigMapNameError {
                    cm_type: CONFIG_MAP_TYPE_CONF,
                });
            }
        } else {
            return Err(error::Error::MissingConfigMapError {
                cm_type: CONFIG_MAP_TYPE_CONF,
                pod_name,
            });
        }

        let mut annotations = BTreeMap::new();
        let (protocol_port, http_port) = container_ports;
        // only add metrics container port and annotation if required
        if let (Some(metrics_port), true) =
            (http_port, self.context.resource.spec.monitoring_enabled())
        {
            annotations.insert(SHOULD_BE_SCRAPED.to_string(), "true".to_string());
            cb.add_container_port(
                ContainerPortBuilder::new(metrics_port.parse()?)
                    .name("metrics")
                    .build(),
            );
        }
        // add protocol port if available
        if let Some(protocol_port) = protocol_port {
            cb.add_container_port(
                ContainerPortBuilder::new(protocol_port.parse()?)
                    .name("protocol")
                    .build(),
            );
        }
        // add web ui port if available
        if let Some(web_ui_port) = http_port {
            cb.add_container_port(
                ContainerPortBuilder::new(web_ui_port.parse()?)
                    .name("http")
                    .build(),
            );
        }

        let pod = PodBuilder::new()
            .metadata(
                ObjectMetaBuilder::new()
                    .generate_name(pod_name)
                    .namespace(&self.context.client.default_namespace)
                    .with_labels(recommended_labels)
                    .with_annotations(annotations)
                    .ownerreference_from_resource(&self.context.resource, Some(true), Some(true))?
                    .build()?,
            )
            .add_stackable_agent_tolerations()
            .add_container(cb.build())
            .node_name(node_name)
            .build()?;

        Ok(self.context.client.create(&pod).await?)
    }

    /// In spark stand alone, workers are started via script and require the master urls to connect to.
    /// If masters change (added/deleted), workers need to be updated accordingly to be able
    /// to fall back on other masters, if the primary master fails.
    /// Therefore we always need to keep the workers updated in terms of available master urls.
    /// Available master urls are hashed and stored as label in the worker pod. If the label differs
    /// from the spec, we need to replace (delete and create) the workers in a rolling fashion.
    pub async fn check_worker_master_urls(&self) -> SparkReconcileResult {
        let worker_pods = filter_pods_for_type(&self.existing_pods, &SparkRole::Worker);
        let master_pods = filter_pods_for_type(&self.existing_pods, &SparkRole::Master);

        let current_hashed_master_urls = pod_utils::get_hashed_master_urls(
            &config::get_master_urls(&master_pods, &self.validated_role_config)?,
        );

        for pod in &worker_pods {
            if let (Some(label_hashed_master_urls), Some(role), Some(group)) = (
                pod.metadata.labels.get(pod_utils::MASTER_URLS_HASH_LABEL),
                pod.metadata.labels.get(APP_COMPONENT_LABEL),
                pod.metadata.labels.get(APP_ROLE_GROUP_LABEL),
            ) {
                if label_hashed_master_urls != &current_hashed_master_urls {
                    debug!(
                        "Pod [{}] with Role [{}] and Group [{}] has an outdated '{}' [{}] - required is [{}], deleting it",
                        &pod.name(),
                        role,
                        group,
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
                self.context.resource.status =
                    self.set_current_version(Some(target_version)).await?.status;
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
        debug!("Deletion Labels: [{:?}]", &self.required_pod_labels());

        Box::pin(async move {
            self.init_status()
                .await?
                .then(self.context.handle_deletion(
                    Box::pin(self.delete_all_pods()),
                    FINALIZER_NAME,
                    true,
                ))
                .await?
                .then(self.context.delete_illegal_pods(
                    self.existing_pods.as_slice(),
                    &self.required_pod_labels(),
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
        let existing_pods = context
            .list_owned(build_common_labels_for_all_managed_resources(
                APP_NAME,
                &context.resource.name(),
            ))
            .await?;
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

        Ok(SparkState {
            validated_role_config: validated_product_config(&context.resource, &self.config)?,
            context,
            existing_pods,
            eligible_nodes,
        })
    }
}

/// This creates an instance of a [`Controller`] which waits for incoming events and reconciles them.
///
/// This is an async method and the returned future needs to be consumed to make progress.
pub async fn create_controller(client: Client, product_config_path: &str) -> OperatorResult<()> {
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

    let product_config = ProductConfigManager::from_yaml_file(product_config_path).unwrap();

    let strategy = SparkStrategy::new(product_config);

    controller
        .run(client, strategy, Duration::from_secs(10))
        .await;

    Ok(())
}
