#![feature(backtrace)]
mod config;
mod error;

use crate::error::Error;

use kube::Api;
use tracing::{debug, error, info, trace};

use k8s_openapi::api::core::v1::{ConfigMap, Container, Pod, PodSpec, Volume};
use kube::api::{ListParams, Meta};
use serde_json::json;

use async_trait::async_trait;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use stackable_operator::client::Client;
use stackable_operator::conditions::ConditionStatus;
use stackable_operator::controller::{Controller, ControllerStrategy, ReconciliationState};
use stackable_operator::error::OperatorResult;
use stackable_operator::reconcile::{
    ReconcileFunctionAction, ReconcileResult, ReconciliationContext,
};
use stackable_operator::{create_config_map, create_tolerations};
use stackable_operator::{finalizer, metadata, podutils};
use stackable_spark_crd::{
    SparkCluster, SparkClusterSpec, SparkClusterStatus, SparkNodeSelector, SparkNodeType,
    SparkVersion,
};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::time::Duration;
use uuid::Uuid;

const FINALIZER_NAME: &str = "spark.stackable.tech/cleanup";

/// Pod label which holds the selector hash in the pods to identify which selector they belong to
const HASH_LABEL: &str = "spark.stackable.tech/hash";
/// Pod label which holds node role / type (master, worker, history-server) in the pods
const TYPE_LABEL: &str = "spark.stackable.tech/type";
/// Pod label which indicates the cluster version it was created for
const VERSION_LABEL: &str = "spark.stackable.tech/currentVersion";

type SparkReconcileResult = ReconcileResult<error::Error>;

struct SparkState {
    context: ReconciliationContext<SparkCluster>,
    spec: SparkClusterSpec,
    status: Option<SparkClusterStatus>,
    pod_information: Option<PodInformation>,
}

/// This will be filled in the beginning of the reconcile method.
/// Nodes are sorted according to their cluster role / node type and their corresponding selector hash.
/// The selector hash identifies a provided selector which shares properties for one or multiple pods.
/// In order to reconcile properly, we need the pods to identify themselves via a hash belonging to a certain selector.
/// This is currently the only way to match a pod to a certain selector and compare current vs desired state.
struct PodInformation {
    // hash for selector -> corresponding pod list
    pub master: HashMap<String, Vec<Pod>>,
    pub worker: HashMap<String, Vec<Pod>>,
    pub history_server: Option<HashMap<String, Vec<Pod>>>,
}

impl PodInformation {
    /// Count the pods according to their node type
    ///
    /// # Arguments
    ///
    /// * `node_type` - SparkNodeType (master/worker/history-server)
    ///
    pub fn get_pod_count(&self, node_type: SparkNodeType) -> usize {
        let mut num_pods = 0;
        match node_type {
            SparkNodeType::Master => num_pods = get_pods_for_node(&self.master).len(),
            SparkNodeType::Worker => num_pods = get_pods_for_node(&self.worker).len(),
            SparkNodeType::HistoryServer => {
                if self.history_server.is_some() {
                    num_pods = get_pods_for_node(self.history_server.as_ref().unwrap()).len()
                }
            }
        }
        num_pods
    }

    /// Get all pods according to their node type
    /// "None" will return every pod belonging to the cluster.
    ///
    /// # Arguments
    ///
    /// * `node_type` - Optional SparkNodeType (master/worker/history-server);
    ///
    pub fn get_all_pods(&self, node_type: Option<SparkNodeType>) -> Vec<Pod> {
        let mut pods: Vec<Pod> = vec![];

        match node_type {
            Some(node_type) => match node_type {
                SparkNodeType::Master => pods = get_pods_for_node(&self.master),
                SparkNodeType::Worker => pods = get_pods_for_node(&self.worker),
                SparkNodeType::HistoryServer => {
                    if self.history_server.is_some() {
                        pods = get_pods_for_node(self.history_server.as_ref().unwrap())
                    }
                }
            },
            None => {
                pods.append(&mut get_pods_for_node(&self.master));
                pods.append(&mut get_pods_for_node(&self.worker));
                if self.history_server.is_some() {
                    pods.append(&mut get_pods_for_node(
                        self.history_server.as_ref().unwrap(),
                    ))
                }
            }
        };

        pods
    }
}

/// Retrieve all pods from a hashed selector (like PodInformation: Master, Worker, HistoryServer)
///
/// # Arguments
///
/// * `hashed` - Map where the key is a selector hash and list of pods owned by that selector
///
fn get_pods_for_node(hashed: &HashMap<String, Vec<Pod>>) -> Vec<Pod> {
    let mut all_pods: Vec<Pod> = vec![];
    for pods in hashed.values() {
        all_pods.append(&mut pods.clone());
    }
    all_pods
}

impl SparkState {
    async fn set_upgrading_condition(
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
                "Upgrading".to_string(),
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
        if self.status.is_none() {
            let status = SparkClusterStatus::default();
            self.context
                .client
                .merge_patch_status(&self.context.resource, &status)
                .await?;
            self.status = Some(status);
        }

        // This should always return either the existing one or the one we just created above.
        let status = self.status.take().unwrap_or_default();
        let spec_version = self.spec.version.clone();

        match (&status.current_version, &status.target_version) {
            (None, None) => {
                // No current_version and no target_version: Must be initial installation.
                // We'll set the Upgrading condition and the target_version to the version from spec.
                info!(
                    "Initial installation, now moving towards version [{}]",
                    spec_version
                );
                self.status = self
                    .set_upgrading_condition(
                        &status.conditions,
                        &format!("Initial installation to version [{:?}]", spec_version),
                        "InitialInstallation",
                        ConditionStatus::True,
                    )
                    .await?
                    .status;
                self.status = self.set_target_version(Some(&spec_version)).await?.status;
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
                self.status = self
                    .set_upgrading_condition(
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
                    self.status = self.set_target_version(Some(&spec_version)).await?.status;
                } else if current_version.is_downgrade(&spec_version)? {
                    message = format!(
                        "Downgrading from [{:?}] to [{:?}]",
                        current_version, &spec_version
                    );
                    reason = "Downgrading";
                    self.status = self.set_target_version(Some(&spec_version)).await?.status;
                } else {
                    message = format!(
                        "No upgrade/downgrade required [{:?}] is still the current_version",
                        current_version
                    );
                    reason = "";
                }

                trace!("{}", message);

                self.status = self
                    .set_upgrading_condition(
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
                if &self.spec.version != target_version {
                    info!("A new target version was requested while we still upgrade from [{}] to [{}], finishing running upgrade/downgrade first", current_version, target_version)
                }
                let message = format!(
                    "Changing version from [{:?}] to [{:?}]",
                    current_version, target_version
                );

                self.status = self
                    .set_upgrading_condition(
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

    /// Read all cluster specific pods. Check for valid labels such as HASH (required to match a certain selector) or TYPE (which indicates master/worker/history-server).
    /// Remove invalid pods which are lacking (or have outdated) required labels.
    /// Sort incoming valid pods into corresponding maps (hash -> Vec<Pod>) for later usage for each node type (master/worker/history-server).
    pub async fn read_existing_pod_information(&mut self) -> SparkReconcileResult {
        trace!(
            "Reading existing pod information for {}",
            self.context.log_name()
        );

        let mut existing_pods = self.context.list_pods().await?;
        trace!(
            "{}: Found [{}] pods",
            self.context.log_name(),
            existing_pods.len()
        );

        // in order to ensure pod hashes that differ from cluster to cluster
        // we need to hash the cluster name (metadata.name) as well
        let hashed_selectors = self.spec.get_hashed_selectors(&self.context.log_name());
        // for node information
        let mut master: HashMap<String, Vec<Pod>> = HashMap::new();
        let mut worker: HashMap<String, Vec<Pod>> = HashMap::new();
        let mut history_server: HashMap<String, Vec<Pod>> = HashMap::new();

        let status = self.status.as_ref().ok_or_else(|| error::Error::ReconcileError(
            "Spark cluster status missing, this is a programming error and should never happen. Please report in our issue tracker.".to_string(),
        ))?;

        while let Some(pod) = existing_pods.pop() {
            if let Some(labels) = pod.metadata.labels.clone() {
                // we require HASH and TYPE label to identify and sort the pods into the NodeInformation
                if let (Some(hash), Some(node_type), Some(version)) = (
                    labels.get(HASH_LABEL),
                    labels.get(TYPE_LABEL),
                    labels.get(VERSION_LABEL),
                ) {
                    let spark_node_type = match SparkNodeType::from_str(node_type) {
                        Ok(nt) => nt,
                        Err(_) => {
                            error!(
                                "Pod [{}] has an invalid type '{}' [{}], deleting it.",
                                Meta::name(&pod),
                                TYPE_LABEL,
                                node_type
                            );
                            self.context.client.delete(&pod).await?;
                            continue;
                        }
                    };

                    if let Some(hashed) = hashed_selectors.get(&spark_node_type) {
                        // valid hash found? -> do we have a matching selector hash?
                        if !hashed.contains_key(hash) {
                            error!(
                                "Pod [{}] has an outdated '{}' [{}], deleting it.",
                                Meta::name(&pod),
                                HASH_LABEL,
                                hash
                            );
                            self.context.client.delete(&pod).await?;
                            continue;
                        }
                    }

                    // check version:
                    // if there is a target version we are updating
                    // if pod version matches spec version
                    if let Some(target_version) = &status.target_version {
                        if &SparkVersion::from_str(version).unwrap() != target_version {
                            info!(
                                "Pod [{}] has an outdated '{}' [{}] - required is [{}], deleting it",
                                Meta::name(&pod),
                                VERSION_LABEL,
                                version,
                                target_version
                            );
                            self.context.client.delete(&pod).await?;
                            continue;
                        }
                    }

                    match spark_node_type {
                        SparkNodeType::Master => {
                            master.entry(hash.to_string()).or_default().push(pod)
                        }
                        SparkNodeType::Worker => {
                            worker.entry(hash.to_string()).or_default().push(pod)
                        }
                        SparkNodeType::HistoryServer => history_server
                            .entry(hash.to_string())
                            .or_default()
                            .push(pod),
                    };
                } else {
                    // some labels missing
                    error!("Pod [{}] is missing one or more required '{:?}' labels, this is illegal, deleting it.",
                         Meta::name(&pod), vec![HASH_LABEL, TYPE_LABEL]);
                    self.context.client.delete(&pod).await?;
                }
            } else {
                // all labels missing
                error!(
                    "Pod [{}] has no labels, this is illegal, deleting it.",
                    Meta::name(&pod),
                );
                self.context.client.delete(&pod).await?;
            }
        }

        self.pod_information = Some(PodInformation {
            master,
            worker,
            history_server: Some(history_server),
        });

        debug!(
            "Status[current/spec] - {} [{}/{}] | {} [{}/{}] | {} [{}/{}]",
            SparkNodeType::Master.as_str(),
            self.pod_information
                .as_ref()
                .unwrap()
                .get_pod_count(SparkNodeType::Master),
            self.spec.master.get_instances(),
            SparkNodeType::Worker.as_str(),
            self.pod_information
                .as_ref()
                .unwrap()
                .get_pod_count(SparkNodeType::Worker),
            self.spec.worker.get_instances(),
            SparkNodeType::HistoryServer.as_str(),
            self.pod_information
                .as_ref()
                .unwrap()
                .get_pod_count(SparkNodeType::HistoryServer),
            match self.spec.history_server.as_ref() {
                None => 0,
                Some(hs) => hs.get_instances(),
            }
        );

        Ok(ReconcileFunctionAction::Continue)
    }

    /// Checks if all pods that currently belong to the cluster are up and running.
    /// That means not "terminating" and "ready"
    /// We always make sure that all masters are running before any workers / history-servers
    async fn check_pods_up_and_running(&self) -> SparkReconcileResult {
        trace!("Reconciliation: Checking if all pods are up and running");

        if let Some(pod_info) = &self.pod_information {
            // master nodes have priority
            if !self.pods_up_and_running(pod_info.get_all_pods(Some(SparkNodeType::Master))) {
                return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
            }

            // worker / history-server are treated equally
            if !self.pods_up_and_running(pod_info.get_all_pods(Some(SparkNodeType::Worker)))
                || !self
                    .pods_up_and_running(pod_info.get_all_pods(Some(SparkNodeType::HistoryServer)))
            {
                return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
            }
        }

        Ok(ReconcileFunctionAction::Continue)
    }
    /// Returns true only if any pod status is neither "terminating" or anything other than "ready"
    ///
    /// # Arguments
    ///
    /// * `pods` - List of pods to check if terminating or up and running (ready)
    ///
    fn pods_up_and_running(&self, pods: Vec<Pod>) -> bool {
        // TODO: move to operator-rs; check discussion https://github.com/stackabletech/spark-operator/pull/17
        let mut terminated_pods = vec![];
        let mut started_pods = vec![];
        for pod in &pods {
            if finalizer::has_deletion_stamp(pod) {
                terminated_pods.push(Meta::name(pod));
            }

            if !podutils::is_pod_running_and_ready(pod) {
                started_pods.push(Meta::name(pod));
            }
        }

        // If pods are currently terminating, wait until they are done terminating.
        // (this could be for restarts or upgrades)
        if !terminated_pods.is_empty() {
            info!("Waiting for Pod(s) to be terminated: {:?}", terminated_pods);
            return false;
        }

        // At the moment we'll wait for all pods of a certain type (especially master),
        // to be available and ready before we might enact any changes to existing ones.
        if !started_pods.is_empty() {
            info!(
                "Waiting for Pod(s) to be running and ready: {:?}",
                started_pods
            );
            return false;
        }

        true
    }

    /// Process the cluster status version for upgrades/downgrades.
    pub async fn process_version(&mut self) -> SparkReconcileResult {
        // If we reach here it means all pods must be running on target_version.
        // We can now set current_version to target_version (if target_version was set) and
        // target_version to None
        if let Some(status) = &self.status.clone() {
            if let Some(target_version) = &status.target_version {
                info!(
                    "Finished upgrade/downgrade to [{}]. Cluster ready!",
                    &target_version
                );

                self.status = self.set_target_version(None).await?.status;
                self.status = self
                    .set_current_version(Some(&target_version))
                    .await?
                    .status;
                self.status = self
                    .set_upgrading_condition(
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

    /// Reconcile the cluster according to provided spec. Start with master nodes and continue to worker and history-server nodes.
    /// Create missing pods or delete excess pods to match the spec.
    ///
    /// # Arguments
    /// * `node_type` - SparkNodeType (master/worker/history-server)
    ///
    pub async fn reconcile_cluster(&self, node_type: &SparkNodeType) -> SparkReconcileResult {
        trace!("Starting {} reconciliation", node_type.as_str());

        match (node_type, &self.pod_information) {
            (SparkNodeType::Master, Some(pod_info)) => {
                self.reconcile_node(node_type, &pod_info.master).await
            }
            (SparkNodeType::Worker, Some(pod_info)) => {
                self.reconcile_node(node_type, &pod_info.worker).await
            }
            (
                SparkNodeType::HistoryServer,
                Some(PodInformation {
                    history_server: Some(history_server),
                    ..
                }),
            ) => self.reconcile_node(node_type, history_server).await,
            _ => Ok(ReconcileFunctionAction::Continue),
        }
    }

    /// Reconcile a SparkNode (master/worker/history-server)
    /// Create or delete pods if the instances do not match the specification
    /// We want to create all pods for each node (master/worker/history-server) immediately.
    /// If we created or deleted anything, we need a requeue to update the pod information for
    /// succeeding methods.
    ///
    /// # Arguments
    /// * `node_type` - SparkNodeType (master/worker/history-server)
    /// * `nodes` - Map where the key is a selector hash and list of pods owned by that selector
    ///
    async fn reconcile_node(
        &self,
        node_type: &SparkNodeType,
        nodes: &HashMap<String, Vec<Pod>>,
    ) -> SparkReconcileResult {
        if let Some(hashed_pods) = self
            .spec
            .get_hashed_selectors(&self.context.log_name())
            .get(node_type)
        {
            let mut applied_changes: bool = false;

            for (hash, selector) in hashed_pods {
                let spec_pod_count = selector.instances;
                let mut current_count: usize = 0;
                // delete pod when 1) hash found but current pod count > spec pod count
                // create pod when 1) no hash found or 2) hash found but current pod count < spec pod count
                if let Some(pods) = nodes.get(hash) {
                    current_count = pods.len();
                    while current_count > spec_pod_count {
                        let pod = pods.get(0).unwrap();
                        self.context.client.delete(pod).await?;
                        // TODO: delete configmaps?
                        debug!("Deleting {} pod '{}'", node_type.as_str(), Meta::name(pod));
                        current_count -= 1;
                        applied_changes = true;
                    }
                }

                while current_count < spec_pod_count {
                    let pod = self.create_pod(selector, node_type, hash).await?;
                    self.create_config_maps(node_type, selector, hash).await?;
                    info!("Creating {} pod '{}'", node_type.as_str(), Meta::name(&pod));
                    current_count += 1;
                    applied_changes = true;
                }
            }

            if applied_changes {
                return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
            }
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    /// Build a pod and create it
    ///
    /// # Arguments
    /// * `selector` - SparkNodeSelector which contains specific pod information
    /// * `node_type` - SparkNodeType (master/worker/history-server)
    /// * `hash` - NodeSelector hash
    ///
    async fn create_pod(
        &self,
        selector: &SparkNodeSelector,
        node_type: &SparkNodeType,
        hash: &str,
    ) -> Result<Pod, Error> {
        let pod = self.build_pod(selector, hash, node_type)?;
        Ok(self.context.client.create(&pod).await?)
    }

    /// Build a pod using its selector and node_type
    ///
    /// # Arguments
    /// * `selector` - SparkNodeSelector which contains specific pod information
    /// * `node_type` - SparkNodeType (master/worker/history-server)
    /// * `hash` - NodeSelector hash
    ///
    fn build_pod(
        &self,
        selector: &SparkNodeSelector,
        hash: &str,
        node_type: &SparkNodeType,
    ) -> Result<Pod, Error> {
        let (containers, volumes) = self.build_containers(node_type, hash);

        Ok(Pod {
            metadata: metadata::build_metadata(
                self.create_pod_name(node_type, hash),
                Some(self.build_labels(node_type, hash)),
                &self.context.resource,
                true,
            )?,
            spec: Some(PodSpec {
                node_name: Some(selector.node_name.clone()),
                tolerations: Some(create_tolerations()),
                containers,
                volumes: Some(volumes),
                ..PodSpec::default()
            }),
            ..Pod::default()
        })
    }

    /// Build required pod containers
    ///
    /// # Arguments
    /// * `node_type` - SparkNodeType (master/worker/history-server)
    /// * `hash` - NodeSelector hash
    ///
    fn build_containers(
        &self,
        node_type: &SparkNodeType,
        hash: &str,
    ) -> (Vec<Container>, Vec<Volume>) {
        let image_name = format!("spark:{}", &self.spec.version);

        // adapt worker command with master url(s)
        let mut command = vec![node_type.get_command(&self.spec.version)];

        if let Some(adapted_command) = config::adapt_container_command(node_type, &self.spec.master)
        {
            command.push(adapted_command);
        }

        let containers = vec![Container {
            image: Some(image_name),
            name: "spark".to_string(),
            command: Some(command),
            volume_mounts: Some(config::create_volume_mounts(&self.spec.log_dir)),
            env: Some(config::create_required_startup_env()),
            ..Container::default()
        }];

        let cm_name = self.create_config_map_name(node_type, hash);
        let volumes = config::create_volumes(&cm_name);

        (containers, volumes)
    }

    /// Create required config maps for the cluster
    ///
    /// # Arguments
    /// * `node_type` - SparkNodeType (master/worker/history-server)
    /// * `hash` - NodeSelector hash
    ///
    async fn create_config_maps(
        &self,
        node_type: &SparkNodeType,
        selector: &SparkNodeSelector,
        hash: &str,
    ) -> Result<(), Error> {
        let config_properties = config::get_config_properties(&self.spec, selector);
        let env_vars = config::get_env_variables(selector);

        let conf = config::convert_map_to_string(&config_properties, " ");
        let env = config::convert_map_to_string(&env_vars, "=");

        let mut data = BTreeMap::new();
        data.insert("spark-defaults.conf".to_string(), conf);
        data.insert("spark-env.sh".to_string(), env);

        let cm_name = self.create_config_map_name(node_type, hash);
        let cm = create_config_map(&self.context.resource, &cm_name, data)?;
        self.context.client.apply_patch(&cm, &cm).await?;

        Ok(())
    }

    /// Provide required labels for pods
    ///
    /// # Arguments
    /// * `node_type` - SparkNodeType (master/worker/history-server)
    /// * `hash` - NodeSelector hash
    ///
    fn build_labels(&self, node_type: &SparkNodeType, hash: &str) -> BTreeMap<String, String> {
        let mut labels = BTreeMap::new();
        labels.insert(TYPE_LABEL.to_string(), node_type.to_string());
        labels.insert(HASH_LABEL.to_string(), hash.to_string());
        labels.insert(VERSION_LABEL.to_string(), self.spec.version.to_string());

        labels
    }

    /// All pod names follow a simple pattern: <spark_cluster_name>-<node_type>-<selector_hash>-<UUID>
    ///
    /// # Arguments
    /// * `node_type` - SparkNodeType (master/worker/history-server)
    /// * `hash` - NodeSelector hash
    ///
    fn create_pod_name(&self, node_type: &SparkNodeType, hash: &str) -> String {
        format!(
            "{}-{}-{}-{}",
            self.context.name(),
            node_type.as_str(),
            hash,
            Uuid::new_v4().as_fields().0.to_string(),
        )
    }

    /// All config map names follow a simple pattern: <name of SparkCluster object>-<NodeType name>-<SelectorHash>-cm
    /// That means multiple pods of one selector share one and the same config map
    ///
    /// # Arguments
    /// * `node_type` - SparkNodeType (master/worker/history-server)
    /// * `hash` - NodeSelector hash
    ///
    fn create_config_map_name(&self, node_type: &SparkNodeType, hash: &str) -> String {
        format!("{}-{}-{}-cm", self.context.name(), node_type.as_str(), hash)
    }
}

impl ReconciliationState for SparkState {
    type Error = error::Error;

    fn reconcile(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<ReconcileFunctionAction, Self::Error>> + Send + '_>>
    {
        Box::pin(async move {
            self.init_status()
                .await?
                .then(self.read_existing_pod_information())
                .await?
                .then(self.check_pods_up_and_running())
                .await?
                .then(self.reconcile_cluster(&SparkNodeType::Master))
                .await?
                .then(self.reconcile_cluster(&SparkNodeType::Worker))
                .await?
                .then(self.reconcile_cluster(&SparkNodeType::HistoryServer))
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

    fn finalizer_name(&self) -> String {
        FINALIZER_NAME.to_string()
    }

    async fn init_reconcile_state(
        &self,
        context: ReconciliationContext<Self::Item>,
    ) -> Result<Self::State, Error> {
        Ok(SparkState {
            spec: context.resource.spec.clone(),
            status: context.resource.status.clone(),
            context,
            pod_information: None,
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

    let controller = Controller::new(spark_api)
        .owns(pods_api, ListParams::default())
        .owns(config_maps_api, ListParams::default());

    let strategy = SparkStrategy::new();

    controller.run(client, strategy).await;
}
