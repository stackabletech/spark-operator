#![feature(backtrace)]
mod config;
mod error;

use crate::error::Error;

use kube::Api;
use tracing::{error, info, trace};

use k8s_openapi::api::core::v1::{ConfigMap, Container, Pod, PodSpec, Volume};
use kube::api::{ListParams, Meta};
use serde_json::json;

use async_trait::async_trait;
use handlebars::Handlebars;
use stackable_operator::client::Client;
use stackable_operator::controller::{Controller, ControllerStrategy, ReconciliationState};
use stackable_operator::reconcile::{
    ReconcileFunctionAction, ReconcileResult, ReconciliationContext,
};
use stackable_operator::{create_config_map, create_tolerations};
use stackable_operator::{finalizer, metadata, podutils};
use stackable_spark_crd::{
    SparkCluster, SparkClusterSpec, SparkClusterStatus, SparkNodeSelector, SparkNodeType,
};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::time::Duration;
use uuid::Uuid;

const FINALIZER_NAME: &str = "spark.stackable.tech/cleanup";

/// label which holds the selector hash in the pods to identify which selector they belong to
const HASH_LABEL: &str = "spark.stackable.tech/hash";
/// label which holds node role / type (master, worker, history-server) in the pods
const TYPE_LABEL: &str = "spark.stackable.tech/type";

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
    // hash for selector -> corresponding pods
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
            SparkNodeType::Master => num_pods = count_pods(&self.master),
            SparkNodeType::Worker => num_pods = count_pods(&self.worker),
            SparkNodeType::HistoryServer => {
                if self.history_server.is_some() {
                    num_pods = count_pods(self.history_server.as_ref().unwrap())
                }
            }
        }
        num_pods
    }
}

/// Count the pods stored in each (hashed) selector
///
/// # Arguments
///
/// * `hashed` - Map where the key is a selector hash and list of pods owned by that selector
///
fn count_pods(hashed: &HashMap<String, Vec<Pod>>) -> usize {
    let mut num_pods = 0;
    for value in hashed.values() {
        num_pods += value.len();
    }
    num_pods
}

impl SparkState {
    /// Process the cluster status:
    /// If no status is set, write image, version and timestamp to status
    /// Check for restart, start, stop commands
    pub async fn process_cluster_status(&mut self) -> SparkReconcileResult {
        if let Some(status) = &self.status {
            if status.version != self.spec.version {
                info!(
                    "Current version [{}] - required version [{}] -> updating cluster...",
                    status.version, &self.spec.version
                );

                for pod in &self.context.list_pods().await? {
                    info!("Deleting pod [{}]", Meta::name(pod));
                    self.context.client.delete(pod).await?;
                }

                &self
                    .context
                    .client
                    .merge_patch_status(&self.context.resource, &self.create_status())
                    .await?;

                return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
            }
        } else {
            info!(
                "Write current version to status: [{}]",
                &self.spec.version.to_string()
            );

            &self
                .context
                .client
                .merge_patch_status(&self.context.resource, &self.create_status())
                .await?;
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    fn create_status(&self) -> SparkClusterStatus {
        SparkClusterStatus {
            version: self.spec.version.clone(),
        }
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

        while let Some(pod) = existing_pods.pop() {
            if let Some(labels) = pod.metadata.labels.clone() {
                // we require HASH and TYPE label to identify and sort the pods into the NodeInformation
                if let (Some(hash), Some(node_type)) =
                    (labels.get(HASH_LABEL), labels.get(TYPE_LABEL))
                {
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

                    // If the pod for this server is currently terminating (this could be for restarts or
                    // upgrades) wait until it's done terminating.
                    if finalizer::has_deletion_stamp(&pod) {
                        info!(
                            "SparkCluster {} is waiting for Pod [{}] to terminate",
                            self.context.log_name(),
                            Meta::name(&pod)
                        );
                        return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
                    }

                    // At the moment we'll wait for all pods to be available and ready before we might enact any changes to existing ones.
                    // TODO: Only do this next check if we want "rolling" functionality
                    if !podutils::is_pod_running_and_ready(&pod) {
                        info!(
                            "SparkCluster {} is waiting for Pod [{}] to be running and ready",
                            self.context.log_name(),
                            Meta::name(&pod)
                        );
                        return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
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
                    continue;
                }
            } else {
                // all labels missing
                error!(
                    "Pod [{}] has no labels, this is illegal, deleting it.",
                    Meta::name(&pod),
                );
                self.context.client.delete(&pod).await?;
                continue;
            }
        }

        self.pod_information = Some(PodInformation {
            master,
            worker,
            history_server: Some(history_server),
        });

        info!(
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

    /// Reconcile the cluster according to provided spec. Start with master nodes and continue to worker and history-server nodes.
    /// Create missing pods or delete excess pods to match the spec.
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
        // TODO: check if nodes terminated / check if nodes up and running
        // TODO: if no master available, reboot workers

        if let Some(hashed_pods) = self
            .spec
            .get_hashed_selectors(&self.context.log_name())
            .get(node_type)
        {
            for (hash, selector) in hashed_pods {
                let spec_pod_count = selector.instances;
                let mut current_count: usize = 0;
                // delete pod when 1) hash found but current pod count > spec pod count
                // create pod when 1) no hash found or 2) hash found but current pod count < spec pod count
                if let Some(pods) = nodes.get(hash) {
                    current_count = pods.len();
                    if current_count > spec_pod_count {
                        let pod = pods.get(0).unwrap();
                        self.context.client.delete(pod).await?;
                        info!("deleting {} pod '{}'", node_type.as_str(), Meta::name(pod));
                        return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
                    }
                }

                if current_count < spec_pod_count {
                    let pod = self.create_pod(selector, node_type, hash).await?;
                    self.create_config_maps(node_type, hash).await?;
                    info!("Creating {} pod '{}'", node_type.as_str(), Meta::name(&pod));
                    return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
                }
            }
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    async fn create_pod(
        &self,
        selector: &SparkNodeSelector,
        node_type: &SparkNodeType,
        hash: &str,
    ) -> Result<Pod, Error> {
        let pod = self.build_pod(selector, hash, node_type)?;
        Ok(self.context.client.create(&pod).await?)
    }

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

    fn build_containers(
        &self,
        node_type: &SparkNodeType,
        hash: &str,
    ) -> (Vec<Container>, Vec<Volume>) {
        // TODO: get version from controller
        let image_name = format!("spark:{}", &self.spec.version);

        // adapt worker command with master url(s)
        let mut command = vec![node_type.get_command(&self.spec.version)];
        let adapted_command = config::adapt_container_command(node_type, &self.spec.master);
        if !adapted_command.is_empty() {
            command.push(adapted_command);
        }

        let containers = vec![Container {
            image: Some(image_name),
            name: "spark".to_string(),
            // TODO: worker -> add master port
            command: Some(command),
            volume_mounts: Some(config::create_volume_mounts(&self.spec.log_dir)),
            env: Some(config::create_required_startup_env()),
            ..Container::default()
        }];

        let cm_name = self.create_config_map_name(node_type, hash);
        let volumes = config::create_volumes(&cm_name);

        (containers, volumes)
    }

    async fn create_config_maps(&self, node_type: &SparkNodeType, hash: &str) -> Result<(), Error> {
        // TODO: remove hardcoded and make configurable and distinguish master / worker vs history-server
        let mut config_properties: HashMap<String, String> = HashMap::new();
        config_properties.insert(
            "spark.history.fs.logDirectory".to_string(),
            "file:///tmp".to_string(),
        );

        config_properties.insert("spark.eventLog.enabled".to_string(), "true".to_string());

        config_properties.insert("spark.eventLog.dir".to_string(), "file:///tmp".to_string());
        let env_vars: HashMap<String, String> = HashMap::new();
        // TODO: use product-conf for validation

        let mut handlebars_config = Handlebars::new();
        handlebars_config
            .register_template_string("conf", "{{#each options}}{{@key}} {{this}}\n{{/each}}")
            .expect("conf template should work");

        let mut handlebars_env = Handlebars::new();
        handlebars_env
            .register_template_string("env", "{{#each options}}{{@key}}={{this}}\n{{/each}}")
            .expect("env template should work");

        let conf = handlebars_config
            .render("conf", &json!({ "options": config_properties }))
            .unwrap();

        let env = handlebars_env
            .render("env", &json!({ "options": env_vars }))
            .unwrap();

        let mut data = BTreeMap::new();
        data.insert("spark-env.sh".to_string(), env);
        data.insert("spark-defaults.conf".to_string(), conf);

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

        labels
    }

    /// All pod names follow a simple pattern: <name of SparkCluster object>-<NodeType name>-<SelectorHash / UUID>
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
            self.process_cluster_status()
                .await?
                .then(self.read_existing_pod_information())
                .await?
                .then(self.reconcile_cluster(&SparkNodeType::Master))
                .await?
                .then(self.reconcile_cluster(&SparkNodeType::Worker))
                .await?
                .then(self.reconcile_cluster(&SparkNodeType::HistoryServer))
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
