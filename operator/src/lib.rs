#![feature(backtrace)]
mod config;
mod error;

use crate::error::Error;

use kube::Api;
use tracing::{debug, error, info, trace};

use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, EnvVar, Pod, PodSpec, Volume, VolumeMount,
};
use kube::api::{ListParams, Meta, ObjectMeta};
use serde_json::json;

use async_trait::async_trait;
use handlebars::Handlebars;
use stackable_operator::client::Client;
use stackable_operator::controller::{Controller, ControllerStrategy, ReconciliationState};
use stackable_operator::metadata;
use stackable_operator::reconcile::{
    ReconcileFunctionAction, ReconcileResult, ReconciliationContext,
};
use stackable_operator::{create_config_map, create_tolerations};
use stackable_spark_crd::{
    SparkCluster, SparkClusterSpec, SparkClusterStatus, SparkNodeSelector, SparkNodeType,
};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use uuid::Uuid;

const FINALIZER_NAME: &str = "spark.stackable.de/cleanup";

const HASH: &str = "spark.stackable.de/hash";
const TYPE: &str = "spark.stackable.de/type";

type SparkReconcileResult = ReconcileResult<error::Error>;

struct SparkState {
    context: ReconciliationContext<SparkCluster>,
    spec: SparkClusterSpec,
    status: Option<SparkClusterStatus>,
    node_information: Option<NodeInformation>,
}

/// This will be filled in the beginning of the reconcile
/// Nodes are sorted according to their cluster role / node type and their corresponding selector hash
struct NodeInformation {
    // hash for selector -> corresponding pods
    pub master: HashMap<String, Vec<Pod>>,
    pub worker: HashMap<String, Vec<Pod>>,
    pub history_server: Option<HashMap<String, Vec<Pod>>>,
}

impl NodeInformation {
    /// Count the pods according to their node type
    ///
    /// # Arguments
    ///
    /// * `node_type` - SparkNodeType (master/worker/history-server)
    ///
    pub fn get_pod_count(&self, node_type: SparkNodeType) -> usize {
        let mut num_pods = 0;
        match node_type {
            SparkNodeType::Master => num_pods = self.count_pods(&self.master),
            SparkNodeType::Worker => num_pods = self.count_pods(&self.worker),
            SparkNodeType::HistoryServer => {
                if self.history_server.is_some() {
                    num_pods = self.count_pods(self.history_server.as_ref().unwrap())
                }
            }
        }
        num_pods
    }

    /// Count the pods stored in each (hashed) selector
    ///
    /// # Arguments
    ///
    /// * `hashed` - Map where the key is a selector hash and list of pods owned by that selector
    ///
    fn count_pods(&self, hashed: &HashMap<String, Vec<Pod>>) -> usize {
        let mut num_pods: usize = 0;
        for value in hashed.values() {
            num_pods += value.len();
        }
        num_pods
    }
}

impl SparkState {
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
                if let (Some(hash), Some(node_type)) = (labels.get(HASH), labels.get(TYPE)) {
                    let spark_node_type = match SparkNodeType::from_str(node_type) {
                        Ok(nt) => nt,
                        Err(_) => {
                            error!(
                                "Pod [{}] has an invalid type '{}' [{}], deleting it.",
                                Meta::name(&pod),
                                TYPE,
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
                                HASH,
                                hash
                            );
                            self.context.client.delete(&pod).await?;
                            continue;
                        }
                    }

                    match spark_node_type {
                        SparkNodeType::Master => self.sort_pod_info(pod, &mut master, hash),
                        SparkNodeType::Worker => self.sort_pod_info(pod, &mut worker, hash),
                        SparkNodeType::HistoryServer => {
                            self.sort_pod_info(pod, &mut history_server, hash)
                        }
                    };
                } else {
                    // some labels missing
                    error!("Pod [{}] is missing one or more required '{:?}' labels, this is illegal, deleting it.",
                         Meta::name(&pod), vec![HASH, TYPE]);
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

        self.node_information = Some(NodeInformation {
            master,
            worker,
            history_server: Some(history_server),
        });

        info!(
            "Status[current/spec] - {} [{}/{}] | {} [{}/{}] | {} [{}/{}]",
            SparkNodeType::Master.as_str(),
            self.node_information
                .as_ref()
                .unwrap()
                .get_pod_count(SparkNodeType::Master),
            self.spec.master.get_instances(),
            SparkNodeType::Worker.as_str(),
            self.node_information
                .as_ref()
                .unwrap()
                .get_pod_count(SparkNodeType::Worker),
            self.spec.worker.get_instances(),
            SparkNodeType::HistoryServer.as_str(),
            self.node_information
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
    pub async fn reconcile_cluster(&self) -> SparkReconcileResult {
        trace!("Starting {} reconciliation", SparkNodeType::Master.as_str());

        if let Some(node_info) = &self.node_information {
            self.reconcile_node(&SparkNodeType::Master, &node_info.master)
                .await?;
            self.reconcile_node(&SparkNodeType::Worker, &node_info.worker)
                .await?;

            if let Some(history_server) = &node_info.history_server {
                self.reconcile_node(&SparkNodeType::HistoryServer, history_server)
                    .await?;
            } else {
                return Ok(ReconcileFunctionAction::Continue);
            }
        } else {
            return Ok(ReconcileFunctionAction::Done);
        }

        Ok(ReconcileFunctionAction::Continue)
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
        //if !self.check_pods_ready(nodes) {
        //    return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(
        //        REQUEUE_SECONDS,
        //    )));
        //}
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
                    }
                }

                if current_count < spec_pod_count {
                    let pod = self.create_pod(selector, node_type, hash).await?;
                    self.create_config_maps(selector, node_type, hash).await?;
                    info!("Creating {} pod '{}'", node_type.as_str(), Meta::name(&pod));
                }
            }
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    // fn check_pods_ready(&self, hashed_pods: &HashMap<String, Vec<Pod>>) -> bool {
    //     for pods in hashed_pods.values() {
    //         for pod in pods {
    //             // terminating
    //             if self.has_deletion_stamp(pod) {
    //                 return false;
    //             }
    //             if self.is_pod_running_and_ready(pod) {
    //                 return false;
    //             }
    //         }
    //     }
    //     return true;
    // }
    //
    // fn has_deletion_stamp(&self, pod: &Pod) -> bool {
    //     if finalizer::has_deletion_stamp(pod) {
    //         info!(
    //             "SparkCluster {} is waiting for Pod [{}] to terminate",
    //             self.context.log_name(),
    //             Meta::name(pod)
    //         );
    //         return true;
    //     }
    //     return false;
    // }
    //
    // fn is_pod_running_and_ready(&self, pod: &Pod) -> bool {
    //     if !podutils::is_pod_running_and_ready(pod) {
    //         info!(
    //             "SparkCluster {} is waiting for Pod [{}] to be running and ready",
    //             self.context.log_name(),
    //             Meta::name(pod)
    //         );
    //         return false;
    //     }
    //     return true;
    // }

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
        Ok(Pod {
            metadata: self.build_pod_metadata(node_type, hash)?,
            spec: Some(self.build_pod_spec(selector, node_type, hash)),
            ..Pod::default()
        })
    }

    fn build_pod_metadata(
        &self,
        node_type: &SparkNodeType,
        hash: &str,
    ) -> Result<ObjectMeta, Error> {
        Ok(ObjectMeta {
            labels: Some(self.build_labels(node_type, hash)),
            name: Some(self.create_pod_name(node_type, hash)),
            namespace: Some(self.context.namespace()),
            owner_references: Some(vec![metadata::object_to_owner_reference::<SparkCluster>(
                self.context.metadata(),
            )?]),
            ..ObjectMeta::default()
        })
    }

    fn build_pod_spec(
        &self,
        selector: &SparkNodeSelector,
        node_type: &SparkNodeType,
        hash: &str,
    ) -> PodSpec {
        let (containers, volumes) = self.build_containers(node_type, hash);

        PodSpec {
            node_name: Some(selector.node_name.clone()),
            tolerations: Some(create_tolerations()),
            containers,
            volumes: Some(volumes),
            ..PodSpec::default()
        }
    }

    fn build_containers(
        &self,
        node_type: &SparkNodeType,
        hash: &str,
    ) -> (Vec<Container>, Vec<Volume>) {
        // TODO: get version from controller
        let image_name = "spark:3.0.1".to_string();

        // adapt worker command with master url(s)
        let mut command = vec![node_type.get_command()];
        let adapted_command = config::adapt_container_command(node_type, &self.spec.master);
        if !adapted_command.is_empty() {
            command.push(adapted_command);
        }

        let containers = vec![Container {
            image: Some(image_name),
            name: "spark".to_string(),
            // TODO: worker -> add master port
            command: Some(command),
            volume_mounts: Some(vec![
                // One mount for the config directory, this will be relative to the extracted package
                VolumeMount {
                    mount_path: "conf".to_string(),
                    name: "config-volume".to_string(),
                    ..VolumeMount::default()
                },
                VolumeMount {
                    mount_path: "/tmp".to_string(), // TODO: Make configurable via crd log dir
                    name: "event-volume".to_string(),
                    ..VolumeMount::default()
                },
            ]),
            // set no daemonize and configroot via env variables:
            // if loaded via spark-default.conf or spark-env.sh the variables are set too late!
            env: Some(vec![
                EnvVar {
                    name: "SPARK_NO_DAEMONIZE".to_string(),
                    value: Some("true".to_string()),
                    ..EnvVar::default()
                },
                EnvVar {
                    name: "SPARK_CONF_DIR".to_string(),
                    value: Some("{{configroot}}/conf".to_string()),
                    ..EnvVar::default()
                },
            ]),
            ..Container::default()
        }];

        let cm_name = self.create_config_map_name(node_type, hash);
        let volumes = vec![
            Volume {
                name: "config-volume".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: Some(cm_name),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            },
            Volume {
                name: "event-volume".to_string(),
                ..Volume::default()
            },
        ];

        (containers, volumes)
    }

    async fn create_config_maps(
        &self,
        selector: &SparkNodeSelector,
        node_type: &SparkNodeType,
        hash: &str,
    ) -> Result<(), Error> {
        // TODO: remove hardcoded and make configurable and distinguish master / worker vs history-server
        let mut config_properties: HashMap<String, String> = HashMap::new();
        config_properties.insert(
            "spark.history.fs.logDirectory".to_string(),
            "/tmp".to_string(),
        );

        config_properties.insert("spark.eventLog.enabled".to_string(), "true".to_string());

        config_properties.insert("spark.eventLog.dir".to_string(), "/tmp".to_string());
        let mut env_vars: HashMap<String, String> = HashMap::new();
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
        labels.insert(TYPE.to_string(), node_type.to_string());
        labels.insert(HASH.to_string(), hash.to_string());

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

    /// Sort valid pods into their corresponding hashed vector maps
    ///
    /// # Arguments
    /// * `pod` - SparkNodeType (master/worker/history-server)
    /// * `hashed_pods` - Map with hash as key and list of pods as value
    /// * `hash` - NodeSelector hash
    ///
    fn sort_pod_info(&self, pod: Pod, hashed_pods: &mut HashMap<String, Vec<Pod>>, hash: &str) {
        if hashed_pods.contains_key(hash) {
            hashed_pods.get_mut(hash).unwrap().push(pod);
        } else {
            hashed_pods.insert(hash.to_string(), vec![pod]);
        }
    }
}

impl ReconciliationState for SparkState {
    type Error = error::Error;

    fn reconcile(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<ReconcileFunctionAction, Self::Error>> + Send + '_>>
    {
        Box::pin(async move {
            self.read_existing_pod_information()
                .await?
                .then(self.reconcile_cluster())
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
        return FINALIZER_NAME.to_string();
    }

    async fn init_reconcile_state(
        &self,
        context: ReconciliationContext<Self::Item>,
    ) -> Result<Self::State, Error> {
        Ok(SparkState {
            spec: context.resource.spec.clone(),
            status: context.resource.status.clone(),
            context,
            node_information: None,
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
