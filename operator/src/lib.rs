#![feature(backtrace)]
mod error;

use crate::error::Error;

use kube::Api;
use tracing::{debug, error, info, trace};

use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, Pod, PodSpec, Volume, VolumeMount,
};
use kube::api::{ListParams, Meta, ObjectMeta};
use serde_json::json;

use handlebars::Handlebars;
use stackable_operator::client::Client;
use stackable_operator::controller::{Controller, ControllerStrategy, ReconciliationState};
use stackable_operator::reconcile::{
    create_requeuing_reconcile_function_action, ReconcileFunctionAction, ReconcileResult,
    ReconciliationContext,
};
use stackable_operator::{create_config_map, create_tolerations, podutils};
use stackable_operator::{finalizer, metadata};
use stackable_spark_crd::{
    SparkCluster, SparkClusterSpec, SparkClusterStatus, SparkNodeSelector, SparkNodeType,
};
use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tokio::macros::support::Future;
use uuid::Uuid;

const FINALIZER_NAME: &str = "spark.stackable.de/cleanup";

const HASH: &str = "spark.stackable.de/hash";
const ROLE: &str = "spark.stackable.de/role";
const REQUIRED_LABELS: &'static [&'static str] = &[HASH, ROLE];

type SparkReconcileResult = ReconcileResult<error::Error>;

struct SparkState {
    context: ReconciliationContext<SparkCluster>,
    spec: SparkClusterSpec,
    status: Option<SparkClusterStatus>,
    node_information: Option<NodeInformation>,
}

struct NodeInformation {
    // e.g. master -> { hash1 -> vec![master_pod_1, ...] },
    //                { hash2 -> vec![..., master_pod_n] }
    //      worker -> { ... }
    pub nodes: HashMap<SparkNodeType, HashMap<String, Vec<Pod>>>,
}

impl NodeInformation {
    pub fn get_pods(&self, node_type: Option<SparkNodeType>) -> Vec<Pod> {
        let mut found_pods: Vec<Pod> = Vec::new();

        for (key, value) in &self.nodes {
            for (hash, pods) in value {
                if node_type == None {
                    found_pods.extend_from_slice(pods);
                } else if Some(*key) == node_type {
                    found_pods.extend_from_slice(pods);
                }
            }
        }

        found_pods
    }

    pub fn get_hashed_pods(&self, node_type: SparkNodeType) -> Option<&HashMap<String, Vec<Pod>>> {
        self.nodes.get(&node_type)
    }
}

impl SparkState {
    pub async fn read_existing_pod_information(&mut self) -> SparkReconcileResult {
        trace!(
            "Reading existing pod information for {}",
            self.context.log_name()
        );

        let existing_pods = self.context.list_pods().await?;
        trace!(
            "{}: Found [{}] pods",
            self.context.log_name(),
            existing_pods.len()
        );

        // split pods into master, worker and history server nodes
        let mut nodes: HashMap<SparkNodeType, HashMap<String, Vec<Pod>>> = HashMap::new();

        'pod_loop: for pod in existing_pods {
            if let Some(labels) = &pod.metadata.labels {
                // check if all required labels exist
                for required_label in REQUIRED_LABELS {
                    if !labels.contains_key(required_label.clone()) {
                        error!("SparkCluster {}: Pod [{:?}] does not have the required '{}' label, this is illegal, deleting it.",
                             self.context.log_name(), pod, required_label);
                        self.context.client.delete(&pod).await?;
                        break 'pod_loop;
                    }
                }
                // pods have required labels
                let hash = labels.get(HASH).unwrap();
                // TODO: check hash match -> delete if no match
                let node_type: SparkNodeType =
                    SparkNodeType::from_str(labels.get(ROLE).unwrap()).unwrap();

                // no node type registered yet
                if !nodes.contains_key(&node_type) {
                    nodes.insert(
                        node_type,
                        [(hash.clone(), vec![pod])].iter().cloned().collect(),
                    );
                }
                // node type found
                else {
                    let mut map = nodes.get_mut(&node_type).unwrap();
                    if !map.contains_key(hash) {
                        // no hash registered yet
                        &map.insert(hash.clone(), vec![pod]);
                    } else {
                        // hash registered
                        let mut found_pods = map.get_mut(hash).unwrap();
                        found_pods.push(pod);
                    }
                }
            }
        }

        self.node_information = Some(NodeInformation { nodes });

        info!(
            "SparkCluster {} -> status[current/spec]: {} [{}/{}] | {} [{}/{}] | {} [{}/{}]",
            self.context.log_name(),
            SparkNodeType::Master.as_str(),
            self.node_information
                .as_ref()
                .unwrap()
                .get_pods(Some(SparkNodeType::Master))
                .len(),
            self.spec.nodes.get_instances(Some(SparkNodeType::Master)),
            SparkNodeType::Worker.as_str(),
            self.node_information
                .as_ref()
                .unwrap()
                .get_pods(Some(SparkNodeType::Worker))
                .len(),
            self.spec.nodes.get_instances(Some(SparkNodeType::Worker)),
            SparkNodeType::HistoryServer.as_str(),
            self.node_information
                .as_ref()
                .unwrap()
                .get_pods(Some(SparkNodeType::HistoryServer))
                .len(),
            self.spec
                .nodes
                .get_instances(Some(SparkNodeType::HistoryServer)),
        );

        Ok(ReconcileFunctionAction::Continue)
    }

    pub async fn reconcile_cluster(&self) -> SparkReconcileResult {
        trace!("{}: Starting reconciliation", self.context.log_name());

        // reconcile masters first
        self.reconcile_nodes(SparkNodeType::Master).await?;
        self.reconcile_nodes(SparkNodeType::Worker).await?;
        self.reconcile_nodes(SparkNodeType::HistoryServer).await?;

        Ok(ReconcileFunctionAction::Continue)
    }

    async fn reconcile_nodes(&self, node_type: SparkNodeType) -> SparkReconcileResult {
        // collect all node selectors for specific node type
        let hashed_selectors = self.spec.nodes.get_hashed_selectors(Some(node_type));

        // collect all pods for specific node type
        let hashed_pods = self
            .node_information
            .as_ref()
            .unwrap()
            .get_hashed_pods(node_type);

        for (hash, selector) in hashed_selectors {
            let desired_pods = selector.instances;
            if let Some(map) = hashed_pods {
                if map.contains_key(&hash) {
                    let pods_for_selector = map.get(&hash).unwrap();
                    let actual_pods = pods_for_selector.len();
                    // delete
                    if actual_pods > desired_pods {
                        let pod = pods_for_selector.get(0).unwrap();
                        self.context.client.delete(pod).await?;
                        info!("deleting {} pod '{:?}'", node_type.as_str(), pod);
                        return Ok(ReconcileFunctionAction::Done);
                    }
                    if actual_pods < desired_pods {
                        let pod = self.create_pod(selector).await?;
                        //let cm = self.create_config_maps(selector).await?;
                        info!("creating {} pod '{:?}'", node_type.as_str(), pod);
                        return Ok(ReconcileFunctionAction::Done);
                    }
                }
            } else {
                // create
                let pod = self.create_pod(selector).await?;
                //let cm = self.create_config_maps(selector).await?;
                info!("creating {} pod '{:?}'", node_type.as_str(), pod);
                return Ok(ReconcileFunctionAction::Done);
            }
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    async fn create_pod(&self, selector: &SparkNodeSelector) -> Result<Pod, Error> {
        let pod = self.build_pod(selector)?;
        Ok(self.context.client.create(&pod).await?)
    }

    fn build_pod(&self, selector: &SparkNodeSelector) -> Result<Pod, Error> {
        Ok(Pod {
            metadata: self.build_pod_metadata(selector)?,
            spec: Some(self.build_pod_spec(selector)),
            ..Pod::default()
        })
    }

    fn build_pod_metadata(&self, selector: &SparkNodeSelector) -> Result<ObjectMeta, Error> {
        Ok(ObjectMeta {
            labels: Some(self.build_labels(selector)?),
            name: Some(self.get_pod_name(selector, false)),
            namespace: Some(self.context.namespace()),
            owner_references: Some(vec![metadata::object_to_owner_reference::<SparkCluster>(
                self.context.metadata(),
            )?]),
            ..ObjectMeta::default()
        })
    }

    fn build_pod_spec(&self, selector: &SparkNodeSelector) -> PodSpec {
        let (containers, volumes) = self.build_containers(selector);

        PodSpec {
            //node_name: Some(self.get_pod_name(selector, false)),
            tolerations: Some(create_tolerations()),
            containers,
            volumes: Some(volumes),
            ..PodSpec::default()
        }
    }

    fn build_containers(&self, selector: &SparkNodeSelector) -> (Vec<Container>, Vec<Volume>) {
        let version = "3.0.1".to_string();
        let image_name = format!(
            "stackable/spark:{}",
            serde_json::json!(version).as_str().unwrap()
        );

        let containers = vec![Container {
            image: Some(image_name),
            name: "spark".to_string(),
            // TODO: worker -> add master port
            command: Some(vec![selector.node_type.get_command()]),
            volume_mounts: Some(vec![
                // One mount for the config directory, this will be relative to the extracted package
                VolumeMount {
                    mount_path: "conf".to_string(),
                    name: "config-volume".to_string(),
                    ..VolumeMount::default()
                },
                // We need a second mount for the data directory
                // because we need to write the myid file into the data directory
                VolumeMount {
                    mount_path: "/tmp/spark-events".to_string(), // TODO: get log dir from crd
                    name: "data-volume".to_string(),
                    ..VolumeMount::default()
                },
            ]),
            ..Container::default()
        }];

        let cm_name_prefix = format!("spark-{}", self.get_pod_name(selector, true));
        let volumes = vec![
            Volume {
                name: "config-volume".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: Some(format!("{}-config", cm_name_prefix)),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            },
            Volume {
                name: "data-volume".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: Some(format!("{}-data", cm_name_prefix)),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            },
        ];

        (containers, volumes)
    }

    async fn create_config_maps(&self, selector: &SparkNodeSelector) -> Result<(), Error> {
        let mut options = HashMap::new();
        // TODO: use product-conf for validation
        options.insert("SPARK_NO_DAEMONIZE".to_string(), "true".to_string());
        options.insert(
            "SPARK_CONF_DIR".to_string(),
            "{{configroot}}/conf".to_string(),
        );

        let mut handlebars = Handlebars::new();
        handlebars
            .register_template_string("conf", "{{#each options}}{{@key}}={{this}}\n{{/each}}")
            .expect("template should work");

        let config = handlebars
            .render("conf", &json!({ "options": options }))
            .unwrap();

        //let config = spark_env
        //    .iter()
        //    .map(|(key, value)| format!("{}={}\n", key.to_string(), value))
        //    .collect();

        // Now we need to create two configmaps per server.
        // The names are "zk-<cluster name>-<node name>-config" and "zk-<cluster name>-<node name>-data"
        // One for the configuration directory...
        let mut data = BTreeMap::new();
        data.insert("spark-env.sh".to_string(), config);

        let cm_name = format!("{}-cm", self.get_pod_name(selector, true));
        let cm = create_config_map(&self.context.resource, &cm_name, data)?;
        info!("{:?}", cm);
        self.context
            .client
            .apply_patch(&cm, serde_json::to_vec(&cm)?)
            .await?;

        Ok(())
    }

    fn build_labels(
        &self,
        selector: &SparkNodeSelector,
    ) -> Result<BTreeMap<String, String>, error::Error> {
        let mut labels = BTreeMap::new();
        labels.insert(ROLE.to_string(), selector.node_type.to_string());
        labels.insert(HASH.to_string(), selector.str_hash());
        Ok(labels)
    }

    /// All pod names follow a simple pattern: <name of SparkCluster object>-<NodeType name>-<SelectorHash / UUID>
    fn get_pod_name(&self, selector: &SparkNodeSelector, use_hash: bool) -> String {
        let identifier = if use_hash {
            selector.str_hash()
        } else {
            Uuid::new_v4().as_fields().0.to_string()
        };

        format!(
            "{}-{}-{}",
            self.context.name(),
            selector.node_type.as_str(),
            identifier,
        )
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

impl ControllerStrategy for SparkStrategy {
    type Item = SparkCluster;
    type State = SparkState;

    fn finalizer_name(&self) -> String {
        return FINALIZER_NAME.to_string();
    }

    fn init_reconcile_state(&self, context: ReconciliationContext<Self::Item>) -> Self::State {
        SparkState {
            spec: context.resource.spec.clone(),
            status: context.resource.status.clone(),
            context,
            node_information: None,
        }
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
