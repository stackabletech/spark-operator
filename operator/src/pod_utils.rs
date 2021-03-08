use crate::config;
use crate::error::Error;
use k8s_openapi::api::core::v1::{
    ConfigMapVolumeSource, Container, Pod, PodSpec, Volume, VolumeMount,
};
use stackable_operator::krustlet::create_tolerations;
use stackable_operator::metadata;
use stackable_operator::reconcile::ReconciliationContext;
use stackable_spark_crd::{SparkCluster, SparkNode, SparkNodeSelector, SparkNodeType};
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use uuid::Uuid;

/// Pod label which holds the selector hash in the pods to identify which selector they belong to
pub const HASH_LABEL: &str = "spark.stackable.tech/hash";
/// Pod label which holds node role / type (master, worker, history-server) in the pods
pub const TYPE_LABEL: &str = "spark.stackable.tech/type";
/// Pod label which indicates the cluster version it was created for
pub const VERSION_LABEL: &str = "spark.stackable.tech/currentVersion";
/// Pod label which indicates the known master urls for a worker pod
pub const SPARK_MASTER_URLS_LABEL: &str = "spark.stackable.tech/masterUrls";

/// Name of the config volume to store configmap data
const CONFIG_VOLUME: &str = "config-volume";
/// Name of the logging / event volume for SparkNode logs required by the history server
const EVENT_VOLUME: &str = "event-volume";

/// Build a pod using its selector and node_type
///
/// # Arguments
/// * `context` - Reconciliation context for cluster name and resource (metadata)
/// * `node_type` - SparkNodeType (master/worker/history-server)
/// * `selector` - SparkNodeSelector which contains specific pod information
/// * `master_node` - SparkNode master to retrieve master urls for worker start commands
/// * `hash` - NodeSelector hash
/// * `version` - Current cluster version
/// * `log_dir` - Logging dir for all nodes to enable history server logs
///
pub fn build_pod(
    context: &ReconciliationContext<SparkCluster>,
    node_type: &SparkNodeType,
    selector: &SparkNodeSelector,
    master_node: &SparkNode,
    hash: &str,
    version: &str,
    log_dir: &Option<String>,
) -> Result<Pod, Error> {
    let cluster_name = &context.name();
    let (containers, volumes) =
        build_containers(master_node, cluster_name, node_type, hash, version, log_dir);

    Ok(Pod {
        metadata: metadata::build_metadata(
            create_pod_name(cluster_name, node_type, hash),
            Some(build_labels(node_type, hash, version, master_node)),
            &context.resource,
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
/// * `master_node` - SparkNode master to retrieve master urls for worker start commands
/// * `cluster_name` - Current cluster name
/// * `node_type` - SparkNodeType (master/worker/history-server)
/// * `hash` - NodeSelector hash
/// * `version` - Current cluster version
/// * `log_dir` - Logging dir for all nodes to enable history server logs
///
fn build_containers(
    master_node: &SparkNode,
    cluster_name: &str,
    node_type: &SparkNodeType,
    hash: &str,
    version: &str,
    log_dir: &Option<String>,
) -> (Vec<Container>, Vec<Volume>) {
    let image_name = format!("spark:{}", version);

    let mut command = vec![node_type.get_command(version)];
    // adapt worker command with master url(s)
    if let Some(master_urls) = config::adapt_worker_command(node_type, master_node) {
        command.push(master_urls);
    }

    let containers = vec![Container {
        image: Some(image_name),
        name: "spark".to_string(),
        command: Some(command),
        volume_mounts: Some(create_volume_mounts(log_dir)),
        env: Some(config::create_required_startup_env()),
        ..Container::default()
    }];

    let cm_name = create_config_map_name(cluster_name, node_type, hash);
    let volumes = create_volumes(&cm_name);

    (containers, volumes)
}

/// Create a volume to store the spark config files and optional an event volume for spark logs
///
/// # Arguments
/// * `configmap_name` - ConfigMap name where the required spark configuration files (spark-defaults.conf and spark-env.sh) are located
///
fn create_volumes(configmap_name: &str) -> Vec<Volume> {
    let volumes = vec![
        Volume {
            name: CONFIG_VOLUME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some(configmap_name.to_string()),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        },
        Volume {
            name: EVENT_VOLUME.to_string(),
            ..Volume::default()
        },
    ];

    volumes
}

/// Create volume mounts for the spark config files and optional an event dir for spark logs
///
/// # Arguments
/// * `log_dir` - Event/Log dir for SparkNodes. History Server reads these logs to offer metrics
///
pub fn create_volume_mounts(log_dir: &Option<String>) -> Vec<VolumeMount> {
    let mut volume_mounts = vec![VolumeMount {
        mount_path: "conf".to_string(),
        name: CONFIG_VOLUME.to_string(),
        ..VolumeMount::default()
    }];
    // if log dir is provided, create another folder for logDir
    if let Some(dir) = log_dir {
        volume_mounts.push(VolumeMount {
            mount_path: dir.clone(),
            name: EVENT_VOLUME.to_string(),
            ..VolumeMount::default()
        });
    }

    volume_mounts
}

/// Provide required labels for pods
///
/// # Arguments
/// * `node_type` - SparkNodeType (master/worker/history-server)
/// * `hash` - NodeSelector hash
/// * `version` - Current cluster version
/// * `master_node` - SparkNode master to retrieve master urls for worker start commands
///
fn build_labels(
    node_type: &SparkNodeType,
    hash: &str,
    version: &str,
    master_node: &SparkNode,
) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert(TYPE_LABEL.to_string(), node_type.to_string());
    labels.insert(HASH_LABEL.to_string(), hash.to_string());
    labels.insert(VERSION_LABEL.to_string(), version.to_string());

    let master_urls = config::get_master_urls(master_node);
    let mut hasher = DefaultHasher::new();
    for url in master_urls {
        url.hash(&mut hasher);
    }
    labels.insert(
        SPARK_MASTER_URLS_LABEL.to_string(),
        hasher.finish().to_string(),
    );

    labels
}

/// All pod names follow a simple pattern: <spark_cluster_name>-<node_type>-<selector_hash>-<UUID>
///
/// # Arguments
/// * `cluster_name` - Current cluster name
/// * `node_type` - SparkNodeType (master/worker/history-server)
/// * `hash` - NodeSelector hash
///
fn create_pod_name(cluster_name: &str, node_type: &SparkNodeType, hash: &str) -> String {
    format!(
        "{}-{}-{}-{}",
        cluster_name,
        node_type.as_str(),
        hash,
        Uuid::new_v4().as_fields().0.to_string(),
    )
}

/// All config map names follow a simple pattern: <name of SparkCluster object>-<NodeType name>-<SelectorHash>-cm
/// That means multiple pods of one selector share one and the same config map
///
/// # Arguments
/// * `cluster_name` - Current cluster name
/// * `node_type` - SparkNodeType (master/worker/history-server)
/// * `hash` - NodeSelector hash
///
pub fn create_config_map_name(cluster_name: &str, node_type: &SparkNodeType, hash: &str) -> String {
    format!("{}-{}-{}-cm", cluster_name, node_type.as_str(), hash)
}
