//! This module contains all Pod related methods.
use crate::config;
use crate::error::Error;
use k8s_openapi::api::core::v1::{
    ConfigMapVolumeSource, Container, Pod, PodSpec, Volume, VolumeMount,
};
use kube::api::Meta;
use stackable_operator::krustlet::create_tolerations;
use stackable_operator::metadata;
use stackable_spark_crd::{SparkCluster, SparkClusterSpec, SparkNode, SparkNodeType};
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
pub const MASTER_URLS_HASH_LABEL: &str = "spark.stackable.tech/masterUrls";

/// Name of the config volume to store configmap data
const CONFIG_VOLUME: &str = "config-volume";
/// Name of the logging / event volume for SparkNode logs required by the history server
const EVENT_VOLUME: &str = "event-volume";

/// Build a pod which represents a SparkNode (Master, Worker, HistoryServer) in the cluster.
///
/// # Arguments
/// * `resource` - SparkCluster
/// * `spec` - SparkClusterSpec to get some options like version or log_dir
/// * `node_type` - SparkNodeType (master/worker/history-server)
/// * `node_name` - Specific node_name (host) of the pod
/// * `hash` - NodeSelector hash
///
pub fn build_pod(
    resource: &SparkCluster,
    spec: &SparkClusterSpec,
    node_type: &SparkNodeType,
    node_name: &str,
    hash: &str,
) -> Result<Pod, Error> {
    let cluster_name = &Meta::name(resource);
    let (containers, volumes) = build_containers(&spec, node_type, cluster_name, hash);

    Ok(Pod {
        metadata: metadata::build_metadata(
            create_pod_name(cluster_name, node_type, hash),
            Some(build_labels(
                node_type,
                hash,
                &spec.version.to_string(),
                &spec.master,
            )),
            resource,
            true,
        )?,
        spec: Some(PodSpec {
            node_name: Some(node_name.to_string()),
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
/// * `spec` - SparkClusterSpec to get some options like version or log_dir
/// * `node_type` - SparkNodeType (master/worker/history-server)
/// * `cluster_name` - Current cluster name
/// * `hash` - NodeSelector hash
///
fn build_containers(
    spec: &SparkClusterSpec,
    node_type: &SparkNodeType,
    cluster_name: &str,
    hash: &str,
) -> (Vec<Container>, Vec<Volume>) {
    let image_name = format!("spark:{}", &spec.version.to_string());

    let mut command = vec![node_type.get_command(&spec.version.to_string())];
    // adapt worker command with master url(s)
    if let Some(master_urls) = config::adapt_worker_command(node_type, &spec.master) {
        command.push(master_urls);
    }

    let containers = vec![Container {
        image: Some(image_name),
        name: "spark".to_string(),
        command: Some(command),
        volume_mounts: Some(create_volume_mounts(&spec.log_dir)),
        env: Some(config::create_required_startup_env()),
        ..Container::default()
    }];

    let cm_name = config::create_config_map_name(cluster_name, node_type, hash);
    let volumes = create_volumes(&cm_name, spec.log_dir.clone());

    (containers, volumes)
}

/// Create a volume to store the spark config files and optional an event volume for spark logs
///
/// # Arguments
/// * `configmap_name` - ConfigMap name where the required spark configuration files (spark-defaults.conf and spark-env.sh) are located
/// * `log_dir` - Event/Log dir for SparkNodes. History Server reads these logs to offer metrics
///
fn create_volumes(configmap_name: &str, log_dir: Option<String>) -> Vec<Volume> {
    let mut volumes = vec![Volume {
        name: CONFIG_VOLUME.to_string(),
        config_map: Some(ConfigMapVolumeSource {
            name: Some(configmap_name.to_string()),
            ..ConfigMapVolumeSource::default()
        }),
        ..Volume::default()
    }];

    if log_dir.is_some() {
        volumes.push(Volume {
            name: EVENT_VOLUME.to_string(),
            ..Volume::default()
        })
    }

    volumes
}

/// Create volume mounts for the spark config files and optional an event dir for spark logs
///
/// # Arguments
/// * `log_dir` - Event/Log dir for SparkNodes. History Server reads these logs to offer metrics
///
fn create_volume_mounts(log_dir: &Option<String>) -> Vec<VolumeMount> {
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

/// Provide required labels for pods. We need to keep track of which workers are
/// connected to which masters. This is accomplished by hashing known master urls
/// and comparing to the pods. If the hash from pod and selector differ, that means
/// we had changes (added / removed) masters and therefore restart the workers.
///
/// # Arguments
/// * `node_type` - SparkNodeType (master/worker/history-server)
/// * `hash` - NodeSelector hash
/// * `version` - Current cluster version
/// * `master_node` - SparkNode master to retrieve master urls
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

    if node_type == &SparkNodeType::Worker {
        labels.insert(
            MASTER_URLS_HASH_LABEL.to_string(),
            get_hashed_master_urls(master_node),
        );
    }

    labels
}

/// Get all master urls and hash them. This is required to keep track of which workers
/// are connected to which masters. In case masters are added / deleted, this hash changes
/// and we need to restart the worker pods to keep them up to date with all known masters.
///
/// # Arguments
/// * `master_node` - SparkNode master to retrieve master urls for pod label hash
///
pub fn get_hashed_master_urls(master_node: &SparkNode) -> String {
    let master_urls = config::get_master_urls(master_node);
    let mut hasher = DefaultHasher::new();
    for url in master_urls {
        url.hash(&mut hasher);
    }
    hasher.finish().to_string()
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

#[cfg(test)]
mod tests {
    use super::*;
    use stackable_spark_crd::SparkNodeSelector;
    use stackable_spark_test_utils::cluster::{Load, TestSparkCluster};

    fn setup() -> SparkCluster {
        let mut cluster: SparkCluster = TestSparkCluster::load();
        // set metadata.uid
        cluster.metadata.uid = Some("123456789".to_string());
        cluster
    }

    fn get_selector_hash(
        selector: &SparkNodeSelector,
        node_type: &SparkNodeType,
        cluster_name: &str,
    ) -> String {
        let mut hasher = DefaultHasher::new();
        selector.hash(&mut hasher);
        node_type.as_str().hash(&mut hasher);
        cluster_name.hash(&mut hasher);
        hasher.finish().to_string()
    }

    #[test]
    fn test_build_master_pod() {
        let cluster = &setup();
        let spec = &cluster.spec.clone();
        let selector = &spec.master.selectors.get(0).unwrap();
        let cluster_name = &cluster.metadata.name.clone().unwrap();
        let node_type = &SparkNodeType::Master;
        let hash = &get_selector_hash(selector, node_type, cluster_name);

        let pod = build_pod(cluster, spec, node_type, &selector.node_name, hash).unwrap();

        // check labels
        assert!(pod.metadata.labels.is_some());

        let labels = pod.metadata.labels.unwrap();
        assert_eq!(labels.get(HASH_LABEL), Some(&hash.to_string()));
        assert_eq!(labels.get(TYPE_LABEL), Some(&node_type.to_string()));
        assert_eq!(labels.get(VERSION_LABEL), Some(&spec.version.to_string()));

        // check node name
        assert_eq!(
            pod.spec.clone().unwrap().node_name,
            Some(selector.node_name.to_string())
        );

        // check containers
        let containers = pod.spec.clone().unwrap().containers;
        assert_eq!(containers.len(), 1);
        let container = containers.get(0).unwrap();
        assert_eq!(
            container.command.clone().unwrap(),
            vec![node_type.get_command(&spec.version.to_string())]
        );
        // only start command for masters
        assert_eq!(container.command.clone().unwrap().len(), 1);
    }

    #[test]
    fn test_build_worker_pod() {
        let cluster = &setup();
        let spec = &cluster.spec.clone();
        let selector = &spec.worker.selectors.get(0).unwrap();
        let cluster_name = &cluster.metadata.name.clone().unwrap();
        let node_type = &SparkNodeType::Worker;
        let hash = &get_selector_hash(selector, node_type, cluster_name);

        let pod = build_pod(cluster, spec, node_type, &selector.node_name, hash).unwrap();

        // check labels
        assert!(pod.metadata.labels.is_some());

        let labels = pod.metadata.labels.unwrap();
        assert_eq!(labels.get(HASH_LABEL), Some(&hash.to_string()));
        assert_eq!(labels.get(TYPE_LABEL), Some(&node_type.to_string()));
        assert_eq!(labels.get(VERSION_LABEL), Some(&spec.version.to_string()));
        assert_eq!(
            labels.get(MASTER_URLS_HASH_LABEL),
            Some(&get_hashed_master_urls(&spec.master))
        );

        // check node name
        assert_eq!(
            pod.spec.clone().unwrap().node_name,
            Some(selector.node_name.to_string())
        );

        // check containers
        let containers = pod.spec.clone().unwrap().containers;
        assert_eq!(containers.len(), 1);

        let container = containers.get(0).unwrap();
        // start command and master urls for workers
        assert_eq!(container.command.clone().unwrap().len(), 2);
    }
}
