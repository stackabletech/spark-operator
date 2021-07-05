//! This module contains all Pod related methods.
use crate::config;
use crate::error::Error;
use k8s_openapi::api::core::v1::{
    ConfigMapVolumeSource, Container, EnvVar, Pod, PodSpec, Volume, VolumeMount,
};
use stackable_operator::krustlet::create_tolerations;
use stackable_operator::labels;
use stackable_operator::metadata;
use stackable_operator::role_utils::CommonConfiguration;
use stackable_spark_crd::{SparkCluster, SparkClusterSpec, SparkRole};
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

/// Value for the APP_NAME_LABEL label key
pub const APP_NAME: &str = "spark";
/// Value for the APP_MANAGED_BY_LABEL label key
pub const MANAGED_BY: &str = "stackable-spark";
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
/// * `labels` - Required labels for the pod
/// * `node_name` - Specific node_name (host) of the pod
/// * `pod_name` - Specific pod_name
/// * `containers` - Pod containers with image, volume mounts, cli arguments and env variables
/// * `volumes` - Pod volumes
///
pub fn build_pod(
    resource: &SparkCluster,
    labels: BTreeMap<String, String>,
    node_name: &str,
    pod_name: &str,
    containers: Vec<Container>,
    volumes: Vec<Volume>,
) -> Result<Pod, Error> {
    Ok(Pod {
        metadata: metadata::build_metadata(pod_name.to_string(), Some(labels), resource, true)?,
        spec: Some(PodSpec {
            node_name: Some(node_name.to_string()),
            tolerations: create_tolerations(),
            containers,
            volumes,
            ..PodSpec::default()
        }),
        ..Pod::default()
    })
}

/// Build required pod containers
///
/// # Arguments
/// * `spec` - SparkClusterSpec to get some options like version or log_dir
/// * `role` - The cluster role (e.g. master, worker, history-server)
/// * `cm_name` - The name of the config map
/// * `master_urls` - Slice of all known master urls
/// * `cli` - Additional CLI arguments
/// * `env` - Additional ENV variables
///
pub fn build_containers_and_volumes(
    spec: &SparkClusterSpec,
    role: &SparkRole,
    cm_name: &str,
    master_urls: &[String],
    cli: Vec<String>,
    env: Vec<EnvVar>,
) -> (Vec<Container>, Vec<Volume>) {
    let image_name = format!("spark:{}", &spec.version.to_string());

    // cli command
    let command = vec![role.get_command(&spec.version.to_string())];
    // cli arguments
    let mut args = vec![];
    // adapt worker command with master url(s)
    if let Some(master_urls) = config::adapt_worker_command(role, master_urls) {
        args.push(master_urls);
    }
    // extends with cli parameter
    args.extend(cli.into_iter());

    let log_dir = if let Some(CommonConfiguration {
        config: Some(cfg), ..
    }) = &spec.config
    {
        cfg.log_dir.clone()
    } else {
        None
    };

    let containers = vec![Container {
        image: Some(image_name),
        name: "spark".to_string(),
        command,
        args,
        volume_mounts: create_volume_mounts(&log_dir),
        env,
        ..Container::default()
    }];

    let volumes = create_volumes(&cm_name, log_dir);

    (containers, volumes)
}

/// Create a volume to store the spark config files and optional an event volume for spark logs.
///
/// # Arguments
/// * `cm_name` - ConfigMap name where the required spark configuration files (spark-defaults.conf and spark-env.sh) are located
/// * `log_dir` - Event/Log dir for SparkNodes. History Server reads these logs to offer metrics
///
fn create_volumes(cm_name: &str, log_dir: Option<String>) -> Vec<Volume> {
    let mut volumes = vec![Volume {
        name: CONFIG_VOLUME.to_string(),
        config_map: Some(ConfigMapVolumeSource {
            name: Some(cm_name.to_string()),
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

/// Create volume mounts for the spark config files and optional an event dir for spark logs.
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
/// Furthermore labels for component, role group, instance and version are provided.
///
/// # Arguments
/// * `role` - The cluster role (e.g. master, worker, history-server)
/// * `role_group` - The role group of the selector
/// * `cluster_name` - The name of the cluster as specified in the custom resource
/// * `version` - The current cluster version
/// * `master_urls` - Slice of all known master urls
///
pub fn build_labels(
    role: &str,
    role_group: &str,
    cluster_name: &str,
    version: &str,
    master_urls: &[String],
) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert(String::from(labels::APP_NAME_LABEL), APP_NAME.to_string());
    labels.insert(
        labels::APP_MANAGED_BY_LABEL.to_string(),
        MANAGED_BY.to_string(),
    );
    labels.insert(String::from(labels::APP_COMPONENT_LABEL), role.to_string());
    labels.insert(
        String::from(labels::APP_ROLE_GROUP_LABEL),
        role_group.to_string(),
    );
    labels.insert(
        String::from(labels::APP_INSTANCE_LABEL),
        cluster_name.to_string(),
    );

    labels.insert(labels::APP_VERSION_LABEL.to_string(), version.to_string());

    if role == SparkRole::Worker.to_string() {
        labels.insert(
            MASTER_URLS_HASH_LABEL.to_string(),
            get_hashed_master_urls(master_urls),
        );
    }

    labels
}

/// All pod names follow a simple pattern: spark-<cluster_name>-<role_group>-<node_type>-<node_name>
///
/// # Arguments
/// * `context_name` - The name of the cluster as specified in the custom resource
/// * `role` - The cluster role (e.g. master, worker, history-server)
/// * `role_group` - The role group of the selector
/// * `node_name` - The node or host name
///
pub fn create_pod_name(
    context_name: &str,
    role: &str,
    role_group: &str,
    node_name: &str,
) -> String {
    format!(
        "{}-{}-{}-{}-{}",
        APP_NAME, context_name, role_group, role, node_name
    )
    .to_lowercase()
}

/// Get all master urls and hash them. This is required to keep track of which workers
/// are connected to which masters. In case masters are added / deleted, this hash changes
/// and we need to restart the worker pods to keep them up to date with all known masters.
///
/// # Arguments
/// * `master_urls` - Slice of all known master urls
///
pub fn get_hashed_master_urls(master_urls: &[String]) -> String {
    let mut hasher = DefaultHasher::new();
    for url in master_urls {
        url.hash(&mut hasher);
    }
    hasher.finish().to_string()
}

/// Filter all existing pods for the specified spark node type.
///
/// # Arguments
/// * `pods` - Slice of all existing pods
/// * `node_type` - The cluster node type (e.g. master, worker, history-server)
///
pub fn filter_pods_for_type(pods: &[Pod], node_type: &SparkRole) -> Vec<Pod> {
    let mut filtered_pods = Vec::new();

    for pod in pods {
        if let Some(component) = pod.metadata.labels.get(labels::APP_COMPONENT_LABEL) {
            if component == &node_type.to_string() {
                filtered_pods.push(pod.clone());
            }
        }
    }

    filtered_pods
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::adapt_worker_command;
    use kube::ResourceExt;

    #[test]
    fn test_build_master_pod() {
        let mut spark_cluster: SparkCluster = stackable_spark_test_utils::setup_test_cluster();
        spark_cluster.metadata.uid = Some("12345".to_string());

        let cluster_name = &spark_cluster.name();
        let node_type = &SparkRole::Master;

        let master_pods = stackable_spark_test_utils::create_master_pods();

        for pod in master_pods {
            assert!(!pod.metadata.labels.is_empty());

            let labels = &pod.metadata.labels;

            assert_eq!(
                labels.get(labels::APP_VERSION_LABEL),
                Some(&spark_cluster.spec.version.to_string())
            );

            assert_eq!(labels.get(labels::APP_INSTANCE_LABEL), Some(cluster_name));

            assert_eq!(
                labels.get(labels::APP_COMPONENT_LABEL),
                Some(&node_type.to_string())
            );

            // check containers
            let containers = pod.spec.clone().unwrap().containers;
            assert_eq!(containers.len(), 1);
            let container = containers.get(0).unwrap();
            assert_eq!(
                container.command,
                vec![node_type.get_command(&spark_cluster.spec.version.to_string())]
            );
            // only start command for masters
            assert_eq!(container.command.len(), 1);
        }
    }

    #[test]
    fn test_build_worker_pod() {
        let mut spark_cluster: SparkCluster = stackable_spark_test_utils::setup_test_cluster();
        spark_cluster.metadata.uid = Some("12345".to_string());

        let master_urls = stackable_spark_test_utils::create_master_urls();
        let cluster_name = &spark_cluster.name();
        let node_type = &SparkRole::Worker;

        let worker_pods = stackable_spark_test_utils::create_worker_pods();

        for pod in worker_pods {
            assert!(!pod.metadata.labels.is_empty());

            let labels = &pod.metadata.labels;

            assert_eq!(
                labels.get(labels::APP_VERSION_LABEL),
                Some(&spark_cluster.spec.version.to_string())
            );

            assert_eq!(labels.get(labels::APP_INSTANCE_LABEL), Some(cluster_name));

            assert_eq!(
                labels.get(labels::APP_COMPONENT_LABEL),
                Some(&node_type.to_string())
            );

            assert_eq!(
                labels.get(MASTER_URLS_HASH_LABEL),
                Some(&get_hashed_master_urls(master_urls.as_slice()))
            );

            // check containers
            let containers = pod.spec.unwrap().containers;
            assert_eq!(containers.len(), 1);

            let container = containers.get(0).unwrap();

            // start command and master urls for workers
            let command = container.command.clone();
            let args = container.args.clone();
            assert_eq!(command.len(), 1);
            assert_eq!(
                command,
                vec![node_type.get_command(&spark_cluster.spec.version.to_string()),]
            );

            assert_eq!(args.len(), 1);
            assert_eq!(
                args,
                vec![adapt_worker_command(node_type, master_urls.as_slice()).unwrap()]
            );
        }
    }

    #[test]
    fn test_filter_pods_for_type() {
        let pods = stackable_spark_test_utils::create_master_pods();
        assert_eq!(pods.len(), 3);
    }
}
