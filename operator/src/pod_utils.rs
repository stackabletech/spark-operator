//! This module contains all Pod related methods.
use k8s_openapi::api::core::v1::Pod;
use stackable_operator::labels;
use stackable_spark_crd::SparkRole;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Value for the APP_NAME_LABEL label key
pub const APP_NAME: &str = "spark";
/// Value for the APP_MANAGED_BY_LABEL label key
pub const MANAGED_BY: &str = "stackable-spark";
/// Pod label which indicates the known master urls for a worker pod
pub const MASTER_URLS_HASH_LABEL: &str = "spark.stackable.tech/masterUrls";

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
    use stackable_spark_crd::SparkCluster;

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
