//! This module contains all Pod related methods.
use stackable_operator::k8s_openapi::api::core::v1::Pod;
use stackable_operator::labels;
use stackable_spark_crd::SparkRole;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Value for the APP_NAME_LABEL label key
pub const APP_NAME: &str = "spark";
/// Value for the APP_MANAGED_BY_LABEL label key
pub const MANAGED_BY: &str = "spark-operator";
/// Pod label which indicates the known master urls for a worker pod
pub const MASTER_URLS_HASH_LABEL: &str = "spark.stackable.tech/masterUrls";

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
        if let Some(labels) = &pod.metadata.labels {
            if let Some(component) = labels.get(labels::APP_COMPONENT_LABEL) {
                if component == &node_type.to_string() {
                    filtered_pods.push(pod.clone());
                }
            }
        }
    }

    filtered_pods
}
