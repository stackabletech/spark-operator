use crate::cluster::{Data, Load, TestSparkCluster};
use k8s_openapi::api::core::v1::Pod;
use serde::de::DeserializeOwned;
use stackable_spark_crd::{SparkCluster, SparkNodeType};
use stackable_spark_operator::pod_utils::build_pod;

pub mod cluster;

// TODO: get default port from config-properties
pub const MASTER_DEFAULT_PORT: usize = 7077;

pub fn setup_test_cluster<T>() -> T
where
    T: DeserializeOwned,
{
    let cluster: T = TestSparkCluster::load();
    cluster
}

pub fn create_master_urls() -> Vec<String> {
    let master_1_url = format!(
        "{}:{}",
        TestSparkCluster::MASTER_1_NODE_NAME,
        TestSparkCluster::MASTER_1_CONFIG_PORT,
    );

    let master_2_url = format!(
        "{}:{}",
        TestSparkCluster::MASTER_2_NODE_NAME,
        TestSparkCluster::MASTER_2_PORT,
    );

    let master_3_url = format!(
        "{}:{}",
        TestSparkCluster::MASTER_3_NODE_NAME,
        MASTER_DEFAULT_PORT,
    );

    vec![master_1_url, master_2_url, master_3_url]
}

pub fn create_master_pods() -> Vec<Pod> {
    let mut spark_cluster: SparkCluster = setup_test_cluster();
    spark_cluster.metadata.uid = Some("12345".to_string());

    let master_urls = create_master_urls();

    vec![
        build_pod(
            &spark_cluster,
            TestSparkCluster::MASTER_1_NODE_NAME,
            TestSparkCluster::MASTER_1_ROLE_GROUP,
            &SparkNodeType::Master,
            master_urls.as_slice(),
        )
        .unwrap(),
        build_pod(
            &spark_cluster,
            TestSparkCluster::MASTER_2_NODE_NAME,
            TestSparkCluster::MASTER_2_ROLE_GROUP,
            &SparkNodeType::Master,
            master_urls.as_slice(),
        )
        .unwrap(),
        build_pod(
            &spark_cluster,
            TestSparkCluster::MASTER_3_NODE_NAME,
            TestSparkCluster::MASTER_3_ROLE_GROUP,
            &SparkNodeType::Master,
            master_urls.as_slice(),
        )
        .unwrap(),
    ]
}
