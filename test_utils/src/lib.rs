use crate::cluster::{Data, Load, TestSparkCluster};
use k8s_openapi::api::core::v1::Pod;
use kube::ResourceExt;
use serde::de::DeserializeOwned;
use stackable_spark_crd::{SparkCluster, SparkRole};
use stackable_spark_operator::pod_utils::{build_containers_and_volumes, build_labels, build_pod};

pub mod cluster;
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
        TestSparkCluster::MASTER_1_OVERRIDE_PORT,
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

    let (master_1_containers, master_1_volumes) = build_containers_and_volumes(
        &spark_cluster.spec,
        &SparkRole::Master,
        "master_1_pod_conf",
        &master_urls.as_slice(),
        Vec::new(),
        Vec::new(),
    );

    let (master_2_containers, master_2_volumes) = build_containers_and_volumes(
        &spark_cluster.spec,
        &SparkRole::Master,
        "master_2_pod_conf",
        &master_urls.as_slice(),
        Vec::new(),
        Vec::new(),
    );

    let (master_3_containers, master_3_volumes) = build_containers_and_volumes(
        &spark_cluster.spec,
        &SparkRole::Master,
        "master_3_pod_conf",
        &master_urls.as_slice(),
        Vec::new(),
        Vec::new(),
    );

    vec![
        build_pod(
            &spark_cluster,
            build_labels(
                &SparkRole::Master.to_string(),
                TestSparkCluster::MASTER_1_ROLE_GROUP,
                &spark_cluster.name(),
                &spark_cluster.spec.version.to_string(),
                master_urls.as_slice(),
            ),
            TestSparkCluster::MASTER_1_NODE_NAME,
            "master_1_pod",
            master_1_containers,
            master_1_volumes,
        )
        .unwrap(),
        build_pod(
            &spark_cluster,
            build_labels(
                &SparkRole::Master.to_string(),
                TestSparkCluster::MASTER_2_ROLE_GROUP,
                &spark_cluster.name(),
                &spark_cluster.spec.version.to_string(),
                master_urls.as_slice(),
            ),
            TestSparkCluster::MASTER_2_NODE_NAME,
            "master_2_pod",
            master_2_containers,
            master_2_volumes,
        )
        .unwrap(),
        build_pod(
            &spark_cluster,
            build_labels(
                &SparkRole::Master.to_string(),
                TestSparkCluster::MASTER_3_ROLE_GROUP,
                &spark_cluster.name(),
                &spark_cluster.spec.version.to_string(),
                master_urls.as_slice(),
            ),
            TestSparkCluster::MASTER_3_NODE_NAME,
            "master_3_pod",
            master_3_containers,
            master_3_volumes,
        )
        .unwrap(),
    ]
}

pub fn create_worker_pods() -> Vec<Pod> {
    let mut spark_cluster: SparkCluster = setup_test_cluster();
    spark_cluster.metadata.uid = Some("12345".to_string());

    let master_urls = create_master_urls();

    let (worker_1_containers, worker_1_volumes) = build_containers_and_volumes(
        &spark_cluster.spec,
        &SparkRole::Worker,
        "worker_1_pod_conf",
        &master_urls.as_slice(),
        Vec::new(),
        Vec::new(),
    );

    let (worker_2_containers, worker_2_volumes) = build_containers_and_volumes(
        &spark_cluster.spec,
        &SparkRole::Worker,
        "worker_2_pod_conf",
        &master_urls.as_slice(),
        Vec::new(),
        Vec::new(),
    );

    vec![
        build_pod(
            &spark_cluster,
            build_labels(
                &SparkRole::Worker.to_string(),
                TestSparkCluster::WORKER_1_ROLE_GROUP,
                &spark_cluster.name(),
                &spark_cluster.spec.version.to_string(),
                master_urls.as_slice(),
            ),
            TestSparkCluster::WORKER_1_NODE_NAME,
            "worker_1_pod",
            worker_1_containers,
            worker_1_volumes,
        )
        .unwrap(),
        build_pod(
            &spark_cluster,
            build_labels(
                &SparkRole::Worker.to_string(),
                TestSparkCluster::WORKER_2_ROLE_GROUP,
                &spark_cluster.name(),
                &spark_cluster.spec.version.to_string(),
                master_urls.as_slice(),
            ),
            TestSparkCluster::WORKER_2_NODE_NAME,
            "worker_2_pod",
            worker_2_containers,
            worker_2_volumes,
        )
        .unwrap(),
    ]
}
