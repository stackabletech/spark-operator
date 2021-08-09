use crate::cluster::{Data, Load, TestSparkCluster};
use k8s_openapi::api::core::v1::Pod;
use serde::de::DeserializeOwned;
use stackable_operator::builder::{ContainerBuilder, ObjectMetaBuilder, PodBuilder};
use stackable_spark_crd::{SparkCluster, SparkRole};
use stackable_spark_operator::config::adapt_worker_command;
use stackable_spark_operator::pod_utils::{
    get_hashed_master_urls, APP_NAME, MASTER_URLS_HASH_LABEL,
};

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
    let version = &spark_cluster.spec.version.to_string();

    vec![
        PodBuilder::new()
            .metadata(
                ObjectMetaBuilder::new()
                    .name("master_1_pod")
                    .with_recommended_labels(
                        &spark_cluster,
                        APP_NAME,
                        version,
                        &SparkRole::Master.to_string(),
                        TestSparkCluster::MASTER_1_ROLE_GROUP,
                    )
                    .ownerreference_from_resource(&spark_cluster, Some(true), Some(false))
                    .unwrap()
                    .build()
                    .unwrap(),
            )
            .add_stackable_agent_tolerations()
            .add_container(
                ContainerBuilder::new("spark")
                    .image(format!("spark:{}", version))
                    .command(vec![SparkRole::Master.get_command(version)])
                    .add_configmapvolume("master_1_pod_config".to_string(), "conf".to_string())
                    .build(),
            )
            .node_name(TestSparkCluster::MASTER_1_NODE_NAME)
            .build()
            .unwrap(),
        PodBuilder::new()
            .metadata(
                ObjectMetaBuilder::new()
                    .name("master_2_pod")
                    .with_recommended_labels(
                        &spark_cluster,
                        APP_NAME,
                        version,
                        &SparkRole::Master.to_string(),
                        TestSparkCluster::MASTER_2_ROLE_GROUP,
                    )
                    .ownerreference_from_resource(&spark_cluster, Some(true), Some(false))
                    .unwrap()
                    .build()
                    .unwrap(),
            )
            .add_stackable_agent_tolerations()
            .add_container(
                ContainerBuilder::new("spark")
                    .image(format!("spark:{}", version))
                    .command(vec![SparkRole::Master.get_command(version)])
                    .add_configmapvolume("master_2_pod_config".to_string(), "conf".to_string())
                    .build(),
            )
            .node_name(TestSparkCluster::MASTER_2_NODE_NAME)
            .build()
            .unwrap(),
        PodBuilder::new()
            .metadata(
                ObjectMetaBuilder::new()
                    .name("master_3_pod")
                    .with_recommended_labels(
                        &spark_cluster,
                        APP_NAME,
                        version,
                        &SparkRole::Master.to_string(),
                        TestSparkCluster::MASTER_3_ROLE_GROUP,
                    )
                    .ownerreference_from_resource(&spark_cluster, Some(true), Some(false))
                    .unwrap()
                    .build()
                    .unwrap(),
            )
            .add_stackable_agent_tolerations()
            .add_container(
                ContainerBuilder::new("spark")
                    .image(format!("spark:{}", version))
                    .command(vec![SparkRole::Master.get_command(version)])
                    .add_configmapvolume("master_3_pod_config".to_string(), "conf".to_string())
                    .build(),
            )
            .node_name(TestSparkCluster::MASTER_3_NODE_NAME)
            .build()
            .unwrap(),
    ]
}

pub fn create_worker_pods() -> Vec<Pod> {
    let mut spark_cluster: SparkCluster = setup_test_cluster();
    spark_cluster.metadata.uid = Some("12345".to_string());
    let version = &spark_cluster.spec.version.to_string();

    let master_urls = create_master_urls();

    vec![
        PodBuilder::new()
            .metadata(
                ObjectMetaBuilder::new()
                    .name("worker_1_pod")
                    .with_recommended_labels(
                        &spark_cluster,
                        APP_NAME,
                        version,
                        &SparkRole::Worker.to_string(),
                        TestSparkCluster::WORKER_1_ROLE_GROUP,
                    )
                    .with_label(
                        MASTER_URLS_HASH_LABEL,
                        &get_hashed_master_urls(&master_urls),
                    )
                    .ownerreference_from_resource(&spark_cluster, Some(true), Some(false))
                    .unwrap()
                    .build()
                    .unwrap(),
            )
            .add_stackable_agent_tolerations()
            .add_container(
                ContainerBuilder::new("spark")
                    .image(format!("spark:{}", version))
                    .command(vec![SparkRole::Worker.get_command(version)])
                    .args(vec![adapt_worker_command(
                        &SparkRole::Worker,
                        master_urls.as_slice(),
                    )
                    .unwrap()])
                    .add_configmapvolume("worker_1_pod_config".to_string(), "conf".to_string())
                    .build(),
            )
            .node_name(TestSparkCluster::WORKER_1_NODE_NAME)
            .build()
            .unwrap(),
        PodBuilder::new()
            .metadata(
                ObjectMetaBuilder::new()
                    .name("worker_2_pod")
                    .with_recommended_labels(
                        &spark_cluster,
                        APP_NAME,
                        version,
                        &SparkRole::Worker.to_string(),
                        TestSparkCluster::WORKER_2_ROLE_GROUP,
                    )
                    .with_label(
                        MASTER_URLS_HASH_LABEL,
                        &get_hashed_master_urls(&master_urls),
                    )
                    .ownerreference_from_resource(&spark_cluster, Some(true), Some(false))
                    .unwrap()
                    .build()
                    .unwrap(),
            )
            .add_stackable_agent_tolerations()
            .add_container(
                ContainerBuilder::new("spark")
                    .image(format!("spark:{}", version))
                    .command(vec![SparkRole::Worker.get_command(version)])
                    .args(vec![adapt_worker_command(
                        &SparkRole::Worker,
                        master_urls.as_slice(),
                    )
                    .unwrap()])
                    .add_configmapvolume("worker_2_pod_config".to_string(), "conf".to_string())
                    .build(),
            )
            .node_name(TestSparkCluster::WORKER_2_NODE_NAME)
            .build()
            .unwrap(),
    ]
}
