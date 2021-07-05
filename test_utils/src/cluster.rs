//! This module provides test functionality to build cluster objects from (YAML) files and matches
//! contained data definitions for testing.
//! Each cluster objects consists of a Load trait and a Data trait.
//! The goal is to provide interchangeable cluster objects for testing.
use serde::de;

/// Loads a cluster object and parses the content to T
pub trait Load<T>
where
    T: de::DeserializeOwned,
{
    type Cluster;

    fn load() -> T;
}

/// Contains object definitions that should match the provided file data
pub trait Data {
    const MASTER_1_NODE_NAME: &'static str;
    const MASTER_1_INSTANCES: usize;
    const MASTER_1_PORT: u16;
    const MASTER_1_WEB_UI_PORT: u16;
    const MASTER_1_OVERRIDE_PORT: u16;
    const MASTER_1_ROLE_GROUP: &'static str;

    const MASTER_2_NODE_NAME: &'static str;
    const MASTER_2_INSTANCES: usize;
    const MASTER_2_PORT: u16;
    const MASTER_2_WEB_UI_PORT: u16;
    const MASTER_2_ROLE_GROUP: &'static str;

    const MASTER_3_NODE_NAME: &'static str;
    const MASTER_3_INSTANCES: usize;
    const MASTER_3_ROLE_GROUP: &'static str;

    const WORKER_1_NODE_NAME: &'static str;
    const WORKER_1_INSTANCES: usize;
    const WORKER_1_CORES: usize;
    const WORKER_1_MEMORY: &'static str;
    const WORKER_1_ENV_MEMORY: &'static str;
    const WORKER_1_PORT: u16;
    const WORKER_1_WEBUI_PORT: u16;
    const WORKER_1_ROLE_GROUP: &'static str;

    const WORKER_2_NODE_NAME: &'static str;
    const WORKER_2_INSTANCES: usize;
    const WORKER_2_CORES: usize;
    const WORKER_2_MEMORY: &'static str;
    const WORKER_2_PORT: u16;
    const WORKER_2_WEBUI_PORT: u16;
    const WORKER_2_ROLE_GROUP: &'static str;

    const HISTORY_SERVER_NODE_NAME: &'static str;
    const HISTORY_SERVER_INSTANCES: usize;
    const HISTORY_SERVER_ROLE_GROUP: &'static str;

    const CLUSTER_VERSION: &'static str;
    const CLUSTER_SECRET: &'static str;
    const CLUSTER_LOG_DIR: &'static str;
    const CLUSTER_MAX_PORT_RETRIES: usize;
}

pub struct TestSparkCluster;

impl<T> Load<T> for TestSparkCluster
where
    T: de::DeserializeOwned,
{
    type Cluster = T;

    fn load() -> T {
        let file = include_str!("../data/test_spark_cluster.yaml");
        serde_yaml::from_str(file).unwrap()
    }
}

impl Data for TestSparkCluster {
    const MASTER_1_NODE_NAME: &'static str = "master_node_1";
    const MASTER_1_INSTANCES: usize = 1;
    const MASTER_1_PORT: u16 = 7078;
    const MASTER_1_WEB_UI_PORT: u16 = 8081;
    const MASTER_1_OVERRIDE_PORT: u16 = 10002;
    const MASTER_1_ROLE_GROUP: &'static str = "master_1";

    const MASTER_2_NODE_NAME: &'static str = "master_node_2";
    const MASTER_2_INSTANCES: usize = 2;
    const MASTER_2_PORT: u16 = 7079;
    const MASTER_2_WEB_UI_PORT: u16 = 8082;
    const MASTER_2_ROLE_GROUP: &'static str = "master_2";

    const MASTER_3_NODE_NAME: &'static str = "master_node_3";
    const MASTER_3_INSTANCES: usize = 1;
    const MASTER_3_ROLE_GROUP: &'static str = "master_3";

    const WORKER_1_NODE_NAME: &'static str = "worker_node_1";
    const WORKER_1_INSTANCES: usize = 1;
    const WORKER_1_CORES: usize = 1;
    const WORKER_1_MEMORY: &'static str = "2g";
    const WORKER_1_ENV_MEMORY: &'static str = "1g";
    const WORKER_1_PORT: u16 = 3031;
    const WORKER_1_WEBUI_PORT: u16 = 8083;
    const WORKER_1_ROLE_GROUP: &'static str = "1core1g";

    const WORKER_2_NODE_NAME: &'static str = "worker_node_2";
    const WORKER_2_INSTANCES: usize = 1;
    const WORKER_2_CORES: usize = 2;
    const WORKER_2_MEMORY: &'static str = "3g";
    const WORKER_2_PORT: u16 = 3032;
    const WORKER_2_WEBUI_PORT: u16 = 8084;
    const WORKER_2_ROLE_GROUP: &'static str = "2core3g";

    const HISTORY_SERVER_NODE_NAME: &'static str = "history_server_node_1";
    const HISTORY_SERVER_INSTANCES: usize = 1;
    const HISTORY_SERVER_ROLE_GROUP: &'static str = "default";

    const CLUSTER_VERSION: &'static str = "3.0.1";
    const CLUSTER_SECRET: &'static str = "secret";
    const CLUSTER_LOG_DIR: &'static str = "/tmp/spark-events";
    const CLUSTER_MAX_PORT_RETRIES: usize = 0;
}
