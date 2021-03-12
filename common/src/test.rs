pub mod resource {
    use serde::de;

    pub trait LoadCluster<T>
    where
        T: de::DeserializeOwned,
    {
        type Cluster;

        fn load_cluster() -> T;
    }

    pub trait ClusterData {
        const MASTER_SELECTOR_1_NODE_NAME: &'static str;
        const MASTER_SELECTOR_1_INSTANCES: usize;
        const MASTER_SELECTOR_1_PORT: usize;
        const MASTER_SELECTOR_1_WEB_UI_PORT: usize;
        const MASTER_SELECTOR_1_CONFIG_PORT: usize;
        const MASTER_SELECTOR_1_ENV_PORT: usize;

        const MASTER_SELECTOR_2_NODE_NAME: &'static str;
        const MASTER_SELECTOR_2_INSTANCES: usize;
        const MASTER_SELECTOR_2_PORT: usize;
        const MASTER_SELECTOR_2_WEB_UI_PORT: usize;
        const MASTER_SELECTOR_2_ENV_PORT: usize;

        const MASTER_SELECTOR_3_NODE_NAME: &'static str;
        const MASTER_SELECTOR_3_INSTANCES: usize;
        const MASTER_SELECTOR_3_PORT: usize;

        const MASTER_SELECTOR_4_NODE_NAME: &'static str;
        const MASTER_SELECTOR_4_INSTANCES: usize;

        const WORKER_SELECTOR_1_NODE_NAME: &'static str;
        const WORKER_SELECTOR_1_INSTANCES: usize;
        const WORKER_SELECTOR_1_CORES: usize;
        const WORKER_SELECTOR_1_MEMORY: &'static str;
        const WORKER_SELECTOR_1_ENV_MEMORY: &'static str;

        const WORKER_SELECTOR_2_NODE_NAME: &'static str;
        const WORKER_SELECTOR_2_INSTANCES: usize;
        const WORKER_SELECTOR_2_CORES: usize;
        const WORKER_SELECTOR_2_MEMORY: &'static str;

        const HISTORY_SERVER_SELECTOR_2_NODE_NAME: &'static str;
        const HISTORY_SERVER_SELECTOR_2_INSTANCES: usize;
        const HISTORY_SERVER_SELECTOR_2_CORES: usize;
        const HISTORY_SERVER_SELECTOR_2_MEMORY: &'static str;

        const CLUSTER_VERSION: &'static str;
        const CLUSTER_SECRET: &'static str;
        const CLUSTER_LOG_DIR: &'static str;
        const CLUSTER_MAX_PORT_RETRIES: usize;
    }

    pub struct TestSparkClusterCorrect;

    impl<T> LoadCluster<T> for TestSparkClusterCorrect
    where
        T: de::DeserializeOwned,
    {
        type Cluster = T;

        fn load_cluster() -> T {
            let yaml = include_str!("../data/test/test_spark_cluster.yaml");
            serde_yaml::from_str(yaml).unwrap()
        }
    }

    impl ClusterData for TestSparkClusterCorrect {
        const MASTER_SELECTOR_1_NODE_NAME: &'static str = "master_node_1";
        const MASTER_SELECTOR_1_INSTANCES: usize = 1;
        const MASTER_SELECTOR_1_PORT: usize = 10000;
        const MASTER_SELECTOR_1_WEB_UI_PORT: usize = 10100;
        const MASTER_SELECTOR_1_CONFIG_PORT: usize = 10001;
        const MASTER_SELECTOR_1_ENV_PORT: usize = 10002;

        const MASTER_SELECTOR_2_NODE_NAME: &'static str = "master_node_2";
        const MASTER_SELECTOR_2_INSTANCES: usize = 2;
        const MASTER_SELECTOR_2_PORT: usize = 20000;
        const MASTER_SELECTOR_2_WEB_UI_PORT: usize = 20200;
        const MASTER_SELECTOR_2_ENV_PORT: usize = 20002;

        const MASTER_SELECTOR_3_NODE_NAME: &'static str = "master_node_3";
        const MASTER_SELECTOR_3_INSTANCES: usize = 1;
        const MASTER_SELECTOR_3_PORT: usize = 30000;

        const MASTER_SELECTOR_4_NODE_NAME: &'static str = "master_node_4";
        const MASTER_SELECTOR_4_INSTANCES: usize = 1;

        const WORKER_SELECTOR_1_NODE_NAME: &'static str = "worker_node_1";
        const WORKER_SELECTOR_1_INSTANCES: usize = 1;
        const WORKER_SELECTOR_1_CORES: usize = 1;
        const WORKER_SELECTOR_1_MEMORY: &'static str = "1g";
        const WORKER_SELECTOR_1_ENV_MEMORY: &'static str = "3g";

        const WORKER_SELECTOR_2_NODE_NAME: &'static str = "worker_node_2";
        const WORKER_SELECTOR_2_INSTANCES: usize = 2;
        const WORKER_SELECTOR_2_CORES: usize = 2;
        const WORKER_SELECTOR_2_MEMORY: &'static str = "2g";

        const HISTORY_SERVER_SELECTOR_2_NODE_NAME: &'static str = "history_server_node_1";
        const HISTORY_SERVER_SELECTOR_2_INSTANCES: usize = 1;
        const HISTORY_SERVER_SELECTOR_2_CORES: usize = 2;
        const HISTORY_SERVER_SELECTOR_2_MEMORY: &'static str = "2g";

        const CLUSTER_VERSION: &'static str = "3.0.1";
        const CLUSTER_SECRET: &'static str = "secret";
        const CLUSTER_LOG_DIR: &'static str = "/tmp/spark-events";
        const CLUSTER_MAX_PORT_RETRIES: usize = 0;
    }
}
