pub mod resource {
    use crate::constants;
    use serde::de;

    pub trait LoadResource<T: de::DeserializeOwned> {
        type Resource;
        fn load_resource() -> Self::Resource;
    }

    pub struct SparkClusterComplete;

    impl<T: de::DeserializeOwned> LoadResource<T> for SparkClusterComplete {
        type Resource = T;

        fn load_resource() -> Self::Resource {
            let yaml = include_str!("../data/test/sparkcluster_complete.yaml");
            serde_yaml::from_str(yaml).unwrap()
        }
    }

    impl SparkClusterComplete {
        pub const MASTER_SELECTOR_1_NODE_NAME: &'static str = "master_node_1";
        pub const MASTER_SELECTOR_1_INSTANCES: usize = 1;
        pub const MASTER_SELECTOR_1_PORT: usize = 10000;
        pub const MASTER_SELECTOR_1_WEB_UI_PORT: usize = 10100;
        pub const MASTER_SELECTOR_1_CONFIG_MASTER_PORT: (&'static str, &'static str) =
            (constants::SPARK_MASTER_PORT_CONF, "10001");
        pub const MASTER_SELECTOR_1_ENV_PORT: (&'static str, &'static str) =
            (constants::SPARK_MASTER_PORT_ENV, "10002");

        pub const MASTER_SELECTOR_2_NODE_NAME: &'static str = "master_node_2";
        pub const MASTER_SELECTOR_2_INSTANCES: usize = 2;
        pub const MASTER_SELECTOR_2_MASTER_PORT: usize = 20000;
        pub const MASTER_SELECTOR_2_MASTER_WEB_UI_PORT: usize = 20200;

        pub const WORKER_SELECTOR_1_NODE_NAME: &'static str = "worker_node_1";
        pub const WORKER_SELECTOR_1_INSTANCES: usize = 1;
        pub const WORKER_SELECTOR_1_CORES: usize = 1;
        pub const WORKER_SELECTOR_1_MEMORY: &'static str = "1g";
        pub const WORKER_SELECTOR_1_ENV_MEMORY: (&'static str, &'static str) =
            (constants::SPARK_WORKER_MEMORY, "3g");

        pub const WORKER_SELECTOR_2_NODE_NAME: &'static str = "worker_node_2";
        pub const WORKER_SELECTOR_2_INSTANCES: usize = 2;
        pub const WORKER_SELECTOR_2_CORES: usize = 2;
        pub const WORKER_SELECTOR_2_MEMORY: &'static str = "2g";

        pub const HISTORY_SERVER_SELECTOR_2_NODE_NAME: &'static str = "history_server_node_1";
        pub const HISTORY_SERVER_SELECTOR_2_INSTANCES: usize = 1;
        pub const HISTORY_SERVER_SELECTOR_2_CORES: usize = 2;
        pub const HISTORY_SERVER_SELECTOR_2_MEMORY: &'static str = "2g";

        pub const CLUSTER_VERSION: &'static str = "3.0.1";
        pub const CLUSTER_SECRET: &'static str = "secret";
        pub const CLUSTER_LOG_DIR: &'static str = "/tmp/spark-events";
        pub const CLUSTER_MAX_PORT_RETRIES: usize = 0;
    }
}
