use k8s_openapi::api::core::v1::EnvVar;
use stackable_spark_crd::{
    ConfigOption, SparkClusterSpec, SparkNode, SparkNodeSelector, SparkNodeType,
};
use std::collections::HashMap;

// basic for startup
const SPARK_NO_DAEMONIZE: &str = "SPARK_NO_DAEMONIZE";
const SPARK_CONF_DIR: &str = "SPARK_CONF_DIR";
// common
const SPARK_EVENT_LOG_ENABLED: &str = "spark.eventLog.enabled";
const SPARK_EVENT_LOG_DIR: &str = "spark.eventLog.dir";
const SPARK_AUTHENTICATE: &str = "spark.authenticate";
const SPARK_AUTHENTICATE_SECRET: &str = "spark.authenticate.secret";
const SPARK_PORT_MAX_RETRIES: &str = "spark.port.maxRetries";
// master
const SPARK_MASTER_PORT_ENV: &str = "SPARK_MASTER_PORT";
const SPARK_MASTER_PORT_CONF: &str = "spark.master.port";
const SPARK_MASTER_WEBUI_PORT: &str = "SPARK_MASTER_WEBUI_PORT";
// worker
const SPARK_WORKER_CORES: &str = "SPARK_WORKER_CORES";
const SPARK_WORKER_MEMORY: &str = "SPARK_WORKER_MEMORY";
const SPARK_WORKER_PORT: &str = "SPARK_WORKER_PORT";
const SPARK_WORKER_WEBUI_PORT: &str = "SPARK_MASTER_WEBUI_PORT";
// history server
const SPARK_HISTORY_FS_LOG_DIRECTORY: &str = "spark.history.fs.logDirectory";
const SPARK_HISTORY_STORE_PATH: &str = "spark.history.store.path";
const SPARK_HISTORY_UI_PORT: &str = "spark.history.ui.port";

/// The worker start command needs to be extended with all known master nodes and ports.
/// The required URLs for the starting command are in format: '<master-node-name>:<master-port'
/// and prefixed with 'spark://'. Multiple masters are separated via ',' e.g.:
/// spark://<master-node-name-1>:<master-port-1>,<master-node-name-2>:<master-port-2>
///
/// # Arguments
/// * `node_type` - SparkNodeType (master/worker/history-server)
/// * `master` - Master SparkNode containing the required settings
///
pub fn adapt_worker_command(node_type: &SparkNodeType, master: &SparkNode) -> Option<String> {
    let mut adapted_command: String = String::new();
    // only for workers
    if node_type != &SparkNodeType::Worker {
        return None;
    }

    let master_urls = get_master_urls(master);
    for url in master_urls {
        if adapted_command.is_empty() {
            adapted_command.push_str("spark://");
        } else {
            adapted_command.push(',');
        }
        adapted_command.push_str(url.as_str());
    }

    Some(adapted_command)
}

/// The master port can be configured and needs to be checked in config / env or general options.
/// Defaults to 7077 if no port is specified.
///
/// # Arguments
/// * `master` - Master SparkNode containing the required node_name and port settings
///
pub fn get_master_urls(master: &SparkNode) -> Vec<String> {
    let mut master_urls = vec![];
    // get all available master selectors
    for selector in &master.selectors {
        // check in conf properties and env variables for port
        // conf properties have higher priority than env variables
        if let Some(conf) = &selector.config {
            if let Some(port) = get_master_port(SPARK_MASTER_PORT_CONF, conf) {
                master_urls.push(create_master_url(&selector.node_name, &port.to_string()));
                continue;
            }
        } else if let Some(env) = &selector.env {
            if let Some(port) = get_master_port(SPARK_MASTER_PORT_ENV, env) {
                master_urls.push(create_master_url(&selector.node_name, &port.to_string()));
                continue;
            }
        } else if let Some(port) = selector.master_port {
            master_urls.push(create_master_url(&selector.node_name, &port.to_string()));
            continue;
        }

        // TODO: default to default value in product conf
        master_urls.push(create_master_url(&selector.node_name, "7077"));
    }

    master_urls
}

/// Create master url in format: <node_name>:<port>
///
/// # Arguments
/// * `node_name` - Master node_name / host name
/// * `port` - Port on which the master is running
///
fn create_master_url(node_name: &str, port: &str) -> String {
    format!("{}:{}", node_name, port)
}

/// Search for a master port in config properties or env variables
///
/// # Arguments
/// * `option_name` - Name of the option to look for e.g. "SPARK_MASTER_PORT"
/// * `options` - Vec of config properties or env variables
///
fn get_master_port(option_name: &str, options: &[ConfigOption]) -> Option<String> {
    for option in options {
        if option.name == option_name {
            return Some(option.value.clone());
        }
    }
    None
}

/// The SPARK_CONFIG_DIR and SPARK_NO_DAEMONIZE must be provided as env variable in the container.
/// SPARK_CONFIG_DIR must be available before the start up of the nodes (master, worker, history-server) to point to our custom configuration.
/// SPARK_NO_DAEMONIZE stops the node processes to be started in the background, which causes the agent to lose track of the processes.
/// The Agent then assumes the processes stopped or died and recreates them over and over again.
pub fn create_required_startup_env() -> Vec<EnvVar> {
    vec![
        EnvVar {
            name: SPARK_NO_DAEMONIZE.to_string(),
            value: Some("true".to_string()),
            ..EnvVar::default()
        },
        EnvVar {
            name: SPARK_CONF_DIR.to_string(),
            value: Some("{{configroot}}/conf".to_string()),
            ..EnvVar::default()
        },
    ]
}

/// Get all required configuration options
/// 1) from spec
/// 2) from node
/// 3) from selector
/// 4) from config properties
///
/// # Arguments
/// * `spec` - SparkCluster spec for common properties
/// * `selector` - SparkClusterSelector containing desired config properties
///
pub fn get_config_properties(
    spec: &SparkClusterSpec,
    selector: &SparkNodeSelector,
) -> HashMap<String, String> {
    let mut config: HashMap<String, String> = HashMap::new();

    let log_dir = &spec.log_dir.clone().unwrap_or_else(|| "/tmp".to_string());

    // common
    config.insert(SPARK_EVENT_LOG_ENABLED.to_string(), "true".to_string());
    config.insert(SPARK_EVENT_LOG_DIR.to_string(), log_dir.to_string());

    if let Some(secret) = &spec.secret {
        config.insert(SPARK_AUTHENTICATE.to_string(), "true".to_string());
        config.insert(SPARK_AUTHENTICATE_SECRET.to_string(), secret.to_string());
    }

    if let Some(max_port_retries) = &spec.max_port_retries {
        config.insert(
            SPARK_PORT_MAX_RETRIES.to_string(),
            max_port_retries.to_string(),
        );
    }

    // history server
    config.insert(
        SPARK_HISTORY_FS_LOG_DIRECTORY.to_string(),
        log_dir.to_string(),
    );
    if let Some(store_path) = &selector.store_path {
        config.insert(SPARK_HISTORY_STORE_PATH.to_string(), store_path.to_string());
    }
    if let Some(port) = &selector.history_ui_port {
        config.insert(SPARK_HISTORY_UI_PORT.to_string(), port.to_string());
    }

    // Add config options -> may override previous settings which is what we want
    // These options refer to the CRD "selector.config" field and allow the user to adapt
    // the cluster with options not offered by the operator.
    // E.g. config.name: "spark.authenticate", config.value = "true"
    if let Some(configuration) = &selector.config {
        for config_option in configuration {
            config.insert(config_option.name.clone(), config_option.value.clone());
        }
    }

    // TODO: validate and add

    config
}

/// Get all required environment variables
/// 1) from spec
/// 2) from node
/// 3) from selector
/// 4) from environment variables
///
/// # Arguments
/// * `selector` - SparkClusterSelector containing desired env variables
///
pub fn get_env_variables(selector: &SparkNodeSelector) -> HashMap<String, String> {
    let mut config: HashMap<String, String> = HashMap::new();

    // master
    if let Some(port) = &selector.master_port {
        config.insert(SPARK_MASTER_PORT_ENV.to_string(), port.to_string());
    }
    if let Some(web_ui_port) = &selector.master_web_ui_port {
        config.insert(SPARK_MASTER_WEBUI_PORT.to_string(), web_ui_port.to_string());
    }

    // worker
    if let Some(cores) = &selector.cores {
        config.insert(SPARK_WORKER_CORES.to_string(), cores.to_string());
    }
    if let Some(memory) = &selector.memory {
        config.insert(SPARK_WORKER_MEMORY.to_string(), memory.to_string());
    }
    if let Some(port) = &selector.worker_port {
        config.insert(SPARK_WORKER_PORT.to_string(), port.to_string());
    }
    if let Some(web_ui_port) = &selector.worker_web_ui_port {
        config.insert(SPARK_WORKER_WEBUI_PORT.to_string(), web_ui_port.to_string());
    }

    // Add environment variables -> may override previous settings which is what we want
    // These options refer to the CRD "selector.env" field and allow the user to adapt
    // the cluster with options not offered by the operator.
    // E.g. env.name: "SPARK_MASTER_PORT", env.value: "12345"
    if let Some(env_variables) = &selector.env {
        for config_option in env_variables {
            config.insert(config_option.name.clone(), config_option.value.clone());
        }
    }

    // TODO: validate and add

    config
}

/// Unroll a map into a String using a given assignment character (for writing config maps)
///
/// # Arguments
/// * `map` - Map containing option_name:option_value pairs
/// * `assignment` - Used character to assign option_value to option_name (e.g. "=", " ", ":" ...)
///
pub fn convert_map_to_string(map: &HashMap<String, String>, assignment: &str) -> String {
    let mut data = String::new();
    for (key, value) in map {
        data.push_str(format!("{}{}{}\n", key, assignment, value).as_str());
    }
    data
}

#[cfg(test)]
mod tests {
    use super::*;
    use stackable_spark_crd::SparkVersion;

    const MASTER_1_NODE_NAME: &str = "master_1";
    const MASTER_1_PORT_CONF_VAL: &str = "10000";
    const MASTER_1_PORT_ENV_VAL: &str = "11000";
    const MASTER_1_PORT: usize = 12000;
    const MASTER_1_WEB_UI_PORT: usize = 13000;
    const MASTER_1_CORES: usize = 1;
    const MASTER_1_MEMORY: &str = "2g";

    const CLUSTER_SECRET: &str = "secret";
    const CLUSTER_LOG_DIR: &str = "/tmp/spark-events";
    const CLUSTER_MAX_PORT_RETRIES: usize = 0;

    const MASTER_2_NODE_NAME: &str = "master_2";
    const MASTER_2_PORT: usize = 22000;

    const MASTER_3_NODE_NAME: &str = "master_3";
    const MASTER_3_DEFAULT_PORT: usize = 7077;

    fn create_spec() -> SparkClusterSpec {
        SparkClusterSpec {
            master: SparkNode {
                selectors: vec![create_selector_1()],
            },
            worker: SparkNode {
                selectors: vec![create_selector_1()],
            },
            history_server: None,
            version: SparkVersion::v3_0_1,
            secret: Some(CLUSTER_SECRET.to_string()),
            log_dir: Some(CLUSTER_LOG_DIR.to_string()),
            max_port_retries: Some(CLUSTER_MAX_PORT_RETRIES),
        }
    }

    fn create_selector_1() -> SparkNodeSelector {
        SparkNodeSelector {
            node_name: MASTER_1_NODE_NAME.to_string(),
            instances: 1,
            config: Some(vec![ConfigOption {
                name: SPARK_MASTER_PORT_CONF.to_string(),
                value: MASTER_1_PORT_CONF_VAL.to_string(),
            }]),
            env: Some(vec![ConfigOption {
                name: SPARK_MASTER_PORT_ENV.to_string(),
                value: MASTER_1_PORT_ENV_VAL.to_string(),
            }]),
            master_port: Some(MASTER_1_PORT),
            master_web_ui_port: Some(MASTER_1_WEB_UI_PORT),
            cores: Some(MASTER_1_CORES),
            memory: Some(MASTER_1_MEMORY.to_string()),
            ..SparkNodeSelector::default()
        }
    }

    fn create_master_node() -> SparkNode {
        SparkNode {
            selectors: vec![
                create_selector_1(),
                SparkNodeSelector {
                    node_name: MASTER_2_NODE_NAME.to_string(),
                    instances: 1,
                    master_port: Some(MASTER_2_PORT),
                    ..SparkNodeSelector::default()
                },
                SparkNodeSelector {
                    node_name: MASTER_3_NODE_NAME.to_string(),
                    instances: 1,
                    ..SparkNodeSelector::default()
                },
            ],
        }
    }

    #[test]
    fn test_get_master_urls() {
        let master = &create_master_node();
        let master_urls = get_master_urls(master);
        assert!(!master_urls.is_empty());
        // For master_1 we expect the config port
        assert!(master_urls.contains(&create_master_url(
            MASTER_1_NODE_NAME,
            MASTER_1_PORT_CONF_VAL
        )));
        // For master_2 we expect the masterPort
        assert!(master_urls.contains(&create_master_url(
            MASTER_2_NODE_NAME,
            &MASTER_2_PORT.to_string()
        )));
        // For master_3 we expect the default port
        assert!(master_urls.contains(&create_master_url(
            MASTER_3_NODE_NAME,
            &MASTER_3_DEFAULT_PORT.to_string()
        )));
    }

    #[test]
    fn test_adapt_worker_command() {
        let master = &create_master_node();
        let command = adapt_worker_command(&SparkNodeType::Worker, master);
        let created_cmd = format!(
            "spark://{}:{},{}:{},{}:{}",
            // For master_1 we expect the config port
            MASTER_1_NODE_NAME,
            MASTER_1_PORT_CONF_VAL,
            // For master_2 we expect the masterPort
            MASTER_2_NODE_NAME,
            MASTER_2_PORT,
            // For master_3 we expect the default port
            MASTER_3_NODE_NAME,
            MASTER_3_DEFAULT_PORT
        );
        assert_eq!(command, Some(created_cmd));
    }

    #[test]
    fn test_create_master_url() {
        assert_eq!(
            create_master_url(MASTER_1_NODE_NAME, MASTER_1_PORT_CONF_VAL),
            format!("{}:{}", MASTER_1_NODE_NAME, MASTER_1_PORT_CONF_VAL)
        );
    }

    #[test]
    fn test_required_startup_env() {
        let env_vars = create_required_startup_env();
        assert!(env_vars.contains(&EnvVar {
            name: SPARK_NO_DAEMONIZE.to_string(),
            value: Some("true".to_string()),
            ..EnvVar::default()
        }));
        assert!(env_vars.contains(&EnvVar {
            name: SPARK_CONF_DIR.to_string(),
            value: Some("{{configroot}}/conf".to_string()),
            ..EnvVar::default()
        }));
    }

    #[test]
    fn test_get_config_properties() {
        let spec = &create_spec();
        let selector = &create_selector_1();
        let config_properties = get_config_properties(spec, selector);

        // log_dir
        assert!(config_properties.contains_key(SPARK_EVENT_LOG_ENABLED));
        assert_eq!(
            config_properties.get(SPARK_EVENT_LOG_ENABLED),
            Some(&"true".to_string())
        );
        assert!(config_properties.contains_key(SPARK_EVENT_LOG_DIR));
        assert_eq!(
            config_properties.get(SPARK_EVENT_LOG_DIR),
            Some(&CLUSTER_LOG_DIR.to_string())
        );

        // secret
        assert!(config_properties.contains_key(SPARK_AUTHENTICATE));
        assert_eq!(
            config_properties.get(SPARK_AUTHENTICATE),
            Some(&"true".to_string())
        );
        assert!(config_properties.contains_key(SPARK_AUTHENTICATE_SECRET));
        assert_eq!(
            config_properties.get(SPARK_AUTHENTICATE_SECRET),
            Some(&CLUSTER_SECRET.to_string())
        );

        // port_max_retry
        assert!(config_properties.contains_key(SPARK_PORT_MAX_RETRIES));
        assert_eq!(
            config_properties.get(SPARK_PORT_MAX_RETRIES),
            Some(&CLUSTER_MAX_PORT_RETRIES.to_string())
        );

        // config options
        assert!(config_properties.contains_key(SPARK_MASTER_PORT_CONF));
        assert_eq!(
            config_properties.get(SPARK_MASTER_PORT_CONF),
            Some(&MASTER_1_PORT_CONF_VAL.to_string())
        );
    }

    #[test]
    fn test_get_env_variables() {
        let selector = &create_selector_1();
        let env_variables = get_env_variables(selector);

        // web ui port
        assert!(env_variables.contains_key(SPARK_MASTER_WEBUI_PORT));
        assert_eq!(
            env_variables.get(SPARK_MASTER_WEBUI_PORT),
            Some(&MASTER_1_WEB_UI_PORT.to_string())
        );

        // cores
        assert!(env_variables.contains_key(SPARK_WORKER_CORES));
        assert_eq!(
            env_variables.get(SPARK_WORKER_CORES),
            Some(&MASTER_1_CORES.to_string())
        );

        // memory
        assert!(env_variables.contains_key(SPARK_WORKER_MEMORY));
        assert_eq!(
            env_variables.get(SPARK_WORKER_MEMORY),
            Some(&MASTER_1_MEMORY.to_string())
        );
    }
}
