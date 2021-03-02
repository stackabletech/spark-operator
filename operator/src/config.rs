use k8s_openapi::api::core::v1::{ConfigMapVolumeSource, EnvVar, Volume, VolumeMount};
use stackable_spark_crd::{
    ConfigOption, SparkClusterSpec, SparkNode, SparkNodeSelector, SparkNodeType,
};
use std::collections::HashMap;

const SPARK_URL_START: &str = "spark://";

// basic for startup
const SPARK_NO_DAEMONIZE: &str = "SPARK_NO_DAEMONIZE";
const SPARK_CONF_DIR: &str = "SPARK_CONF_DIR";
// common
const SPARK_EVENT_LOG_ENABLED: &str = "spark.eventLog.enabled";
const SPARK_EVENT_LOG_DIR: &str = "spark.eventLog.dir";
const SPARK_AUTHENTICATE: &str = "spark.authenticate";
const SPARK_AUTHENTICATE_SECRET: &str = "spark.authenticate.secret";
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
/// The required URLs are in format: 'spark://<master-node-name>:<master-port'
/// Multiple masters are separated via ','
/// The master port can be configured and needs to be checked in config / env or general options.
/// Defaults to 7077 if no port is specified.
///
/// # Arguments
/// * `node_type` - SparkNodeType (master/worker/history-server)
/// * `master` - Master SparkNode containing the required settings
///
pub fn adapt_container_command(node_type: &SparkNodeType, master: &SparkNode) -> Option<String> {
    let mut master_url: String = String::new();
    // only for workers
    if node_type != &SparkNodeType::Worker {
        return None;
    }
    // get all available master selectors
    for selector in &master.selectors {
        // check in conf properties and env variables for port
        // conf properties have higher priority than env variables
        if let Some(conf) = &selector.config {
            if let Some(master) =
                search_master_port(&selector.node_name, SPARK_MASTER_PORT_CONF, conf)
            {
                master_url.push_str(master.as_str());
                continue;
            }
        } else if let Some(env) = &selector.env {
            if let Some(master) =
                search_master_port(&selector.node_name, SPARK_MASTER_PORT_ENV, env)
            {
                master_url.push_str(master.as_str());
                continue;
            }
        } else if let Some(port) = selector.master_port {
            master_url
                .push_str(format!("{}{}:{},", SPARK_URL_START, selector.node_name, port).as_str());
            continue;
        }

        // TODO: default to default value in product conf
        master_url
            .push_str(format!("{}{}:{},", SPARK_URL_START, selector.node_name, "7077").as_str());
    }

    Some(master_url)
}

/// Search for a master port in config properties or env variables
///
/// # Arguments
/// * `node_name` - Node IP / DNS address
/// * `option_name` - Name of the option to look for e.g. "SPARK_MASTER_PORT"
/// * `options` - Vec of config properties or env variables
///
fn search_master_port(
    node_name: &str,
    option_name: &str,
    options: &[ConfigOption],
) -> Option<String> {
    for option in options {
        if option.name == option_name {
            return Some(format!(
                "{}{}:{},",
                SPARK_URL_START, node_name, option.value
            ));
        }
    }
    None
}

const CONFIG_VOLUME: &str = "config-volume";
const EVENT_VOLUME: &str = "event-volume";

/// Create volume mounts for the spark config files and optional an event dir for spark logs
///
/// # Arguments
/// * `log_dir` - Event/Log dir for SparkNodes. History Server reads these logs to offer metrics
///
pub fn create_volume_mounts(log_dir: &Option<String>) -> Vec<VolumeMount> {
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

/// Create a volume to store the spark config files and optional an event volume for spark logs
///
/// # Arguments
/// * `configmap_name` - ConfigMap name where the required spark configuration files (spark-defaults.conf and spark-env.sh) are located
///
pub fn create_volumes(configmap_name: &str) -> Vec<Volume> {
    let volumes = vec![
        Volume {
            name: CONFIG_VOLUME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some(configmap_name.to_string()),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        },
        Volume {
            name: EVENT_VOLUME.to_string(),
            ..Volume::default()
        },
    ];

    volumes
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
