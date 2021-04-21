//! This module contains all methods that are responsible for setting / adapting configuration
//! parameters in the Pods and respective ConfigMaps.

use k8s_openapi::api::core::v1::{ConfigMap, EnvVar};
use stackable_operator::config_map::create_config_map;
use stackable_operator::error::OperatorResult;
use stackable_spark_common::constants::*;
use stackable_spark_crd::{
    Config, ConfigOption, MasterConfig, NodeGroup, SparkCluster, SparkNodeType,
};
use std::collections::{BTreeMap, HashMap};

/// The worker start command needs to be extended with all known master nodes and ports.
/// The required URLs for the starting command are in format: '<master-node-name>:<master-port'
/// and prefixed with 'spark://'. Multiple masters are separated via ',' e.g.:
/// spark://<master-node-name-1>:<master-port-1>,<master-node-name-2>:<master-port-2>
///
/// # Arguments
/// * `node_type` - SparkNodeType (master/worker/history-server)
/// * `master` - Master SparkNode containing the required settings
///
pub fn adapt_worker_command(
    node_type: &SparkNodeType,
    masters: &NodeGroup<MasterConfig>,
) -> Option<String> {
    let mut adapted_command: String = String::new();
    // only for workers
    if node_type != &SparkNodeType::Worker {
        return None;
    }

    let master_urls = get_master_urls(masters);
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
pub fn get_master_urls(master: &NodeGroup<MasterConfig>) -> Vec<String> {
    let mut master_urls = vec![];
    // get all available master selectors
    for selector in master.selectors.values() {
        // check in conf properties and env variables for port
        // conf properties have higher priority than env variables
        if let Some(config) = &selector.config {
            if let Some(port) = get_master_port(SPARK_MASTER_PORT_CONF, &config.spark_defaults) {
                master_urls.push(create_master_url("", &port.to_string()));
                continue;
            } else if let Some(port) = get_master_port(SPARK_MASTER_PORT_ENV, &config.spark_env_sh)
            {
                master_urls.push(create_master_url("", &port.to_string()));
                continue;
            } else if let Some(port) = &config.master_port {
                master_urls.push(create_master_url("", &port.to_string()));
                continue;
            }
        }

        // TODO: default to default value in product conf
        //master_urls.push(create_master_url(&selector.node_name, "7077"));
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
fn get_master_port(
    option_name: &str,
    user_defined_options: &Option<Vec<ConfigOption>>,
) -> Option<String> {
    if let Some(options) = user_defined_options {
        for option in options {
            if option.name == option_name {
                return Some(option.value.clone());
            }
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

/// Unroll a map into a String using a given assignment character (for writing config maps)
///
/// # Arguments
/// * `map` - Map containing option_name:option_value pairs
/// * `assignment` - Used character to assign option_value to option_name (e.g. "=", " ", ":" ...)
///
fn convert_map_to_string(map: &HashMap<String, String>, assignment: &str) -> String {
    let mut data = String::new();
    for (key, value) in map {
        data.push_str(format!("{}{}{}\n", key, assignment, value).as_str());
    }
    data
}

/// All config map names follow a simple pattern: <pod_name>-<config>
/// That means multiple pods of one selector share one and the same config map
///
/// # Arguments
///
pub fn create_config_map_name(pod_name: &str) -> String {
    format!("{}-config", pod_name)
}

/// Create all required config maps and respective config map data.
///
/// # Arguments
/// * `resource` - SparkCluster
///
pub fn create_config_maps<T>(
    resource: &SparkCluster,
    config: T,
    pod_name: &str,
) -> OperatorResult<ConfigMap>
where
    T: Config,
{
    let spark_defaults = config.get_spark_defaults_conf(&resource.spec);
    let spark_env_sh = config.get_spark_env_sh();

    let conf = convert_map_to_string(&spark_defaults, " ");
    let env = convert_map_to_string(&spark_env_sh, "=");

    let mut data = BTreeMap::new();
    data.insert(SPARK_DEFAULTS_CONF.to_string(), conf);
    data.insert(SPARK_ENV_SH.to_string(), env);

    let cm_name = create_config_map_name(pod_name);
    let cm = create_config_map(resource, &cm_name, data)?;

    Ok(cm)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pod_utils::create_pod_name;
    use stackable_spark_common::constants;
    use stackable_spark_test_utils::cluster::{Data, Load, TestSparkCluster};

    fn setup() -> SparkCluster {
        let mut cluster: SparkCluster = TestSparkCluster::load();
        // set metadata.uid
        cluster.metadata.uid = Some("123456789".to_string());
        cluster
    }

    // TODO: get default port from config-properties
    const MASTER_DEFAULT_PORT: usize = 7077;

    // #[test]
    // fn test_adapt_worker_command() {
    //     let master = &setup().spec.master;
    //     let command = adapt_worker_command(&SparkNodeType::Worker, master);
    //     let created_cmd = format!(
    //         "spark://{}:{},{}:{},{}:{},{}:{}",
    //         // For master_1 we expect the config port
    //         TestSparkCluster::MASTER_SELECTOR_1_NODE_NAME,
    //         TestSparkCluster::MASTER_SELECTOR_1_CONFIG_PORT,
    //         // For master_2 we expect the env port
    //         TestSparkCluster::MASTER_SELECTOR_2_NODE_NAME,
    //         TestSparkCluster::MASTER_SELECTOR_2_ENV_PORT,
    //         // For master_3 we expect the normal port
    //         TestSparkCluster::MASTER_SELECTOR_3_NODE_NAME,
    //         TestSparkCluster::MASTER_SELECTOR_3_PORT,
    //         // For master_4 we expect the default port
    //         TestSparkCluster::MASTER_SELECTOR_4_NODE_NAME,
    //         MASTER_DEFAULT_PORT
    //     );
    //     assert_eq!(command, Some(created_cmd));
    // }
    //
    // #[test]
    // fn test_get_master_urls() {
    //     let master = &setup().spec.master;
    //     let master_urls = get_master_urls(master);
    //     assert!(!master_urls.is_empty());
    //     // For master_1 we expect the config port
    //     assert!(master_urls.contains(&create_master_url(
    //         TestSparkCluster::MASTER_SELECTOR_1_NODE_NAME,
    //         &TestSparkCluster::MASTER_SELECTOR_1_CONFIG_PORT.to_string()
    //     )));
    //     // For master_2 we expect the env port
    //     assert!(master_urls.contains(&create_master_url(
    //         TestSparkCluster::MASTER_SELECTOR_2_NODE_NAME,
    //         &TestSparkCluster::MASTER_SELECTOR_2_ENV_PORT.to_string()
    //     )));
    //     // For master_3 we expect the normal port
    //     assert!(master_urls.contains(&create_master_url(
    //         TestSparkCluster::MASTER_SELECTOR_3_NODE_NAME,
    //         &TestSparkCluster::MASTER_SELECTOR_3_PORT.to_string()
    //     )));
    //     // For master_4 we expect the default port
    //     assert!(master_urls.contains(&create_master_url(
    //         TestSparkCluster::MASTER_SELECTOR_4_NODE_NAME,
    //         &MASTER_DEFAULT_PORT.to_string()
    //     )));
    // }
    //
    // #[test]
    // fn test_create_master_url() {
    //     assert_eq!(
    //         create_master_url(
    //             TestSparkCluster::MASTER_SELECTOR_1_NODE_NAME,
    //             &TestSparkCluster::MASTER_SELECTOR_1_CONFIG_PORT.to_string()
    //         ),
    //         format!(
    //             "{}:{}",
    //             TestSparkCluster::MASTER_SELECTOR_1_NODE_NAME,
    //             &TestSparkCluster::MASTER_SELECTOR_1_CONFIG_PORT.to_string()
    //         )
    //     );
    // }
    //
    // #[test]
    // fn test_required_startup_env() {
    //     let env_vars = create_required_startup_env();
    //     assert!(env_vars.contains(&EnvVar {
    //         name: SPARK_NO_DAEMONIZE.to_string(),
    //         value: Some("true".to_string()),
    //         ..EnvVar::default()
    //     }));
    //     assert!(env_vars.contains(&EnvVar {
    //         name: SPARK_CONF_DIR.to_string(),
    //         value: Some("{{configroot}}/conf".to_string()),
    //         ..EnvVar::default()
    //     }));
    // }
    //
    // #[test]
    // fn test_get_config_properties() {
    //     let spec = &setup().spec;
    //     let selector = &spec.master.selectors.get(0).unwrap();
    //     let config_properties = get_config_properties(spec, selector);
    //
    //     // log_dir
    //     assert!(config_properties.contains_key(constants::SPARK_EVENT_LOG_ENABLED));
    //     assert_eq!(
    //         config_properties.get(constants::SPARK_EVENT_LOG_ENABLED),
    //         Some(&"true".to_string())
    //     );
    //     assert!(config_properties.contains_key(constants::SPARK_EVENT_LOG_DIR));
    //     assert_eq!(
    //         config_properties.get(constants::SPARK_EVENT_LOG_DIR),
    //         Some(&TestSparkCluster::CLUSTER_LOG_DIR.to_string())
    //     );
    //
    //     // secret
    //     assert!(config_properties.contains_key(constants::SPARK_AUTHENTICATE));
    //     assert_eq!(
    //         config_properties.get(constants::SPARK_AUTHENTICATE),
    //         Some(&"true".to_string())
    //     );
    //     assert!(config_properties.contains_key(constants::SPARK_AUTHENTICATE_SECRET));
    //     assert_eq!(
    //         config_properties.get(constants::SPARK_AUTHENTICATE_SECRET),
    //         Some(&TestSparkCluster::CLUSTER_SECRET.to_string())
    //     );
    //
    //     // port_max_retry
    //     assert!(config_properties.contains_key(constants::SPARK_PORT_MAX_RETRIES));
    //     assert_eq!(
    //         config_properties.get(constants::SPARK_PORT_MAX_RETRIES),
    //         Some(&TestSparkCluster::CLUSTER_MAX_PORT_RETRIES.to_string())
    //     );
    //
    //     // config options
    //     assert!(config_properties.contains_key(constants::SPARK_MASTER_PORT_CONF));
    //     assert_eq!(
    //         config_properties.get(constants::SPARK_MASTER_PORT_CONF),
    //         Some(&TestSparkCluster::MASTER_SELECTOR_1_CONFIG_PORT.to_string())
    //     );
    // }
    //
    // #[test]
    // fn test_get_env_variables() {
    //     let spec = &setup().spec;
    //     let selector = &spec.worker.selectors.get(0).unwrap();
    //     let env_variables = get_env_variables(&selector);
    //
    //     // memory
    //     assert!(env_variables.contains_key(constants::SPARK_WORKER_MEMORY));
    //     assert_eq!(
    //         env_variables.get(constants::SPARK_WORKER_MEMORY),
    //         Some(&TestSparkCluster::WORKER_SELECTOR_1_ENV_MEMORY.to_string())
    //     );
    //
    //     // cores
    //     assert!(env_variables.contains_key(constants::SPARK_WORKER_CORES));
    //     assert_eq!(
    //         env_variables.get(constants::SPARK_WORKER_CORES),
    //         Some(&TestSparkCluster::WORKER_SELECTOR_1_CORES.to_string())
    //     );
    // }
    //
    // #[test]
    // fn test_create_config_map_name() {
    //     let cluster_name = &setup().metadata.name.unwrap();
    //     let pod_name = create_pod_name(cluster_name, "default", &SparkNodeType::Master.to_string());
    //
    //     assert_eq!(
    //         create_config_map_name(&pod_name),
    //         format!("{}-config", pod_name)
    //     );
    // }
    //
    // #[test]
    // fn test_create_config_maps() {
    //     let hash = "12345";
    //     let cluster = &setup();
    //     let selector = &cluster.spec.master.selectors.get(0).unwrap();
    //
    //     let config_maps = create_config_maps(&cluster, &cluster.spec, selector).unwrap();
    //
    //     assert!(!config_maps.is_empty());
    //
    //     let cm = config_maps.get(0).unwrap();
    //     let cm_data = cm.data.clone().unwrap();
    //     assert!(cm_data.contains_key(constants::SPARK_DEFAULTS_CONF));
    //     assert!(cm_data.contains_key(constants::SPARK_ENV_SH));
    //
    //     assert!(cm_data
    //         .get(constants::SPARK_DEFAULTS_CONF)
    //         .unwrap()
    //         .contains(constants::SPARK_AUTHENTICATE));
    //
    //     assert!(cm_data
    //         .get(constants::SPARK_ENV_SH)
    //         .unwrap()
    //         .contains(constants::SPARK_MASTER_PORT_ENV));
    //
    //     // TODO: add more asserts
    // }
}
