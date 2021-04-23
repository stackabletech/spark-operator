//! This module contains all methods that are responsible for setting / adapting configuration
//! parameters in the Pods and respective ConfigMaps.

use k8s_openapi::api::core::v1::{ConfigMap, EnvVar};
use stackable_operator::config_map::create_config_map;
use stackable_operator::error::OperatorResult;
use stackable_spark_common::constants::*;
use stackable_spark_crd::{Config, SparkCluster, SparkNodeType};
use std::collections::{BTreeMap, HashMap};

/// The worker start command needs to be extended with all known master nodes and ports.
/// The required URLs for the starting command are in format: '<master-node-name>:<master-port'
/// and prefixed with 'spark://'. Multiple masters are separated via ',' e.g.:
/// spark://<master-node-name-1>:<master-port-1>,<master-node-name-2>:<master-port-2>
///
/// # Arguments
/// * `node_type` - The cluster node type (e.g. master, worker, history-server)
/// * `master_urls` - Slice of master urls in format <node_name>:<port>
///
pub fn adapt_worker_command(node_type: &SparkNodeType, master_urls: &[String]) -> Option<String> {
    let mut adapted_command: String = String::new();
    // only for workers
    if node_type != &SparkNodeType::Worker {
        return None;
    }

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

/// All config map names follow a simple pattern: <pod_name>-<config>.
///
/// # Arguments
/// * `pod_name` - The name of the pod the config map belongs to
///
pub fn create_config_map_name(pod_name: &str) -> String {
    format!("{}-config", pod_name)
}

/// Create all required config maps and respective config map data.
///
/// # Arguments
/// * `resource` - SparkCluster
/// * `config` - The custom resource config
/// * `cm_name` - The desired config map name
///
pub fn create_config_map_with_data<T>(
    resource: &SparkCluster,
    config: Option<T>,
    cm_name: &str,
) -> OperatorResult<ConfigMap>
where
    T: Config,
{
    let mut spark_defaults = HashMap::new();
    let mut spark_env_sh = HashMap::new();

    if let Some(conf) = config {
        spark_defaults = conf.get_spark_defaults_conf(&resource.spec);
        spark_env_sh = conf.get_spark_env_sh();
    }

    let conf = convert_map_to_string(&spark_defaults, " ");
    let env = convert_map_to_string(&spark_env_sh, "=");

    let mut data = BTreeMap::new();
    data.insert(SPARK_DEFAULTS_CONF.to_string(), conf);
    data.insert(SPARK_ENV_SH.to_string(), env);

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
