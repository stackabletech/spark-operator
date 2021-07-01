//! This module contains all methods that are responsible for setting / adapting configuration
//! parameters in the Pods and respective ConfigMaps.

use k8s_openapi::api::core::v1::{ConfigMap, EnvVar};
use stackable_operator::config_map::create_config_map;
use stackable_operator::error::OperatorResult;
use stackable_spark_common::constants::*;
use stackable_spark_crd::{SparkCluster, SparkRole};
use std::collections::BTreeMap;

/// The worker start command needs to be extended with all known master nodes and ports.
/// The required URLs for the starting command are in format: '<master-node-name>:<master-port'
/// and prefixed with 'spark://'. Multiple masters are separated via ',' e.g.:
/// spark://<master-node-name-1>:<master-port-1>,<master-node-name-2>:<master-port-2>
///
/// # Arguments
/// * `node_type` - The cluster node type (e.g. master, worker, history-server)
/// * `master_urls` - Slice of master urls in format <node_name>:<port>
///
pub fn adapt_worker_command(node_type: &SparkRole, master_urls: &[String]) -> Option<String> {
    let mut adapted_command: String = String::new();
    // only for workers
    if node_type != &SparkRole::Worker {
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
fn convert_map_to_string(map: &BTreeMap<String, String>, assignment: &str) -> String {
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
{
    // let mut spark_defaults = BTreeMap::new();
    // let mut spark_env_sh = BTreeMap::new();
    //
    // if let Some(conf) = config {
    //     spark_defaults = conf.get_spark_defaults_conf(&resource.spec);
    //     spark_env_sh = conf.get_spark_env_sh();
    // }
    //
    // let conf = convert_map_to_string(&spark_defaults, " ");
    // let env = convert_map_to_string(&spark_env_sh, "=");
    //
    // let mut data = BTreeMap::new();
    // data.insert(SPARK_DEFAULTS_CONF.to_string(), conf);
    // data.insert(SPARK_ENV_SH.to_string(), env);
    //
    // let cm = create_config_map(resource, &cm_name, data)?;
    //
    // Ok(cm)
    Ok(ConfigMap::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use stackable_spark_common::constants;
    use stackable_spark_test_utils::cluster::{Data, TestSparkCluster};

    #[test]
    fn test_adapt_worker_command() {
        let master_urls = stackable_spark_test_utils::create_master_urls();

        let command = adapt_worker_command(&SparkRole::Worker, master_urls.as_slice()).unwrap();

        for url in master_urls {
            assert!(command.contains(&url));
        }
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
    fn test_get_spark_defaults() {
        let spark_cluster: SparkCluster = stackable_spark_test_utils::setup_test_cluster();

        let spark_defaults = spark_cluster
            .spec
            .get_config(&SparkRole::Master, TestSparkCluster::MASTER_1_ROLE_GROUP)
            .unwrap()
            .get_spark_defaults_conf(&spark_cluster.spec);

        assert!(spark_defaults.contains_key(constants::SPARK_DEFAULTS_EVENT_LOG_DIR));
        assert_eq!(
            spark_defaults.get(constants::SPARK_DEFAULTS_EVENT_LOG_DIR),
            Some(&TestSparkCluster::CLUSTER_LOG_DIR.to_string())
        );

        assert!(spark_defaults.contains_key(constants::SPARK_DEFAULTS_AUTHENTICATE_SECRET));
        assert_eq!(
            spark_defaults.get(constants::SPARK_DEFAULTS_AUTHENTICATE_SECRET),
            Some(&TestSparkCluster::CLUSTER_SECRET.to_string())
        );

        assert!(spark_defaults.contains_key(constants::SPARK_DEFAULTS_PORT_MAX_RETRIES));
        assert_eq!(
            spark_defaults.get(constants::SPARK_DEFAULTS_PORT_MAX_RETRIES),
            Some(&TestSparkCluster::CLUSTER_MAX_PORT_RETRIES.to_string())
        );

        assert!(spark_defaults.contains_key(constants::SPARK_DEFAULTS_MASTER_PORT));
        assert_eq!(
            spark_defaults.get(constants::SPARK_DEFAULTS_MASTER_PORT),
            Some(&TestSparkCluster::MASTER_1_CONFIG_PORT.to_string())
        );
    }

    #[test]
    fn test_get_env_variables() {
        let spark_cluster: SparkCluster = stackable_spark_test_utils::setup_test_cluster();

        let spark_env_sh = spark_cluster
            .spec
            .get_config(&SparkRole::Worker, TestSparkCluster::WORKER_1_ROLE_GROUP)
            .unwrap()
            .get_spark_env_sh();

        assert!(spark_env_sh.contains_key(constants::SPARK_ENV_WORKER_PORT));
        assert_eq!(
            spark_env_sh.get(constants::SPARK_ENV_WORKER_PORT),
            Some(&TestSparkCluster::WORKER_1_PORT.to_string())
        );

        assert!(spark_env_sh.contains_key(constants::SPARK_ENV_WORKER_WEBUI_PORT));
        assert_eq!(
            spark_env_sh.get(constants::SPARK_ENV_WORKER_WEBUI_PORT),
            Some(&TestSparkCluster::WORKER_1_WEBUI_PORT.to_string())
        );

        assert!(spark_env_sh.contains_key(constants::SPARK_ENV_WORKER_MEMORY));
        assert_eq!(
            spark_env_sh.get(constants::SPARK_ENV_WORKER_MEMORY),
            Some(&TestSparkCluster::WORKER_1_ENV_MEMORY.to_string())
        );

        assert!(spark_env_sh.contains_key(constants::SPARK_ENV_WORKER_CORES));
        assert_eq!(
            spark_env_sh.get(constants::SPARK_ENV_WORKER_CORES),
            Some(&TestSparkCluster::WORKER_1_CORES.to_string())
        );
    }

    #[test]
    fn test_create_config_map_name() {
        let pod_name = "my_pod";

        assert_eq!(
            create_config_map_name(&pod_name),
            format!("{}-config", pod_name)
        );
    }

    #[test]
    fn test_create_config_maps() {
        let mut spark_cluster: SparkCluster = stackable_spark_test_utils::setup_test_cluster();
        spark_cluster.metadata.uid = Some("12345".to_string());

        let config = spark_cluster
            .spec
            .get_config(&SparkRole::Worker, TestSparkCluster::WORKER_1_ROLE_GROUP);

        let pod_name = "my_pod";
        let cm_name = create_config_map_name(pod_name);

        let config_map = create_config_map_with_data(&spark_cluster, config, &cm_name).unwrap();

        let cm_data = config_map.data.unwrap();
        assert!(cm_data.contains_key(constants::SPARK_DEFAULTS_CONF));
        assert!(cm_data.contains_key(constants::SPARK_ENV_SH));

        assert!(cm_data
            .get(constants::SPARK_DEFAULTS_CONF)
            .unwrap()
            .contains(constants::SPARK_DEFAULTS_AUTHENTICATE_SECRET));

        assert!(cm_data
            .get(constants::SPARK_ENV_SH)
            .unwrap()
            .contains(constants::SPARK_ENV_WORKER_PORT));

        // TODO: add more asserts
    }
}
