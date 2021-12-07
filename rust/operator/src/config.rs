//! This module contains all methods that are responsible for setting / adapting configuration
//! parameters in the Pods and respective ConfigMaps.

use std::collections::{BTreeMap, HashMap};

use stackable_operator::error::OperatorResult;
use stackable_operator::k8s_openapi::api::core::v1::{Pod, PodStatus};
use stackable_operator::labels::{APP_COMPONENT_LABEL, APP_ROLE_GROUP_LABEL};
use stackable_operator::product_config::types::PropertyNameKind;
use stackable_operator::product_config::ProductConfigManager;
use stackable_operator::product_config_utils::{
    config_for_role_and_group, transform_all_roles_to_config, validate_all_roles_and_groups_config,
    ValidatedRoleConfigByPropertyKind,
};
use stackable_spark_crd::{SparkCluster, SparkRole};
use stackable_spark_operator::constants::*;

/// The worker start command needs to be extended with all known master nodes and ports.
/// The required URLs for the starting command are in format: '<master-node-name>:<master-port'
/// and prefixed with 'spark://'. Multiple masters are separated via ',' e.g.:
/// spark://<master-node-name-1>:<master-port-1>,<master-node-name-2>:<master-port-2>
///
/// # Arguments
/// * `role`        - The cluster role (e.g. master, worker, history-server)
/// * `master_urls` - Slice of master urls in format <node_name>:<port>
///
pub fn adapt_worker_command(role: &SparkRole, master_urls: &[String]) -> Option<String> {
    let mut adapted_command: String = String::new();
    // only for workers
    if role != &SparkRole::Worker {
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

/// Unroll a map into a String using a given assignment character (for writing config maps)
///
/// # Arguments
/// * `map`        - Map containing option_name:option_value pairs
/// * `assignment` - Used character to assign option_value to option_name (e.g. "=", " ", ":" ...)
///
pub fn convert_map_to_string(map: &BTreeMap<String, String>, assignment: &str) -> String {
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

/// Filter all existing pods for master node type and retrieve the role group config
/// Extract the nodeName from the pod and the specified port from the config to create
/// the master urls for each pod.
///
/// # Arguments
/// * `pods`             - Slice of all existing pods
/// * `validated_config` - The precalculated role and role group configuration with properties
///                        split by PropertyNameKind
pub fn get_master_urls(
    pods: &[Pod],
    validated_config: &ValidatedRoleConfigByPropertyKind,
) -> OperatorResult<Vec<String>> {
    let mut master_urls = Vec::new();

    for pod in pods {
        if let Some(labels) = &pod.metadata.labels {
            if let (Some(role), Some(group)) = (
                labels.get(APP_COMPONENT_LABEL),
                labels.get(APP_ROLE_GROUP_LABEL),
            ) {
                if role != &SparkRole::Master.to_string() {
                    continue;
                }

                let validated_config_for_role_and_group =
                    config_for_role_and_group(role, group, validated_config)?;

                // TODO: fall back on container ports or default from product config
                //    The "7077" is a placeholder. This value will always be overwritten
                //    by the default or recommended value in the product config (if not set).
                let mut port = "7077";
                // The order for spark properties:
                // SparkConf > spark-submit / spark-shell > spark-defaults.conf > spark-env.sh
                // We only check spark-defaults.conf and spark-env.sh
                // 1) check spark-defaults.sh
                if let Some(spark_defaults_conf) = validated_config_for_role_and_group
                    .get(&PropertyNameKind::File(SPARK_DEFAULTS_CONF.to_string()))
                {
                    if let Some(defaults_conf_port) =
                        spark_defaults_conf.get(SPARK_DEFAULTS_MASTER_PORT)
                    {
                        port = defaults_conf_port;
                    }
                    // 2) check spark-env.sh
                    else if let Some(spark_env_sh) = validated_config_for_role_and_group
                        .get(&PropertyNameKind::File(SPARK_ENV_SH.to_string()))
                    {
                        if let Some(env_port) = spark_env_sh.get(SPARK_ENV_MASTER_PORT) {
                            port = env_port;
                        }
                    }
                }

                if let Some(PodStatus {
                    pod_ip: Some(ip), ..
                }) = &pod.status
                {
                    master_urls.push(create_master_url(ip, port))
                }
            }
        }
    }

    Ok(master_urls)
}

/// Defines all required spark roles (Master, Worker, History-Server) and their required
/// configuration. In this case we need two files: `spark-defaults.conf` and `spark-env.sh`.
/// Additionally require some env variables like `SPARK_NO_DAEMONIZE` and `SPARK_CONFIG_DIR`,
/// (which will be added automatically by the product config).
///
/// The roles and their configs are then validated and complemented by the product config.
///
/// # Arguments
/// * `resource`        - The SparkCluster containing the role definitions.
/// * `product_config`  - The product config to validate and complement the user config.
///
pub fn validated_product_config(
    resource: &SparkCluster,
    product_config: &ProductConfigManager,
) -> OperatorResult<ValidatedRoleConfigByPropertyKind> {
    let mut roles = HashMap::new();
    roles.insert(
        SparkRole::Master.to_string(),
        (
            vec![
                PropertyNameKind::File(SPARK_ENV_SH.to_string()),
                PropertyNameKind::File(SPARK_DEFAULTS_CONF.to_string()),
                PropertyNameKind::File(SPARK_METRICS_PROPERTIES.to_string()),
                PropertyNameKind::Env,
            ],
            resource.spec.masters.clone().into(),
        ),
    );

    roles.insert(
        SparkRole::Worker.to_string(),
        (
            vec![
                PropertyNameKind::File(SPARK_ENV_SH.to_string()),
                PropertyNameKind::File(SPARK_DEFAULTS_CONF.to_string()),
                PropertyNameKind::File(SPARK_METRICS_PROPERTIES.to_string()),
                PropertyNameKind::Env,
            ],
            resource.spec.workers.clone().into(),
        ),
    );

    if let Some(history_servers) = &resource.spec.history_servers {
        roles.insert(
            SparkRole::HistoryServer.to_string(),
            (
                vec![
                    PropertyNameKind::File(SPARK_ENV_SH.to_string()),
                    PropertyNameKind::File(SPARK_DEFAULTS_CONF.to_string()),
                    PropertyNameKind::File(SPARK_METRICS_PROPERTIES.to_string()),
                    PropertyNameKind::Env,
                ],
                history_servers.clone().into(),
            ),
        );
    }

    let role_config = transform_all_roles_to_config(resource, roles);

    validate_all_roles_and_groups_config(
        &resource.spec.version.to_string(),
        &role_config,
        product_config,
        false,
        false,
    )
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
