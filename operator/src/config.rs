use k8s_openapi::api::core::v1::{ConfigMapVolumeSource, EnvVar, Volume, VolumeMount};
use stackable_spark_crd::{SparkNode, SparkNodeType};

const SPARK_URL_START: &str = "spark://";

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
pub fn adapt_container_command(node_type: &SparkNodeType, master: &SparkNode) -> String {
    let mut master_url: String = String::new();
    // only for workers
    if node_type != &SparkNodeType::Worker {
        return master_url;
    }
    // get all available master selectors
    'selectors: for selector in &master.selectors {
        // check in conf properties and env variables for port
        // conf properties have higher priority than env variables
        // TODO: add direct spec property for port from crd here
        // if Some(port) = selector.port { .. continue} else if {...}

        // use config over env
        if let Some(conf) = &selector.config {
            for config_option in conf {
                if config_option.name == "spark.master.port" {
                    master_url.push_str(
                        format!(
                            "{}{}:{},",
                            SPARK_URL_START, selector.node_name, config_option.value
                        )
                        .as_str(),
                    );
                    // found
                    continue 'selectors;
                }
            }
        }

        if let Some(env) = &selector.env {
            for config_option in env {
                if config_option.name == "SPARK_MASTER_PORT" {
                    master_url.push_str(
                        format!(
                            "{}{}:{},",
                            SPARK_URL_START, selector.node_name, config_option.value
                        )
                        .as_str(),
                    );
                    // found
                    continue 'selectors;
                }
            }
        }

        // TODO: default to default value in product conf
        // nothing found: default
        master_url
            .push_str(format!("{}{}:{},", SPARK_URL_START, selector.node_name, "7077").as_str());
    }

    master_url
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
            name: "SPARK_NO_DAEMONIZE".to_string(),
            value: Some("true".to_string()),
            ..EnvVar::default()
        },
        EnvVar {
            name: "SPARK_CONF_DIR".to_string(),
            value: Some("{{configroot}}/conf".to_string()),
            ..EnvVar::default()
        },
    ]
}
