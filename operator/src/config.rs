use stackable_spark_crd::{SparkNode, SparkNodeType};

/// Create all required configuration options for a SparkNodeSelector
/// Including common settings from the corresponding SparkNode
/// Including settings from spec root
// pub fn create_config_options(
//     spec: &SparkClusterSpec,
//     node: &SparkNode,
//     selector: &SparkNodeSelector,
// ) -> HashMap<String, String> {
//     let mut options: HashMap<String, String> = HashMap::new();
//     // collect options
//     // validate and add
//
//     return options;
// }
//
// fn collect_config_options(spec: &SparkClusterSpec, node: &SparkNode, selector: &SparkNodeSelector) {
//     //-> Vec<Result<HashMap<String, String>>> {
//     // 1) all options set in selector
//
//     // 2) all options set in node
//
//     // 3) all options in spec root
//
//     // 4) all options set in env variables for "spark-env.sh" -> may overwrite, no validation?
//
//     // 5) all options set in config properties for "spark-defaults.conf" -> may overwrite, no validation?
// }
//
// pub fn add_and_validate(options: &mut HashMap<String, String>, key: String, value: String) {}

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