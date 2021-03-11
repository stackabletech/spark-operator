// spark config files
pub const SPARK_DEFAULTS_CONF: &str = "spark-defaults.conf";
pub const SPARK_ENV_SH: &str = "spark-env.sh";

// basic for startup
pub const SPARK_NO_DAEMONIZE: &str = "SPARK_NO_DAEMONIZE";
pub const SPARK_CONF_DIR: &str = "SPARK_CONF_DIR";
// common
pub const SPARK_EVENT_LOG_ENABLED: &str = "spark.eventLog.enabled";
pub const SPARK_EVENT_LOG_DIR: &str = "spark.eventLog.dir";
pub const SPARK_AUTHENTICATE: &str = "spark.authenticate";
pub const SPARK_AUTHENTICATE_SECRET: &str = "spark.authenticate.secret";
pub const SPARK_PORT_MAX_RETRIES: &str = "spark.port.maxRetries";
// master
pub const SPARK_MASTER_PORT_ENV: &str = "SPARK_MASTER_PORT";
pub const SPARK_MASTER_PORT_CONF: &str = "spark.master.port";
pub const SPARK_MASTER_WEBUI_PORT: &str = "SPARK_MASTER_WEBUI_PORT";
// worker
pub const SPARK_WORKER_CORES: &str = "SPARK_WORKER_CORES";
pub const SPARK_WORKER_MEMORY: &str = "SPARK_WORKER_MEMORY";
pub const SPARK_WORKER_PORT: &str = "SPARK_WORKER_PORT";
pub const SPARK_WORKER_WEBUI_PORT: &str = "SPARK_MASTER_WEBUI_PORT";
// history server
pub const SPARK_HISTORY_FS_LOG_DIRECTORY: &str = "spark.history.fs.logDirectory";
pub const SPARK_HISTORY_STORE_PATH: &str = "spark.history.store.path";
pub const SPARK_HISTORY_UI_PORT: &str = "spark.history.ui.port";
