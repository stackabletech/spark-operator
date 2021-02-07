use std::backtrace::Backtrace;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Kubernetes reported error: {source}")]
    KubeError {
        #[from]
        source: kube::Error,
        backtrace: Backtrace,
    },

    #[error("Error from Operator framework: {source}")]
    OperatorError {
        #[from]
        source: stackable_operator::error::Error,
        backtrace: Backtrace,
    },

    #[error("Error from serde_json: {source}")]
    SerdeError {
        #[from]
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[error("Pod contains invalid node type: {source}")]
    InvalidNodeType {
        #[from]
        source: stackable_spark_crd::CrdError,
        backtrace: Backtrace,
    },

    #[error("Error during reconciliation: {0}")]
    ReconcileError(String),
}
