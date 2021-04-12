#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Kubernetes reported error: {source}")]
    KubeError {
        #[from]
        source: kube::Error,
    },

    #[error("Error from Operator framework: {source}")]
    OperatorError {
        #[from]
        source: stackable_operator::error::Error,
    },

    #[error("Error from serde_json: {source}")]
    SerdeError {
        #[from]
        source: serde_json::Error,
    },

    #[error("Error from semver: {source}")]
    SemVerError {
        #[from]
        source: semver::SemVerError,
    },

    #[error("Error during reconciliation: {0}")]
    ReconcileError(String),

    #[error("Pod contains invalid node type: {source}")]
    InvalidNodeType {
        #[from]
        source: stackable_spark_crd::CrdError,
    },
}
