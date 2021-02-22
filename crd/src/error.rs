#[derive(Debug, thiserror::Error)]
pub enum CrdError {
    #[error("Pod contains invalid role: {node_type}")]
    InvalidNodeType { node_type: String },
}
