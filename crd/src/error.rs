//! This module provides custom CRD errors that may occur during parsing.

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum CrdError {
    #[error("Pod contains invalid role: {node_type}")]
    InvalidNodeType { node_type: String },
}
