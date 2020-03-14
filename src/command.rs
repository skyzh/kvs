//! defines logging

use serde::{Deserialize, Serialize};

/// Kvs Client Request
#[derive(Serialize, Deserialize, Debug)]
pub enum CommandRequest {
    Set { key: String, value: String },
    Remove { key: String },
    Get { key: String },
}

/// Kvs Server Response
#[derive(Serialize, Deserialize, Debug)]
pub enum CommandResponse {
    Success {},
    Error { reason: String },
    Value { value: Option<String> },
    KeyNotFound {},
}
