//! defines logging

use serde::{Deserialize, Serialize};

/// Command
#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}
