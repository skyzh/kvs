//! defines logging

use serde::{Serialize, Deserialize};

/// Command
#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}
