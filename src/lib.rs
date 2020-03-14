//! defines KvStore struct which implements a simple in-memory key-value storage

pub mod error;
mod log;
mod engine;
mod store;
mod command;

pub use engine::KvsEngine;
pub use store::KvStore;
pub use command::{CommandRequest, CommandResponse};

use error::KvStoreError;

pub type Result<T> = std::result::Result<T, KvStoreError>;
