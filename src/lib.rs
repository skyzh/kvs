//! defines KvStore struct which implements a simple in-memory key-value storage

pub mod client;
mod command;
mod engine;
pub mod error;
mod log;
pub mod server;
mod sled_engine;
mod store;

pub use command::{CommandRequest, CommandResponse};
pub use engine::KvsEngine;
pub use sled_engine::SledEngine;
pub use store::KvStore;

use error::KvStoreError;

pub type Result<T> = std::result::Result<T, KvStoreError>;
