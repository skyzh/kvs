//! defines KvStore struct which implements a simple in-memory key-value storage

pub mod error;
mod log;
mod engine;
mod store;

pub use engine::KvsEngine;
pub use store::KvStore;

use error::KvStoreError;

pub type Result<T> = std::result::Result<T, KvStoreError>;
