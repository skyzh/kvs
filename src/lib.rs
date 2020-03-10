//! defines KvStore struct which implements a simple in-memory key-value storage
use std::collections::HashMap;
use std::path::PathBuf;
use crate::backend::SerdeBackend;

mod log;
mod backend;

pub type Result<T> = std::result::Result<T, ()>;

/// KvStore struct stores key-value information
pub struct KvStore {
    backend: SerdeBackend
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        SerdeBackend::new(path).and_then(|backend| Ok(Self { backend }))
    }

    /// set the corresponding `key` to `value`
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        Err(())
    }

    /// get `value` of the corresponding `key`
    ///
    /// If the `key` hasn't been stored in memory, `None` will be returned
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        Err(())
    }

    /// remove key-value pair with `key`
    ///
    /// If the key doesn't exist in memory, this function will panic
    pub fn remove(&mut self, key: String) -> Result<()> {
        Err(())
    }
}
