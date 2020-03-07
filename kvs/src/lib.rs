//! defines KvStore struct which implements a simple in-memory key-value storage
use std::collections::HashMap;

/// KvStore struct stores key-value information
pub struct KvStore {
    data: HashMap<String, String>
}

impl KvStore {
    /// create a new `KvStore` object
    pub fn new() -> Self {
        Self { data: HashMap::new() }
    }

    /// set the corresponding `key` to `value`
    pub fn set(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }

    /// get `value` of the corresponding `key`
    ///
    /// If the `key` hasn't been stored in memory, `None` will be returned
    pub fn get(&self, key: String) -> Option<String> {
        self.data.get(&key).and_then(|value| Some(value.clone()))
    }

    /// remove key-value pair with `key`
    ///
    /// If the key doesn't exist in memory, this function will panic
    pub fn remove(&mut self, key: String) {
        self.data.remove(&key).unwrap();
    }
}
