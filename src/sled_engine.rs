use crate::error::KvStoreError;
use crate::KvsEngine;
use crate::Result;
use std::path::PathBuf;

pub struct SledEngine {
    engine: sled::Db,
}

impl SledEngine {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let engine = sled::open(path.into())?;
        Ok(Self { engine })
    }
}

impl KvsEngine for SledEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.engine.insert(key.as_str(), value.as_str())?;
        self.engine.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        self.engine
            .get(key.as_str())
            .and_then(|x| Ok(x.map(|x| std::str::from_utf8(&*x).unwrap().to_string())))
            .map_err(|x| x.into())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if !self.engine.contains_key(key.as_str())? {
            return Err(KvStoreError::KeyNotFound { key });
        }
        self.engine.remove(key.as_str())?;
        self.engine.flush()?;
        Ok(())
    }
}
