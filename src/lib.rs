//! defines KvStore struct which implements a simple in-memory key-value storage
mod log;

use std::path::PathBuf;
use std::fs::File;
use std::io::{BufWriter, BufReader};
use failure::_core::marker::PhantomData;
use serde::{Serialize, Deserialize};
use crate::log::Command;
use std::collections::HashMap;
use failure::Error;
use std::ffi::OsStr;


pub type Result<T> = std::result::Result<T, Error>;

/// KvStore struct stores key-value information
pub struct KvStore {
    path: PathBuf,
    writer: BufWriter<File>,
    keydir: HashMap<String, (u64, usize)>,
    readers: HashMap<u64, BufReader<File>>,
    generation_cnt: u64,
}

impl KvStore {
    fn all_generations(path: &PathBuf) -> Result<Vec<u64>> {
        let mut ids = std::fs::read_dir(&path)?
            .flat_map(|f| -> Result<_> { Ok(f?.path()) })
            .filter(|f| f.is_file()) // && f.extension().map_or(false, |x| x == "db"))
            .flat_map(|f| f.file_name()
                .and_then(|x| x.to_str())
                .map(|x| x.parse::<u64>()))
            .flatten()
            .collect::<Vec<u64>>();
        ids.sort();
        Ok(ids)
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let generation_cnt: u64;
        let mut readers: HashMap<u64, BufReader<File>> = Default::default();
        let mut keydir: HashMap<String, (u64, usize)> = Default::default();
        if path.exists() {
            let generations = Self::all_generations(&path)?;
            generation_cnt = generations.last().map_or(0, |x| *x) + 1;
            for generation in generations {
                let mut path = path.clone();
                path.push(generation.to_string());
                let mut reader = BufReader::new(File::open(path)?);
                let mut de = serde_json::Deserializer::from_reader(&mut reader);
                loop {
                    match Command::deserialize(&mut de) {
                        Ok(cmd) => match cmd {
                            Command::Set { key, value } => { keydir.insert(key, (0, 0)); }
                            Command::Remove { key } => { keydir.remove(&key); }
                        },
                        Err(x) => break
                    };
                }
                readers.insert(generation, reader);
            }
        } else {
            std::fs::create_dir_all(&path)?;
            generation_cnt = 0;
        }

        let mut new_generation_path = path.clone();
        new_generation_path.push(generation_cnt.to_string());
        let file = File::create(new_generation_path)?;
        Ok(Self {
            path,
            writer: BufWriter::new(file),
            keydir,
            readers,
            generation_cnt,
        })
    }

    /// set the corresponding `key` to `value`
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Command::Set { key, value })?;
        Ok(())
    }

    /// get `value` of the corresponding `key`
    ///
    /// If the `key` hasn't been stored in memory, `None` will be returned
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(None)
    }

    /// remove key-value pair with `key`
    ///
    /// If the key doesn't exist in memory, this function will panic
    pub fn remove(&mut self, key: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Command::Remove { key });
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::KvStore;
    use crate::log::Command;
    use std::path::PathBuf;

    const DB_FILE: &str = "./database.test";

    fn setup() {
        std::fs::remove_dir_all(DB_FILE);
    }

    #[test]
    fn create_backend() {
        setup();
        let backend = KvStore::open(PathBuf::from(DB_FILE)).unwrap();
        assert_eq!(backend.generation_cnt, 0);
    }

    #[test]
    fn create_backend_file_exists() {
        setup();
        {
            let backend = KvStore::open(PathBuf::from(DB_FILE)).unwrap();
            assert_eq!(backend.generation_cnt, 0);
        }
        {
            let backend = KvStore::open(PathBuf::from(DB_FILE)).unwrap();
            assert_eq!(backend.generation_cnt, 1);
        }
    }

    #[test]
    fn write_log() {
        setup();
        let mut backend = KvStore::open(PathBuf::from(DB_FILE)).unwrap();
        for i in 0..100 {
            backend.set(i.to_string(), "233".to_string()).unwrap();
        }
    }

    #[test]
    fn write_log_multiple_generation() {
        setup();
        for j in 0..10 {
            let mut backend = KvStore::open(PathBuf::from(DB_FILE)).unwrap();
            for i in 0..100 {
                backend.set(i.to_string(), "233".to_string()).unwrap();
            }
        }
    }

    #[test]
    fn write_log_replay() {
        setup();
        {
            let mut backend = KvStore::open(PathBuf::from(DB_FILE)).unwrap();
            for i in 0..100 {
                backend.set(i.to_string(), "233".to_string()).unwrap();
            }
        }
        let mut backend = KvStore::open(PathBuf::from(DB_FILE)).unwrap();
        for i in 0..100 {
            assert!(backend.keydir.contains_key(&i.to_string()));
        }
    }

    #[test]
    fn write_log_replay_latest() {
        setup();
        {
            for j in 0..10 {
                let mut backend = KvStore::open(PathBuf::from(DB_FILE)).unwrap();
                for i in 0..100 {
                    backend.set(i.to_string(), j.to_string()).unwrap();
                }
            }
        }
        let mut backend = KvStore::open(PathBuf::from(DB_FILE)).unwrap();
        for i in 0..100 {
            assert!(backend.keydir.contains_key(&i.to_string()));
        }
    }
}
