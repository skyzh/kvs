//! defines KvStore struct which implements a simple in-memory key-value storage
mod log;
mod error;

use std::path::PathBuf;
use std::fs::File;
use std::io::{BufWriter, BufReader, Seek, SeekFrom};
use crate::log::Command;
use std::collections::HashMap;
use failure::Error;
use serde::Deserialize;

pub type Result<T> = std::result::Result<T, Error>;

/// KvStore struct stores key-value information
pub struct KvStore {
    path: PathBuf,
    writer: SequentialWriter<File>,
    keydir: HashMap<String, (u64, u64)>,
    files: HashMap<u64, File>,
    generation_cnt: u64,
}

struct SequentialWriter<T: std::io::Write> {
    writer: BufWriter<T>,
    written_bytes: u64,
}

impl<T: std::io::Write> SequentialWriter<T> {
    pub fn new(writer: BufWriter<T>, written_bytes: u64) -> Self {
        Self {
            writer,
            written_bytes,
        }
    }

    pub fn bytes_written(&self) -> u64 {
        self.written_bytes
    }

    pub fn get_mut(&mut self) -> &mut BufWriter<T> {
        &mut self.writer
    }
}

impl<T: std::io::Write> std::io::Write for SequentialWriter<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.writer.write(buf).and_then(|x| {
            self.written_bytes += x as u64;
            Ok(x)
        })
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
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
        let mut files: HashMap<u64, File> = Default::default();
        let mut keydir: HashMap<String, (u64, u64)> = Default::default();
        if path.exists() {
            let generations = Self::all_generations(&path)?;
            generation_cnt = generations.last().map_or(0, |x| *x) + 1;
            for generation in generations {
                let mut path = path.clone();
                path.push(generation.to_string());
                let mut reader = BufReader::new(File::open(path)?);
                let mut de = serde_json::Deserializer::from_reader(&mut reader)
                    .into_iter::<Command>();
                loop {
                    let offset = de.byte_offset();
                    match de.next() {
                        Some(result) => match result {
                            Ok(cmd) => match cmd {
                                Command::Set { key, value } => { keydir.insert(key, (generation, offset as u64)); }
                                Command::Remove { key } => { keydir.remove(&key); }
                            },
                            Err(x) => break
                        },
                        None => break
                    };
                }
                files.insert(generation, reader.into_inner());
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
            writer: SequentialWriter::new(BufWriter::new(file), 0),
            keydir,
            files,
            generation_cnt,
        })
    }

    /// set the corresponding `key` to `value`
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let offset = self.writer.bytes_written();
        self.keydir.insert(key.clone(), (self.generation_cnt, offset));
        serde_json::to_writer(&mut self.writer, &Command::Set { key, value })?;
        Ok(())
    }

    fn get_file(&mut self, fileno: u64) -> Option<&mut File> {
        if fileno == self.generation_cnt {
            Some(self.writer.get_mut().get_mut())
        } else {
            self.files.get_mut(&fileno)
        }
    }

    /// get `value` of the corresponding `key`
    ///
    /// If the `key` hasn't been stored in memory, `None` will be returned
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if !self.keydir.contains_key(&key) {
            return Ok(None);
        }
        let (fileno, offset) = self.keydir.get(&key).unwrap();
        let fileno = *fileno;
        let offset = *offset;
        let file = self.get_file(fileno).unwrap();
        file.seek(SeekFrom::Start(offset));
        let cmd = Command::deserialize(&mut serde_json::Deserializer::from_reader(BufReader::new(file)))?;
        match cmd {
            Command::Set { key, value } => Ok(Some(value)),
            Command::Remove { key } => panic!("233")
        }
    }

    /// remove key-value pair with `key`
    ///
    /// If the key doesn't exist in memory, this function will panic
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.keydir.contains_key(&key) {
            panic!("")
        }
        self.keydir.remove(&key);
        serde_json::to_writer(&mut self.writer, &Command::Remove { key })?;
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::KvStore;
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
        let backend = KvStore::open(PathBuf::from(DB_FILE)).unwrap();
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
            assert_eq!(backend.get(i.to_string()).unwrap(), Some("9".to_string()))
        }
    }
}
