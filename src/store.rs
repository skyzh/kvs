use crate::error::KvStoreError;
use crate::log::Command;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::PathBuf;
use crate::{Result, KvsEngine};

/// KvStore struct stores key-value information
pub struct KvStore {
    path: PathBuf,
    writer: SequentialWriter<File>,
    keydir: HashMap<String, (u64, u64)>,
    files: HashMap<u64, File>,
    generation_cnt: u64,
    compaction_cnt: u64,
    compaction_in_progress: bool,
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

    pub fn into_inner(self) -> BufWriter<T> {
        self.writer
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
            .filter(|f| f.is_file() && f.extension().map_or(false, |x| x == "db"))
            .flat_map(|f| {
                f.file_name()
                    .and_then(|x| x.to_str())
                    .map(|x| &x[0..x.len() - 3])
                    .map(|x| x.parse::<u64>())
            })
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
                path.push(format!("{}.db", generation));
                let mut reader = BufReader::new(File::open(path)?);
                let mut de =
                    serde_json::Deserializer::from_reader(&mut reader).into_iter::<Command>();
                loop {
                    let offset = de.byte_offset();
                    match de.next() {
                        Some(result) => match result {
                            Ok(cmd) => match cmd {
                                Command::Set { key, .. } => {
                                    keydir.insert(key, (generation, offset as u64));
                                }
                                Command::Remove { key } => {
                                    keydir.remove(&key);
                                }
                            },
                            Err(_x) => break,
                        },
                        None => break,
                    };
                }
                files.insert(generation, reader.into_inner());
            }
        } else {
            std::fs::create_dir_all(&path)?;
            generation_cnt = 0;
        }

        let mut new_generation_path = path.clone();
        new_generation_path.push(format!("{}.db", generation_cnt));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(new_generation_path)?;
        Ok(Self {
            path,
            writer: SequentialWriter::new(BufWriter::new(file), 0),
            keydir,
            files,
            generation_cnt,
            compaction_cnt: 0,
            compaction_in_progress: false,
        })
    }

    fn get_file(&mut self, fileno: u64) -> Result<&mut File> {
        if fileno == self.generation_cnt {
            self.writer.flush()?;
            Ok(self.writer.get_mut().get_mut())
        } else {
            self.files
                .get_mut(&fileno)
                .ok_or_else(|| KvStoreError::InvalidFileHandler {})
        }
    }

    /// try compact log
    fn try_compaction(&mut self) -> Result<()> {
        self.compaction_cnt += 1;
        if self.compaction_cnt >= 5000 {
            self.compaction_cnt = 0;
            self.compaction()?;
        }
        Ok(())
    }

    /// compact log
    ///
    /// If crash, you should immediately drop KvStore object.
    fn compaction(&mut self) -> Result<()> {
        if self.compaction_in_progress {
            return Ok(());
        }
        self.compaction_in_progress = true;

        // phase 1: write all logs into next generation
        // phase 2: remove all files before current generation
        // phase 3: update all internal structures

        // cache all generations
        let generations = Self::all_generations(&self.path)?;

        // open new generation
        let mut new_generation_path = self.path.clone();
        self.generation_cnt += 1;
        new_generation_path.push(format!("{}.db", self.generation_cnt));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(new_generation_path)?;

        let new_writer = SequentialWriter::new(BufWriter::new(file), 0);
        let previous_writer = std::mem::replace(&mut self.writer, new_writer);
        let file = previous_writer
            .into_inner()
            .into_inner()
            .map_err(|_| KvStoreError::IntoInner {})?;
        self.files.insert(self.generation_cnt - 1, file);

        // get all keys
        let keys: Vec<String> = self.keydir.keys().cloned().collect();

        // write to new log
        for key in keys.into_iter() {
            let value = self
                .get(key.clone())?
                .ok_or(KvStoreError::KeyNotFound { key: key.clone() })?;
            self.set(key, value)?;
        }

        // remove all files before current generation
        for g_cnt in generations {
            self.files.remove(&g_cnt);
            let mut path = self.path.clone();
            path.push(format!("{}.db", g_cnt));
            std::fs::remove_file(path)?;
        }
        self.compaction_in_progress = false;

        Ok(())
    }
}

impl KvsEngine for KvStore {
    /// get `value` of the corresponding `key`
    ///
    /// If the `key` hasn't been stored in memory, `None` will be returned
    fn get(&mut self, key: String) -> Result<Option<String>> {
        if !self.keydir.contains_key(&key) {
            return Ok(None);
        }
        let (fileno, offset) = self.keydir.get(&key).unwrap();
        let fileno = *fileno;
        let offset = *offset;
        let mut file = self.get_file(fileno)?.try_clone()?;
        file.seek(SeekFrom::Start(offset))?;
        let cmd = Command::deserialize(&mut serde_json::Deserializer::from_reader(
            BufReader::new(file),
        ))?;
        match cmd {
            Command::Set { value, .. } => Ok(Some(value)),
            Command::Remove { .. } => panic!("invalid record"),
        }
    }

    /// remove key-value pair with `key`
    ///
    /// If the key doesn't exist in memory, this function will panic
    fn remove(&mut self, key: String) -> Result<()> {
        if !self.keydir.contains_key(&key) {
            return Err(KvStoreError::KeyNotFound { key });
        }
        self.keydir.remove(&key);
        serde_json::to_writer(&mut self.writer, &Command::Remove { key })?;

        self.try_compaction()?;

        self.writer.flush()?;

        Ok(())
    }
    /// set the corresponding `key` to `value`
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let offset = self.writer.bytes_written();
        let do_compaction = self.keydir.contains_key(&key);
        self.keydir
            .insert(key.clone(), (self.generation_cnt, offset));
        serde_json::to_writer(&mut self.writer, &Command::Set { key, value })?;

        if do_compaction {
            self.try_compaction()?
        };

        self.writer.flush()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{KvStore, KvsEngine};
    use std::path::PathBuf;

    const DB_FILE: &str = "./database.test";

    fn setup() {
        std::fs::remove_dir_all(DB_FILE).ok();
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
        for _j in 0..10 {
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

    #[test]
    fn get_nonexist_key() {
        setup();
        let mut backend = KvStore::open(PathBuf::from(DB_FILE)).unwrap();
        assert_eq!(backend.get("2333".into()).unwrap(), None)
    }

    #[test]
    fn get_current_key() {
        setup();
        let mut backend = KvStore::open(PathBuf::from(DB_FILE)).unwrap();
        backend.set("2333".into(), "2333".into()).unwrap();
        assert_eq!(backend.get("2333".into()).unwrap(), Some("2333".into()))
    }

    #[test]
    fn compaction() {
        setup();
        let mut backend = KvStore::open(PathBuf::from(DB_FILE)).unwrap();
        backend.set("2333".into(), "2333".into()).unwrap();
        backend.set("2333".into(), "2334".into()).unwrap();
        backend.compaction().unwrap();
        assert_eq!(backend.generation_cnt, 1);
        let mut x = PathBuf::from(DB_FILE);
        x.push(PathBuf::from("0.db"));
        assert!(!x.exists());
    }

    #[test]
    fn auto_compaction() {
        setup();
        let mut backend = KvStore::open(PathBuf::from(DB_FILE)).unwrap();
        for j in 0..10 {
            for i in 0..1000 {
                backend.set(i.to_string(), j.to_string()).unwrap();
            }
        }
        assert_ne!(backend.generation_cnt, 0);
    }
}
