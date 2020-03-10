use std::path::PathBuf;
use crate::Result;

pub struct SerdeBackend {
    path: PathBuf
}

impl SerdeBackend {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        Ok(Self { path })
    }
}
