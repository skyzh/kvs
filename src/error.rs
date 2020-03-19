use failure::Fail;

#[derive(Debug, Fail)]
pub enum KvStoreError {
    #[fail(display = "key not found: {}", key)]
    KeyNotFound { key: String },
    #[fail(
        display = "parameter not found: {}, required by command {}",
        parameter, required_by
    )]
    CliError {
        parameter: String,
        required_by: String,
    },
    #[fail(display = "unknown command")]
    CliUnknownCommand {},
    #[fail(display = "internal error: invalid file handler")]
    InvalidFileHandler {},
    #[fail(display = "internal error: failed to acquire file")]
    IntoInner {},
    #[fail(display = "{}", _0)]
    IOError(#[fail(cause)] std::io::Error),
    #[fail(display = "{}", _0)]
    SerdeError(#[fail(cause)] serde_json::error::Error),
    #[fail(display = "error from server: {}", reason)]
    RequestError { reason: String },
    #[fail(display = "{}", _0)]
    SledError(#[fail(cause)] sled::Error),
}

impl std::convert::From<std::io::Error> for KvStoreError {
    fn from(err: std::io::Error) -> Self {
        KvStoreError::IOError(err)
    }
}

impl std::convert::From<serde_json::error::Error> for KvStoreError {
    fn from(err: serde_json::error::Error) -> Self {
        KvStoreError::SerdeError(err)
    }
}

impl std::convert::From<sled::Error> for KvStoreError {
    fn from(err: sled::Error) -> Self {
        KvStoreError::SledError(err)
    }
}
