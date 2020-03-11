#[derive(Debug, Fail)]
enum KvStoreError {
    #[fail(display = "key not found: {}", key)]
    KeyNotFound {
        key: String,
    },
    #[fail(display = "parameter not found: {}", key)]
    CliError {
        parameter: String,
    }
}
