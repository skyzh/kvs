use clap::clap_app;
use kvs::error::KvStoreError;
use kvs::server::KvsServer;
use kvs::{KvStore, KvsEngine, SledEngine};
use slog::{info, o, Drain};
use std::fs::File;
use std::io::{Read, Write};
use std::net::TcpListener;

fn get_current_engine() -> Option<String> {
    let mut current_engine = String::new();
    match File::open(".config") {
        Ok(mut file) => match file.read_to_string(&mut current_engine) {
            Ok(_) => Some(current_engine),
            _ => None,
        },
        _ => None,
    }
}

fn main() -> Result<(), failure::Error> {
    let matches = clap_app!(kvs_server =>
        (version: env!("CARGO_PKG_VERSION"))
        (author: env!("CARGO_PKG_AUTHORS"))
        (about: "A key-value store server")
        (@arg ADDR: --addr +takes_value "addr")
        (@arg ENGINE: --engine +required +takes_value "engine")
    )
    .get_matches();

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = slog::Logger::root(drain, o!());

    let addr = matches.value_of("ADDR").unwrap_or("127.0.0.1:4000");

    let engine = matches.value_of("ENGINE").ok_or(KvStoreError::CliError {
        parameter: "engine".into(),
        required_by: "".into(),
    })?;

    if let Some(current_engine) = get_current_engine() {
        if engine != current_engine {
            return Err(KvStoreError::CliError {
                parameter: "engine".into(),
                required_by: "".into(),
            }
            .into());
        }
    }

    info!(log, "{} initializing", env!("CARGO_PKG_NAME");
        "addr" => &addr,
        "engine" => &engine,
        "version" => env!("CARGO_PKG_VERSION"));

    let mut config_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(".config")?;
    write!(config_file, "{}", engine)?;

    let kvs_engine: Box<dyn KvsEngine>;

    match engine {
        "sled" => kvs_engine = Box::new(SledEngine::open(std::env::current_dir()?)?),
        "kvs" => kvs_engine = Box::new(KvStore::open(std::env::current_dir()?)?),
        _ => {
            return Err(KvStoreError::CliError {
                parameter: "engine".into(),
                required_by: "".into(),
            }
            .into());
        }
    }

    let listener = TcpListener::bind(addr)?;

    KvsServer::new(listener, kvs_engine).serve(&log)?;

    Ok(())
}
