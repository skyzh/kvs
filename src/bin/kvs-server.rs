use clap::clap_app;
use kvs::error::KvStoreError;
use kvs::{KvStore, KvsEngine};
use slog::{o, info, Drain};
use std::fs::File;
use std::io::{Read, Write};

fn get_current_engine() -> Option<String> {
    let mut current_engine = String::new();
    match File::open(".config") {
        Ok(mut file) => match file.read_to_string(&mut current_engine) {
            Ok(_) => Some(current_engine),
            _ => None
        },
        _ => None
    }
}

fn main() -> Result<(), failure::Error> {
    let matches = clap_app!(kvs_server =>
        (version: "0.1.0")
        (author: "Alex Chi <iskyzh@gmail.com>")
        (about: "A key-value store server")
        (@arg ADDR: --addr +takes_value "addr")
        (@arg ENGINE: --engine +required +takes_value "engine")
    ).get_matches();

    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let log = slog::Logger::root(drain, o!());
    info!(log, "server initializing...");

    let addr = matches.value_of("ADDR").unwrap_or("127.0.0.1:4000");

    let engine = matches.value_of("ENGINE").ok_or(KvStoreError::CliError {
        parameter: "engine".into(),
        required_by: "".into(),
    })?;

    if let Some(current_engine) = get_current_engine() {
        if engine != current_engine {
            Err(KvStoreError::CliError {
                parameter: "engine".into(),
                required_by: "".into(),
            })?;
        }
    }
    write!(File::open(".config")?, "{}", engine)?;

    let kvs_engine: Box<dyn KvsEngine>;

    match engine {
        "sled" => { panic!(""); }
        "kvs" => { kvs_engine = Box::new(KvStore::open(std::env::current_dir()?)?) }
        _ => {
            Err(KvStoreError::CliError {
                parameter: "engine".into(),
                required_by: "".into(),
            })?;
        }
    }

    Ok(())
}
