use clap::clap_app;
use kvs::error::KvStoreError;
use kvs::{KvStore, KvsEngine, CommandRequest, CommandResponse};
use slog::{o, info, Drain};
use std::fs::File;
use std::io::{Read, Write, BufReader, BufWriter, BufRead};
use std::net::TcpListener;

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
        (version: env!("CARGO_PKG_VERSION"))
        (author: env!("CARGO_PKG_AUTHORS"))
        (about: "A key-value store server")
        (@arg ADDR: --addr +takes_value "addr")
        (@arg ENGINE: --engine +required +takes_value "engine")
    ).get_matches();

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
            Err(KvStoreError::CliError {
                parameter: "engine".into(),
                required_by: "".into(),
            })?;
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

    let mut kvs_engine: Box<dyn KvsEngine>;

    match engine {
        "sled" => { panic!(""); }
        "kvs" => { kvs_engine = Box::new(KvStore::open(std::env::current_dir()?)?) }
        _ => {
            return Err(KvStoreError::CliError {
                parameter: "engine".into(),
                required_by: "".into(),
            }.into());
        }
    }

    let listener = TcpListener::bind(addr)?;

    for connection in listener.incoming() {
        let mut connection = connection?;
        info!(log, "new connection"; "peer" => connection.peer_addr()?);
        let mut reader = BufReader::new(&mut connection);
        let mut line = String::new();
        reader.read_line(&mut line)?;
        drop(reader);
        let response = match serde_json::from_str(line.as_str())? {
            CommandRequest::Get { key } => {
                info!(log, "client"; "command" => "get" ,"key" => &key);
                match kvs_engine.get(key) {
                    Ok(value) => CommandResponse::Value { value },
                    Err(e) => CommandResponse::Error { reason: format!("{:?}", e).into() }
                }
            }
            CommandRequest::Set { key, value } => {
                info!(log, "client"; "command" => "set", "key" => &key, "value" => &value);
                match kvs_engine.set(key, value) {
                    Ok(_) => CommandResponse::Success {},
                    Err(e) => CommandResponse::Error { reason: format!("{:?}", e).into() }
                }
            }
            CommandRequest::Remove { key } => {
                info!(log, "client"; "command" => "rm", "key" => &key);
                match kvs_engine.remove(key) {
                    Ok(_) => CommandResponse::Success {},
                    Err(e) => {
                        if let KvStoreError::KeyNotFound { .. } = e {
                            CommandResponse::KeyNotFound {}
                        } else {
                            CommandResponse::Error { reason: format!("{:?}", e).into() }
                        }
                    }
                }
            }
        };
        let mut writer = BufWriter::new(connection);
        serde_json::to_writer(&mut writer, &response)?;
    }

    Ok(())
}
