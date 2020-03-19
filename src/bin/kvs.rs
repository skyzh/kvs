use clap::clap_app;
use kvs::error::KvStoreError;
use kvs::{KvStore, KvsEngine};

fn main() -> Result<(), failure::Error> {
    let matches = clap_app!(kvs =>
        (version: env!("CARGO_PKG_VERSION"))
        (author: env!("CARGO_PKG_AUTHORS"))
        (about: "A key-value store")
        (@subcommand set =>
            (about: "set key-value pair")
            (@arg KEY: +required "key")
            (@arg VALUE: +required "value")
        )
        (@subcommand get =>
            (about: "get key-value pair by key")
            (@arg KEY: +required "key")
        )
        (@subcommand rm =>
            (about: "remove key-value pair by key")
            (@arg KEY: +required "key")
        )
    )
    .get_matches();

    let mut kvstore = KvStore::open(std::env::current_dir()?)?;
    match matches.subcommand() {
        ("set", Some(cmd)) => {
            let key = cmd.value_of("KEY").ok_or(KvStoreError::CliError {
                parameter: "key".into(),
                required_by: "set".into(),
            })?;
            let value = cmd.value_of("VALUE").ok_or(KvStoreError::CliError {
                parameter: "value".into(),
                required_by: "set".into(),
            })?;
            kvstore.set(key.into(), value.into())?;
        }
        ("get", Some(cmd)) => {
            let key = cmd.value_of("KEY").ok_or(KvStoreError::CliError {
                parameter: "KEY".into(),
                required_by: "get".into(),
            })?;
            let value = kvstore.get(key.into())?;
            match value {
                Some(ref x) => {
                    println!("{}", x);
                }
                None => {
                    // return Err(KvStoreError::KeyNotFound { key: key.into() }.into());
                    println!("Key not found");
                }
            };
        }
        ("rm", Some(cmd)) => {
            let key = cmd.value_of("KEY").ok_or(KvStoreError::CliError {
                parameter: "key".into(),
                required_by: "rm".into(),
            })?;
            kvstore.remove(key.into()).map_err(|e| {
                if let KvStoreError::KeyNotFound { .. } = e {
                    println!("Key not found")
                }
                e
            })?;
        }
        _ => {
            eprintln!("unknown command");
            return Err(KvStoreError::CliUnknownCommand {}.into());
        }
    }
    Ok(())
}
