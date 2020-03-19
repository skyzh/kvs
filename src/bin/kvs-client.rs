use clap::clap_app;
use kvs::error::KvStoreError;
use kvs::{CommandRequest, CommandResponse};
use std::io::{BufReader, BufWriter, Write};
use std::net::TcpStream;
use std::process::exit;

fn main() -> Result<(), failure::Error> {
    let matches = clap_app!(kvs_client =>
        (version: env!("CARGO_PKG_VERSION"))
        (author: env!("CARGO_PKG_AUTHORS"))
        (about: "A key-value store client")
        (@subcommand set =>
            (about: "set key-value pair")
            (@arg KEY: +required "key")
            (@arg VALUE: +required "value")
            (@arg ADDR: --addr +takes_value "addr")
        )
        (@subcommand get =>
            (about: "get key-value pair by key")
            (@arg KEY: +required "key")
            (@arg ADDR: --addr +takes_value "addr")
        )
        (@subcommand rm =>
            (about: "remove key-value pair by key")
            (@arg KEY: +required "key")
            (@arg ADDR: --addr +takes_value "addr")
        )
    )
    .get_matches();

    let command;
    let addr;

    {
        match matches.subcommand() {
            ("set", Some(cmd)) => {
                let key = cmd
                    .value_of("KEY")
                    .ok_or(KvStoreError::CliError {
                        parameter: "key".into(),
                        required_by: "set".into(),
                    })?
                    .into();
                let value = cmd
                    .value_of("VALUE")
                    .ok_or(KvStoreError::CliError {
                        parameter: "value".into(),
                        required_by: "set".into(),
                    })?
                    .into();

                addr = cmd.value_of("ADDR").unwrap_or("127.0.0.1:4000");
                command = CommandRequest::Set { key, value };
            }
            ("get", Some(cmd)) => {
                let key = cmd
                    .value_of("KEY")
                    .ok_or(KvStoreError::CliError {
                        parameter: "KEY".into(),
                        required_by: "get".into(),
                    })?
                    .into();

                addr = cmd.value_of("ADDR").unwrap_or("127.0.0.1:4000");
                command = CommandRequest::Get { key };
            }
            ("rm", Some(cmd)) => {
                let key = cmd
                    .value_of("KEY")
                    .ok_or(KvStoreError::CliError {
                        parameter: "key".into(),
                        required_by: "rm".into(),
                    })?
                    .into();

                addr = cmd.value_of("ADDR").unwrap_or("127.0.0.1:4000");
                command = CommandRequest::Remove { key };
            }
            _ => {
                eprintln!("unknown command");
                return Err(KvStoreError::CliUnknownCommand {}.into());
            }
        }
    }

    let mut connection = TcpStream::connect(addr)?;
    let mut writer = BufWriter::new(&mut connection);
    serde_json::to_writer(&mut writer, &command)?;
    writeln!(writer)?;
    drop(writer);
    let reader = BufReader::new(&mut connection);

    match serde_json::from_reader(reader)? {
        CommandResponse::Error { reason } => {
            eprintln!("{}", reason);
            return Err(KvStoreError::RequestError { reason }.into());
        }
        CommandResponse::Success {} => {}
        CommandResponse::Value { value } => match value {
            Some(value) => println!("{}", value),
            None => println!("Key not found"),
        },
        CommandResponse::KeyNotFound { .. } => {
            eprintln!("Key not found");
            exit(1);
        }
    }
    Ok(())
}
