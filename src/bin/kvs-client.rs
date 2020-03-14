use clap::clap_app;
use kvs::error::KvStoreError;

fn main() -> Result<(), failure::Error> {
    let matches = clap_app!(kvs_client =>
        (version: "0.1.0")
        (author: "Alex Chi <iskyzh@gmail.com>")
        (about: "A key-value store client")
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
        (@arg ADDR: --addr +takes_value "addr")
    ).get_matches();

    let addr = matches.value_of("ADDR").unwrap_or("127.0.0.1:4000");

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
        }
        ("get", Some(cmd)) => {
            let key = cmd.value_of("KEY").ok_or(KvStoreError::CliError {
                parameter: "KEY".into(),
                required_by: "get".into(),
            })?;
        }
        ("rm", Some(cmd)) => {
            let key = cmd.value_of("KEY").ok_or(KvStoreError::CliError {
                parameter: "key".into(),
                required_by: "rm".into(),
            })?;
        }
        _ => {
            eprintln!("unknown command");
            return Err(KvStoreError::CliUnknownCommand {}.into());
        }
    }
    Ok(())
}
