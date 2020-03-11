use clap::clap_app;
use kvs::KvStore;

fn main() -> Result<(), failure::Error> {
    let matches = clap_app!(kvs =>
        (version: "0.1.0")
        (author: "Alex Chi <iskyzh@gmail.com>")
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

    let mut kvstore = KvStore::open(std::env::current_dir())?;
    match matches.subcommand_name() {
        Some("set") => {
            let key = matches.value_of("key")?;
            let value = matches.value_of("value")?;
            kvstore.set(key.to_string(), value.to_string())?;
        }
        Some("get") => {
            let key = matches.value_of("key")?;
            let value = kvstore.get(key.to_string())??;
            println!("{}", value);
        }
        Some("rm") => {
            let key = matches.value_of("key")?;
            kvstore.remove(key.to_string());
        }
        _ => {
            eprintln!("unknown command");
            return failure::Error();
        }
    }
    Ok(())
}
