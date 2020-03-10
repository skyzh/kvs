use clap::clap_app;

fn main() -> Result<(), &'static str> {
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

    match matches.subcommand_name() {
        Some("set") => {
            eprintln!("unimplemented");
            Err("unimplemented")
        }
        Some("get") => {
            eprintln!("unimplemented");
            Err("unimplemented")
        }
        Some("rm") => {
            eprintln!("unimplemented");
            Err("unimplemented")
        }
        _ => {
            eprintln!("unknown command");
            Err("unknown command")
        }
    }
}
