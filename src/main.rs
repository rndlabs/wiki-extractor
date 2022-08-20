use clap::{App, Arg};
use std::process;
use wiki_extractor::{run, Config};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send>>;

fn main() -> Result<()> {
    let matches = App::new("wiki_extractor")
        .version("0.1.0")
        .about("Marshalling the world's knowledge 🌐📚🐝")
        .author("mfw78")
        .arg(
            Arg::with_name("input")
                .short('i')
                .long("input")
                .value_name("FILE")
                .help("Sets the input file to use")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("output")
                .short('o')
                .long("output-dir")
                .value_name("DIR")
                .help("Sets the output directory to use")
                .takes_value(true)
                .required(true),
        )
        .get_matches();
    // TODO: Write detailed usage instructions

    let config = Config::new(matches).unwrap_or_else(|err| {
        eprintln!("Problem parsing configuration variables: {}", err);
        process::exit(1)
    });

    // run the program
    if let Err(e) = run(config) {
        eprintln!("Application error: {}", e);
        process::exit(1);
    }

    Ok(())
}
