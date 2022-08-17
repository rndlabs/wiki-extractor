use clap::{App, Arg};
use std::{error::Error, process};
use wiki_extractor::{run, Config};

fn main() -> Result<(), Box<dyn Error>> {
    let matches = App::new("wiki_extractor")
        .version("0.1.0")
        .about("Extracts and enhances the Wikipedia ZIM file")
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

    let input = matches.value_of("input").unwrap();
    let output = matches.value_of("output").unwrap();

    let config = Config {
        input: input.to_string(),
        output: output.to_string(),
    };

    // run the program
    if let Err(e) = run(config) {
        eprintln!("Application error: {}", e);
        process::exit(1);
    }

    Ok(())
}
