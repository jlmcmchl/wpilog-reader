use std::{env, fs::File, io::Read};

use wpilog_reader::mcap::{MCap, Parse, Record};

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut file = File::open(&args[1]).unwrap();

    let mut content = Vec::new();
    file.read_to_end(&mut content).unwrap();

    let parsed_log = MCap::parse(&content);

    match parsed_log {
        Ok((_, log)) => {
            println!("Parse successful - entries: {}", log.records.len());
            for record in log.records {
                println!("{:?}", Record::try_from(record));
            }
        }
        Err(e) => println!("{}", e),
    }
}
