use std::{env, fs::File, io::Read};

use wpilog_reader::wpilog::{
    parser::parse_wpilog,
    types::{ControlRecord, Record},
};

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut file = File::open(&args[1]).unwrap();

    let mut content = Vec::new();
    file.read_to_end(&mut content).unwrap();

    let parsed_log = parse_wpilog(&content);

    match parsed_log {
        Ok((_, log)) => println!(
            "Parse successful - {} entries with {} records",
            log.records
                .iter()
                .filter(|rec| matches!(rec.data, Record::Control(ControlRecord::Start(_))))
                .count(),
            log.records.len()
        ),
        Err(e) => println!("{}", e),
    }
}
