use std::{env, fs::File, io::Read, path::Path};

use wpilog_reader::{
    parser::{
        parse_array, parse_array_ref_with_len, parse_boolean, parse_double, parse_float,
        parse_int64, parse_string_full, parse_string_with_len, parse_wpilog,
    },
    types::{MetadataEntry, Record, WpiLog},
};

fn export_types(typ_file: &Path, log: &[MetadataEntry]) {
    let mut csvwriter = csv::Writer::from_path(typ_file).unwrap();

    csvwriter.write_field("timestamp").unwrap();

    for entry in log {
        csvwriter.write_field(entry.name).unwrap();
    }
    csvwriter.write_record(None::<&[u8]>).unwrap();

    csvwriter.write_field("0").unwrap();
    for entry in log {
        csvwriter.write_field(entry.typ).unwrap();
    }
    csvwriter.write_record(None::<&[u8]>).unwrap();

    csvwriter.flush().unwrap();
}

fn export_metadata(metadata_file: &Path, log: &[MetadataEntry]) {
    let mut csvwriter = csv::Writer::from_path(metadata_file).unwrap();

    csvwriter.write_field("timestamp").unwrap();

    for entry in log {
        csvwriter.write_field(entry.name).unwrap();
    }
    csvwriter.write_record(None::<&[u8]>).unwrap();

    csvwriter.write_field("0").unwrap();
    for entry in log {
        csvwriter.write_field(entry.metadata).unwrap();
    }
    csvwriter.write_record(None::<&[u8]>).unwrap();

    csvwriter.flush().unwrap();
}

fn export_data(data_file: &Path, log: &WpiLog, metadata: &[MetadataEntry]) {
    let mut csvwriter = csv::Writer::from_path(data_file).unwrap();

    let mut template_record = vec![None];

    csvwriter.write_field("timestamp").unwrap();
    println!("timestamp, double");

    for entry in metadata {
        for field in entry.fields() {
            csvwriter.write_field(field.clone()).unwrap();
            template_record.push(None);
            println!("{} - {}", entry.typ, field);
        }
    }
    csvwriter.write_record(None::<&[u8]>).unwrap();

    // develop some sort of index to which index in row[] does the entry start
    let mut start_indices = Vec::new();
    let mut last_start = 0;
    let mut last_len = 1;
    for entry in metadata {
        start_indices.push(last_start + last_len);
        last_start += last_len;
        last_len = entry.field_count();
    }

    println!("{:?}", start_indices);

    for record in &log.records {
        let mut row = template_record.clone();
        row[0] = Some(format!("{}", record.timestamp_us as f64 / 1_000_000.0));
        match &record.data {
            Record::Control(_) => {}
            Record::Data(data) => {
                let (ind, metadata) = metadata
                    .iter()
                    .enumerate()
                    .find(|(_, entry)| entry.entry_id == record.entry_id)
                    .unwrap();
                let start = start_indices[ind];

                match metadata.typ {
                    "boolean" => {
                        let (_, val) = parse_boolean(data.data).unwrap();
                        row[start] = Some(format!("{:X?}", val as u8));
                    }
                    "int64" => {
                        let (_, val) = parse_int64(data.data).unwrap();
                        row[start] = Some(format!("{}", val));
                    }
                    "float" => {
                        let (_, val) = parse_float(data.data).unwrap();
                        row[start] = Some(format!("{}", val));
                    }
                    "double" => {
                        let (_, val) = parse_double(data.data).unwrap();
                        row[start] = Some(format!("{}", val));
                    }
                    "string" => {
                        let (_, val) = parse_string_full(data.data).unwrap();
                        row[start] = Some(val.to_string());
                    }
                    "boolean[]" => {
                        let (_, val) = parse_array(parse_boolean, data.data).unwrap();
                        row[start] = Some(format!("{}", val.len()));
                        val.iter().enumerate().for_each(|(offset, val)| {
                            row[start + offset + 1] = Some(format!("{:X?}", *val as u8));
                        });
                    }
                    "int64[]" => {
                        let (_, val) = parse_array(parse_int64, data.data).unwrap();
                        row[start] = Some(format!("{}", val.len()));
                        val.iter().enumerate().for_each(|(offset, val)| {
                            row[start + offset + 1] = Some(format!("{}", *val));
                        });
                    }
                    "float[]" => {
                        let (_, val) = parse_array(parse_float, data.data).unwrap();
                        row[start] = Some(format!("{}", val.len()));
                        val.iter().enumerate().for_each(|(offset, val)| {
                            row[start + offset + 1] = Some(format!("{}", *val));
                        });
                    }
                    "double[]" => {
                        let (_, val) = parse_array(parse_double, data.data).unwrap();
                        row[start] = Some(format!("{}", val.len()));
                        val.iter().enumerate().for_each(|(offset, val)| {
                            row[start + offset + 1] = Some(format!("{}", *val));
                        });
                    }
                    "string[]" => {
                        let (_, val) =
                            parse_array_ref_with_len(parse_string_with_len, data.data).unwrap();
                        row[start] = Some(serde_json::to_string(&val).unwrap());
                    } // Do we care to properly handle this?
                    _ => {
                        // raw, treat like an unsafe string
                        row[start] = Some(format!("{:X?}", data.data));
                    }
                }

                for field in row {
                    match field {
                        Some(val) => csvwriter.write_field(val).unwrap(),
                        None => csvwriter.write_field(&[]).unwrap(),
                    }
                }

                csvwriter.write_record(None::<&[u8]>).unwrap();
            }
        }
    }

    csvwriter.flush().unwrap();
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut infile = File::open(&args[1]).unwrap();

    let mut content = Vec::new();
    infile.read_to_end(&mut content).unwrap();

    let parsed_log = parse_wpilog(&content).unwrap().1;

    let metadata = parsed_log.get_entry_metadata();

    // sort(&mut organized_log);

    let data_fname = args[1].clone() + ".data.csv";
    let data_file = Path::new(&data_fname);

    export_data(data_file, &parsed_log, &metadata);

    let types_fname = args[1].clone() + ".types.csv";
    let types_file = Path::new(&types_fname);

    export_types(types_file, &metadata);

    let metadata_fname = args[1].clone() + ".metadata.csv";
    let metadata_file = Path::new(&metadata_fname);

    export_metadata(metadata_file, &metadata);
}
