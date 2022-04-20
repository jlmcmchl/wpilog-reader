use std::{env, fs::File, io::Read, path::Path};

use wpilog_reader::{
    parser::{
        parse_array, parse_array_ref_with_len, parse_boolean, parse_double, parse_float,
        parse_int64, parse_string_full, parse_string_with_len, parse_wpilog,
    },
    types::{MetadataEntry, Record, WpiLog, WpiRecord},
};

fn insert_data_into_row(
    data: &[u8],
    metadata: &MetadataEntry,
    row: &mut [Option<String>],
    start: usize,
) {
    match metadata.typ {
        "boolean" => {
            let (_, val) = parse_boolean(data).unwrap();
            row[start] = Some(format!("{:X?}", val as u8));
        }
        "int64" => {
            let (_, val) = parse_int64(data).unwrap();
            row[start] = Some(format!("{}", val));
        }
        "float" => {
            let (_, val) = parse_float(data).unwrap();
            row[start] = Some(format!("{}", val));
        }
        "double" => {
            let (_, val) = parse_double(data).unwrap();
            row[start] = Some(format!("{}", val));
        }
        "string" => {
            let (_, val) = parse_string_full(data).unwrap();
            row[start] = Some(val.to_string());
        }
        "boolean[]" => {
            let (_, val) = parse_array(parse_boolean, data).unwrap();

            if metadata.should_expand() {
                row[start] = Some(format!("{}", val.len()));
                val.iter().enumerate().for_each(|(offset, val)| {
                    row[start + offset + 1] = Some(format!("{:X?}", *val as u8));
                });
            } else {
                row[start] = Some(serde_json::to_string(&val).unwrap());
            }
        }
        "int64[]" => {
            let (_, val) = parse_array(parse_int64, data).unwrap();

            if metadata.should_expand() {
                row[start] = Some(format!("{}", val.len()));
                val.iter().enumerate().for_each(|(offset, val)| {
                    row[start + offset + 1] = Some(format!("{}", *val));
                });
            } else {
                row[start] = Some(serde_json::to_string(&val).unwrap());
            }
        }
        "float[]" => {
            let (_, val) = parse_array(parse_float, data).unwrap();

            if metadata.should_expand() {
                row[start] = Some(format!("{}", val.len()));
                val.iter().enumerate().for_each(|(offset, val)| {
                    row[start + offset + 1] = Some(format!("{}", *val));
                });
            } else {
                row[start] = Some(serde_json::to_string(&val).unwrap());
            }
        }
        "double[]" => {
            let (_, val) = parse_array(parse_double, data).unwrap();

            if metadata.should_expand() {
                row[start] = Some(format!("{}", val.len()));
                val.iter().enumerate().for_each(|(offset, val)| {
                    row[start + offset + 1] = Some(format!("{}", *val));
                });
            } else {
                row[start] = Some(serde_json::to_string(&val).unwrap());
            }
        }
        "string[]" => {
            let (_, val) = parse_array_ref_with_len(parse_string_with_len, data).unwrap();
            row[start] = Some(serde_json::to_string(&val).unwrap());
        } // Do we care to properly handle this?
        _ => {
            // raw, treat like an unsafe string
            row[start] = Some(format!("{:X?}", data));
        }
    }
}

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

fn export_data(
    data_file: &Path,
    log: &[WpiRecord],
    metadata: &[MetadataEntry],
    start: u64,
    end: u64,
) {
    let mut csvwriter = csv::Writer::from_path(data_file).unwrap();

    csvwriter.write_field("timestamp").unwrap();

    let field_count: usize = metadata.iter().map(|entry| entry.field_count()).sum();
    let template_record = vec![None; field_count + 1];

    for entry in metadata {
        for field in entry.fields() {
            csvwriter.write_field(field.clone()).unwrap();
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

    let state = state_before_timestamp(log, metadata, start);

    let state_timestamp = state
        .iter()
        .map(|record| record.map(|rec| rec.timestamp_us))
        .max();
    match state_timestamp {
        // state has something and we should use it
        Some(timestamp) => {
            let mut row = template_record.clone();
            row[0] = Some(format!("{}", timestamp.unwrap() as f64 / 1_000_000.0));

            for (ind, record) in state.iter().enumerate() {
                let start = start_indices[ind];

                if let Some(WpiRecord {
                    data: Record::Data(data),
                    ..
                }) = record
                {
                    let metadata = &metadata[ind];

                    insert_data_into_row(data, metadata, &mut row, start);
                };
            }

            for field in row {
                match field {
                    Some(val) => csvwriter.write_field(val).unwrap(),
                    None => csvwriter.write_field(&[]).unwrap(),
                }
            }

            csvwriter.write_record(None::<&[u8]>).unwrap();
        }
        // state is empty
        None => {}
    }

    let mut row = template_record.clone();
    let mut current_timestamp = 0;

    for record in log
        .iter()
        .filter(|record| start <= record.timestamp_us && record.timestamp_us <= end && matches!(record.data, Record::Data(_)))
    {
        let first_entry = row.get_mut(0).unwrap();
        if first_entry.is_none() {
            row[0] = Some(format!("{}", record.timestamp_us as f64 / 1_000_000.0));
            current_timestamp = record.timestamp_us;
        } else if record.timestamp_us != current_timestamp {
            // write row, setup for new row
            for field in row {
                match field {
                    Some(val) => csvwriter.write_field(val).unwrap(),
                    None => csvwriter.write_field(&[]).unwrap(),
                }
            }

            csvwriter.write_record(None::<&[u8]>).unwrap();

            row = template_record.clone();
            current_timestamp = record.timestamp_us;
        }

        match &record.data {
            Record::Data(data) => {
                let (ind, metadata) = metadata
                    .iter()
                    .enumerate()
                    .find(|(_, entry)| {
                        entry.entry_id == record.entry_id
                            && entry.start_time <= record.timestamp_us
                            && entry.end_time >= record.timestamp_us
                    })
                    .unwrap();
                let start = start_indices[ind];

                insert_data_into_row(data, metadata, &mut row, start);
            },
            _ => {}
        }
    }

    csvwriter.flush().unwrap();
}

fn get_enabled_periods(log: &WpiLog, metadata: &[MetadataEntry]) -> Option<Vec<(u64, u64)>> {
    let min_timestamp = log
        .records
        .iter()
        .min_by_key(|record| record.timestamp_us)
        .unwrap();
    let max_timestamp = log
        .records
        .iter()
        .max_by_key(|record| record.timestamp_us)
        .unwrap();

    metadata
        .iter()
        .find(|record| record.name == "DS:enabled")
        .map(|entry| {
            // println!("{:?}", entry);
            log.records
                .iter()
                .filter(|record| record.entry_id == entry.entry_id)
                .fold(Vec::new(), |mut agg, record| {
                    // println!("{:} {:?}", record.timestamp_us, record.data);
                    if let Record::Data(slice) = record.data {
                        if slice[0] == 1 {
                            agg.push((Some(record.timestamp_us), None));
                        } else if slice[0] == 0 {
                            match agg.last_mut() {
                                Some(span) => {
                                    span.1 = Some(record.timestamp_us);
                                }
                                None => {}
                            }
                        }
                    }
                    agg
                })
        })
        .map(|periods| {
            periods
                .iter()
                .map(|period| {
                    (
                        period.0.unwrap_or(min_timestamp.timestamp_us),
                        period.1.unwrap_or(max_timestamp.timestamp_us),
                    )
                })
                .collect::<Vec<_>>()
        })
}

fn state_before_timestamp<'a, 'b>(
    log: &'b [WpiRecord<'a>],
    metadata: &[MetadataEntry],
    ts: u64,
) -> Vec<Option<&'b WpiRecord<'a>>> {
    let mut state: Vec<Option<&WpiRecord>> = vec![None; metadata.len()];

    for record in log
        .iter()
        .filter(|record| matches!(record.data, Record::Data(_)))
    {
        if record.timestamp_us >= ts {
            break;
        }

        metadata
            .iter()
            .enumerate()
            .find(|(_, metadata)| {
                metadata.entry_id == record.entry_id
                    && metadata.start_time <= record.timestamp_us
                    && metadata.end_time >= record.timestamp_us
            })
            .iter()
            .for_each(|(ind, _)| state[*ind] = Some(record));
    }

    state
}

fn main() {
    let start = std::time::Instant::now();
    let args: Vec<String> = env::args().collect();
    let in_path = Path::new(&args[1]);
    let mut infile = File::open(in_path).unwrap();

    let mut content = Vec::new();
    infile.read_to_end(&mut content).unwrap();

    let mut parsed_log = parse_wpilog(&content).unwrap().1;

    let metadata = parsed_log.get_entry_metadata();

    parsed_log.sort();

    let enabled_periods = get_enabled_periods(&parsed_log, &metadata);
    // println!("{:?}", enabled_periods);

    let types_fname = format!(
        "{}/{}-types.csv",
        in_path.parent().unwrap().to_str().unwrap(),
        in_path.file_stem().unwrap().to_str().unwrap()
    );
    let types_file = Path::new(&types_fname);

    export_types(types_file, &metadata);

    let metadata_fname = format!(
        "{}/{}-metadata.csv",
        in_path.parent().unwrap().to_str().unwrap(),
        in_path.file_stem().unwrap().to_str().unwrap()
    );
    let metadata_file = Path::new(&metadata_fname);

    export_metadata(metadata_file, &metadata);

    match enabled_periods {
        Some(periods) => {
            for period in periods {
                let data_fname = format!(
                    "{}/{}-data_{}-{}.csv",
                    in_path.parent().unwrap().to_str().unwrap(),
                    in_path.file_stem().unwrap().to_str().unwrap(),
                    period.0,
                    period.1
                );
                let data_file = Path::new(&data_fname);

                export_data(
                    data_file,
                    &parsed_log.records,
                    &metadata,
                    period.0,
                    period.1,
                );
            }
        }
        None => {}
    }

    let end = std::time::Instant::now();
    println!("took: {:?}", end - start);
}
