use crate::parser::{
    parse_array, parse_array_ref_with_len, parse_boolean, parse_double, parse_float, parse_int64,
    parse_raw, parse_string_full, parse_string_with_len,
};
use crate::types::{ControlRecord, DataEntry, DataVec, OrganizedLog, Record, WpiLog};

pub fn reorganize(input: WpiLog) -> OrganizedLog {
    let mut records = Vec::new();

    for start_record in input.start_records {
        match start_record.data {
            Record::Control(ControlRecord::Start(start)) => {
                let data = match start.typ {
                    "boolean" => DataVec::Boolean(Vec::new()),
                    "int64" => DataVec::Int64(Vec::new()),
                    "float" => DataVec::Float(Vec::new()),
                    "double" => DataVec::Double(Vec::new()),
                    "string" => DataVec::String(Vec::new()),
                    "boolean[]" => DataVec::BooleanArray(Vec::new()),
                    "int64[]" => DataVec::Int64Array(Vec::new()),
                    "float[]" => DataVec::FloatArray(Vec::new()),
                    "double[]" => DataVec::DoubleArray(Vec::new()),
                    "string[]" => DataVec::StringArray(Vec::new()),
                    _ => DataVec::Raw(Vec::new()),
                };

                let entry = DataEntry {
                    entry_id: start.entry_id,
                    name: start.name,
                    typ: start.typ,
                    metadata: start.metadata,
                    max_len: None,
                    timestamps: Vec::new(),
                    data,
                };

                records.push(entry);
            }
            _ => unreachable!(),
        }
    }

    for set_metadata_record in input.set_metadata_records {
        match set_metadata_record.data {
            Record::Control(ControlRecord::SetMetadata(set_metadata)) => {
                records
                    .iter_mut()
                    .filter(|record| record.entry_id == set_metadata.entry_id)
                    .for_each(|record| record.metadata = set_metadata.metadata);
            }
            _ => unreachable!(),
        }
    }

    for record in input.data_records {
        match record.data {
            Record::Data(data) => {
                records
                    .iter_mut()
                    .filter(|rec: &&mut DataEntry| rec.entry_id == record.entry_id)
                    .for_each(|rec| match &mut rec.data {
                        DataVec::Raw(vec) => {
                            rec.timestamps.push(record.timestamp_us);
                            vec.push(parse_raw(data.data).unwrap().1);
                        }
                        DataVec::Boolean(vec) => {
                            rec.timestamps.push(record.timestamp_us);
                            vec.push(parse_boolean(data.data).unwrap().1);
                        }
                        DataVec::Int64(vec) => {
                            rec.timestamps.push(record.timestamp_us);
                            vec.push(parse_int64(data.data).unwrap().1);
                        }
                        DataVec::Float(vec) => {
                            rec.timestamps.push(record.timestamp_us);
                            vec.push(parse_float(data.data).unwrap().1);
                        }
                        DataVec::Double(vec) => {
                            rec.timestamps.push(record.timestamp_us);
                            vec.push(parse_double(data.data).unwrap().1);
                        }
                        DataVec::String(vec) => {
                            rec.timestamps.push(record.timestamp_us);
                            vec.push(parse_string_full(data.data).unwrap().1);
                        }
                        DataVec::BooleanArray(vec) => {
                            let point = parse_array(parse_boolean, data.data).unwrap().1;

                            rec.max_len = Some(rec.max_len.unwrap_or_default().max(point.len()));
                            vec.push(point);
                        }
                        DataVec::Int64Array(vec) => {
                            let point = parse_array(parse_int64, data.data).unwrap().1;

                            rec.max_len = Some(rec.max_len.unwrap_or_default().max(point.len()));
                            vec.push(point);
                        }
                        DataVec::FloatArray(vec) => {
                            let point = parse_array(parse_float, data.data).unwrap().1;

                            rec.max_len = Some(rec.max_len.unwrap_or_default().max(point.len()));
                            vec.push(point);
                        }
                        DataVec::DoubleArray(vec) => {
                            let point = parse_array(parse_double, data.data).unwrap().1;

                            rec.max_len = Some(rec.max_len.unwrap_or_default().max(point.len()));
                            vec.push(point);
                        }
                        DataVec::StringArray(vec) => {
                            let point = parse_array_ref_with_len(parse_string_with_len, data.data)
                                .unwrap()
                                .1;

                            rec.max_len = Some(rec.max_len.unwrap_or_default().max(point.len()));
                            vec.push(point);
                        }
                    });
            }
            _ => unreachable!(),
        }
    }

    OrganizedLog {
        sessioned_data: records,
    }
}

pub fn sort(log: &mut OrganizedLog) {
    for record in &mut log.sessioned_data {
        let mut sortable_timestamps = record.timestamps.iter().enumerate().collect::<Vec<_>>();
        sortable_timestamps.sort_by_cached_key(|(_, timestamp)| **timestamp);

        let new_timestamps = sortable_timestamps
            .iter()
            .map(|(_, timestamp)| **timestamp)
            .collect::<Vec<_>>();
        let new_data = match &mut record.data {
            DataVec::Raw(vec) => {
                let mut new_vec = Vec::with_capacity(vec.len());
                for (ind, _) in sortable_timestamps {
                    new_vec.push(vec[ind]);
                }
                DataVec::Raw(new_vec)
            }
            DataVec::Boolean(vec) => {
                let mut new_vec = Vec::with_capacity(vec.len());
                for (ind, _) in sortable_timestamps {
                    new_vec.push(vec[ind]);
                }
                DataVec::Boolean(new_vec)
            }
            DataVec::Int64(vec) => {
                let mut new_vec = Vec::with_capacity(vec.len());
                for (ind, _) in sortable_timestamps {
                    new_vec.push(vec[ind]);
                }
                DataVec::Int64(new_vec)
            }
            DataVec::Float(vec) => {
                let mut new_vec = Vec::with_capacity(vec.len());
                for (ind, _) in sortable_timestamps {
                    new_vec.push(vec[ind]);
                }
                DataVec::Float(new_vec)
            }
            DataVec::Double(vec) => {
                let mut new_vec = Vec::with_capacity(vec.len());
                for (ind, _) in sortable_timestamps {
                    new_vec.push(vec[ind]);
                }
                DataVec::Double(new_vec)
            }
            DataVec::String(vec) => {
                let mut new_vec = Vec::with_capacity(vec.len());
                for (ind, _) in sortable_timestamps {
                    new_vec.push(vec[ind]);
                }
                DataVec::String(new_vec)
            }
            DataVec::BooleanArray(vec) => {
                let mut new_vec = Vec::with_capacity(vec.len());
                for (ind, _) in sortable_timestamps {
                    new_vec.push(vec[ind].clone());
                }
                DataVec::BooleanArray(new_vec)
            }
            DataVec::Int64Array(vec) => {
                let mut new_vec = Vec::with_capacity(vec.len());
                for (ind, _) in sortable_timestamps {
                    new_vec.push(vec[ind].clone());
                }
                DataVec::Int64Array(new_vec)
            }
            DataVec::FloatArray(vec) => {
                let mut new_vec = Vec::with_capacity(vec.len());
                for (ind, _) in sortable_timestamps {
                    new_vec.push(vec[ind].clone());
                }
                DataVec::FloatArray(new_vec)
            }
            DataVec::DoubleArray(vec) => {
                let mut new_vec = Vec::with_capacity(vec.len());
                for (ind, _) in sortable_timestamps {
                    new_vec.push(vec[ind].clone());
                }
                DataVec::DoubleArray(new_vec)
            }
            DataVec::StringArray(vec) => {
                let mut new_vec = Vec::with_capacity(vec.len());
                for (ind, _) in sortable_timestamps {
                    new_vec.push(vec[ind].clone());
                }
                DataVec::StringArray(new_vec)
            }
        };

        record.timestamps = new_timestamps;
        record.data = new_data;
    }
}

/* This is probably a bad idea
// the log _should_ be sorted, but it can function without
pub fn to_array<'a>(log: &'a OrganizedLog) -> Vec<Vec<Option<DataType<'a>>>> {
    let entry_count = log.sessioned_data.len();

    let mut next_index = vec![0; entry_count];

    let mut records = Vec::new();

    loop {
        let mut record = vec![None; entry_count + 1];

        if let Some((index, entry)) =
            log.sessioned_data
                .iter()
                .enumerate()
                .min_by_key(|(ind, entry)| {
                    if entry.data.is_empty() || next_index[*ind] == entry.data.len() {
                        u64::MAX
                    } else {
                        entry.data[next_index[*ind]].timestamp
                    }
                })
        {
            if entry.data.is_empty() || next_index[index] == entry.data.len() {
                break;
            }

            record[0] = Some(DataType::Double(
                entry.data[next_index[index]].timestamp as f64 / 1000000.0,
            ));

            record[index + 1] = Some(entry.data[next_index[index]].data.clone());
            next_index[index] += 1;
        } else {
            break;
        }

        records.push(record);
    }

    records
}
*/
