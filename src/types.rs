use std::collections::HashMap;

#[derive(Default, Debug, Clone)]
pub struct WpiLog<'a> {
    pub major_version: u8,
    pub minor_version: u8,
    pub extra_header: &'a str,
    pub records: Vec<WpiRecord<'a>>,
}

impl<'a> WpiLog<'a> {
    pub fn get_entry_metadata(&self) -> Vec<MetadataEntry<'a>> {
        let mut map = HashMap::new();

        for entry in &self.records {
            match &entry.data {
                Record::Control(ControlRecord::Start(start)) => {
                    if map.contains_key(&start.entry_id) {
                        panic!("duped entry ids");
                    }
                    map.entry(start.entry_id).or_insert_with(|| MetadataEntry {
                        entry_id: start.entry_id,
                        name: start.name,
                        typ: start.typ,
                        metadata: start.metadata,
                        entry_count: 0,
                        all_same_length: None,
                        finished: false,
                    });
                }
                Record::Control(ControlRecord::SetMetadata(set_metadata)) => {
                    if let Some(entry) = map.get_mut(&set_metadata.entry_id) {
                        entry.metadata = set_metadata.metadata;
                    }
                }
                Record::Control(ControlRecord::Finish(finish)) => {
                    if let Some(entry) = map.get_mut(&finish.entry_id) {
                        entry.finished = true;
                    }
                }
                Record::Data(data) => {
                    map.get_mut(&entry.entry_id).and_then::<(), _>(|record| {
                        if record.entry_count == 0 {
                            record.all_same_length = match record.typ {
                                "boolean" | "int64" | "float" | "double" | "string" => None,
                                "boolean[]" => Some((data.data.len(), 1)),
                                "int64[]" => Some( (data.data.len() / 8, 8)),
                                "float[]" => Some( (data.data.len() / 4, 4)),
                                "double[]" => Some((data.data.len() / 8, 8)),
                                "string[]" => None, // Do we care to handle this?
                                _ => None,
                            }
                        } else {
                            record.all_same_length = record.all_same_length.and_then(|(len, div)| {
                                if len == data.data.len() / div {
                                    Some((len, div))
                                } else {
                                    None
                                }
                            });
                        }

                        record.entry_count += 1;

                        None
                    });
                }
            }
        }

        map.values().cloned().collect()
    }
}

#[derive(Debug, Clone)]
pub struct WpiRecord<'a> {
    pub entry_id: u32,
    pub timestamp_us: u64,
    pub data: Record<'a>,
}

#[derive(Debug, Clone)]
pub enum Record<'a> {
    Data(DataRecord<'a>),
    Control(ControlRecord<'a>),
}

#[derive(Default, Debug, Clone)]
pub struct DataRecord<'a> {
    pub data: &'a [u8],
}

#[derive(Debug, Clone)]
pub enum ControlRecord<'a> {
    Start(StartRecord<'a>),
    Finish(FinishRecord),
    SetMetadata(SetMetadataRecord<'a>),
}

#[derive(Default, Debug, Clone)]
pub struct StartRecord<'a> {
    pub entry_id: u32,
    pub name: &'a str,
    pub typ: &'a str,
    pub metadata: &'a str,
}

#[derive(Default, Debug, Clone)]
pub struct FinishRecord {
    pub entry_id: u32,
}

#[derive(Default, Debug, Clone)]
pub struct SetMetadataRecord<'a> {
    pub entry_id: u32,
    pub metadata: &'a str,
}

#[derive(Debug, Clone, Default)]
pub struct MetadataEntry<'a> {
    pub entry_id: u32,
    pub name: &'a str,
    pub typ: &'a str,
    pub metadata: &'a str,
    pub entry_count: usize,
    pub(crate) all_same_length: Option<(usize, usize)>,
    pub finished: bool,
}

impl<'a> MetadataEntry<'a> {
    pub fn is_array(&self) -> bool {
        self.typ.ends_with("[]")
    }

    pub fn fields(&self) -> Vec<String> {
        if self.is_array() && self.all_same_length.is_some() && self.entry_count > 16 {
            (0..self.all_same_length.unwrap_or_default().0)
                .map(|i| format!("{}/[{}]", self.name, i))
                .fold(vec![format!("{}/len", self.name)], |mut v, entry| {
                    v.push(entry);
                    v
                })
        } else {
            vec![self.name.to_string()]
        }
    }

    pub fn field_count(&self) -> usize {
        self.all_same_length.unwrap_or_default().0 + 1
    }
}

#[derive(Debug, Clone)]
pub enum DataType<'a> {
    Raw(&'a [u8]),
    Boolean(bool),
    Int64(i64),
    Float(f32),
    Double(f64),
    String(&'a str),
}
