#[derive(Default, Debug, Clone)]
pub struct WpiLog<'a> {
    pub major_version: u8,
    pub minor_version: u8,
    pub extra_header: &'a str,
    pub start_records: Vec<WpiRecord<'a>>,
    pub set_metadata_records: Vec<WpiRecord<'a>>,
    pub finish_records: Vec<WpiRecord<'a>>,
    pub data_records: Vec<WpiRecord<'a>>,
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

#[derive(Default, Debug, Clone)]
pub struct OrganizedLog<'a> {
    pub sessioned_data: Vec<DataEntry<'a>>,
}

#[derive(Debug, Clone)]
pub struct DataEntry<'a> {
    pub entry_id: u32,
    pub name: &'a str,
    pub typ: &'a str,
    pub metadata: &'a str,
    pub timestamps: Vec<u64>,
    pub data: DataVec<'a>,
    pub(crate) max_len: Option<usize>,
}

impl<'a> DataEntry<'a> {
    pub fn is_array(&self) -> bool {
        self.typ.ends_with("[]")
    }

    fn max_len(&self) -> usize {
        self.max_len.unwrap_or_default()
    }

    pub fn fields(&mut self) -> Vec<String> {
        match self.data {
            DataVec::Raw(_)
            | DataVec::Boolean(_)
            | DataVec::Int64(_)
            | DataVec::Float(_)
            | DataVec::Double(_)
            | DataVec::String(_) => vec![self.name.to_string()],
            DataVec::BooleanArray(_)
            | DataVec::Int64Array(_)
            | DataVec::FloatArray(_)
            | DataVec::DoubleArray(_)
            | DataVec::StringArray(_) => {
                let max_len = self.max_len();
                (0..max_len).map(|i| format!("{}/[{}]", self.name, i)).fold(
                    vec![format!("{}/len", self.name)],
                    |mut v, entry| {
                        v.push(entry);
                        v
                    },
                )
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum DataVec<'a> {
    Raw(Vec<&'a [u8]>),
    Boolean(Vec<bool>),
    Int64(Vec<i64>),
    Float(Vec<f32>),
    Double(Vec<f64>),
    String(Vec<&'a str>),
    BooleanArray(Vec<Vec<bool>>),
    Int64Array(Vec<Vec<i64>>),
    FloatArray(Vec<Vec<f32>>),
    DoubleArray(Vec<Vec<f64>>),
    StringArray(Vec<Vec<&'a str>>),
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
