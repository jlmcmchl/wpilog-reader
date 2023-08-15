use std::collections::HashMap;

use nom::{
    multi::length_data,
    number::complete::{le_u16, le_u32, le_u64, le_u8},
    IResult, Parser,
};

use crate::mcap::parse_utils::parse_map;

use self::parse_utils::{parse_array, parse_str, parse_tuple};

pub trait Parse<'a, T> {
    fn parse(input: &'a [u8]) -> IResult<&'a [u8], T>;
}

#[derive(Debug, Clone)]
pub struct MCap<'a> {
    pub records: Vec<RawRecord<'a>>,
}

impl<'a> Parse<'a, MCap<'a>> for MCap<'a> {
    fn parse(input: &'a [u8]) -> IResult<&'a [u8], MCap<'a>> {
        let (input, inner) = nom::sequence::delimited(
            Magic::parse,
            nom::multi::many0(RawRecord::parse),
            Magic::parse,
        )(input)?;

        Ok((input, MCap { records: inner }))
    }
}

impl<'a> Parse<'a, Record<'a>> for Record<'a> {
    fn parse(input: &'a [u8]) -> IResult<&'a [u8], Record<'a>> {
        nom::branch::alt((
            nom::combinator::into(Header::parse),
            nom::combinator::into(Footer::parse),
            nom::combinator::into(Schema::parse),
            nom::combinator::into(Channel::parse),
            nom::combinator::into(Message::parse),
            nom::combinator::into(Chunk::parse),
            nom::combinator::into(MessageIndex::parse),
            nom::combinator::into(ChunkIndex::parse),
            nom::combinator::into(Attachment::parse),
            nom::combinator::into(Metadata::parse),
            nom::combinator::into(DataEnd::parse),
            nom::combinator::into(AttachmentIndex::parse),
            nom::combinator::into(MetadataIndex::parse),
            nom::combinator::into(Statistics::parse),
            nom::combinator::into(SummaryOffset::parse),
        ))(input)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RawRecord<'a> {
    pub tag: u8,
    pub content: &'a [u8],
}

impl<'a> Parse<'a, RawRecord<'a>> for RawRecord<'a> {
    fn parse(input: &'a [u8]) -> IResult<&'a [u8], RawRecord<'a>> {
        let (input, tag) = nom::number::complete::le_u8(input)?;

        let (input, content) = length_data(le_u64)(input)?;

        Ok((input, RawRecord { tag, content }))
    }
}

impl<'a> TryFrom<RawRecord<'a>> for Record<'a> {
    type Error = nom::Err<nom::error::Error<&'a [u8]>>;

    fn try_from(value: RawRecord<'a>) -> Result<Self, Self::Error> {
        let (_, record) = match value.tag {
            0x01 => nom::combinator::map(Header::parse, Record::from)(value.content),
            0x02 => nom::combinator::map(Footer::parse, Record::from)(value.content),
            0x03 => nom::combinator::map(Schema::parse, Record::from)(value.content),
            0x04 => nom::combinator::map(Channel::parse, Record::from)(value.content),
            0x05 => nom::combinator::map(Message::parse, Record::from)(value.content),
            0x06 => nom::combinator::map(Chunk::parse, Record::from)(value.content),
            0x07 => nom::combinator::map(MessageIndex::parse, Record::from)(value.content),
            0x08 => nom::combinator::map(ChunkIndex::parse, Record::from)(value.content),
            0x09 => nom::combinator::map(Attachment::parse, Record::from)(value.content),
            0x0C => nom::combinator::map(Metadata::parse, Record::from)(value.content),
            0x0F => nom::combinator::map(DataEnd::parse, Record::from)(value.content),
            0x0A => nom::combinator::map(AttachmentIndex::parse, Record::from)(value.content),
            0x0D => nom::combinator::map(MetadataIndex::parse, Record::from)(value.content),
            0x0B => nom::combinator::map(Statistics::parse, Record::from)(value.content),
            0x0E => nom::combinator::map(SummaryOffset::parse, Record::from)(value.content),
            _ => unreachable!(),
        }?;

        Ok(record)
    }
}

#[derive(Debug, Clone)]
pub enum Record<'a> {
    Magic(Magic),
    Header(Header<'a>),
    Footer(Footer),
    Schema(Schema<'a>),
    Channel(Channel<'a>),
    Message(Message),
    Chunk(Chunk<'a>),
    MessageIndex(MessageIndex),
    ChunkIndex(ChunkIndex<'a>),
    Attachment(Attachment),
    Metadata(Metadata),
    DataEnd(DataEnd),
    AttachmentIndex(AttachmentIndex),
    MetadataIndex(MetadataIndex),
    Statistics(Statistics),
    SummaryOffset(SummaryOffset),
}

#[derive(Debug, Clone, Copy)]
pub enum RecordType {
    Magic,
    Header,
    Footer,
    Schema,
    Channel,
    Message,
    Chunk,
    MessageIndex,
    ChunkIndex,
    Attachment,
    Metadata,
    DataEnd,
    AttachmentIndex,
    MetadataIndex,
    Statistics,
    SummaryOffset,
}

impl<'a> From<&Record<'a>> for RecordType {
    fn from(value: &Record<'a>) -> Self {
        match value {
            Record::Magic(_) => RecordType::Magic,
            Record::Header(_) => RecordType::Header,
            Record::Footer(_) => RecordType::Footer,
            Record::Schema(_) => RecordType::Schema,
            Record::Channel(_) => RecordType::Channel,
            Record::Message(_) => RecordType::Message,
            Record::Chunk(_) => RecordType::Chunk,
            Record::MessageIndex(_) => RecordType::MessageIndex,
            Record::ChunkIndex(_) => RecordType::ChunkIndex,
            Record::Attachment(_) => RecordType::Attachment,
            Record::Metadata(_) => RecordType::Metadata,
            Record::DataEnd(_) => RecordType::DataEnd,
            Record::AttachmentIndex(_) => RecordType::AttachmentIndex,
            Record::MetadataIndex(_) => RecordType::MetadataIndex,
            Record::Statistics(_) => RecordType::Statistics,
            Record::SummaryOffset(_) => RecordType::SummaryOffset,
        }
    }
}

impl TryFrom<u8> for RecordType {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(RecordType::Header),
            0x02 => Ok(RecordType::Footer),
            0x03 => Ok(RecordType::Schema),
            0x04 => Ok(RecordType::Channel),
            0x05 => Ok(RecordType::Message),
            0x06 => Ok(RecordType::Chunk),
            0x07 => Ok(RecordType::MessageIndex),
            0x08 => Ok(RecordType::ChunkIndex),
            0x09 => Ok(RecordType::Attachment),
            0x0C => Ok(RecordType::Metadata),
            0x0F => Ok(RecordType::DataEnd),
            0x0A => Ok(RecordType::AttachmentIndex),
            0x0D => Ok(RecordType::MetadataIndex),
            0x0B => Ok(RecordType::Statistics),
            0x0E => Ok(RecordType::SummaryOffset),
            _ => Err(format!("Unknown record type: {value}").to_owned()),
        }
    }
}

impl<'a> Record<'a> {
    pub fn get_op(self) -> u8 {
        match self {
            Record::Magic(_) => 0x89,
            Record::Header(_) => 0x01,
            Record::Footer(_) => 0x02,
            Record::Schema(_) => 0x03,
            Record::Channel(_) => 0x04,
            Record::Message(_) => 0x05,
            Record::Chunk(_) => 0x06,
            Record::MessageIndex(_) => 0x07,
            Record::ChunkIndex(_) => 0x08,
            Record::Attachment(_) => 0x09,
            Record::Metadata(_) => 0x0C,
            Record::DataEnd(_) => 0x0F,
            Record::AttachmentIndex(_) => 0x0A,
            Record::MetadataIndex(_) => 0x0D,
            Record::Statistics(_) => 0x0B,
            Record::SummaryOffset(_) => 0x0E,
        }
    }
}

macro_rules! create_empty_record {
    ($sname:ident) => {
        #[derive(Default, Debug, Clone, Copy)]
        pub struct $sname {}

        impl<'a> From<$sname> for Record<'a> {
            fn from(value: $sname) -> Self {
                Record::$sname(value)
            }
        }

        impl<'a> Parse<'a, $sname> for $sname {
            fn parse(input: &'a [u8]) -> IResult<&'a [u8], $sname> {
                Ok((input, Default::default()))
            }
        }
    };
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Magic {}

impl<'a> Parse<'a, Magic> for Magic {
    fn parse(input: &'a [u8]) -> IResult<&'a [u8], Magic> {
        let (input, _) = nom::bytes::complete::tag(b"\x89MCAP\x30\r\n")(input)?;

        Ok((input, Default::default()))
    }
}

impl<'a> From<Magic> for Record<'a> {
    fn from(value: Magic) -> Self {
        Record::Magic(value)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Header<'a> {
    profile: &'a str,
    library: &'a str,
}

impl<'a> Parse<'a, Header<'a>> for Header<'a> {
    fn parse(input: &'a [u8]) -> IResult<&'a [u8], Header> {
        let (input, profile) = parse_utils::parse_str(input)?;
        let (input, library) = parse_utils::parse_str(input)?;

        Ok((input, Header { profile, library }))
    }
}

impl<'a> From<Header<'a>> for Record<'a> {
    fn from(value: Header<'a>) -> Self {
        Record::Header(value)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Footer {
    summary_start: u64,
    summary_offset_start: u64,
    summary_crc: u32,
}

impl<'a> Parse<'a, Footer> for Footer {
    fn parse(input: &'a [u8]) -> IResult<&'a [u8], Footer> {
        let (input, summary_start) = le_u64(input)?;
        let (input, summary_offset_start) = le_u64(input)?;
        let (input, summary_crc) = le_u32(input)?;

        Ok((
            input,
            Footer {
                summary_start,
                summary_offset_start,
                summary_crc,
            },
        ))
    }
}

impl<'a> From<Footer> for Record<'a> {
    fn from(value: Footer) -> Self {
        Record::Footer(value)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Schema<'a> {
    id: u16,
    name: &'a str,
    encoding: &'a str,
    data: &'a str,
}

impl<'a> Parse<'a, Schema<'a>> for Schema<'a> {
    fn parse(input: &'a [u8]) -> IResult<&'a [u8], Schema<'a>> {
        let (input, id) = le_u16(input)?;
        let (input, name) = parse_str(input)?;
        let (input, encoding) = parse_str(input)?;
        let (input, data) = parse_str(input)?;

        Ok((
            input,
            Schema {
                id,
                name,
                encoding,
                data,
            },
        ))
    }
}

impl<'a> From<Schema<'a>> for Record<'a> {
    fn from(value: Schema<'a>) -> Self {
        Record::Schema(value)
    }
}

#[derive(Debug, Clone)]
pub struct Channel<'a> {
    id: u16,
    schema_id: u16,
    topic: &'a str,
    message_encoding: &'a str,
    metadata: HashMap<&'a str, &'a str>,
}

impl<'a> Parse<'a, Channel<'a>> for Channel<'a> {
    fn parse(input: &'a [u8]) -> IResult<&'a [u8], Channel<'a>> {
        let (input, id) = le_u16(input)?;
        let (input, schema_id) = le_u16(input)?;
        let (input, topic) = parse_str(input)?;
        let (input, message_encoding) = parse_str(input)?;
        let (input, metadata) =
            parse_utils::parse_map(Box::new(parse_str), Box::new(parse_str)).parse(input)?;

        Ok((
            input,
            Channel {
                id,
                schema_id,
                topic,
                message_encoding,
                metadata,
            },
        ))
    }
}

impl<'a> From<Channel<'a>> for Record<'a> {
    fn from(value: Channel<'a>) -> Self {
        Record::Channel(value)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Chunk<'a> {
    message_start_time: u64,
    message_end_time: u64,
    uncompressed_size: u64,
    uncompressed_crc: u32,
    compression: &'a str,
    records: &'a [u8],
}

impl<'a> Parse<'a, Chunk<'a>> for Chunk<'a> {
    fn parse(input: &'a [u8]) -> IResult<&'a [u8], Chunk<'a>> {
        let (input, message_start_time) = le_u64(input)?;
        let (input, message_end_time) = le_u64(input)?;
        let (input, uncompressed_size) = le_u64(input)?;
        let (input, uncompressed_crc) = le_u32(input)?;
        let (input, compression) = parse_str(input)?;
        let (input, records) = length_data(le_u64)(input)?;

        Ok((
            input,
            Chunk {
                message_start_time,
                message_end_time,
                uncompressed_size,
                uncompressed_crc,
                compression,
                records,
            },
        ))
    }
}

impl<'a> From<Chunk<'a>> for Record<'a> {
    fn from(value: Chunk<'a>) -> Self {
        Record::Chunk(value)
    }
}

#[derive(Debug, Clone)]
pub struct MessageIndex {
    channel_id: u16,
    records: Vec<(u64, u64)>,
}

impl<'a> Parse<'a, MessageIndex> for MessageIndex {
    fn parse(input: &'a [u8]) -> IResult<&'a [u8], MessageIndex> {
        let (input, channel_id) = le_u16(input)?;
        let (input, records) = parse_array(parse_tuple(le_u64, le_u64)).parse(input)?;

        Ok((
            input,
            MessageIndex {
                channel_id,
                records,
            },
        ))
    }
}

impl<'a> From<MessageIndex> for Record<'a> {
    fn from(value: MessageIndex) -> Self {
        Record::MessageIndex(value)
    }
}

#[derive(Debug, Clone)]
pub struct Statistics {
    message_count: u64,
    schema_count: u16,
    channel_count: u32,
    attachment_count: u32,
    metadata_count: u32,
    chunk_count: u32,
    message_start_time: u64,
    message_end_time: u64,
    channel_message_counts: HashMap<u16, u64>,
}

impl<'a> Parse<'a, Statistics> for Statistics {
    fn parse(input: &'a [u8]) -> IResult<&'a [u8], Statistics> {
        let (input, message_count) = le_u64(input)?;
        let (input, schema_count) = le_u16(input)?;
        let (input, channel_count) = le_u32(input)?;
        let (input, attachment_count) = le_u32(input)?;
        let (input, metadata_count) = le_u32(input)?;
        let (input, chunk_count) = le_u32(input)?;
        let (input, message_start_time) = le_u64(input)?;
        let (input, message_end_time) = le_u64(input)?;
        let (input, channel_message_counts) =
            parse_map(Box::new(le_u16), Box::new(le_u64)).parse(input)?;

        Ok((
            input,
            Statistics {
                message_count,
                schema_count,
                channel_count,
                attachment_count,
                metadata_count,
                chunk_count,
                message_start_time,
                message_end_time,
                channel_message_counts,
            },
        ))
    }
}

impl<'a> From<Statistics> for Record<'a> {
    fn from(value: Statistics) -> Self {
        Record::Statistics(value)
    }
}

#[derive(Debug, Clone)]
pub struct DataEnd {
    data_section_crc: u32,
}

impl<'a> Parse<'a, DataEnd> for DataEnd {
    fn parse(input: &'a [u8]) -> IResult<&'a [u8], DataEnd> {
        let (input, data_section_crc) = le_u32(input)?;

        Ok((input, DataEnd { data_section_crc }))
    }
}

impl<'a> From<DataEnd> for Record<'a> {
    fn from(value: DataEnd) -> Self {
        Record::DataEnd(value)
    }
}

#[derive(Debug, Clone)]
pub struct ChunkIndex<'a> {
    message_start_time: u64,
    message_end_time: u64,
    chunk_start_offset: u64,
    chunk_length: u64,
    message_index_offsets: HashMap<u16, u64>,
    message_index_length: u64,
    compression: &'a str,
    compressed_size: u64,
    uncompressed_size: u64,
}

impl<'a> Parse<'a, ChunkIndex<'a>> for ChunkIndex<'a> {
    fn parse(input: &'a [u8]) -> IResult<&'a [u8], ChunkIndex<'a>> {
        let (input, message_start_time) = le_u64(input)?;
        let (input, message_end_time) = le_u64(input)?;
        let (input, chunk_start_offset) = le_u64(input)?;
        let (input, chunk_length) = le_u64(input)?;
        let (input, message_index_offsets) =
            parse_map(Box::new(le_u16), Box::new(le_u64)).parse(input)?;
        let (input, message_index_length) = le_u64(input)?;
        let (input, compression) = parse_str(input)?;
        let (input, compressed_size) = le_u64(input)?;
        let (input, uncompressed_size) = le_u64(input)?;

        Ok((
            input,
            ChunkIndex {
                message_start_time,
                message_end_time,
                chunk_start_offset,
                chunk_length,
                message_index_offsets,
                message_index_length,
                compression,
                compressed_size,
                uncompressed_size,
            },
        ))
    }
}

impl<'a> From<ChunkIndex<'a>> for Record<'a> {
    fn from(value: ChunkIndex<'a>) -> Self {
        Record::ChunkIndex(value)
    }
}

#[derive(Debug, Clone)]
pub struct SummaryOffset {
    group_opcode: u8,
    group_start: u64,
    group_length: u64,
}

impl<'a> Parse<'a, SummaryOffset> for SummaryOffset {
    fn parse(input: &'a [u8]) -> IResult<&'a [u8], SummaryOffset> {
        let (input, group_opcode) = le_u8(input)?;
        let (input, group_start) = le_u64(input)?;
        let (input, group_length) = le_u64(input)?;

        Ok((
            input,
            SummaryOffset {
                group_opcode,
                group_start,
                group_length,
            },
        ))
    }
}

impl<'a> From<SummaryOffset> for Record<'a> {
    fn from(value: SummaryOffset) -> Self {
        Record::SummaryOffset(value)
    }
}

create_empty_record!(Message);
create_empty_record!(Attachment);
create_empty_record!(Metadata);
create_empty_record!(AttachmentIndex);
create_empty_record!(MetadataIndex);

mod parse_utils {
    use std::{collections::HashMap, hash::Hash};

    use nom::{error::ParseError, multi::length_data, number::complete::le_u32, IResult, Parser};

    pub fn parse_str(input: &[u8]) -> IResult<&[u8], &str> {
        nom::combinator::map_res(length_data(le_u32), |slice| std::str::from_utf8(slice))(input)
    }

    pub struct MapParser<'a, 'b, K, V, E>
    where
        E: ParseError<&'a [u8]>,
        'b: 'a,
    {
        key_parser: Box<dyn Parser<&'a [u8], K, E> + 'b>,
        value_parser: Box<dyn Parser<&'a [u8], V, E> + 'b>,
    }

    impl<'a, 'b, K, V, E> Parser<&'a [u8], HashMap<K, V>, E> for MapParser<'a, 'b, K, V, E>
    where
        E: ParseError<&'a [u8]>,
        K: Eq + Hash,
    {
        fn parse(&mut self, input: &'a [u8]) -> IResult<&'a [u8], HashMap<K, V>, E> {
            let (input, mut content) = length_data(le_u32)(input)?;

            let mut map = HashMap::new();

            while !content.is_empty() {
                let input = content;

                let (input, k) = self.key_parser.parse(input)?;
                let (input, v) = self.value_parser.parse(input)?;

                map.insert(k, v);

                content = input;
            }

            Ok((input, map))
        }
    }

    pub fn parse_map<'a, 'b, K, V, E, F, G>(
        key_parser: Box<F>,
        value_parser: Box<G>,
    ) -> impl Parser<&'a [u8], HashMap<K, V>, E>
    where
        K: Eq + Hash,
        E: ParseError<&'a [u8]>,
        F: Parser<&'a [u8], K, E> + 'b,
        G: Parser<&'a [u8], V, E> + 'b,
        'b: 'a,
    {
        MapParser {
            key_parser,
            value_parser,
        }
    }

    pub fn parse_tuple<'a, O1, O2, E: ParseError<&'a [u8]>, F, G>(
        first: F,
        second: G,
    ) -> impl Parser<&'a [u8], (O1, O2), E>
    where
        F: Parser<&'a [u8], O1, E>,
        G: Parser<&'a [u8], O2, E>,
    {
        nom::sequence::pair(first, second)
    }

    pub fn parse_array<'a, O, E: ParseError<&'a [u8]>, F>(
        parser: F,
    ) -> impl Parser<&'a [u8], Vec<O>, E>
    where
        F: Parser<&'a [u8], O, E>,
    {
        length_data(le_u32).and_then(nom::multi::many0(parser))
    }
}
