use nom::IResult;

use crate::types::*;

pub fn parse_wpilog(input: &[u8]) -> IResult<&[u8], WpiLog> {
    let (input, _) = nom::bytes::complete::tag("WPILOG")(input)?;
    let (input, minor_version) = nom::number::complete::le_u8(input)?;
    let (input, major_version) = nom::number::complete::le_u8(input)?;
    let (input, extra_header) = parse_string_with_len(input)?;

    if major_version == 1 && minor_version == 0 {
        let (input, records) =
            nom::combinator::all_consuming(nom::multi::many0(parse_wpilog_record))(input)?;
        Ok((
            input,
            WpiLog {
                major_version,
                minor_version,
                extra_header,
                records,
            },
        ))
    } else {
        Err(nom::Err::Failure(nom::error::make_error(
            input,
            nom::error::ErrorKind::Fail,
        )))
    }
}

fn parse_u32(input: &[u8], len: u8) -> IResult<&[u8], u32> {
    let mut agg = 0;
    let mut input = input;

    for iter in 0..=len {
        let (rest, entry_id) = nom::number::complete::le_u8(input)?;
        agg |= (entry_id as u32) << (8 * iter);

        input = rest;
    }

    Ok((input, agg))
}

fn parse_u64(input: &[u8], len: u8) -> IResult<&[u8], u64> {
    let mut agg = 0;
    let mut input = input;

    for iter in 0..=len {
        let (rest, entry_id) = nom::number::complete::le_u8(input)?;
        agg |= (entry_id as u64) << (8 * iter);

        input = rest;
    }

    Ok((input, agg))
}

fn parse_wpilog_record(input: &[u8]) -> IResult<&[u8], WpiRecord> {
    let (input, header_len) = nom::number::complete::le_u8(input)?;
    let entry_id_len = header_len & 0x3;
    let payload_size_len = (header_len >> 2) & 0x3;
    let timestamp_len = (header_len >> 4) & 0x7;
    let (input, entry_id) = parse_u32(input, entry_id_len)?;
    let (input, payload_size) = parse_u32(input, payload_size_len)?;
    let (input, timestamp_us) = parse_u64(input, timestamp_len)?;
    let (input, data) = nom::bytes::complete::take(payload_size)(input)?;

    let data = if entry_id == 0 {
        // control record, must be parsed
        let (_, record) = nom::combinator::all_consuming(parse_control_record)(data)?;

        Record::Control(record)
    } else {
        // data record
        Record::Data(DataRecord { data })
    };

    Ok((
        input,
        WpiRecord {
            entry_id,
            timestamp_us,
            data,
        },
    ))
}

fn parse_control_record(input: &[u8]) -> IResult<&[u8], ControlRecord> {
    let (rest, control_record_type) = nom::number::complete::le_u8(input)?;

    match control_record_type {
        0 => {
            // Start record
            parse_start_record(rest)
        }
        1 => {
            // Finish record
            parse_finish_record(rest)
        }
        2 => {
            // set metadata record
            parse_set_metadata_record(rest)
        }
        _ => Err(nom::Err::Failure(nom::error::make_error(
            input,
            nom::error::ErrorKind::NoneOf,
        ))),
    }
}

fn parse_start_record(input: &[u8]) -> IResult<&[u8], ControlRecord> {
    let (input, entry_id) = nom::number::complete::le_u32(input)?;
    let (input, name) = parse_string_with_len(input)?;
    let (input, typ) = parse_string_with_len(input)?;
    let (input, metadata) = parse_string_with_len(input)?;

    Ok((
        input,
        ControlRecord::Start(StartRecord {
            entry_id,
            name,
            typ,
            metadata,
        }),
    ))
}

fn parse_finish_record(input: &[u8]) -> IResult<&[u8], ControlRecord> {
    let (input, entry_id) = nom::number::complete::le_u32(input)?;

    Ok((input, ControlRecord::Finish(FinishRecord { entry_id })))
}

fn parse_set_metadata_record(input: &[u8]) -> IResult<&[u8], ControlRecord> {
    let (input, entry_id) = nom::number::complete::le_u32(input)?;
    let (input, metadata) = parse_string_with_len(input)?;

    Ok((
        input,
        ControlRecord::SetMetadata(SetMetadataRecord { entry_id, metadata }),
    ))
}

pub fn parse_string_with_len(input: &[u8]) -> IResult<&[u8], &str> {
    let (input, str_len) = nom::number::complete::le_u32(input)?;
    let (input, str_data) = nom::bytes::complete::take(str_len)(input)?;

    Ok((input, unsafe { std::str::from_utf8_unchecked(str_data) }))
}

pub fn parse_string_full(input: &[u8]) -> IResult<&[u8], &str> {
    Ok((&[], unsafe { std::str::from_utf8_unchecked(input) }))
}

pub fn parse_raw(input: &[u8]) -> IResult<&[u8], &[u8]> {
    Ok((&[], input))
}

pub fn parse_boolean(input: &[u8]) -> IResult<&[u8], bool> {
    let (input, byte) = nom::number::complete::le_u8(input)?;

    Ok((input, byte == 1))
}

pub fn parse_int64(input: &[u8]) -> IResult<&[u8], i64> {
    let (input, number) = nom::number::complete::le_i64(input)?;

    Ok((input, number))
}

pub fn parse_float(input: &[u8]) -> IResult<&[u8], f32> {
    let (input, number) = nom::number::complete::le_f32(input)?;

    Ok((input, number))
}

pub fn parse_double(input: &[u8]) -> IResult<&[u8], f64> {
    let (input, number) = nom::number::complete::le_f64(input)?;

    Ok((input, number))
}

pub fn parse_array<T>(
    func: impl Fn(&[u8]) -> IResult<&[u8], T>,
    input: &[u8],
) -> IResult<&[u8], Vec<T>> {
    nom::combinator::all_consuming(nom::multi::many0(func))(input)
}

pub fn parse_array_ref_with_len<T>(
    func: impl Fn(&[u8]) -> IResult<&[u8], &T>,
    input: &[u8],
) -> IResult<&[u8], Vec<&T>>
where
    T: ?Sized,
{
    let (input, count) = nom::number::complete::le_u32(input)?;

    nom::combinator::all_consuming(nom::multi::many_m_n(count as usize, count as usize, func))(
        input,
    )
}
