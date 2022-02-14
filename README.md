# About
This project is intended for use with `*.wpilog` files, with multiple working pieces for debugging, validation and export.

# Examples
Each example is designed for a different set of information about a log - validation, basic parsing checks, metadata collection, and export.

## Parsing
- `cargo run --example parse_log <path to wpilog file>`

Expected output is either a nom error - which, in that case, either the parser is out of date, or the log is corrupted. First check is to ensure the log version is set correctly - should be 0x0001 in little-endian, or 1.0.

## Validation
- `cargo run --example validate_log <path to wpilog file>`

Expected output is something like the following:

```text
Parse successful - 213 entries with 804366 records
```

If successful, this output will match the validation info provided by wpilib's `datalogtool`.

## Metadata
- `cargo run --example parse_log <path to wpilog file>`

Expected output is a structured description of the log file - each entry will be described as follows:

- entry id #
- name
- type
- metadata (as set by the datalog entry)
- data record count reffering to this entry
- if type is a numeric array and constant length, the array length
- if the entry was marked as finished

## Export to CSV
- `cargo run --example export_log_to_csv <path to wpilog file>`

This converts the specified WPILog to a csv file, without carrying state through each line. This may be an included feature in the future, but at the moment it saves on size. Considering these CSV files are hundreds of thousands of lines long, it's probably best kept as such.

## Finding Event windows in CSV files
- `python slice_enable_periods.py --help`

I recommend checking out the help information for this script - it describes in detail what each argument does. 

The general gist (and default behavior) of the script is to break a csv file down into smaller CSV files, each containing one or more enabled periods, as well as a 10 second buffer before and after. These individual periods are then written to their own files, and labelled with the timestamps contained within. I recommend keeping the log name included within the output file name, but you do you. Otherwise, it's going to be difficult to trace back the origin of the filtered event.