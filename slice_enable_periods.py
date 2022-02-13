import csv
import argparse
from collections import defaultdict

def contained_by_any_range(val, vec):
    for (start, end) in vec:
        if start <= val <= end:
            return True
    return False

def contained_by_range(val, range):
    return range[0] <= val <= range[1]


def read_csv(path, condition, toggle):
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        state = defaultdict(str)
        ranges = []

        events_log = [(False, 0)]

        last_eval = False

        for row in reader:
            for key in row:
                if row[key] != '':
                    state[key] = row[key]
            
            if toggle:
                evaluated_condition = eval(condition)

                if evaluated_condition and not last_eval:
                    print('enabled at', row['timestamp'])
                    events_log.append((True, float(row['timestamp'])))
                    last_eval = True
                elif not evaluated_condition and last_eval:
                    print('disabled at', row['timestamp'])
                    events_log.append((False, float(row['timestamp'])))
                    last_eval = False
            else:
                if eval(condition):
                    ranges.append((float(row['timestamp']), float(row['timestamp'])))
            rows.append(row)
        
        if toggle:
            start_time = 0

            for event in events_log:
                if event[0]:
                    start_time = event[1]
                else:
                    if start_time != 0:
                        # start_time = 0 is always during disabled
                        ranges.append((start_time, event[1]))
                        start_time = 0
            
            # if we're still enabled at the very end, extend enable range to the end
            if start_time != 0:
                ranges.append((start_time, float(row['timestamp'])))
        keys = [*row.keys()]
    
    return (rows, keys, ranges)


def compress_ranges(ranges):
    compressed_ranges = []

    for (start, end) in ranges:
        if len(compressed_ranges) == 0:
            compressed_ranges.append((max(start-args.before, 0), end+args.after))
        else:
            if start - args.before < compressed_ranges[-1][1]:
                compressed_ranges[-1] = (compressed_ranges[-1][0], end + args.after)
            else:
                compressed_ranges.append((max(start-args.before, 0), end+args.after))


    return compressed_ranges


def write_files(output, rows, keys, multiple, ranges):
    if multiple: 
        rows_index = 0
        for (ind, range) in enumerate(ranges):
            with open(output + '_' + str(range[0]) + '-' + str(range[1]) + '.csv', 'w', newline='') as f:
                print(f'writing file {ind}, range={range}')
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()

                was_in_range = False
                while rows_index < len(rows):
                    row = rows[rows_index]
                    in_range = contained_by_range(float(row['timestamp']), range)
                    if was_in_range and not in_range:
                        break
                    elif in_range:
                        was_in_range = True
                        writer.writerow(row)
                    rows_index += 1
    else:
        with open(output + '.csv', 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()

            for row in rows:
                if contained_by_any_range(float(row['timestamp']), ranges):
                    writer.writerow(row)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract Enable Periods from wpilog CSV exports. Multiple overlapping windows are treated as one.")
    parser.add_argument('-i', '--input', help='input CSV file representing a WPILog')
    parser.add_argument('-o', '--output', help='output filename to write filtered results to')
    parser.add_argument('-B', '--before',  help='seconds before enable period to include', default=10)
    parser.add_argument('-A', '--after',  help='seconds after enable period to include', default=10)
    parser.add_argument('-m', '--multiple', action='store_true', help='output one file per range instead of one total. NOT IMPLEMENTED')
    parser.add_argument('-c', '--condition', default="state['DS:enabled'] == '1'", help='condition to check for defining your logging window')
    parser.add_argument('-t', '--no-toggle', action='store_false', help='if set condition specifies an event. if false, condition specifies a window')

    args = parser.parse_args()
    args.before = int(args.before)
    args.after = int(args.after)

    if args.output[-4:] == '.csv':
        args.output = args.output[:-4]

    keys = []
    rows = []

    (rows, keys, ranges) = read_csv(args.input, args.condition, args.no_toggle)
    
    ranges = compress_ranges(ranges)

    write_files(args.output, rows, keys, args.multiple, ranges)

    
