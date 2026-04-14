import csv
import json
import os
import re
from datetime import datetime
from pathlib import Path


def load_config(config_path="info.json"):
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def parse_date(date_str, date_format):
    try:
        return datetime.strptime(date_str, date_format)
    except ValueError:
        date_format_no_tz = date_format.replace(' %z', '').replace('%z', '')
        date_str_no_tz = re.sub(r'\s*[+-]\d{4}$', '', date_str)
        return datetime.strptime(date_str_no_tz, date_format_no_tz)


def extract_infohash(magnet_link, pattern):
    match = re.search(pattern, magnet_link)
    return match.group(1) if match else None


def get_output_row(row, config):
    csv_config = config['csv_processing']
    output_columns = csv_config.get('output_columns', ['torrent_name', 'infohash', 'magnet_link'])

    column_values = {
        'torrent_name': row[csv_config['torrent_name_column_index']],
        'magnet_link': row[csv_config['magnet_link_column_index']],
        'infohash': extract_infohash(
            row[csv_config['magnet_link_column_index']],
            csv_config['infohash_pattern']
        )
    }

    for i, value in enumerate(row):
        column_values[f'col_{i}'] = value

    return [column_values.get(col, '') for col in output_columns]


def file_exists_with_extensions(base_path, extensions):
    base = Path(base_path).with_suffix('')
    return any(Path(str(base) + ext).exists() for ext in extensions)


def process_csv_files(config):
    csv_config = config['csv_processing']
    debug_config = config.get('debug', {})
    verbose = debug_config.get('verbose', False)
    log_skipped = debug_config.get('log_skipped_rows', False)

    input_pattern = csv_config['input_file_pattern']
    date_column = csv_config['date_column_index']
    date_format = csv_config['date_format']
    output_columns = csv_config.get('output_columns', ['torrent_name', 'infohash', 'magnet_link'])
    output_ext = csv_config['output_extensions']
    create_subdir = csv_config.get('create_subdirectory', True)
    skip_extensions = csv_config.get('skip_if_exists', ['.csv', '.archive', '.old'])

    if 'infohash' not in output_columns:
        print("Error: 'infohash' must be in output_columns to use as filename.")
        return

    infohash_idx = output_columns.index('infohash')

    for filename in os.listdir('.'):
        if not filename.endswith('.csv'):
            continue
        if not re.match(input_pattern, filename):
            continue

        if verbose:
            print(f"Processing: {filename}")

        directory_name = Path(filename).stem if create_subdir else '.'
        if create_subdir:
            Path(directory_name).mkdir(exist_ok=True)
            if verbose:
                print(f"  Output directory: {directory_name}")

        rows = []
        with open(filename, mode='r', newline='', encoding='utf-8') as csvfile:
            for row in csv.reader(csvfile):
                try:
                    rows.append((parse_date(row[date_column], date_format), row))
                except (IndexError, ValueError) as e:
                    if log_skipped:
                        print(f"  Skipping row due to error: {e}")

        if verbose:
            print(f"  Found {len(rows)} valid rows")

        written_count = 0
        skipped_count = 0
        for (_, row) in rows:
            output_row = get_output_row(row, config)
            infohash = output_row[infohash_idx]

            if not infohash:
                if log_skipped:
                    print(f"  Skipping row: no infohash extracted")
                skipped_count += 1
                continue

            base_filepath = os.path.join(directory_name, infohash)

            if file_exists_with_extensions(base_filepath, skip_extensions):
                if verbose:
                    print(f"  Skipping {infohash}: already exists")
                skipped_count += 1
                continue

            output_path = base_filepath + output_ext['active']
            with open(output_path, mode='w', newline='', encoding='utf-8') as f:
                csv.writer(f).writerow(output_row)

            written_count += 1

        if verbose:
            print(f"  Written {written_count} new, skipped {skipped_count}")


def main():
    config_path = "info.json"

    if not os.path.exists(config_path):
        print(f"Error: '{config_path}' not found in current directory.")
        return 1

    config = load_config(config_path)

    if config.get('debug', {}).get('verbose', False):
        print(f"Loaded: {config_path}")
        print(f"Series: {config.get('series_id', 'N/A')} ({config.get('year', 'N/A')})")
        print()

    process_csv_files(config)
    return 0


if __name__ == "__main__":
    exit(main())