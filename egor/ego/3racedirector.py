import csv
import json
import os
import re
from datetime import datetime
from pathlib import Path


def load_config(config_path="info.json"):
    """Load configuration from info.json file."""
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def parse_date(date_str, date_format):
    """Parse the date string into a datetime object using the configured format."""
    # Handle timezone format variations
    # Python's %z expects +0000 format, but some formats might differ
    try:
        return datetime.strptime(date_str, date_format)
    except ValueError:
        # Try without timezone if the format fails
        date_format_no_tz = date_format.replace(' %z', '').replace('%z', '')
        date_str_no_tz = re.sub(r'\s*[+-]\d{4}$', '', date_str)
        return datetime.strptime(date_str_no_tz, date_format_no_tz)


def extract_infohash(magnet_link, pattern):
    """Extract infohash from a magnet link using the configured pattern."""
    match = re.search(pattern, magnet_link)
    return match.group(1) if match else None


def get_output_row(row, config):
    """Extract the configured output columns from a row."""
    csv_config = config['csv_processing']
    output_columns = csv_config.get('output_columns', ['torrent_name', 'infohash', 'magnet_link'])
    
    # Map column names to their values
    column_values = {
        'torrent_name': row[csv_config['torrent_name_column_index']],
        'magnet_link': row[csv_config['magnet_link_column_index']],
        'infohash': extract_infohash(
            row[csv_config['magnet_link_column_index']], 
            csv_config['infohash_pattern']
        )
    }
    
    # Add additional columns if they exist in the row
    # This allows for flexible column mapping
    for i, value in enumerate(row):
        column_values[f'col_{i}'] = value
    
    return [column_values.get(col, '') for col in output_columns]


def file_exists_with_extensions(base_path, extensions):
    """Check if a file exists with any of the given extensions."""
    base = Path(base_path).with_suffix('')
    for ext in extensions:
        if Path(str(base) + ext).exists():
            return True
    return False


def matches_pattern(filename, pattern):
    """Check if a filename matches the given regex pattern."""
    return bool(re.match(pattern, filename))


def process_csv_files(config):
    """Process CSV files according to the configuration."""
    csv_config = config['csv_processing']
    debug_config = config.get('debug', {})
    verbose = debug_config.get('verbose', False)
    
    # Get configuration values
    input_pattern = csv_config['input_file_pattern']
    date_column = csv_config['date_column_index']
    date_format = csv_config['date_format']
    torrent_name_column = csv_config['torrent_name_column_index']
    magnet_link_column = csv_config['magnet_link_column_index']
    infohash_pattern = csv_config['infohash_pattern']
    output_ext = csv_config['output_extensions']
    create_subdir = csv_config.get('create_subdirectory', True)
    skip_extensions = csv_config.get('skip_if_exists', ['.csv', '.archive', '.old'])
    
    # Find matching CSV files
    for filename in os.listdir('.'):
        if not filename.endswith('.csv'):
            continue
            
        if not matches_pattern(filename, input_pattern):
            continue
        
        if verbose:
            print(f"Processing: {filename}")
        
        # Determine output directory
        if create_subdir:
            directory_name = Path(filename).stem  # Remove the '.csv' extension
            if not os.path.exists(directory_name):
                os.makedirs(directory_name)
                if verbose:
                    print(f"  Created directory: {directory_name}")
        else:
            directory_name = '.'
        
        # Read all rows and sort by date
        rows = []
        with open(filename, mode='r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                try:
                    date_str = row[date_column]
                    date_obj = parse_date(date_str, date_format)
                    rows.append((date_obj, row))
                except (IndexError, ValueError) as e:
                    if debug_config.get('log_skipped_rows', False):
                        print(f"  Skipping row due to error: {e}")
                    continue
        
        # Sort by the datetime object
        rows.sort(key=lambda x: x[0])
        
        if verbose:
            print(f"  Found {len(rows)} valid rows")
        
        # Write sorted rows to numbered files
        written_count = 0
        for i, (_, row) in enumerate(rows, start=1):
            # Construct the output file path
            output_filename = f"{i}{output_ext['active']}"
            output_filepath = os.path.join(directory_name, output_filename)
            
            # Build the base path for checking existing files
            base_filepath = os.path.join(directory_name, str(i))
            
            # Check if any version of this file already exists
            if file_exists_with_extensions(base_filepath, skip_extensions):
                continue
            
            # Extract the output row data
            output_row = get_output_row(row, config)
            
            # Only write if we have valid data (check infohash specifically)
            infohash_idx = csv_config.get('output_columns', ['torrent_name', 'infohash', 'magnet_link']).index('infohash') if 'infohash' in csv_config.get('output_columns', ['torrent_name', 'infohash', 'magnet_link']) else None
            
            if infohash_idx is not None and not output_row[infohash_idx]:
                if debug_config.get('log_skipped_rows', False):
                    print(f"  Skipping row {i}: no infohash extracted")
                continue
            
            # Write the output file
            with open(output_filepath, mode='w', newline='', encoding='utf-8') as output_csvfile:
                writer = csv.writer(output_csvfile)
                writer.writerow(output_row)
            
            written_count += 1
        
        if verbose:
            print(f"  Written {written_count} new files")


def main():
    """Main entry point."""
    # Load configuration from info.json in the current directory
    config_path = "info.json"
    
    if not os.path.exists(config_path):
        print(f"Error: Configuration file '{config_path}' not found.")
        print("Please ensure info.json exists in the current directory.")
        return 1
    
    config = load_config(config_path)
    
    if config.get('debug', {}).get('verbose', False):
        print(f"Loaded configuration from: {config_path}")
        print(f"Series ID: {config.get('series_id', 'N/A')}")
        print(f"Year: {config.get('year', 'N/A')}")
        print()
    
    process_csv_files(config)
    return 0


if __name__ == "__main__":
    exit(main())