import csv
import re
import os

# Define the pattern to match the round and resolution
round_pattern = re.compile(r'(R\d+|x\d+|Round\s?\d+)', re.IGNORECASE)
resolution_pattern = re.compile(r'(1080[Pp]|SD|4K|2160[Pp]|UHD| HD)', re.IGNORECASE)

# Function to normalize the round format
def normalize_round(event_description):
    match = round_pattern.search(event_description)
    if match:
        round_num = re.sub(r'\D', '', match.group())  # Remove non-digit characters
        return f"r{int(round_num):02d}"  # Format with leading zero
    return "r00"  # Default to round 00 if no round number is found

# Function to normalize the resolution format
def normalize_resolution(res_str):
    match = resolution_pattern.search(res_str)
    if match:
        res_str = match.group().upper()
        if '1080P' in res_str or 'FHD' in res_str or ' HD' in res_str:
            return 'FHD'
        elif 'SD' in res_str:
            return 'SD'
        elif '2160P' in res_str or '4K' in res_str or 'UHD' in res_str:
            return '4K'
    return 'SD'  # Default to SD

# Function to read existing GUIDs from a file
def read_existing_guids(filename):
    guids = set()
    if os.path.exists(filename):
        with open(filename, mode='r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if len(row) > 1:
                    guids.add(row[1])  # Assuming the GUID is in the second column
    return guids

# Function to process records and append to respective files without duplicates
def process_records(input_file):
    existing_guids = {}  # Dictionary to store GUIDs for each file

    with open(input_file, mode='r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            try:
                if len(row) < 2:
                    continue

                # Extract event description and GUID
                event_description = row[0]
                guid = row[1]
                
                # Extract year from event description
                year_match = re.search(r'(\d{4})', event_description)
                if not year_match:
                    continue
                
                year = year_match.group(1)
                normalized_round = normalize_round(event_description)
                
                # Check if resolution pattern exists in the event description
                resolution_match = resolution_pattern.search(event_description)
                normalized_resolution = 'SD'  # Default
                if resolution_match:
                    normalized_resolution = normalize_resolution(resolution_match.group())
                
                # Construct the filename
                filename = f"{year}{normalized_round}{normalized_resolution}.csv"
                
                # Read existing GUIDs for the file if not already done
                if filename not in existing_guids:
                    existing_guids[filename] = read_existing_guids(filename)
                
                # Check if the GUID is already in the file
                if guid not in existing_guids[filename]:
                    # Append the record to the respective file
                    with open(filename, mode='a', newline='', encoding='utf-8') as outfile:
                        writer = csv.writer(outfile)
                        writer.writerow(row)
                        existing_guids[filename].add(guid)
                
            except Exception:
                continue

# Main execution
if __name__ == "__main__":
    input_file = '../f1db.csv'
    process_records(input_file)
