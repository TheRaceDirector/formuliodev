import csv
import re
import os

# Define the pattern to match the round and resolution
round_pattern = re.compile(r'\.Round\.(\d+)', re.IGNORECASE)
resolution_pattern = re.compile(r'(1080[Pp]|SD|4K|2160[Pp])', re.IGNORECASE)

# Function to normalize the round format
def normalize_round(event_description):
    match = round_pattern.search(event_description)
    if match:
        round_num = match.group(1)  # Extract the round number part
        return f"r{int(round_num):02d}"  # Format with leading zero
    return "r00"  # Default to r00 if no round is found

# Function to normalize the resolution format
def normalize_resolution(res_str):
    match = resolution_pattern.search(res_str)
    if match:
        res_str = match.group().upper()
        if '1080P' in res_str or 'FHD' in res_str:
            return 'FHD'
        elif 'SD' in res_str:
            return 'SD'
        elif '2160P' in res_str or '4K' in res_str:
            return '4K'
    return None

# Function to read existing GUIDs from a file
def read_existing_guids(filename):
    guids = set()
    if os.path.exists(filename):
        with open(filename, mode='r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                guids.add(row[1])  # Assuming the GUID is in the second column (index 1)
    return guids

# Function to process records and append to respective files without duplicates
def process_records(input_file):
    existing_guids = {}  # Dictionary to store GUIDs for each file

    with open(input_file, mode='r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            # Extract year, round, resolution, and GUID from the record
            event_description = row[0]
            guid = row[1]  # Assuming the GUID is in the second column
            year_match = re.search(r'(\d{4})', event_description)
            resolution_match = resolution_pattern.search(event_description)

            # Debug prints
            # print(f"Processing: {event_description}")
            # print(f"Year: {year_match.group(1) if year_match else 'None'}")
            # print(f"Resolution: {resolution_match.group() if resolution_match else 'None'}")

            if year_match and resolution_match:
                year = year_match.group(1)
                normalized_round = normalize_round(event_description)  # Will return "r00" if no round found
                normalized_resolution = normalize_resolution(resolution_match.group())
                
                if normalized_resolution:  # Only proceed if we have a valid resolution
                    # Construct the filename
                    filename = f"{year}{normalized_round}{normalized_resolution}.csv"
                    # print(f"Filename: {filename}")

                    # Read existing GUIDs for the file if not already done
                    if filename not in existing_guids:
                        existing_guids[filename] = read_existing_guids(filename)

                    # Check if the GUID is already in the file
                    if guid not in existing_guids[filename]:
                        # Append the record to the respective file
                        with open(filename, mode='a', newline='', encoding='utf-8') as outfile:
                            writer = csv.writer(outfile)
                            writer.writerow(row)
                            existing_guids[filename].add(guid)  # Add the new GUID to the set
                        # print(f"Appended: {row}")
                    else:
                        # print(f"Duplicate GUID: {guid}")
                        pass
process_records('../f1db.csv')
