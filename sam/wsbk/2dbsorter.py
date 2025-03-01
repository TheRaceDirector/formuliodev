import csv
import re
import os

# Define the patterns to match
round_pattern = re.compile(r'(R\d+|x\d+|Round\s?\d+|\.\d{2}\.)', re.IGNORECASE)
resolution_pattern = re.compile(r'(1080[Pp]|SD|4K|2160[Pp]| HD)', re.IGNORECASE)
day_pattern = re.compile(r'(Friday|Saturday|Sunday)', re.IGNORECASE)

# Function to normalize the round format
def normalize_round(title):
    # First try to find direct round pattern like .01.
    direct_round = re.search(r'\.(\d{2})\.', title)
    if direct_round:
        return f"r{direct_round.group(1)}"
    
    # Otherwise use the existing pattern
    match = round_pattern.search(title)
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
                guids.add(row[2])  # GUID is now in the third column (index 2)
    return guids

# Function to determine day priority (for sorting)
def get_day_priority(title):
    if 'Friday' in title:
        return 1
    elif 'Saturday' in title:
        return 2
    elif 'Sunday' in title:
        return 3
    return 4  # Default for unknown days

# Function to process records and append to respective files without duplicates
def process_records(input_file):
    existing_guids = {}  # Dictionary to store GUIDs for each file
    file_records = {}   # Dictionary to collect records by filename before writing

    with open(input_file, mode='r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)  # Skip the header row
        
        for row in reader:
            # Extract fields from the row
            title = row[0]
            link = row[1]
            guid = row[2]
            pub_date = row[3]
            
            # Extract year, round, and resolution from the title
            year_match = re.search(r'(\d{4})', title)
            
            if year_match:
                year = year_match.group(1)
                normalized_round = normalize_round(title)
                normalized_resolution = normalize_resolution(title)

                if normalized_resolution:
                    # Construct the filename
                    filename = f"{year}{normalized_round}{normalized_resolution}.csv"

                    # Read existing GUIDs for the file if not already done
                    if filename not in existing_guids:
                        existing_guids[filename] = read_existing_guids(filename)

                    # Check if the GUID is already in the file
                    if guid not in existing_guids[filename]:
                        # Collect record for this file, to be sorted later
                        if filename not in file_records:
                            file_records[filename] = []
                        
                        # Store the record with all fields
                        file_records[filename].append({
                            'row': row,
                            'day_priority': get_day_priority(title),
                            'title': title
                        })
                        existing_guids[filename].add(guid)  # Add the new GUID to the set

    # Now write the sorted records to each file
    for filename, records in file_records.items():
        # Sort records by day priority (Friday, Saturday, Sunday)
        sorted_records = sorted(records, key=lambda x: x['day_priority'])
        
        # Append the sorted records to the file
        with open(filename, mode='a', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            for record in sorted_records:
                writer.writerow(record['row'])

# Main execution
process_records('../f1db.csv')
