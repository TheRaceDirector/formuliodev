import re
import csv
import json
import os

# Load countries and calendar from JSON file
config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'f1_config.json')
with open(config_path, 'r') as json_file:
    config = json.load(json_file)

countries = config["countries"]
round_to_country = config["calendar"]

# Generate round_thumbnails dictionary
round_thumbnails = {round_num: countries.get(country, 'https://i.ibb.co/yc9mg54D/un.png') for round_num, country in round_to_country.items()}
# Default thumbnail if not found
default_thumbnail = 'https://i.ibb.co/yc9mg54D/un.png'

# Compile the regular expression for matching round numbers
round_regex = re.compile(r'R(\d+)|Round\.(\d+)', re.IGNORECASE)

# Function to format the title based on specific rules
def format_title(filename, round_part, torrent_name):
    # Remove file extension and path
    basename = os.path.basename(filename)
    basename = re.sub(r'\.(mkv|mp4)$', '', basename, flags=re.IGNORECASE)

    # Split the filename into parts
    parts = basename.split('.')

    # Extract meaningful parts, dropping SkyF1HD, 1080P, etc.
    filtered_parts = []
    for part in parts:
        if part not in ['SkyF1HD', '1080P', 'F1', '2025', 'mkv', 'mp4'] and not re.match(r'^\d+$', part):
            filtered_parts.append(part)

    # Join parts with spaces for better readability
    return ' '.join(filtered_parts).strip()

# Process the CSV file
def process_csv(file_path, output_file_path):
    output_data = {}
    global_session_counter = 1
    processed_files = set()  # Track unique files to avoid duplicates
    title_tracker = {}  # Track titles in the '00' round to avoid duplicates

    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row

        for row in reader:
            torrent_name = row[0]
            filename = row[1]
            infohash = row[2]
            file_index = int(row[3])

            # Create a unique identifier for this file
            file_key = f"{infohash}:{file_index}"

            # Skip duplicates
            if file_key in processed_files:
                continue

            processed_files.add(file_key)

            # Extract the round number
            round_match = round_regex.search(filename)
            if round_match:
                round_number = round_match.group(1) or round_match.group(2)
            else:
                round_number = '00'  # Default for non-race content

            # Clean and format the title
            clean_title = format_title(filename, '', torrent_name)

            # Skip if we've already processed a file with this title in round '00'
            if round_number == '00' and clean_title in title_tracker:
                continue

            # Add title to tracker if it's in round '00'
            if round_number == '00':
                title_tracker[clean_title] = True

            # Format session number
            session_number = f"{global_session_counter:02d}"
            global_session_counter += 1

            # Get thumbnail URL
            thumbnail = round_thumbnails.get(round_number, default_thumbnail)

            # Create the key for the output dictionary
            key = f'hpytt0202505:{round_number}:{session_number}'

            # Add to output data
            output_data[key] = [{
                'title': clean_title,
                'thumbnail': thumbnail,
                'infoHash': infohash,
                'fileIdx': file_index
            }]

    # Manually format the output data as a string
    new_data = ""
    for key, value in output_data.items():
        new_data += f"'{key}': {value},\n"

    # Write the output data to the file
    with open(output_file_path, 'w') as output_file:
        output_file.write(new_data)

# Example usage
file_path = 'content.csv'
output_file_path = '6processed.txt'
process_csv(file_path, output_file_path)
