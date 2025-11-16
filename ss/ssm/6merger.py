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
round_thumbnails = {round_num: countries.get(country, '') for round_num, country in round_to_country.items()}

# Compile the regular expressions - FIXED
round_regex = re.compile(r'\.R(\d+)\.', re.IGNORECASE)
grand_prix_regex = re.compile(r'R\d+\.([^.]+)GP', re.IGNORECASE)
session_regex = re.compile(r'GP\.(.+?)\.International', re.IGNORECASE)
valid_extension_regex = re.compile(r'\.(mkv|mp4)$', re.IGNORECASE)
pre_season_regex = re.compile(r'Pre\.Season', re.IGNORECASE)

def format_title(filename, session_name, grand_prix_name):
    # For pre-season testing or other non-race content, use a simpler format
    if pre_season_regex.search(filename):
        # Extract just the meaningful parts from the filename
        parts = filename.split('.')
        # Filter out unnecessary parts
        filtered_parts = [part for part in parts if re.match(r'(Pre|Season|Testing|Session|\d+)', part, re.IGNORECASE)]
        return ' '.join(filtered_parts).strip()
    else:
        # Standard race weekend formatting
        session_name = session_name.replace('.', ' ').title().strip()
        return f"{session_name} - {grand_prix_name}GP"

def process_csv(file_path, output_file_path):
    output_data = {}
    episode_counters = {}

    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            torrent_name = row[0]
            filename = row[1]
            
            # Extract just the filename without the path
            actual_filename = filename
            if '/' in filename:
                actual_filename = filename.split('/')[-1]
            elif '\\' in filename:
                actual_filename = filename.split('\\')[-1]
                
            if not valid_extension_regex.search(actual_filename):
                continue

            round_match = round_regex.search(actual_filename)
            
            # Handle pre-season testing or other special cases
            if pre_season_regex.search(torrent_name) or pre_season_regex.search(actual_filename):
                round_number = '00'  # Use '00' for pre-season testing
                thumbnail = round_thumbnails.get(round_number, '')
            else:
                round_number = round_match.group(1) if round_match else '00'
                thumbnail = round_thumbnails.get(round_number, '')

            if round_number not in episode_counters:
                episode_counters[round_number] = 0

            gp_match = grand_prix_regex.search(actual_filename)
            grand_prix_name = gp_match.group(1) if gp_match else "Unknown Grand Prix"

            session_match = session_regex.search(actual_filename)
            session_name = session_match.group(1) if session_match else "Unknown Session"

            # For pre-season testing, use a simplified title format
            if pre_season_regex.search(torrent_name) or pre_season_regex.search(actual_filename):
                session_parts = re.findall(r'Session\.(\d+)', torrent_name)
                if not session_parts:
                    session_parts = re.findall(r'Session\.(\d+)', actual_filename)
                    
                if session_parts:
                    formatted_title = f"Pre Season Testing Session {session_parts[0]}"
                else:
                    formatted_title = "Pre Season Testing"
            else:
                formatted_title = format_title(actual_filename, session_name, grand_prix_name)

            infohash = row[2]
            file_index = int(row[3])

            episode_counters[round_number] += 1
            episode_number = episode_counters[round_number]

            key = f'hpytt0202504:{round_number}:{episode_number:02}'

            if round_number not in output_data:
                output_data[round_number] = []
            output_data[round_number].append((key, [{
                'title': formatted_title,
                'thumbnail': thumbnail,
                'infoHash': infohash,
                'fileIdx': file_index,
                'filename': actual_filename
            }]))

    try:
        with open(output_file_path, 'r') as output_file:
            existing_data = output_file.read()
    except FileNotFoundError:
        existing_data = ""

    new_data = ""
    for round_number in sorted(output_data.keys(), key=lambda x: int(x) if x.isdigit() else float('inf')):
        for key, value in output_data[round_number]:
            new_data += f"'{key}': {json.dumps(value)},\n"

    if new_data.strip() != existing_data.strip():
        with open(output_file_path, 'w') as output_file:
            output_file.write(new_data)

# Example usage
file_path = 'content.csv'
output_file_path = '6processed.txt'
process_csv(file_path, output_file_path)