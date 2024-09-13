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

# Compile the regular expressions
round_regex = re.compile(r'Round\.(\d+)', re.IGNORECASE)
grand_prix_regex = re.compile(r'Round\.\d+\.([^.]+)GP', re.IGNORECASE)
session_regex = re.compile(r'GP\.(.+?)\.International', re.IGNORECASE)
valid_extension_regex = re.compile(r'\.(mkv|mp4)$', re.IGNORECASE)

def format_title(session_name, grand_prix_name):
    session_name = session_name.replace('_', ' ').replace('.', ' ').title().strip()
    grand_prix_name = grand_prix_name.replace(session_name.replace("GP", ""), '').strip()
    return f"{session_name} - {grand_prix_name}GP"

def process_csv(file_path, output_file_path):
    output_data = {}
    episode_counters = {}

    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            filename = row[1].split('/')[-1]
            
            if not valid_extension_regex.search(filename):
                continue

            round_match = round_regex.search(filename)
            round_number = round_match.group(1) if round_match else 'Unknown'

            if round_number not in episode_counters:
                episode_counters[round_number] = 0

            gp_match = grand_prix_regex.search(filename)
            grand_prix_name = gp_match.group(1) if gp_match else "Unknown Grand Prix"

            session_match = session_regex.search(filename)
            session_name = session_match.group(1) if session_match else "Unknown Session"

            formatted_title = format_title(session_name, grand_prix_name)

            infohash = row[2]
            file_index = int(row[3])

            episode_counters[round_number] += 1
            episode_number = episode_counters[round_number]

            key = f'hpytt0202404:{round_number}:{episode_number:02}'
            
            # Get the thumbnail URL for the round, defaulting to an empty string if not found
            thumbnail = round_thumbnails.get(round_number, '')

            if round_number not in output_data:
                output_data[round_number] = []
            output_data[round_number].append((key, [{
                'name': formatted_title,
                'thumbnail': thumbnail,
                'infoHash': infohash,
                'fileIdx': file_index
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