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

# Compile the regular expression for matching round numbers and extracting parts of the filename
round_regex = re.compile(r'R(\d+)|Round\.(\d+)', re.IGNORECASE)
title_regex = re.compile(r'Prix\.(.+?)\.(?:2160P|Sky)', re.IGNORECASE)
grand_prix_regex = re.compile(r'R\d+\.(.+?)\.Grand\.Prix', re.IGNORECASE)

def extract_session_number(filename):
    match = re.match(r'(\d+)', filename)
    return match.group(1) if match else None

def format_title(filename, round_part, torrent_name):
    title_part = "Unknown Session"

    match = title_regex.search(filename)
    if match:
        title_part = match.group(1)
        title_part = re.sub(r'\.(mkv|mp4)$', '', title_part, flags=re.IGNORECASE)
        title_part = title_part.replace('.', ' ')

    gp_match = grand_prix_regex.search(filename)
    grand_prix_name = gp_match.group(1).replace('.', ' ') if gp_match else "Unknown"

    if title_part == "Unknown Session" and grand_prix_name == "Unknown":
        title_part = torrent_name.split("2025.")[1].split("Sky")[0].strip().replace('.', ' ')

    if grand_prix_name == "Unknown":
        return title_part.strip()
    else:
        return f"{title_part} - {grand_prix_name} Grand Prix".strip()

def process_csv(file_path, output_file_path):
    output_data = {}
    session_counter = 1  # Initialize session counter

    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            round_match = round_regex.search(row[1])
            if round_match:
                round_number = round_match.group(1) or round_match.group(2)
                round_number = round_number.zfill(2)
                round_part = row[1].split('R' + round_number)[1].split('.')[0]
            else:
                round_number = '00'  # Default to '00' if no round number is found
                round_part = ''

            filename = row[1].split('/')[-1]
            session_number = extract_session_number(filename)

            if session_number is None:
                session_number = f"{session_counter:02d}"
                session_counter += 1

            infohash = row[2]
            file_index = int(row[3])

            key = f'hpytt0202505:{round_number}:{session_number}'
            formatted_title = format_title(filename, round_part, row[0])
            thumbnail = round_thumbnails.get(round_number, '')

            if key not in output_data:
                output_data[key] = [{
                    'title': formatted_title,
                    'thumbnail': thumbnail,
                    'infoHash': infohash,
                    'fileIdx': file_index
                }]

    try:
        with open(output_file_path, 'r') as output_file:
            existing_data = output_file.read()
    except FileNotFoundError:
        existing_data = ""

    new_data = ""
    for key, value in output_data.items():
        new_data += f"'{key}': {value},\n"

    if new_data.strip() != existing_data.strip():
        with open(output_file_path, 'w') as output_file:
            output_file.write(new_data)

# Example usage
file_path = 'content.csv'
output_file_path = '6processed.txt'
process_csv(file_path, output_file_path)
