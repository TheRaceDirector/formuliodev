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
round_regex = re.compile(r'2025x(\d+)', re.IGNORECASE)
title_regex = re.compile(r'(\d+)\.(.+?)\.(mp4|mkv)', re.IGNORECASE)
grand_prix_regex = re.compile(r'2025x\d+\.(.+?)\.(Qualifying|Race|Sprint|Sky|SD)', re.IGNORECASE)

# Function to format the title based on specific rules
def format_title(filename, grand_prix_name):
    title_part = "Unknown"  # Default value in case no match is found
    match = title_regex.search(filename)
    if match:
        title_part = match.group(2)  # Get the descriptive part of the filename
        title_part = title_part.replace('.', ' ')  # Replace dots with spaces
    return f"{title_part} - {grand_prix_name}".strip()

# Process the CSV file
def process_csv(file_path, output_file_path):
    output_data = {}
    episode_counter = {}  # Dictionary to keep track of episode numbers per round

    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            # Extract the round number and Grand Prix name
            round_match = round_regex.search(row[0])
            gp_match = grand_prix_regex.search(row[0])
            if round_match and gp_match:
                round_number = round_match.group(1)
                grand_prix_name = gp_match.group(1).replace('.', ' ')  # Replace dots with spaces
            else:
                round_number = 'Unknown'
                grand_prix_name = 'Unknown'
            
            # Extract the infohash and file index number
            filename = row[1]
            # Extract just the filename without the path
            actual_filename = filename
            if '/' in filename:
                actual_filename = filename.split('/')[-1]
            elif '\\' in filename:
                actual_filename = filename.split('\\')[-1]
                
            infohash = row[2]
            file_index = int(row[3])
            
            # Manage episode number per round
            round_key = f'2025:{round_number}'
            if round_key not in episode_counter:
                episode_counter[round_key] = 0
            episode_counter[round_key] += 1
            
            # Create the key for the output dictionary using round year, round number, and formatted episode number
            formatted_episode_number = f"{episode_counter[round_key]:02}"  # Format with leading zero if needed
            key = f'hpytt0202507:{round_number}:{formatted_episode_number}'
            
            # Format the title
            formatted_title = format_title(actual_filename, grand_prix_name)
            
            # Get the thumbnail URL for the round, defaulting to an empty string if not found
            thumbnail = round_thumbnails.get(round_number, '')
            
            # Append the data to the output dictionary
            output_data[key] = [{
                'title': formatted_title,
                'thumbnail': thumbnail,
                'infoHash': infohash,
                'fileIdx': file_index,
                'filename': actual_filename
            }]

    # Read existing data from the output file
    try:
        with open(output_file_path, 'r') as output_file:
            existing_data = output_file.read()
    except FileNotFoundError:
        existing_data = ""

    # Convert output data to string for comparison
    new_data = ""
    for key, value in output_data.items():
        new_data += f"'{key}': {json.dumps(value)},\n"

    # Write the output data to the file only if it's different
    if new_data.strip() != existing_data.strip():
        with open(output_file_path, 'w') as output_file:
            output_file.write(new_data)

# Example usage
file_path = 'content.csv'
output_file_path = '6processed.txt'
process_csv(file_path, output_file_path)
