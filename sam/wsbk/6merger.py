import re
import csv
import json
import os

# Load countries and calendar from JSON file
config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'wsbk_config.json')
try:
    with open(config_path, 'r') as json_file:
        config = json.load(json_file)
    countries = config.get("countries", {})
    round_to_country = config.get("calendar", {})
except (FileNotFoundError, json.JSONDecodeError):
    # Default values if file not found or invalid
    countries = {}
    round_to_country = {}

# Generate round_thumbnails dictionary
round_thumbnails = {round_num: countries.get(country, 'https://i.ibb.co/yc9mg54D/un.png') for round_num, country in round_to_country.items()}
# Default thumbnail if not found
default_thumbnail = 'https://i.ibb.co/yc9mg54D/un.png'

# Function to extract round number from filename
def extract_round_number(filename):
    # Look for patterns like "2024.01." or similar that indicate round numbers
    round_match = re.search(r'\.(\d{2})\.\w+\.Grand\.Prix', filename, re.IGNORECASE)
    if round_match:
        return round_match.group(1)
    return '00'  # Default if no round number found

# Function to extract session number from filename
def extract_session_number(filename):
    # Extract session number from file prefix like "09 - " or "14 - "
    session_match = re.search(r'\/(\d{2})\s*-\s*', filename)
    if session_match:
        return session_match.group(1)
    return None

# Function to format the title based on specific rules
def format_title(filename):
    # Extract the actual filename part after the last /
    basename = filename.split('/')[-1] if '/' in filename else filename
    
    # Remove file extension
    basename = re.sub(r'\.(mkv|mp4)$', '', basename, flags=re.IGNORECASE)
    
    # Remove any leading numbers and dashes (like "09 - ")
    basename = re.sub(r'^\d+\s*-\s*', '', basename)
    
    # Split the filename into parts
    parts = basename.split('.')
    
    # Extract meaningful parts, dropping unnecessary elements
    filtered_parts = []
    for part in parts:
        if (part not in ['SkyF1HD', 'F1', '2026', 'mkv', 'mp4', 'WEB', 'ENGLISH', 'ZXX', 'MULTi'] 
            and not re.match(r'^\d+$', part)):
            filtered_parts.append(part)
    
    # Join parts with spaces for better readability
    title = ' '.join(filtered_parts).strip()
    
    # Remove resolution and everything after it
    resolution_pattern = r'(1080[pP]|720[pP]|4[kK]|FHD|fhd|2160[pP])'
    resolution_match = re.search(resolution_pattern, title)
    if resolution_match:
        # Keep only the title before the resolution
        resolution_start = resolution_match.start()
        title = title[:resolution_start].strip()
    
    return title.strip()


# Process the CSV file
def process_csv(file_path, output_file_path):
    output_data = {}
    processed_files = set()  # Track unique files to avoid duplicates
    
    # Track the next available episode number for auto-incrementing
    next_episode_number = 1
    used_episode_numbers = set()  # Track which episode numbers are already used
    
    # First pass to collect all explicitly defined episode numbers
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader, None)  # Skip header if present
        
        for row in reader:
            if len(row) < 4:
                continue
                
            filename = row[1]
            session_number = extract_session_number(filename)
            
            if session_number:
                used_episode_numbers.add(int(session_number))
    
    # Find the next available episode number after all explicitly defined ones
    if used_episode_numbers:
        next_episode_number = max(used_episode_numbers) + 1

    # Process the actual data
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader, None)  # Skip header if present
        
        for row in reader:
            if len(row) < 4:
                continue  # Skip invalid rows
                
            torrent_name = row[0]
            filename = row[1]
            infohash = row[2]
            try:
                file_index = int(row[3])
            except ValueError:
                continue  # Skip if file_index is not a valid integer
            
            # Create a unique identifier for this file
            file_key = f"{infohash}:{file_index}"
            
            # Skip duplicates
            if file_key in processed_files:
                continue
            
            processed_files.add(file_key)
            
            # Extract the round number from the filename
            round_number = extract_round_number(filename)
            
            # Extract the session number from the filename or generate one
            session_number = extract_session_number(filename)
            if not session_number:
                session_number = f"{next_episode_number:02d}"
                next_episode_number += 1
                
            # Clean and format the title
            clean_title = format_title(filename)
            
            # Get thumbnail URL
            thumbnail = round_thumbnails.get(round_number, default_thumbnail)
            
            # Create the key for the output dictionary
            key = f'hpytt0202614:{round_number}:{session_number}'
            
            # Extract just the filename part (removing the folder path)
            actual_filename = filename
            if '/' in filename:
                actual_filename = filename.split('/')[-1]
            elif '\\' in filename:
                actual_filename = filename.split('\\')[-1]
            
            # Add to output data
            output_data[key] = [{
                'title': clean_title,
                'thumbnail': thumbnail,
                'infoHash': infohash,
                'fileIdx': file_index,
                'filename': actual_filename  # Add the actual filename without path
            }]
    
    # Manually format the output data as a string
    new_data = ""
    for key, value in sorted(output_data.items()):
        new_data += f"'{key}': {json.dumps(value)},\n"

    # Write the output data to the file
    with open(output_file_path, 'w') as output_file:
        output_file.write(new_data)

# Example usage
file_path = 'content.csv'
output_file_path = '6processed.txt'
process_csv(file_path, output_file_path)
