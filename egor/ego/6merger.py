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

def format_title(filename):
    # Remove file extension and path
    basename = os.path.basename(filename)
    basename = re.sub(r'\.(mkv|mp4)$', '', basename, flags=re.IGNORECASE)
    
    # print(f"Processing: {basename}")  # Debug print
    
    # Try a more direct approach to find the Grand Prix name pattern
    # Directly look for "Australian Grand Prix" in the final title
    
    # First get the basic title
    parts = basename.split('.')
    
    # Extract meaningful parts, dropping SkyF1HD, 1080P, etc.
    filtered_parts = []
    for part in parts:
        if part not in ['SkyF1HD', '1080P', '2025', 'mkv', 'mp4'] and not re.match(r'^\d+$', part):
            # Remove the round number from the title
            round_part_match = re.match(r'^R\d+\s*(.*)', part, re.IGNORECASE)
            if round_part_match:
                filtered_part = round_part_match.group(1)
                filtered_parts.append(filtered_part)
            else:
                filtered_parts.append(part)
    
    # Join parts with spaces for better readability
    title = ' '.join(filtered_parts).strip()
    
    # print(f"Initial title: {title}")  # Debug print
    
    # Now check if the title contains "XXX Grand Prix"
    grand_prix_match = re.search(r'(\w+)\s+Grand\s+Prix', title, re.IGNORECASE)
    
    if grand_prix_match:
        # print(f"Found Grand Prix in title: {grand_prix_match.group(0)}")
        grand_prix_text = grand_prix_match.group(0)
        
        # Remove the Grand Prix from the beginning of the title
        title = re.sub(r'^' + re.escape(grand_prix_text) + r'\s+', '', title)
        
        # Add it to the end
        title = f"{title} - {grand_prix_text}"
        
    # Clean up any double spaces
    title = re.sub(r'\s+', ' ', title).strip()
    
    # print(f"Final title: {title}")  # Debug print
    return title


# Process the CSV file
def process_csv(file_path, output_file_path):
    output_data = {}
    round_session_counters = {}  # Track session counters per round
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
            clean_title = format_title(filename)
            
            # Skip if we've already processed a file with this title in round '00'
            if round_number == '00' and clean_title in title_tracker:
                continue
                
            # Add title to tracker if it's in round '00'
            if round_number == '00':
                title_tracker[clean_title] = True
                
            # Initialize counter for this round if not already set
            if round_number not in round_session_counters:
                round_session_counters[round_number] = 1
                
            # Get the session number for this round and increment it
            session_number = f"{round_session_counters[round_number]:02d}"
            round_session_counters[round_number] += 1
            
            # Get thumbnail URL
            thumbnail = round_thumbnails.get(round_number, default_thumbnail)
            
            # Create the key for the output dictionary
            key = f'hpytt0202501:{round_number}:{session_number}'
            
            # Extract just the filename part (removing the folder path)
            # This handles both forward slashes and backslashes
            actual_filename = filename
            if '/' in filename:
                actual_filename = filename.split('/')[-1]
            elif '\\' in filename:
                actual_filename = filename.split('\\')[-1]
            
            # Add to output data with the actual filename
            output_data[key] = [{
                'title': clean_title,
                'thumbnail': thumbnail,
                'infoHash': infohash,
                'fileIdx': file_index,
                'filename': actual_filename  # Just the filename without path
            }]
    
    # Manually format the output data as a string
    new_data = ""
    for key, value in output_data.items():
        new_data += f"'{key}': {value},\n"

    # Get absolute path to the output file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    absolute_output_path = os.path.join(script_dir, output_file_path)
    
    # Read existing data if file exists
    try:
        with open(absolute_output_path, 'r') as output_file:
            existing_data = output_file.read()
    except FileNotFoundError:
        existing_data = ""
    
    # Only write if the data has changed
    if new_data.strip() != existing_data.strip():
        with open(absolute_output_path, 'w') as output_file:
            output_file.write(new_data)

# Example usage
file_path = 'content.csv'
output_file_path = '6processed.txt'
process_csv(file_path, output_file_path)
