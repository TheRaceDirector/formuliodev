import re
import csv
import os

# Compile the regular expression for matching round numbers and extracting parts of the filename
round_regex = re.compile(r'Round\.(\d+)', re.IGNORECASE)
grand_prix_regex = re.compile(r'Round\.\d+\.([^.]+GP)', re.IGNORECASE)
session_regex = re.compile(r'GP\.(.+?)\.F1\.Live', re.IGNORECASE)
valid_extension_regex = re.compile(r'\.(mkv|mp4)$', re.IGNORECASE)

# Function to format the title based on specific rules
def format_title(session_name, grand_prix_name):
    # Normalize session name by replacing periods and adjusting common terms
    session_name = session_name.replace('.', ' ').title()

    # Ensure the Grand Prix name does not redundantly include the session name
    grand_prix_name = grand_prix_name.replace(session_name.replace("GP", ""), '').strip()
    return f"{session_name} - {grand_prix_name}"

# Process the CSV file
def process_csv(file_path, output_file_path):
    output_data = {}
    episode_counters = {}  # To keep track of episode numbers within each season

    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            filename = row[1].split('/')[-1]
            
            # Ignore files without valid extensions
            if not valid_extension_regex.search(filename):
                print(f"Skipping file due to invalid extension: {filename}")
                continue

            # Extract the round number using the regular expression
            round_match = round_regex.search(filename)
            round_number = round_match.group(1) if round_match else 'Unknown'

            # Initialize episode counter for each round
            if round_number not in episode_counters:
                episode_counters[round_number] = 0

            # Extract the Grand Prix name
            gp_match = grand_prix_regex.search(filename)
            grand_prix_name = gp_match.group(1) if gp_match else "Unknown Grand Prix"

            # Extract the session
            session_match = session_regex.search(filename)
            session_name = session_match.group(1) if session_match else "Unknown Session"

            # Format the title
            formatted_title = format_title(session_name, grand_prix_name)

            # Extract the infohash and file index number
            infohash = row[2]
            file_index = int(row[3])

            # Increment episode counter for the current round
            episode_counters[round_number] += 1
            episode_number = episode_counters[round_number]

            # Create the key for the output dictionary
            key = f'hpytt0202403:{round_number}:{episode_number:02}'
            if round_number not in output_data:
                output_data[round_number] = []
            output_data[round_number].append((key, [{
                'title': formatted_title,
                'infoHash': infohash,
                'fileIdx': file_index
            }]))

    # Generate new content
    new_content = ""
    for round_number in sorted(output_data.keys(), key=int):
        for key, value in output_data[round_number]:
            new_content += f"        '{key}': {value},\n"

    # Check if the file exists and read its content
    if os.path.exists(output_file_path):
        with open(output_file_path, 'r') as file:
            existing_content = file.read()
    else:
        existing_content = ""

    # Write the output data to the file only if there is new or updated content
    if new_content.strip() != existing_content.strip():
        with open(output_file_path, 'w') as output_file:
            output_file.write(new_content)
        print("File updated with new content.")
    else:
        print("No new content to update.")

# Example usage
file_path = 'content.csv'
output_file_path = '5processed.txt'
process_csv(file_path, output_file_path)
