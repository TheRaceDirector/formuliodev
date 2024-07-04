import re
import csv

# Compile the regular expression for matching round numbers and extracting parts of the filename
round_regex = re.compile(r'R(\d+)|Round\.(\d+)', re.IGNORECASE)
title_regex = re.compile(r'Prix\.(.+?)\.(?:2160P|Sky)', re.IGNORECASE)
grand_prix_regex = re.compile(r'R\d+\.(.+?)\.Grand\.Prix', re.IGNORECASE)

# Function to extract the session number from the start of the filename
def extract_session_number(filename):
    match = re.match(r'(\d+)', filename)
    return match.group(1) if match else None

# Function to format the title based on specific rules
def format_title(filename, round_part):
    # Initialize title_part with a default value
    title_part = "Unknown Session"
    
    # Extract the part after "Prix"
    match = title_regex.search(filename)
    if match:
        title_part = match.group(1)
        # Remove unwanted parts and format
        title_part = re.sub(r'\.(mkv|mp4)$', '', title_part, flags=re.IGNORECASE)  # Remove file extension
        title_part = title_part.replace('.', ' ')  # Replace dots with spaces

    # Extract the Grand Prix name
    gp_match = grand_prix_regex.search(filename)
    grand_prix_name = gp_match.group(1).replace('.', ' ') if gp_match else "Unknown"

    # Combine with the round part and return
    return f"{title_part} - {grand_prix_name} Grand Prix".strip()

# Process the CSV file
def process_csv(file_path, output_file_path):
    output_data = {}
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            # Extract the round number using the regular expression
            round_match = round_regex.search(row[1])
            if round_match:
                round_number = round_match.group(1) or round_match.group(2)
                round_part = row[1].split('R' + round_number)[1].split('.')[0]  # Extract part after round number
            else:
                round_number = 'Unknown'
                round_part = ''
            
            # Extract the session number from the start of the filename
            filename = row[1].split('/')[-1]
            session_number = extract_session_number(filename)
            
            # Extract the infohash and file index number
            infohash = row[2]
            file_index = int(row[3])
            
            # Create the key for the output dictionary
            key = f'hpytt0202405:{round_number}:{session_number}'
            
            # Format the title
            formatted_title = format_title(filename, round_part)
            
            # Append the data to the output dictionary only if the key doesn't exist
            if key not in output_data:
                output_data[key] = [{
                    'title': formatted_title,
                    'infoHash': infohash,
                    'fileIdx': file_index
                }]

    # Read existing data from the output file
    try:
        with open(output_file_path, 'r') as output_file:
            existing_data = output_file.read()
    except FileNotFoundError:
        existing_data = ""

    # Manually format the output data as a string
    new_data = ""
    for key, value in output_data.items():
        new_data += f"        '{key}': {value},\n"

    # Write the output data to the file only if it's different from the existing data
    if new_data.strip() != existing_data.strip():
        with open(output_file_path, 'w') as output_file:
            output_file.write(new_data)

# Example usage
file_path = 'content.csv'
output_file_path = '5processed.txt'
process_csv(file_path, output_file_path)
