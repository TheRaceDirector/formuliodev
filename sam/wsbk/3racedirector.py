import csv
import os
import re
from datetime import datetime

# Function to parse the date string into a datetime object
def parse_date(date_str):
    return datetime.strptime(date_str, '%d %b %Y %H:%M:%S +0000')

# Function to extract infohash from a magnet link
def extract_infohash(magnet_link):
    match = re.search(r'btih:([a-zA-Z0-9]+)', magnet_link)
    return match.group(1) if match else None

# Function to determine day priority for sorting
def get_day_priority(title):
    if re.search(r'Friday', title, re.IGNORECASE):
        return 1
    elif re.search(r'Saturday', title, re.IGNORECASE):
        return 2
    elif re.search(r'Sunday', title, re.IGNORECASE):
        return 3
    return 4  # Default for unknown days or no day specified

# Function to process CSV files and create directories and numbered CSV files
def process_csv_files():
    for filename in os.listdir('.'):
        # Check if the file is a CSV file and starts with "2"
        if filename.startswith('2') and filename.endswith('.csv'):
            directory_name = filename[:-4]  # Remove the '.csv' extension
            # Create a directory for the CSV file if it doesn't exist
            if not os.path.exists(directory_name):
                os.makedirs(directory_name)
            
            # Read all rows and sort by day first, then date
            rows = []
            with open(filename, mode='r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    torrent_name = row[0]
                    date_str = row[3]  # Assuming the date is in the fourth column
                    date_obj = parse_date(date_str)
                    day_priority = get_day_priority(torrent_name)
                    rows.append((day_priority, date_obj, row))
                
                # Sort first by day priority (Friday, Saturday, Sunday, others)
                # then by date for entries with the same day
                rows.sort(key=lambda x: (x[0], x[1]))  
            
            # Write sorted rows to numbered CSV files
            for i, (_, _, row) in enumerate(rows, start=1):
                torrent_name = row[0]  # Assuming the torrent name is in the first column
                magnet_link = row[1]  # Assuming the magnet link is in the second column
                infohash = extract_infohash(magnet_link)
                
                # Construct the CSV file name using a sequential number
                csv_filename = f"{i}.csv"
                archive_filename = f"{i}.archive"
                old_filename = f"{i}.old"
                csv_filepath = os.path.join(directory_name, csv_filename)
                archive_filepath = os.path.join(directory_name, archive_filename)
                old_filepath = os.path.join(directory_name, old_filename)
                
                # Check if the .csv, .archive, or .old file already exists
                if not os.path.exists(csv_filepath) and not os.path.exists(archive_filepath) and not os.path.exists(old_filepath):
                    # Write the torrent name, infohash, and magnet link to the CSV file
                    if infohash:  # Only write if the infohash was successfully extracted
                        with open(csv_filepath, mode='w', newline='', encoding='utf-8') as output_csvfile:
                            writer = csv.writer(output_csvfile)
                            writer.writerow([torrent_name, infohash, magnet_link])

# Call the function to process the CSV files
process_csv_files()
