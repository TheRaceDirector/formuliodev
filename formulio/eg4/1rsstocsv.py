import feedparser
import csv
import os
from datetime import datetime
import base64

# Define the keywords to search for in the item titles, non case sensitive
keywords = ["Formula 1", "Formula1", "Formula.1", "Formula+1"]
year = "2024"  # Define the year to search for in the item titles

# Define the path to the CSV file
csv_file_path = 'f1db.csv'

# Function to format the publication date by removing the day of the week
def format_pubdate(pubDate):
    date_object = datetime.strptime(pubDate, '%a, %d %b %Y %H:%M:%S %z')
    return date_object.strftime('%d %b %Y %H:%M:%S %z')

# Function to check if an entry is already in the CSV file based on GUID
def is_duplicate(guid, existing_guids):
    return guid in existing_guids

# Function to append data to the CSV file
def append_to_csv(data, csv_file_path):
    with open(csv_file_path, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(data)

# Function to read existing GUIDs from the CSV file
def get_existing_guids(csv_file_path):
    existing_guids = set()
    if os.path.isfile(csv_file_path):
        with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if 'guid' in row:
                    existing_guids.add(row['guid'])
    return existing_guids

# Read existing GUIDs from the CSV file
existing_guids = get_existing_guids(csv_file_path)

# Check if the CSV file exists, if not create it with the header
if not os.path.isfile(csv_file_path):
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['title', 'link', 'guid', 'pubDate'])

# Read the list of base64 encoded RSS feed URLs from the file
with open('RssMagnets.txt', 'r') as file:
    encoded_rss_urls = file.read().splitlines()

# Decode the base64 encoded URLs
rss_urls = [base64.b64decode(encoded_url).decode('utf-8') for encoded_url in encoded_rss_urls]

# Process each RSS feed URL
for url in rss_urls:
    # Parse the RSS feed
    feed = feedparser.parse(url)
    
    # Process each item in the feed
    for entry in feed.entries:
        # Check if the title contains any of the keywords and the year "2024"
        if any(keyword.lower() in entry.title.lower() for keyword in keywords) and year in entry.title:
            # Format the publication date
            formatted_pubDate = format_pubdate(entry.published)
            # Check for duplicates based on GUID
            if not is_duplicate(entry.guid, existing_guids):
                # Append the relevant data to the CSV file
                append_to_csv([entry.title, entry.link, entry.guid, formatted_pubDate], csv_file_path)
                # Add the GUID to the set of existing GUIDs
                existing_guids.add(entry.guid)

print("RSS feeds processed and relevant data appended to f1db.csv.")
