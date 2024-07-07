import feedparser
import csv
import os
import base64
from datetime import datetime
import requests

# Define the keywords to search for in the item titles, non case sensitive
keywords = ["Formula 1", "Formula1", "Formula.1", "Formula+1"]
year = "2024"  # Define the year to search for in the item titles

# Define the path to the CSV file
script_dir = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(script_dir, 'f1db.csv')

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
rss_magnets_path = os.path.join(script_dir, 'feed.txt')
with open(rss_magnets_path, 'r') as file:
    encoded_rss_urls = file.read().splitlines()

# Decode each RSS feed URL
rss_urls = [base64.b64decode(encoded_url).decode('utf-8') for encoded_url in encoded_rss_urls]

# Function to fetch and parse RSS feed
def fetch_and_parse_rss(url, timeout=20):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        return feedparser.parse(response.content)
    except (requests.RequestException, feedparser.NonXMLContentType):
        return None

# Process each RSS feed URL
for url in rss_urls:
    feed = fetch_and_parse_rss(url)
    if feed:
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
        break  # Exit the loop if we successfully processed a feed

print("RSS feeds processed and relevant data appended to f1db.csv.")
