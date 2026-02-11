import feedparser
import csv
import os
import base64
from datetime import datetime, timezone
import requests
import re
from dateutil import parser as date_parser

# Define the keywords to search for in the item titles, non-case sensitive
keywords = ["WSBK", "WorldSBK"]
year = "2026"  # Define the year to search for in the item titles

# Define the path to the CSV file
script_dir = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(script_dir, 'f1db.csv')

def smart_date_parse(date_string):
    try:
        # Try parsing as a Unix timestamp first
        timestamp = float(date_string)
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    except ValueError:
        pass
    try:
        # Use dateutil's parser, which can handle many formats
        parsed_date = date_parser.parse(date_string)

        # If the parsed date doesn't have a timezone, assume UTC
        if parsed_date.tzinfo is None:
            parsed_date = parsed_date.replace(tzinfo=timezone.utc)

        return parsed_date
    except ValueError:
        print(f"Warning: Unable to parse date '{date_string}'. Using default.")
        return datetime(2026, 1, 1, tzinfo=timezone.utc)

def format_pubdate(pubDate):
    parsed_date = smart_date_parse(pubDate)
    return parsed_date.strftime('%d %b %Y %H:%M:%S %z')

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

# Function to fetch and parse RSS feed
def fetch_and_parse_rss(url, timeout=20):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return feedparser.parse(response.content)
    except requests.Timeout:
        print(f"Timeout error for URL: {url}")
        return None
    except requests.RequestException as e:
        print(f"Request error for URL {url}: {str(e)}")
        return None
    except feedparser.NonXMLContentType:
        print(f"Non-XML content type for URL: {url}")
        return None

# Unified function to process feeds
def process_feed(feed, existing_guids):
    new_entries = []
    for entry in feed.entries:
        if any(keyword.lower() in entry.title.lower() for keyword in keywords) and year in entry.title:
            pubDate = entry.get('published', entry.get('pubDate', ''))
            formatted_pubDate = format_pubdate(pubDate)
            guid = entry.get('id', entry.get('guid', ''))
            if isinstance(guid, dict):
                guid = guid.get('', '')
            if guid.startswith("http"):
                guid = guid.split("/")[-1]
            if not is_duplicate(guid, existing_guids):
                new_entries.append([entry.title, entry.link, guid, formatted_pubDate])
                existing_guids.add(guid)
    return new_entries

# Main script logic
existing_guids = get_existing_guids(csv_file_path)

# Check if the CSV file exists, if not create it with the header
if not os.path.isfile(csv_file_path):
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['title', 'link', 'guid', 'pubDate'])

# Read and group the RSS URLs from feed.txt
rss_magnets_path = os.path.join(script_dir, 'feed.txt')
with open(rss_magnets_path, 'r') as file:
    lines = file.read().splitlines()

groups = {}
current_group = None

for line in lines:
    if line.startswith("Source"):
        current_group = line.strip()
        groups[current_group] = []
    elif current_group:
        groups[current_group].append(base64.b64decode(line).decode('utf-8'))

# Process each group using the unified process_feed function
for group_name, urls in groups.items():
    print(f"Processing {group_name}")
    for url in urls:
        feed = fetch_and_parse_rss(url)
        if feed:
            new_entries = process_feed(feed, existing_guids)
            for entry in new_entries:
                append_to_csv(entry, csv_file_path)
            print(f"Processed {len(new_entries)} new entries from {group_name}")
            break  # Stop trying other URLs in this group once one is successful
    else:
        print(f"Failed to process any URLs for {group_name}")

print("RSS feeds processed and relevant data appended to f1db.csv.")
