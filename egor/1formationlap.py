import feedparser
import csv
import os
import base64
from datetime import datetime
import requests
import re

# Define the keywords to search for in the item titles, non-case sensitive
keywords = ["Formula 1", "Formula1", "Formula.1", "Formula+1"]
year = "2025"  # Define the year to search for in the item titles

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

# Function to fetch and parse RSS feed
def fetch_and_parse_rss(url, timeout=20):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        return feedparser.parse(response.content)
    except requests.Timeout:
        return None
    except requests.RequestException:
        return None
    except feedparser.NonXMLContentType:
        return None

# Function to process TGX RSS feed
def process_tgx_feed(feed, existing_guids):
    new_entries = []
    for entry in feed.entries:
        if any(keyword.lower() in entry.title.lower() for keyword in keywords) and year in entry.title:
            formatted_pubDate = format_pubdate(entry.published)
            if not is_duplicate(entry.guid, existing_guids):
                new_entries.append([entry.title, entry.link, entry.guid, formatted_pubDate])
                existing_guids.add(entry.guid)
    return new_entries

# Function to process Reddit RSS feed
def process_reddit_feed(feed, existing_guids):
    new_entries = []
    for entry in feed.entries:
        if any(keyword.lower() in entry.title.lower() for keyword in keywords) and year in entry.title:
            content = entry.content[0].value
            magnet_link_match = re.search(r'(magnet:\?xt=urn:btih:[^"]+)', content)
            if magnet_link_match:
                link = magnet_link_match.group(1)
                link = link.replace('&amp;', '&')
                link = re.sub(r'</p>.*$', '', link)
                guid = link.split(':')[-1].split('&')[0]

                input_date = datetime.strptime(entry.updated, "%Y-%m-%dT%H:%M:%S%z")
                pubDate = input_date.strftime("%d %b %Y %H:%M:%S +0000")

                if not is_duplicate(guid, existing_guids):
                    new_entries.append([entry.title, link, guid, pubDate])
                    existing_guids.add(guid)
    return new_entries

# Mapping of groups to their corresponding processing functions
group_to_function_mapping = {
    'Source A': process_tgx_feed,
    'Source B': process_reddit_feed,
    # Add other mappings here as needed
}

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

# Process each group using its associated function
for group_name, urls in groups.items():
    processing_function = group_to_function_mapping.get(group_name)
    
    if processing_function is None:
        print(f"No processing function defined for {group_name}. Skipping.")
        continue
    
    for url in urls:
        feed = fetch_and_parse_rss(url)
        if feed:
            new_entries = processing_function(feed, existing_guids)
            for entry in new_entries:
                append_to_csv(entry, csv_file_path)
            break  # Stop trying other URLs in this group once one is successful

print("RSS feeds processed and relevant data appended to f1db.csv.")
