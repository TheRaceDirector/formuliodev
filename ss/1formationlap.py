import feedparser
import csv
import os
import base64
from datetime import datetime, timezone
import requests
import re
import shutil
from dateutil import parser as date_parser

# Define the keywords to search for in the item titles, non-case sensitive
keywords = ["Formula 1", "Formula1", "Formula.1", "Formula+1"]
year = "2026"  # Define the year to search for in the item titles

# Define the path to the CSV file
script_dir = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(script_dir, 'f1db.csv')

def smart_date_parse(date_string):
    try:
        # Check if the input is empty or None
        if not date_string:
            return datetime.now(timezone.utc)
            
        # Try parsing as a Unix timestamp first
        timestamp = float(date_string)
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    except (ValueError, TypeError):
        pass
    
    try:
        # Use dateutil's parser, which can handle many formats
        parsed_date = date_parser.parse(date_string)
        
        # If the parsed date doesn't have a timezone, assume UTC
        if parsed_date.tzinfo is None:
            parsed_date = parsed_date.replace(tzinfo=timezone.utc)
        
        return parsed_date
    except (ValueError, TypeError):
        # Default date if all parsing fails
        return datetime.now(timezone.utc)

def format_pubdate(pubDate):
    parsed_date = smart_date_parse(pubDate)
    return parsed_date.strftime('%d %b %Y %H:%M:%S %z')

# Function to check if an entry is already in the CSV file based on GUID
def is_duplicate(guid, existing_guids):
    return guid in existing_guids

# Function to append data to the CSV file
def append_to_csv(data, csv_file_path):
    # Check if file exists and has content
    file_exists = os.path.isfile(csv_file_path) and os.path.getsize(csv_file_path) > 0
    
    with open(csv_file_path, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        # Only write header if file is new/empty
        if not file_exists:
            writer.writerow(['title', 'link', 'guid', 'pubDate'])
        writer.writerow(data)

# Function to read existing GUIDs from the CSV file
def get_existing_guids(csv_file_path):
    existing_guids = set()
    if os.path.isfile(csv_file_path):
        try:
            with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    if 'guid' in row:
                        existing_guids.add(row['guid'])
        except Exception as e:
            print(f"Error reading GUIDs from CSV: {e}")
    return existing_guids

# Function to ensure CSV has the correct format
def ensure_csv_format():
    if not os.path.isfile(csv_file_path):
        # If file doesn't exist, we'll create it when needed
        return
    
    try:
        # Try to read the existing file to verify it's valid
        existing_data = []
        with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader, None)
            if not headers or headers != ['title', 'link', 'guid', 'pubDate']:
                print(f"Warning: CSV headers don't match expected format. Creating backup and fixing.")
                # Store existing data for migration
                if headers:  # Only append headers if they exist
                    existing_data.append(headers)  
                for row in reader:
                    existing_data.append(row)
                
                # Backup the original file
                backup_path = csv_file_path + '.backup'
                shutil.copy2(csv_file_path, backup_path)
                
                # Create a new file with correct headers but preserve data
                with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(['title', 'link', 'guid', 'pubDate'])
                    # Write all the existing data rows (skip the header if it exists)
                    for row in existing_data[1:] if headers else existing_data:
                        # Pad the row if it's shorter than expected
                        padded_row = row + [''] * (4 - len(row)) if len(row) < 4 else row
                        writer.writerow(padded_row[:4])  # Trim if longer than expected
                print("CSV format fixed successfully")
            
    except Exception as e:
        print(f"Error checking CSV format: {e}")
        # Backup the problematic file
        backup_path = csv_file_path + '.error'
        try:
            shutil.copy2(csv_file_path, backup_path)
            # Create a fresh file
            with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['title', 'link', 'guid', 'pubDate'])
            print("Created new CSV file with correct headers")
        except Exception as backup_error:
            print(f"Error creating backup: {backup_error}")

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
        feed = feedparser.parse(response.content)
        
        if not feed.entries and hasattr(feed, 'bozo') and feed.bozo:
            print(f"Warning: Feed parsing error. Trying direct parsing...")
            feed = feedparser.parse(url)
            
        return feed
    except Exception:
        # Try direct parsing as a fallback
        try:
            return feedparser.parse(url)
        except Exception:
            return None

# Unified function to process feeds
def process_feed(feed, existing_guids):
    new_entries = []
    
    if not feed or not hasattr(feed, 'entries'):
        return new_entries
        
    for entry in feed.entries:
        # Skip entries without title
        if not hasattr(entry, 'title'):
            continue
            
        if any(keyword.lower() in entry.title.lower() for keyword in keywords) and year in entry.title:
            # Get the publication date with fallbacks
            pubDate = None
            if hasattr(entry, 'published'):
                pubDate = entry.published
            elif hasattr(entry, 'pubDate'):
                pubDate = entry.pubDate
            elif hasattr(entry, 'updated'):
                pubDate = entry.updated
            
            formatted_pubDate = format_pubdate(pubDate) if pubDate else format_pubdate('')
            
            # Get the GUID with multiple fallbacks
            guid = None
            if hasattr(entry, 'id'):
                guid = entry.id
            elif hasattr(entry, 'guid'):
                guid = entry.guid
            elif hasattr(entry, 'link'):
                guid = entry.link
            
            # Process complex GUID structures
            if isinstance(guid, dict):
                if '' in guid:
                    guid = guid['']
                elif 'text' in guid:
                    guid = guid['text']
                else:
                    guid = str(guid)
                    
            # Make sure we have a string
            guid = str(guid) if guid else ''
                
            # For URLs, extract the last part as the GUID
            if guid.startswith("http"):
                guid = guid.split("/")[-1]
                
            # Skip entries without a valid GUID
            if not guid:
                continue
                
            # Make sure we have a link
            link = entry.link if hasattr(entry, 'link') else ''
            
            if not is_duplicate(guid, existing_guids):
                new_entries.append([entry.title, link, guid, formatted_pubDate])
                existing_guids.add(guid)
                
    return new_entries

# Main script logic
def main():
    print("RSS Feed Processor")
    print("-----------------")
    
    # Ensure CSV has correct format before proceeding
    ensure_csv_format()
    
    existing_guids = get_existing_guids(csv_file_path)
    print(f"Loaded {len(existing_guids)} existing entries")

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

    # Process each group
    total_new_entries = 0
    for group_name, urls in groups.items():
        success = False
        for url in urls:
            feed = fetch_and_parse_rss(url)
            if feed and hasattr(feed, 'entries'):
                new_entries = process_feed(feed, existing_guids)
                for entry in new_entries:
                    append_to_csv(entry, csv_file_path)
                
                if new_entries:
                    print(f"{group_name}: Found {len(new_entries)} new entries")
                    total_new_entries += len(new_entries)
                    success = True
                    break  # Stop trying other URLs in this group once one is successful
        
        if not success:
            print(f"{group_name}: No new entries found")

    print(f"\nSummary: Added {total_new_entries} new entries to database")
    print("Process completed successfully")

if __name__ == "__main__":
    main()
