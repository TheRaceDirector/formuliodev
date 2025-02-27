import feedparser
import csv
import os
import base64
from datetime import datetime
import requests
import re
import shutil

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
        except Exception:
            pass

# Function to fetch and parse RSS feed
def fetch_and_parse_rss(url, timeout=20):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, timeout=timeout, headers=headers)
        response.raise_for_status()
        return feedparser.parse(response.content)
    except:
        return None

# Function to fetch and parse RSS feed directly with feedparser
def fetch_and_parse_rss_direct(url, timeout=20):
    try:
        return feedparser.parse(url)
    except:
        return None

# Function to process TGX RSS feed
def process_tgx_feed(feed, existing_guids):
    new_entries = []
    
    if not feed or not hasattr(feed, 'entries') or not feed.entries:
        return new_entries
    
    for entry in feed.entries:
        if any(keyword.lower() in entry.title.lower() for keyword in keywords) and year in entry.title:
            if hasattr(entry, 'published'):
                formatted_pubDate = format_pubdate(entry.published)
            elif hasattr(entry, 'updated'):
                formatted_pubDate = format_pubdate(entry.updated)
            else:
                formatted_pubDate = datetime.now().strftime('%d %b %Y %H:%M:%S +0000')
                
            if not is_duplicate(entry.guid, existing_guids):
                new_entries.append([entry.title, entry.link, entry.guid, formatted_pubDate])
                existing_guids.add(entry.guid)
    
    return new_entries

# Function to process Reddit RSS feed
def process_reddit_feed(feed, existing_guids):
    new_entries = []
    
    if not feed or not hasattr(feed, 'entries') or not feed.entries:
        return new_entries
    
    for entry in feed.entries:
        if any(keyword.lower() in entry.title.lower() for keyword in keywords) and year in entry.title:
            # Check what content fields are available
            content = None
            if hasattr(entry, 'content') and entry.content:
                content = entry.content[0].value
            elif hasattr(entry, 'summary'):
                content = entry.summary
            elif hasattr(entry, 'description'):
                content = entry.description
            else:
                continue
                
            # Try to extract magnet link using a more flexible regex
            magnet_link_match = re.search(r'(magnet:\?xt=urn:btih:[^\s"<]+)', content)
            if magnet_link_match:
                link = magnet_link_match.group(1)
                link = link.replace('&amp;', '&')
                guid = link.split(':')[-1].split('&')[0]
                
                # Handle different date formats
                input_date = None
                if hasattr(entry, 'updated'):
                    try:
                        input_date = datetime.strptime(entry.updated, "%Y-%m-%dT%H:%M:%S%z")
                    except ValueError:
                        try:
                            # Try alternative format
                            input_date = datetime.strptime(entry.updated, "%Y-%m-%dT%H:%M:%S.%f%z")
                        except ValueError:
                            input_date = datetime.now()
                elif hasattr(entry, 'published'):
                    try:
                        input_date = datetime.strptime(entry.published, "%Y-%m-%dT%H:%M:%S%z")
                    except ValueError:
                        try:
                            input_date = datetime.strptime(entry.published, "%Y-%m-%dT%H:%M:%S.%f%z")
                        except ValueError:
                            input_date = datetime.now()
                else:
                    input_date = datetime.now()
                    
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

    # Process each group using its associated function
    total_new_entries = 0
    for group_name, urls in groups.items():
        processing_function = group_to_function_mapping.get(group_name)
        
        if processing_function is None:
            print(f"{group_name}: No processing function defined. Skipped.")
            continue
        
        success = False
        for url in urls:
            # Try with requests + feedparser first
            feed = fetch_and_parse_rss(url)
            if not feed or not hasattr(feed, 'entries') or not feed.entries:
                feed = fetch_and_parse_rss_direct(url)
            
            if feed and hasattr(feed, 'entries') and feed.entries:
                new_entries = processing_function(feed, existing_guids)
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
