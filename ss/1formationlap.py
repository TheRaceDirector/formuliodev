import feedparser
import csv
import os
import base64
from datetime import datetime, timezone
import requests
import re
from dateutil import parser as date_parser
import shutil
import time
import random
import json
import sys
import io

# Ensure stdout/stderr handle Unicode universally
# (Windows defaults to cp1252; Docker/Linux may vary by locale)
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'buffer'):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Load configuration
script_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(os.path.dirname(script_dir), 'data')

# Load year from general.json
general_config_path = os.path.join(data_dir, 'general.json')
with open(general_config_path, 'r', encoding='utf-8') as f:
    general_config = json.load(f)
    year = general_config["year"]

# Load keywords from f1_config.json
f1_config_path = os.path.join(data_dir, 'f1_config.json')
with open(f1_config_path, 'r', encoding='utf-8') as f:
    f1_config = json.load(f)
    keywords = f1_config["keywords"]

csv_file_path = os.path.join(script_dir, 'f1db.csv')

DEBUG = False

RATE_LIMIT_DELAY = 3
MAX_RETRIES = 3
RETRY_DELAY = 5

USER_AGENTS = {
    'bt4gpx': {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/rss+xml, application/xml, text/xml, */*',
        'Accept-Language': 'en-US,en;q=0.9',
    },
    'reddit': {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'max-age=0',
        'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
    },
    'default': {
        'User-Agent': 'Python-RSS-Reader/1.0 (Feed Aggregator)',
        'Accept': 'application/rss+xml, application/xml, text/xml, */*',
    }
}


def extract_btih(magnet_or_link):
    """Extract the btih hash from a magnet link. Returns None if not found."""
    match = re.search(r'xt=urn:btih:([a-fA-F0-9]{40})', magnet_or_link)
    if match:
        return match.group(1).lower()
    match = re.search(r'xt=urn:btih:([a-zA-Z0-9]{32})', magnet_or_link)
    if match:
        return match.group(1).lower()
    return None


def smart_date_parse(date_string):
    """Parse various date formats into a datetime object."""
    try:
        timestamp = float(date_string)
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    except (ValueError, TypeError):
        pass

    try:
        parsed_date = date_parser.parse(date_string)
        if parsed_date.tzinfo is None:
            parsed_date = parsed_date.replace(tzinfo=timezone.utc)
        return parsed_date
    except (ValueError, TypeError):
        if DEBUG:
            print(f"Warning: Unable to parse date '{date_string}'. Using current time.")
        return datetime.now(tz=timezone.utc)


def format_pubdate(pubDate):
    """Format publication date consistently."""
    parsed_date = smart_date_parse(pubDate)
    return parsed_date.strftime('%d %b %Y %H:%M:%S %z')


def is_duplicate(btih, existing_btihs):
    """Check if an entry is already in the CSV file based on btih hash."""
    return btih in existing_btihs


def append_to_csv(data, csv_file_path):
    """Append data to the CSV file."""
    with open(csv_file_path, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(data)


def get_existing_btihs(csv_file_path):
    """Read existing btih hashes from the CSV file by scanning the link column."""
    existing_btihs = set()
    if os.path.isfile(csv_file_path):
        try:
            with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    link = row.get('link', '')
                    btih = extract_btih(link)
                    if btih:
                        existing_btihs.add(btih)
                    guid = row.get('guid', '')
                    if guid and re.match(r'^[a-fA-F0-9]{40}$', guid):
                        existing_btihs.add(guid.lower())
        except Exception as e:
            print(f"Error reading btihs from CSV: {e}")
    return existing_btihs


def ensure_csv_format():
    """Ensure CSV has the correct format and headers."""
    if not os.path.isfile(csv_file_path):
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['title', 'link', 'guid', 'pubDate'])
        return

    try:
        with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader, None)

            if not headers or headers != ['title', 'link', 'guid', 'pubDate']:
                print(f"Warning: CSV headers don't match expected format. Creating backup and fixing.")
                backup_path = csv_file_path + '.backup'
                shutil.copy2(csv_file_path, backup_path)

                csvfile.seek(0)
                existing_data = list(csv.reader(csvfile))

                with open(csv_file_path, 'w', newline='', encoding='utf-8') as new_csvfile:
                    writer = csv.writer(new_csvfile)
                    writer.writerow(['title', 'link', 'guid', 'pubDate'])

                    start_idx = 1 if headers else 0
                    for row in existing_data[start_idx:]:
                        padded_row = (row + [''] * 4)[:4]
                        writer.writerow(padded_row)

                print("CSV format corrected successfully")

    except Exception as e:
        print(f"Error checking CSV format: {e}")
        if os.path.isfile(csv_file_path):
            backup_path = csv_file_path + '.error'
            try:
                shutil.copy2(csv_file_path, backup_path)
            except Exception:
                pass

        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['title', 'link', 'guid', 'pubDate'])
        print("Created new CSV file with correct headers")


def fetch_and_parse_rss(url, feed_type='default', timeout=20):
    """Fetch and parse RSS feed with rate limiting and retries."""
    headers = USER_AGENTS.get(feed_type, USER_AGENTS['default'])

    for attempt in range(MAX_RETRIES):
        try:
            if DEBUG:
                print(f"  Attempt {attempt + 1}/{MAX_RETRIES}: Fetching {url}")

            time.sleep(random.uniform(0.5, 1.5))

            response = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)

            if DEBUG:
                print(f"  Status Code: {response.status_code}")
                print(f"  Content Type: {response.headers.get('Content-Type', 'Unknown')}")
                print(f"  Content Length: {len(response.content)} bytes")

            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', RETRY_DELAY * (2 ** attempt)))
                delay = retry_after + random.uniform(1, 3)
                print(f"  ! Rate limited (429). Waiting {delay:.1f} seconds before retry...")
                time.sleep(delay)
                continue

            response.raise_for_status()

            feed = feedparser.parse(response.content)

            if DEBUG:
                if hasattr(feed, 'bozo') and feed.bozo:
                    print(f"  Warning: Feed parsing issues detected")
                if hasattr(feed, 'entries'):
                    print(f"  Entries found: {len(feed.entries)}")
                if hasattr(feed, 'feed'):
                    print(f"  Feed title: {feed.feed.get('title', 'N/A')}")

            time.sleep(RATE_LIMIT_DELAY)
            return feed

        except requests.Timeout:
            print(f"  x Timeout error (attempt {attempt + 1}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY * (2 ** attempt) + random.uniform(0, 2)
                print(f"  Retrying in {delay:.1f} seconds...")
                time.sleep(delay)
            continue

        except requests.HTTPError as e:
            print(f"  x HTTP Error: {e.response.status_code}")
            if 400 <= e.response.status_code < 500 and e.response.status_code != 429:
                return None
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY * (2 ** attempt) + random.uniform(0, 2)
                print(f"  Retrying in {delay:.1f} seconds...")
                time.sleep(delay)
            continue

        except requests.RequestException as e:
            print(f"  x Request error: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY * (2 ** attempt) + random.uniform(0, 2)
                print(f"  Retrying in {delay:.1f} seconds...")
                time.sleep(delay)
            continue

        except Exception as e:
            print(f"  x Parse error: {str(e)}")
            return None

    print(f"  x Failed after {MAX_RETRIES} attempts")
    return None


def matches_keywords(title):
    """Check if title matches any of our keywords."""
    title_lower = title.lower()
    has_keyword = any(keyword.lower() in title_lower for keyword in keywords)
    if not has_keyword:
        has_keyword = bool(re.search(r'\bF1\b', title, re.IGNORECASE))
    return has_keyword


def process_bt4gpx_feed(feed, existing_btihs):
    """Process bt4gpx RSS feed entries."""
    new_entries = []

    if not feed or not hasattr(feed, 'entries') or not feed.entries:
        return new_entries

    if DEBUG:
        print(f"  Processing {len(feed.entries)} entries...")

    for entry in feed.entries:
        if not matches_keywords(entry.title) or year not in entry.title:
            continue

        link = entry.get('link', '')
        btih = extract_btih(link)
        if not btih:
            if DEBUG:
                print(f"    x No btih found in link for '{entry.title[:60]}'")
            continue

        if is_duplicate(btih, existing_btihs):
            if DEBUG:
                print(f"    x Duplicate (btih: {btih[:16]}...): '{entry.title[:50]}'")
            continue

        pubDate = entry.get('published', entry.get('pubDate', entry.get('updated', '')))
        if pubDate:
            formatted_pubDate = format_pubdate(pubDate)
        else:
            formatted_pubDate = datetime.now(tz=timezone.utc).strftime('%d %b %Y %H:%M:%S %z')

        new_entries.append([entry.title, link, btih, formatted_pubDate])
        existing_btihs.add(btih)

        if DEBUG:
            print(f"    + MATCH - {entry.title[:50]} (btih: {btih[:16]}...)")

    return new_entries


def process_reddit_feed(feed, existing_btihs):
    """Process Reddit RSS feed entries and extract magnet links."""
    new_entries = []

    if not feed or not hasattr(feed, 'entries') or not feed.entries:
        return new_entries

    if DEBUG:
        print(f"  Processing {len(feed.entries)} entries...")

    for entry in feed.entries:
        if not matches_keywords(entry.title) or year not in entry.title:
            continue

        content = None
        if hasattr(entry, 'content') and entry.content:
            content = entry.content[0].value
        elif hasattr(entry, 'summary'):
            content = entry.summary
        elif hasattr(entry, 'description'):
            content = entry.description
        else:
            if DEBUG:
                print(f"      x No content field found")
            continue

        magnet_link_match = re.search(r'(magnet:\?xt=urn:btih:[^\s"<]+)', content)
        if not magnet_link_match:
            if DEBUG:
                print(f"      x No magnet link found")
            continue

        link = magnet_link_match.group(1)
        link = link.replace('&amp;', '&')

        btih = extract_btih(link)
        if not btih:
            if DEBUG:
                print(f"      x No btih found in magnet link")
            continue

        if is_duplicate(btih, existing_btihs):
            if DEBUG:
                print(f"      x Duplicate (btih: {btih[:16]}...)")
            continue

        pubDate = entry.get('updated', entry.get('published', ''))
        if pubDate:
            formatted_pubDate = format_pubdate(pubDate)
        else:
            formatted_pubDate = datetime.now(tz=timezone.utc).strftime('%d %b %Y %H:%M:%S %z')

        new_entries.append([entry.title, link, btih, formatted_pubDate])
        existing_btihs.add(btih)

        if DEBUG:
            print(f"      + MATCH - {entry.title[:50]} (btih: {btih[:16]}...)")

    return new_entries


FEED_PROCESSORS = {
    'bt4gpx': process_bt4gpx_feed,
    'reddit': process_reddit_feed,
}


def parse_feed_file(feed_file_path):
    """Parse the feed.txt file and return list of (type, url) tuples."""
    feeds = []

    # FIXED: explicit encoding so it works on Windows, Docker, and all Linux locales
    with open(feed_file_path, 'r', encoding='utf-8') as file:
        current_type = None

        for line in file:
            line = line.strip()

            if not line or line.startswith('#'):
                continue

            if line.startswith('type='):
                current_type = line.split('=', 1)[1].strip()
                continue

            if current_type:
                try:
                    url = base64.b64decode(line).decode('utf-8')
                    feeds.append((current_type, url))
                except Exception as e:
                    print(f"Error decoding URL '{line[:20]}...': {e}")
            else:
                print(f"Warning: URL found without type declaration: {line[:20]}...")

    return feeds


def main():
    """Main script logic."""
    print("RSS Feed Processor")
    print("-----------------")
    print(f"Rate limit delay: {RATE_LIMIT_DELAY}s between requests")
    print(f"Max retries: {MAX_RETRIES}\n")

    ensure_csv_format()
    existing_btihs = get_existing_btihs(csv_file_path)
    print(f"Loaded {len(existing_btihs)} existing entries\n")

    feed_file_path = os.path.join(script_dir, 'feed.txt')
    if not os.path.isfile(feed_file_path):
        print(f"Error: feed.txt not found at {feed_file_path}")
        return

    feeds = parse_feed_file(feed_file_path)
    print(f"Found {len(feeds)} feed(s) to process\n")

    total_new_entries = 0
    start_time = time.time()

    for idx, (feed_type, url) in enumerate(feeds, 1):
        print(f"[{idx}/{len(feeds)}] Processing {feed_type} feed...")

        processor = FEED_PROCESSORS.get(feed_type)
        if not processor:
            print(f"  Warning: Unknown feed type '{feed_type}'. Skipping.")
            continue

        feed = fetch_and_parse_rss(url, feed_type=feed_type)
        if not feed or not hasattr(feed, 'entries'):
            print(f"  Failed to fetch or parse feed\n")
            continue

        if not feed.entries:
            print(f"  No entries found in feed\n")
            continue

        new_entries = processor(feed, existing_btihs)

        for entry in new_entries:
            append_to_csv(entry, csv_file_path)

        if new_entries:
            print(f"  Found {len(new_entries)} new entries")
            total_new_entries += len(new_entries)
        else:
            print(f"  No new entries (checked {len(feed.entries)} items)")

        print()

    elapsed_time = time.time() - start_time

    print(f"{'='*50}")
    print(f"Summary: Added {total_new_entries} new entries to database")
    print(f"Total entries in database: {len(existing_btihs)}")
    print(f"Time taken: {elapsed_time:.1f} seconds")
    print("Process completed successfully")


if __name__ == "__main__":
    main()
