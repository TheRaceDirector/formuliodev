import libtorrent as lt
import time
import os
import csv
import glob
import threading

# Set the desired quality for directory filtering
quality = '4K'

# Function to convert magnet link to torrent info with a timeout
def magnet_to_torrent_info(magnet_uri, output_dir, timeout=30):
    def download_metadata(ses, handle, result):
        try:
            start_time = time.time()
            while not handle.status().has_metadata:
                time.sleep(1)
                if time.time() - start_time > timeout:
                    raise TimeoutError(f"Timeout reached ({timeout} seconds)")
            result.append(handle.torrent_file())
        except Exception as e:
            print(f"Exception during metadata download: {e}")
        finally:
            ses.pause()
            ses.remove_torrent(handle)

    ses = lt.session({'listen_interfaces': '0.0.0.0:6881'})
    magnet_params = lt.parse_magnet_uri(magnet_uri)
    magnet_params.save_path = output_dir
    magnet_params.flags |= lt.torrent_flags.upload_mode | lt.torrent_flags.paused
    handle = ses.add_torrent(magnet_params)

    # Pause the torrent to prevent downloading
    handle.pause()

    # Print the magnet link being processed
    print(f'Downloading metadata for: {magnet_uri}')

    result = []
    thread = threading.Thread(target=download_metadata, args=(ses, handle, result))
    thread.start()
    thread.join(timeout)

    if not result:
        print(f'Timeout reached for magnet link: {magnet_uri}')
        return None

    return result[0]

# Function to process CSV files and write to content.csv
def process_csv_files():
    content_file_path = 'content.csv'
    existing_files = set()

    # Ensure content.csv is created with header if it doesn't exist
    if not os.path.isfile(content_file_path):
        with open(content_file_path, 'w', newline='') as content_file:
            content_writer = csv.writer(content_file)
            content_writer.writerow(['torrent file name', 'filename within torrent', 'infohash', 'file index'])
    else:
        # Read existing entries to avoid duplicates
        with open(content_file_path, 'r', newline='') as content_file:
            content_reader = csv.reader(content_file)
            next(content_reader)  # Skip header
            for row in content_reader:
                if len(row) >= 2:
                    existing_files.add(row[1])  # Add filename within torrent to set

    # Search for directories starting with "2" and ending with the specified quality
    for subdir in filter(lambda d: os.path.isdir(d) and d.startswith('2') and d.endswith(quality), os.listdir('.')):
        # Search for CSV files with sequential naming
        csv_files = sorted(glob.glob(os.path.join(subdir, '[0-9]*.csv')), key=lambda f: int(os.path.splitext(os.path.basename(f))[0]))
        for csv_file in csv_files:
            all_magnets_successful = True  # Flag to track if all magnets were processed successfully
            new_entries = []  # Store new entries to write all at once
            
            # Process each CSV file
            with open(csv_file, 'r') as file:
                for line in file:
                    # Manually split the line into torrent name, infohash, and magnet link
                    parts = line.rsplit(',', 2)
                    if len(parts) != 3:
                        print(f"Error: Incorrect format in file {csv_file}")
                        continue
                    torrent_name, infohash, magnet_link = parts
                    torrent_name = torrent_name.strip()
                    infohash = infohash.strip()

                    # Get the torrent info from the magnet link with a timeout
                    torrent_info = magnet_to_torrent_info(magnet_link.strip(), subdir)
                    if torrent_info is None:
                        all_magnets_successful = False
                        continue

                    # Get the list of files
                    file_storage = torrent_info.files()
                    num_files = file_storage.num_files()

                    # Check each file and add to new_entries if not a duplicate
                    for file_index in range(num_files):
                        file_entry = file_storage.file_path(file_index)
                        if file_entry not in existing_files:
                            new_entries.append([torrent_name, file_entry, infohash, file_index])
                            existing_files.add(file_entry)  # Add to set to prevent duplicates within this run
                            print(f'New file found: {file_entry} from torrent {torrent_name}')
                        else:
                            print(f'Skipping duplicate: {file_entry} from torrent {torrent_name}')
            
            # Write all new entries at once
            if new_entries:
                with open(content_file_path, 'a', newline='') as content_file:
                    content_writer = csv.writer(content_file)
                    for entry in new_entries:
                        content_writer.writerow(entry)
                        print(f'Added file {entry[1]} from torrent {entry[0]} with index {entry[3]} to content.csv')

            # Rename the CSV file to .archive only if all magnets were successful
            if all_magnets_successful:
                archive_file_path = csv_file.rsplit('.', 1)[0] + '.archive'
                os.rename(csv_file, archive_file_path)
                print(f'Renamed {csv_file} to {archive_file_path}')
            else:
                print(f'Not renaming {csv_file} as some magnets failed to fetch metadata.')

# Call the function to start processing
process_csv_files()
