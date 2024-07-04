import libtorrent as lt
import time
import os
import csv
import glob

# Set the desired quality for directory filtering
quality = 'FHD'

# Function to convert magnet link to torrent info
def magnet_to_torrent_info(magnet_uri, output_dir):
    ses = lt.session({'listen_interfaces': '0.0.0.0:6881'})
    magnet_params = lt.parse_magnet_uri(magnet_uri)
    magnet_params.save_path = output_dir
    magnet_params.flags |= lt.torrent_flags.upload_mode | lt.torrent_flags.paused
    handle = ses.add_torrent(magnet_params)

    # Pause the torrent to prevent downloading
    handle.pause()

    # Print the magnet link being processed
    print(f'Downloading metadata for: {magnet_uri}')

    # Wait for the metadata to be downloaded
    while not handle.status().has_metadata:
        time.sleep(1)

    ses.pause()
    info = handle.torrent_file()
    ses.remove_torrent(handle)
    return info

# Function to process CSV files and write to content.csv
def process_csv_files():
    content_file_path = 'content.csv'

    # Ensure content.csv is created with header if it doesn't exist
    if not os.path.isfile(content_file_path):
        with open(content_file_path, 'w', newline='') as content_file:
            content_writer = csv.writer(content_file)
            content_writer.writerow(['torrent file name', 'filename within torrent', 'infohash', 'file index'])

    # Search for directories starting with "2" and ending with the specified quality
    for subdir in filter(lambda d: os.path.isdir(d) and d.startswith('2') and d.endswith(quality), os.listdir('.')):
        # Search for CSV files with sequential naming
        csv_files = sorted(glob.glob(os.path.join(subdir, '[0-9]*.csv')), key=lambda f: int(os.path.splitext(os.path.basename(f))[0]))
        for csv_file in csv_files:
            # Process each CSV file
            with open(csv_file, 'r') as file, open(content_file_path, 'a', newline='') as content_file:
                content_writer = csv.writer(content_file)
                for line in file:
                    # Manually split the line into torrent name, infohash, and magnet link
                    parts = line.rsplit(',', 2)
                    if len(parts) != 3:
                        print(f"Error: Incorrect format in file {csv_file}")
                        continue
                    torrent_name, infohash, magnet_link = parts
                    torrent_name = torrent_name.strip()
                    infohash = infohash.strip()

                    # Get the torrent info from the magnet link
                    torrent_info = magnet_to_torrent_info(magnet_link.strip(), subdir)
                    if torrent_info is None:
                        continue

                    # Get the list of files
                    file_storage = torrent_info.files()
                    num_files = file_storage.num_files()

                    # Write the required information to content.csv
                    for file_index in range(num_files):
                        file_entry = file_storage.file_path(file_index)
                        content_writer.writerow([torrent_name, file_entry, infohash, file_index])
                        print(f'Added file {file_entry} from torrent {torrent_name} with index {file_index} to content.csv')

            # Rename the CSV file to .archive after processing
            archive_file_path = csv_file.rsplit('.', 1)[0] + '.archive'
            os.rename(csv_file, archive_file_path)
            print(f'Renamed {csv_file} to {archive_file_path}')

# Call the function to start processing
process_csv_files()
