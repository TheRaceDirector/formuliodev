import os
import re

def get_round_number(dir_name):
    match = re.match(r'2025r(\d+)FHD', dir_name)
    return int(match.group(1)) if match else None

def rename_files_in_directory(directory):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            base_name, extension = os.path.splitext(filename)
            if extension != '.old':
                new_file_path = os.path.join(directory, base_name + '.old')
                os.rename(file_path, new_file_path)
                print(f'Renamed {file_path} to {new_file_path}')

def main():
    current_directory = os.getcwd()
    round_dirs = [d for d in os.listdir(current_directory) if os.path.isdir(d) and get_round_number(d) is not None]

    round_dirs.sort(key=get_round_number, reverse=True)

    # Collect rounds to archive
    rounds_to_archive = []
    
    # Add rounds older than top 7
    if len(round_dirs) > 7:
        rounds_to_archive.extend(round_dirs[3:])
    
    # Always add round 00 if it exists and isn't already in the list
    round_00_dir = '2025r00FHD'
    if round_00_dir in round_dirs and round_00_dir not in rounds_to_archive:
        rounds_to_archive.append(round_00_dir)
    
    if not rounds_to_archive:
        print('No rounds to archive.')
        return

    for old_round in rounds_to_archive:
        print(f'Processing {old_round}')
        rename_files_in_directory(old_round)

if __name__ == "__main__":
    main()
