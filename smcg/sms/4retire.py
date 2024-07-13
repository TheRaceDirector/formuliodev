import os
import re

def get_round_number(dir_name):
    match = re.match(r'2024r(\d+)SD', dir_name)
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

    if len(round_dirs) <= 3:
        print('There are 3 or fewer rounds, no files to rename.')
        return

    for old_round in round_dirs[3:]:
        print(f'Processing {old_round}')
        rename_files_in_directory(old_round)

if __name__ == "__main__":
    main()
