import re
import os

def process_file(filename):
    # Read the file
    try:
        with open(filename, 'r') as file:
            lines = file.readlines()
    except IOError as e:
        print(f"Error reading file: {e}")
        return

    # Extract season numbers
    season_pattern = re.compile(r"'hpytt\d+:(\d{2}):")
    seasons = set()
    for line in lines:
        match = season_pattern.search(line)
        if match:
            seasons.add(int(match.group(1)))

    # Get the latest 3 seasons
    latest_seasons = sorted(seasons, reverse=True)[:3]

    # Filter lines
    filtered_lines = []
    for line in lines:
        match = season_pattern.search(line)
        if match and int(match.group(1)) in latest_seasons:
            filtered_lines.append(line)

    # Write the filtered content back to the file
    try:
        with open(filename, 'w') as file:
            file.writelines(filtered_lines)
    except IOError as e:
        print(f"Error writing to file: {e}")
        return

    print(f"Processed {filename}. Kept seasons: {', '.join(map(str, latest_seasons))}")

# Main execution
if __name__ == "__main__":
    filename = "6processed.txt"
    if os.path.exists(filename):
        process_file(filename)
    else:
        print(f"Error: {filename} not found in the current directory.")