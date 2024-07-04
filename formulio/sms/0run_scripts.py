import os
import subprocess

# Paths to the scripts (adjust these paths as per your directory structure)
script_directory = os.path.dirname(os.path.abspath(__file__))
scripts = [
    '1rsstocsv.py',
    '2dbsorter.py',
    '3racedirector.py',
    '4torrenttocontent.py',
    '5merger.py'
]

# Function to run the scripts sequentially
def run_scripts():
    for script in scripts:
        script_path = os.path.join(script_directory, script)
        print(f"Running {script_path}...")
        subprocess.run(['python3', script_path], check=True, cwd=script_directory)
        print(f"Completed {script_path}")
    print("All scripts completed.")

# Initial run (remove this if not needed, as it will be triggered externally)
run_scripts()
