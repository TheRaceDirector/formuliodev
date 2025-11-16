import os
import subprocess
import logging

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Paths to the scripts (adjust these paths as per your directory structure)
script_directory = os.path.dirname(os.path.abspath(__file__))
scripts = [
#   '1rsstocsv.py',
    '2dbsorter.py',
    '3racedirector.py',
    '4retire.py',
    '5torrenttocontent.py',
    '6merger.py',
#   '7podium.py'
]

# Function to run the scripts sequentially
def run_scripts():
    for script in scripts:
        script_path = os.path.join(script_directory, script)
        logging.debug(f"Running {script_path}...")

        try:
            result = subprocess.run(['python3', script_path], check=True, cwd=script_directory, capture_output=True, text=True)
            logging.debug(f"Completed {script_path}")
            logging.debug(f"Output:\n{result.stdout}")
            if result.stderr:
                logging.error(f"Errors:\n{result.stderr}")

        except subprocess.CalledProcessError as e:
            logging.error(f"Error running {script_path}: {e}")
            logging.error(f"Output:\n{e.output}")
            logging.error(f"Errors:\n{e.stderr}")
            break

    logging.debug("All scripts completed.")

# Initial run (remove this if not needed, as it will be triggered externally)
if __name__ == "__main__":
    run_scripts()
