FROM python:3.10.12

# Set the working directory
WORKDIR /usr/src/app

# Copy requirements.txt first for Docker cache optimization
COPY requirements.txt ./

# Install necessary packages
RUN apt-get update && \
    apt-get install -y build-essential libboost-system-dev libboost-python-dev libssl-dev libtorrent-rasterbar-dev

# Install Python dependencies
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

# Copy the rest of the application code
COPY . .

# Make all 0run_scripts.py scripts executable
RUN find . -name "0run_scripts.py" -exec chmod +x {} +

# Run all 0run_scripts.py scripts before starting the application
RUN find . -name "0run_scripts.py" -exec python3 {} \;

# Command to run your application (replace with your actual command)
CMD ["python3", "formulio-addon.py"]
