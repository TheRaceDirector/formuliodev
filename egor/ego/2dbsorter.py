import csv
import re
import os
import json
from pathlib import Path


class RecordProcessor:
    """Processes CSV records based on configuration from info.json"""
    
    def __init__(self, config_path: str = None):
        """
        Initialize the processor with configuration from info.json
        
        Args:
            config_path: Path to info.json. If None, looks in the script's directory.
        """
        self.script_dir = Path(__file__).parent.resolve()
        
        if config_path is None:
            config_path = self.script_dir / "info.json"
        
        self.config = self._load_config(config_path)
        self.processing_config = self.config.get("record_processing", {})
        self.patterns = self._compile_patterns()
        self.existing_guids = {}
    
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from info.json"""
        config_path = Path(config_path)
        
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _compile_patterns(self) -> dict:
        """Compile regex patterns from configuration"""
        patterns_config = self.processing_config.get("patterns", {})
        compiled = {}
        
        for pattern_name, pattern_def in patterns_config.items():
            regex = pattern_def.get("regex", "")
            flags = re.IGNORECASE if pattern_def.get("case_insensitive", False) else 0
            compiled[pattern_name] = re.compile(regex, flags)
        
        return compiled
    
    def normalize_round(self, round_str: str) -> str:
        """
        Normalize the round format based on configuration
        
        Args:
            round_str: Raw round string from the record
            
        Returns:
            Normalized round string (e.g., "r01") or None if no match
        """
        pattern = self.patterns.get("round")
        if not pattern:
            return None
        
        match = pattern.search(round_str)
        if match:
            round_num = re.sub(r'\D', '', match.group())  # Remove non-digit characters
            format_template = self.processing_config.get("patterns", {}).get("round", {}).get(
                "normalize_format", "r{num:02d}"
            )
            # Parse the format template and apply it
            return format_template.format(num=int(round_num))
        return None
    
    def normalize_resolution(self, res_str: str) -> str:
        """
        Normalize the resolution format based on configuration mappings
        
        Args:
            res_str: Raw resolution string from the record
            
        Returns:
            Normalized resolution string (e.g., "FHD") or None if no match
        """
        pattern = self.patterns.get("resolution")
        if not pattern:
            return None
        
        match = pattern.search(res_str)
        if match:
            matched_value = match.group().upper()
            mappings = self.processing_config.get("patterns", {}).get("resolution", {}).get("mappings", {})
            
            # Look up the mapping
            for key, value in mappings.items():
                if key.upper() in matched_value or matched_value in key.upper():
                    return value
            
            # Return the matched value if no mapping found
            return matched_value
        return None
    
    def read_existing_guids(self, filename: str) -> set:
        """
        Read existing GUIDs from a file to prevent duplicates
        
        Args:
            filename: Path to the CSV file
            
        Returns:
            Set of existing GUIDs in the file
        """
        guids = set()
        guid_index = self.processing_config.get("deduplication", {}).get("key_column_index", 1)
        
        if os.path.exists(filename):
            with open(filename, mode='r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    if len(row) > guid_index:
                        guids.add(row[guid_index])
        return guids
    
    def build_output_filename(self, year: str, normalized_round: str, normalized_resolution: str) -> str:
        """
        Build the output filename from template
        
        Args:
            year: Extracted year
            normalized_round: Normalized round string
            normalized_resolution: Normalized resolution string
            
        Returns:
            Formatted filename
        """
        template = self.processing_config.get("output_filename_template", "{year}{round}{quality}.csv")
        output_dir = self.processing_config.get("output_directory", ".")
        
        filename = template.format(
            year=year,
            round=normalized_round,
            quality=normalized_resolution
        )
        
        # Resolve path relative to script directory
        output_path = self.script_dir / output_dir / filename
        return str(output_path)
    
    def get_input_file_path(self) -> str:
        """Get the resolved input file path"""
        input_file = self.processing_config.get("input_file", "../f1db.csv")
        return str(self.script_dir / input_file)
    
    def process_records(self, input_file: str = None):
        """
        Process records and append to respective files without duplicates
        
        Args:
            input_file: Path to input CSV. If None, uses config value.
        """
        if input_file is None:
            input_file = self.get_input_file_path()
        
        columns_config = self.processing_config.get("columns", {})
        description_index = columns_config.get("description_index", 0)
        guid_index = columns_config.get("guid_index", 1)
        
        dedup_enabled = self.processing_config.get("deduplication", {}).get("enabled", True)
        
        year_pattern = self.patterns.get("year")
        round_pattern = self.patterns.get("round")
        resolution_pattern = self.patterns.get("resolution")
        
        if not all([year_pattern, round_pattern, resolution_pattern]):
            raise ValueError("Missing required patterns in configuration")
        
        with open(input_file, mode='r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            
            for row in reader:
                if len(row) <= max(description_index, guid_index):
                    continue
                
                event_description = row[description_index]
                guid = row[guid_index]
                
                # Extract components
                year_match = year_pattern.search(event_description)
                round_match = round_pattern.search(event_description)
                resolution_match = resolution_pattern.search(event_description)
                
                if year_match and round_match and resolution_match:
                    year = year_match.group(1)
                    normalized_round = self.normalize_round(round_match.group())
                    normalized_resolution = self.normalize_resolution(resolution_match.group())
                    
                    if not all([normalized_round, normalized_resolution]):
                        continue
                    
                    # Build filename
                    filename = self.build_output_filename(year, normalized_round, normalized_resolution)
                    
                    # Handle deduplication
                    if dedup_enabled:
                        if filename not in self.existing_guids:
                            self.existing_guids[filename] = self.read_existing_guids(filename)
                        
                        if guid in self.existing_guids[filename]:
                            continue
                    
                    # Ensure directory exists
                    os.makedirs(os.path.dirname(filename) or '.', exist_ok=True)
                    
                    # Append the record
                    with open(filename, mode='a', newline='', encoding='utf-8') as outfile:
                        writer = csv.writer(outfile)
                        writer.writerow(row)
                        
                        if dedup_enabled:
                            self.existing_guids[filename].add(guid)


def main():
    """Main entry point"""
    # Look for info.json in the same directory as the script
    script_dir = Path(__file__).parent.resolve()
    config_path = script_dir / "info.json"
    
    try:
        processor = RecordProcessor(config_path)
        processor.process_records()
        print("Processing complete.")
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Make sure info.json exists in the same directory as this script.")
    except json.JSONDecodeError as e:
        print(f"Error parsing info.json: {e}")
    except Exception as e:
        print(f"Error during processing: {e}")
        raise


if __name__ == "__main__":
    main()