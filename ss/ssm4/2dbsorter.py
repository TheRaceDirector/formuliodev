import csv
import re
import os
import json
from pathlib import Path


class RecordProcessor:
    """Processes CSV records based on configuration from info.json"""
    
    def __init__(self, config_path: str = None):
        self.script_dir = Path(__file__).parent.resolve()
        
        if config_path is None:
            config_path = self.script_dir / "info.json"
        
        self.config = self._load_config(config_path)
        self.processing_config = self.config.get("record_processing", {})
        self.patterns = self._compile_patterns()
        self.existing_guids = {}
    
    def _load_config(self, config_path: str) -> dict:
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _compile_patterns(self) -> dict:
        patterns_config = self.processing_config.get("patterns", {})
        compiled = {}
        
        for pattern_name, pattern_def in patterns_config.items():
            regex = pattern_def.get("regex", "")
            flags = re.IGNORECASE if pattern_def.get("case_insensitive", False) else 0
            compiled[pattern_name] = re.compile(regex, flags)
        
        # Compile inclusion/exclusion filters (backwards compatible — optional)
        filter_config = self.processing_config.get("filters", {})

        include_patterns = filter_config.get("include", [])
        exclude_patterns = filter_config.get("exclude", [])
        filter_flags = re.IGNORECASE if filter_config.get("case_insensitive", True) else 0

        compiled["_include_filters"] = [
            re.compile(p, filter_flags) for p in include_patterns
        ]
        compiled["_exclude_filters"] = [
            re.compile(p, filter_flags) for p in exclude_patterns
        ]
        
        return compiled
    
    def _passes_filters(self, text: str) -> bool:
        """
        Returns True if the text passes inclusion and exclusion filters.
        - If include filters are defined, at least one must match.
        - If exclude filters are defined, none must match.
        - If neither is defined, all records pass (backwards compatible).
        """
        include_filters = self.patterns.get("_include_filters", [])
        exclude_filters = self.patterns.get("_exclude_filters", [])

        if include_filters:
            if not any(p.search(text) for p in include_filters):
                return False

        if exclude_filters:
            if any(p.search(text) for p in exclude_filters):
                return False

        return True

    def normalize_round(self, round_str: str) -> str:
        pattern = self.patterns.get("round")
        if not pattern:
            return None
        match = pattern.search(round_str)
        if match:
            round_num = re.sub(r'\D', '', match.group())
            format_template = self.processing_config.get("patterns", {}).get("round", {}).get(
                "normalize_format", "r{num:02d}"
            )
            return format_template.format(num=int(round_num))
        return None
    
    def normalize_resolution(self, res_str: str) -> str:
        pattern = self.patterns.get("resolution")
        if not pattern:
            return None
        match = pattern.search(res_str)
        if match:
            matched_value = match.group().upper()
            mappings = self.processing_config.get("patterns", {}).get("resolution", {}).get("mappings", {})
            for key, value in mappings.items():
                if key.upper() in matched_value or matched_value in key.upper():
                    return value
            return matched_value
        return None
    
    def read_existing_guids(self, filename: str) -> set:
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
        template = self.processing_config.get("output_filename_template", "{year}{round}{quality}.csv")
        output_dir = self.processing_config.get("output_directory", ".")
        filename = template.format(
            year=year,
            round=normalized_round,
            quality=normalized_resolution
        )
        output_path = self.script_dir / output_dir / filename
        return str(output_path)
    
    def get_input_file_path(self) -> str:
        input_file = self.processing_config.get("input_file", "../f1db.csv")
        return str(self.script_dir / input_file)
    
    def process_records(self, input_file: str = None):
        if input_file is None:
            input_file = self.get_input_file_path()
        
        columns_config = self.processing_config.get("columns", {})
        description_index = columns_config.get("description_index", 0)
        guid_index = columns_config.get("guid_index", 1)
        # NEW: optional fallback column to search for resolution if not in title
        resolution_fallback_index = columns_config.get("resolution_fallback_index", None)
        
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

                if not self._passes_filters(event_description):
                    continue
                
                year_match = year_pattern.search(event_description)
                round_match = round_pattern.search(event_description)
                resolution_match = resolution_pattern.search(event_description)

                # NEW: if resolution not found in title, try the fallback column (e.g. magnet link)
                if not resolution_match and resolution_fallback_index is not None:
                    if len(row) > resolution_fallback_index:
                        resolution_match = resolution_pattern.search(row[resolution_fallback_index])
                
                if year_match and round_match and resolution_match:
                    year = year_match.group(1)
                    normalized_round = self.normalize_round(round_match.group())
                    normalized_resolution = self.normalize_resolution(resolution_match.group())
                    
                    if not all([normalized_round, normalized_resolution]):
                        continue
                    
                    filename = self.build_output_filename(year, normalized_round, normalized_resolution)
                    
                    if dedup_enabled:
                        if filename not in self.existing_guids:
                            self.existing_guids[filename] = self.read_existing_guids(filename)
                        if guid in self.existing_guids[filename]:
                            continue
                    
                    os.makedirs(os.path.dirname(filename) or '.', exist_ok=True)
                    
                    with open(filename, mode='a', newline='', encoding='utf-8') as outfile:
                        writer = csv.writer(outfile)
                        writer.writerow(row)
                        if dedup_enabled:
                            self.existing_guids[filename].add(guid)


def main():
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