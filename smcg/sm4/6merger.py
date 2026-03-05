#!/usr/bin/env python3
"""
6merger.py - Generic torrent content processor for Stremio addons

This script processes torrent content CSV files and generates formatted output
for use with Stremio addons. Configuration is read from info.json in the same directory.

Usage:
    python 6merger.py [--dry-run] [--verbose]
"""

import re
import csv
import json
import os
import sys
import argparse
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from pathlib import Path


# ============================================================================
# Configuration Classes
# ============================================================================

@dataclass
class ParsingConfig:
    """Configuration for parsing torrent names and filenames."""
    round_pattern: str = ""
    grand_prix_pattern: str = ""
    fallback_gp_pattern: str = ""
    title_pattern: str = ""
    fallback_title_pattern: str = ""
    session_types: List[str] = field(default_factory=list)
    quality_markers: List[str] = field(default_factory=list)
    title_format: str = "{title} - {grand_prix}"
    title_cleanup: Dict[str, bool] = field(default_factory=dict)


@dataclass
class Config:
    """Main configuration container."""
    series_id: str
    year: int
    sport: str
    quality: str
    parsing: ParsingConfig
    data_paths: Dict[str, str]
    column_mapping: Dict[str, List[str]]
    output: Dict[str, Any]
    debug: Dict[str, bool]
    
    # Compiled regex patterns (built after loading)
    _round_regex: Optional[re.Pattern] = field(default=None, repr=False)
    _gp_regex: Optional[re.Pattern] = field(default=None, repr=False)
    _fallback_gp_regex: Optional[re.Pattern] = field(default=None, repr=False)
    _title_regex: Optional[re.Pattern] = field(default=None, repr=False)
    _fallback_title_regex: Optional[re.Pattern] = field(default=None, repr=False)


# ============================================================================
# Script Directory & Path Handling
# ============================================================================

SCRIPT_DIR = Path(__file__).parent.resolve()


def resolve_path(path_str: str) -> Path:
    """Resolve a path relative to the script directory."""
    path = Path(path_str)
    if not path.is_absolute():
        path = SCRIPT_DIR / path
    return path.resolve()


# ============================================================================
# Configuration Loading
# ============================================================================

def load_info_json() -> Dict:
    """Load the info.json configuration file."""
    info_path = SCRIPT_DIR / 'info.json'
    
    if not info_path.exists():
        print(f"Error: info.json not found in {SCRIPT_DIR}")
        print("Please create an info.json file with the required configuration.")
        sys.exit(1)
    
    try:
        with open(info_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in info.json: {e}")
        sys.exit(1)


def build_pattern(pattern: str, config: Dict) -> str:
    """
    Build a regex pattern by substituting placeholders.
    
    Supported placeholders:
        {year} - The configured year
        {session_types} - Alternation of session types
        {quality_markers} - Alternation of quality markers
    """
    year = config.get('year', 2026)
    parsing = config.get('parsing', {})
    
    session_types = parsing.get('session_types', [])
    quality_markers = parsing.get('quality_markers', [])
    
    # Build alternation patterns
    session_alt = '|'.join(re.escape(s) for s in session_types) if session_types else 'Race|Qualifying'
    quality_alt = '|'.join(re.escape(q) for q in quality_markers) if quality_markers else '1080p|2160p'
    
    # Substitute placeholders
    result = pattern.replace('{year}', str(year))
    result = result.replace('{session_types}', session_alt)
    result = result.replace('{quality_markers}', quality_alt)
    
    return result


def compile_patterns(config: Config, raw_config: Dict) -> None:
    """Compile all regex patterns from configuration."""
    parsing = raw_config.get('parsing', {})
    torrent_parsing = parsing.get('torrent_name', {})
    filename_parsing = parsing.get('filename', {})
    
    # Round pattern
    round_pattern = torrent_parsing.get('round_pattern', r'(\d{4})x(\d+)')
    round_pattern = build_pattern(round_pattern, raw_config)
    try:
        config._round_regex = re.compile(round_pattern, re.IGNORECASE)
    except re.error as e:
        print(f"Warning: Invalid round_pattern '{round_pattern}': {e}")
        config._round_regex = re.compile(r'(\d{4})x(\d+)', re.IGNORECASE)
    
    # Grand Prix pattern
    gp_pattern = torrent_parsing.get('grand_prix_pattern', '')
    if gp_pattern:
        gp_pattern = build_pattern(gp_pattern, raw_config)
        try:
            config._gp_regex = re.compile(gp_pattern, re.IGNORECASE)
        except re.error as e:
            print(f"Warning: Invalid grand_prix_pattern '{gp_pattern}': {e}")
            config._gp_regex = None
    
    # Fallback GP pattern
    fallback_gp = torrent_parsing.get('fallback_gp_pattern', '')
    if fallback_gp:
        fallback_gp = build_pattern(fallback_gp, raw_config)
        try:
            config._fallback_gp_regex = re.compile(fallback_gp, re.IGNORECASE)
        except re.error as e:
            print(f"Warning: Invalid fallback_gp_pattern: {e}")
            config._fallback_gp_regex = None
    
    # Title pattern
    title_pattern = filename_parsing.get('title_pattern', r'^(\d+)\.(.+?)\.(mp4|mkv)$')
    try:
        config._title_regex = re.compile(title_pattern, re.IGNORECASE)
    except re.error as e:
        print(f"Warning: Invalid title_pattern: {e}")
        config._title_regex = re.compile(r'^(\d+)\.(.+?)\.(mp4|mkv)$', re.IGNORECASE)
    
    # Fallback title pattern
    fallback_title = filename_parsing.get('fallback_title_pattern', r'^(.+?)\.(mp4|mkv)$')
    try:
        config._fallback_title_regex = re.compile(fallback_title, re.IGNORECASE)
    except re.error as e:
        print(f"Warning: Invalid fallback_title_pattern: {e}")
        config._fallback_title_regex = re.compile(r'^(.+?)\.(mp4|mkv)$', re.IGNORECASE)


def load_config() -> Tuple[Config, Dict]:
    """Load and validate configuration, returning Config object and raw dict."""
    raw = load_info_json()
    
    # Extract parsing config
    parsing_raw = raw.get('parsing', {})
    parsing = ParsingConfig(
        session_types=parsing_raw.get('session_types', []),
        quality_markers=parsing_raw.get('quality_markers', []),
        title_format=parsing_raw.get('title_format', '{title} - {grand_prix}'),
        title_cleanup=parsing_raw.get('title_cleanup', {})
    )
    
    # Build main config
    config = Config(
        series_id=raw.get('series_id', 'unknown'),
        year=raw.get('year', 2026),
        sport=raw.get('sport', 'unknown'),
        quality=raw.get('quality', 'FHD'),
        parsing=parsing,
        data_paths=raw.get('data_paths', {}),
        column_mapping=raw.get('column_mapping', {}),
        output=raw.get('output', {}),
        debug=raw.get('debug', {})
    )
    
    # Compile regex patterns
    compile_patterns(config, raw)
    
    return config, raw


def load_sport_config(config: Config) -> Dict:
    """Load sport-specific configuration (countries, calendar, etc.)."""
    config_file = config.data_paths.get('config_file')
    
    if not config_file:
        return {'countries': {}, 'calendar': {}}
    
    config_path = resolve_path(config_file)
    
    if not config_path.exists():
        if config.debug.get('verbose'):
            print(f"Warning: Sport config not found at {config_path}")
        return {'countries': {}, 'calendar': {}}
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"Warning: Invalid JSON in sport config: {e}")
        return {'countries': {}, 'calendar': {}}


# ============================================================================
# CSV Column Detection
# ============================================================================

def detect_columns(header: List[str], column_mapping: Dict[str, List[str]]) -> Dict[str, int]:
    """
    Detect column indices from header row using flexible matching.
    
    Returns a dict mapping logical column names to their indices.
    """
    indices = {}
    header_lower = [h.lower().strip() for h in header]
    
    for logical_name, possible_names in column_mapping.items():
        for possible_name in possible_names:
            possible_lower = possible_name.lower()
            for i, h in enumerate(header_lower):
                if possible_lower == h or possible_lower in h:
                    indices[logical_name] = i
                    break
            if logical_name in indices:
                break
    
    return indices


def get_default_column_mapping() -> Dict[str, List[str]]:
    """Return default column mapping if not specified in config."""
    return {
        'torrent_name': ['torrent file name', 'torrent_name', 'torrent', 'name'],
        'filename': ['filename within torrent', 'filename', 'file', 'path'],
        'infohash': ['infohash', 'hash', 'info_hash'],
        'file_index': ['file index', 'file_index', 'fileindex', 'index', 'idx'],
        'filesize': ['filesize_gb', 'filesize', 'size', 'file_size', 'size_gb']
    }


# ============================================================================
# Text Processing
# ============================================================================

def extract_filename(filepath: str) -> str:
    """Extract just the filename from a path, handling various separators."""
    # Handle both forward and backslashes
    filepath = filepath.replace('\\', '/')
    if '/' in filepath:
        return filepath.split('/')[-1]
    return filepath


def clean_title(title: str, cleanup_config: Dict[str, bool]) -> str:
    """Clean up a title string based on configuration."""
    result = title
    
    if cleanup_config.get('replace_dots', True):
        result = result.replace('.', ' ')
    
    if cleanup_config.get('replace_underscores', True):
        result = result.replace('_', ' ')
    
    if cleanup_config.get('title_case', False):
        result = result.title()
    
    # Remove extra whitespace
    result = ' '.join(result.split())
    
    return result.strip()


def extract_round_info(torrent_name: str, config: Config) -> Tuple[str, str]:
    """
    Extract round number and grand prix name from torrent name.
    
    Returns (round_number, grand_prix_name) tuple.
    """
    round_number = 'Unknown'
    grand_prix_name = 'Unknown'
    
    # Extract round number
    if config._round_regex:
        match = config._round_regex.search(torrent_name)
        if match:
            # Handle both single group and double group patterns
            groups = match.groups()
            round_number = groups[-1]  # Last group is always the round number
    
    # Extract grand prix name - try main pattern first
    if config._gp_regex:
        match = config._gp_regex.search(torrent_name)
        if match:
            grand_prix_name = match.group(1)
    
    # Try fallback pattern if main pattern failed
    if grand_prix_name == 'Unknown' and config._fallback_gp_regex:
        match = config._fallback_gp_regex.search(torrent_name)
        if match:
            grand_prix_name = match.group(1)
    
    # Clean up grand prix name
    grand_prix_name = clean_title(grand_prix_name, config.parsing.title_cleanup)
    
    return round_number, grand_prix_name


def extract_title(filename: str, config: Config) -> str:
    """Extract the title portion from a filename."""
    title = 'Unknown'
    
    # Try main title pattern
    if config._title_regex:
        match = config._title_regex.search(filename)
        if match:
            groups = match.groups()
            # Second group is typically the title in patterns like "01.Title.mp4"
            if len(groups) >= 2:
                title = groups[1]
            else:
                title = groups[0]
    
    # Try fallback pattern
    if title == 'Unknown' and config._fallback_title_regex:
        match = config._fallback_title_regex.search(filename)
        if match:
            title = match.group(1)
    
    # Clean up title
    title = clean_title(title, config.parsing.title_cleanup)
    
    return title


def format_title(title: str, grand_prix: str, config: Config) -> str:
    """Format the final title using the configured template."""
    template = config.parsing.title_format
    
    result = template.replace('{title}', title)
    result = result.replace('{grand_prix}', grand_prix)
    result = result.replace('{quality}', config.quality)
    result = result.replace('{year}', str(config.year))
    
    return result.strip()


# ============================================================================
# Thumbnail Resolution
# ============================================================================

def build_thumbnail_map(sport_config: Dict) -> Dict[str, str]:
    """Build a mapping from round number to thumbnail URL."""
    countries = sport_config.get('countries', {})
    calendar = sport_config.get('calendar', {})
    
    return {
        round_num: countries.get(country, '')
        for round_num, country in calendar.items()
    }


# ============================================================================
# Main Processing
# ============================================================================

def process_row(
    row: List[str],
    col_indices: Dict[str, int],
    config: Config,
    thumbnail_map: Dict[str, str],
    episode_counter: Dict[str, int],
    seen_entries: set
) -> Optional[Dict]:
    """
    Process a single CSV row and return formatted output dict.
    
    Returns None if row should be skipped.
    """
    # Validate row has enough columns
    required_cols = ['torrent_name', 'filename', 'infohash', 'file_index']
    for col in required_cols:
        if col not in col_indices:
            return None
        if col_indices[col] >= len(row):
            return None
    
    # Extract values
    torrent_name = row[col_indices['torrent_name']].strip()
    filename = row[col_indices['filename']].strip()
    infohash = row[col_indices['infohash']].strip()
    
    try:
        file_index = int(row[col_indices['file_index']])
    except (ValueError, TypeError):
        return None
    
    # Extract filesize if available
    filesize = ''
    if 'filesize' in col_indices and col_indices['filesize'] < len(row):
        filesize = row[col_indices['filesize']].strip()
    
    # Extract actual filename from path
    actual_filename = extract_filename(filename)

    # Skip non-video files
    file_ext = os.path.splitext(actual_filename)[1].lower()
    if file_ext not in {'.mkv', '.mp4', '.avi', '.ts'}:
        if config.debug.get('log_skipped_rows'):
            print(f"  Skipped (non-video): {actual_filename}")
        return None
    
    # Extract round info
    round_number, grand_prix_name = extract_round_info(torrent_name, config)
    
    # Skip if round number couldn't be determined and deduplication is enabled
    if round_number == 'Unknown' and config.output.get('deduplicate', True):
        if config.debug.get('log_skipped_rows'):
            print(f"  Skipped (unknown round): {torrent_name}")
        return None
    
    # Extract and format title (do this BEFORE deduplication check)
    title = extract_title(actual_filename, config)
    formatted_title = format_title(title, grand_prix_name, config)
    
    # Skip files where title couldn't be extracted (e.g. wrong source)
    if title == 'Unknown':
        if config.debug.get('log_skipped_rows'):
            print(f"  Skipped (no title match): {actual_filename}")
        return None
    
    # Deduplication check - based on round and title, not infohash
    if config.output.get('deduplicate', True):
        # Use round + formatted title as the unique key
        # This will keep only the first occurrence of each session
        unique_key = (round_number, formatted_title)
        if unique_key in seen_entries:
            if config.debug.get('verbose'):
                print(f"  Skipped duplicate: {formatted_title}")
            return None
        seen_entries.add(unique_key)
    
    # Episode counting (after deduplication)
    round_key = f'{config.year}:{round_number}'
    if round_key not in episode_counter:
        episode_counter[round_key] = 0
    episode_counter[round_key] += 1
    
    # Get thumbnail
    thumbnail = thumbnail_map.get(round_number, '')
    
    # Determine season number
    try:
        season = int(round_number)
    except ValueError:
        season = 0
    
    # Build output row
    output = {
        'series_id': config.series_id,
        'season': season,
        'episode': episode_counter[round_key],
        'title': formatted_title,
        'thumbnail': thumbnail,
        'infoHash': infohash,
        'fileIdx': file_index,
        'filename': actual_filename
    }
    
    # Optional fields
    if config.output.get('include_filesize', True):
        output['filesize'] = filesize
    
    if config.output.get('include_quality', False):
        output['quality'] = config.quality
    
    return output


def process_csv(config: Config, sport_config: Dict, dry_run: bool = False) -> int:
    """
    Process the input CSV and generate output CSV.
    
    Returns the number of entries processed.
    """
    input_path = resolve_path(config.data_paths.get('input_csv', 'content.csv'))
    output_path = resolve_path(config.data_paths.get('output_csv', '6processed.csv'))
    
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        return 0
    
    # Get column mapping
    column_mapping = config.column_mapping or get_default_column_mapping()
    
    # Build thumbnail map
    thumbnail_map = build_thumbnail_map(sport_config)
    
    # Processing state
    output_data = []
    episode_counter = {}
    seen_entries = set()
    skipped_count = 0
    
    # Process input file
    with open(input_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        
        try:
            header = next(reader)
        except StopIteration:
            print("Error: Input CSV is empty")
            return 0
        
        # Detect columns
        col_indices = detect_columns(header, column_mapping)
        
        if config.debug.get('verbose'):
            print(f"Detected columns: {col_indices}")
        
        # Validate required columns
        required = ['torrent_name', 'filename', 'infohash', 'file_index']
        missing = [col for col in required if col not in col_indices]
        if missing:
            print(f"Error: Missing required columns: {missing}")
            print(f"Available columns: {header}")
            return 0
        
        # Process rows
        for row_num, row in enumerate(reader, start=2):
            if not row or all(cell.strip() == '' for cell in row):
                continue
            
            result = process_row(
                row, col_indices, config, thumbnail_map,
                episode_counter, seen_entries
            )
            
            if result:
                output_data.append(result)
            else:
                skipped_count += 1
    
    if config.debug.get('verbose'):
        print(f"Processed {len(output_data)} entries, skipped {skipped_count}")
    
    # Sort output
    sort_keys = config.output.get('sort_by', ['season', 'episode'])
    output_data.sort(key=lambda x: tuple(x.get(k, 0) for k in sort_keys))
    
    if dry_run:
        print(f"Dry run: would write {len(output_data)} entries to {output_path}")
        return len(output_data)
    
    # Check if data has changed
    existing_data = []
    if output_path.exists():
        try:
            with open(output_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                existing_data = list(reader)
        except Exception:
            pass
    
    # Determine fieldnames based on output config
    fieldnames = ['series_id', 'season', 'episode', 'title', 'thumbnail', 'infoHash', 'fileIdx']
    if config.output.get('include_filesize', True):
        fieldnames.append('filesize')
    if config.output.get('include_quality', False):
        fieldnames.append('quality')
    fieldnames.append('filename')
    
    # Compare data
    def normalize(row):
        return tuple(str(row.get(k, '')) for k in fieldnames)
    
    existing_normalized = [normalize(r) for r in existing_data]
    new_normalized = [normalize(r) for r in output_data]
    
    if new_normalized != existing_normalized:
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(output_data)
        print(f"Updated {output_path} ({len(output_data)} entries)")
    else:
        print(f"No changes to {output_path}")
    
    return len(output_data)


# ============================================================================
# Entry Point
# ============================================================================

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Process torrent content CSV for Stremio addon'
    )
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without writing files')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose output')
    args = parser.parse_args()
    
    print(f"6merger.py running from: {SCRIPT_DIR}")
    
    # Load configuration
    config, raw_config = load_config()
    
    # Override verbose setting from command line
    if args.verbose:
        config.debug['verbose'] = True
    
    if config.debug.get('verbose'):
        print(f"Series ID: {config.series_id}")
        print(f"Year: {config.year}")
        print(f"Sport: {config.sport}")
        print(f"Quality: {config.quality}")
    
    # Load sport-specific config
    sport_config = load_sport_config(config)
    
    if config.debug.get('verbose'):
        calendar_count = len(sport_config.get('calendar', {}))
        print(f"Loaded {calendar_count} rounds from sport config")
    
    # Process CSV
    count = process_csv(config, sport_config, dry_run=args.dry_run)
    
    if count == 0:
        sys.exit(1)


if __name__ == '__main__':
    main()