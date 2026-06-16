#!/usr/bin/env python3
"""
6merger.py - Generic torrent content processor for Stremio addons

This script processes torrent content CSV files and generates formatted output
for use with Stremio addons. Configuration is read from info.json in the same directory.

Key design guarantee
---------------------
Deduplication is CONTENT based, keyed ONLY on (round, session). The grand-prix
name is treated as cosmetic and never participates in dedup. For each
(round, session) the FIRST occurrence in the input wins (first-come-first-served),
so a round never contains two of the same session (e.g. two "Free Practice Three").

The session identity is derived automatically from the filename (strip the
leading episode number, the F1/year/round/GP prefix, and the quality/source
suffix). No per-source configuration is required, so every existing info.json
keeps working unchanged.

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
    session_key_pattern: str = ""          # optional override; auto-derived if absent
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
    _session_key_regex: Optional[re.Pattern] = field(default=None, repr=False)


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

    session_alt = '|'.join(re.escape(s) for s in session_types) if session_types else 'Race|Qualifying'
    quality_alt = '|'.join(re.escape(q) for q in quality_markers) if quality_markers else '1080p|2160p'

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

    # Optional explicit session-key pattern. If not provided, session keys are
    # derived automatically (see derive_session_key).
    session_key = filename_parsing.get('session_key_pattern', '')
    if session_key:
        try:
            config._session_key_regex = re.compile(session_key, re.IGNORECASE)
        except re.error as e:
            print(f"Warning: Invalid session_key_pattern: {e}")
            config._session_key_regex = None
    else:
        config._session_key_regex = None


def load_config() -> Tuple[Config, Dict]:
    """Load and validate configuration, returning Config object and raw dict."""
    raw = load_info_json()

    parsing_raw = raw.get('parsing', {})
    filename_raw = parsing_raw.get('filename', {})
    parsing = ParsingConfig(
        session_key_pattern=filename_raw.get('session_key_pattern', ''),
        session_types=parsing_raw.get('session_types', []),
        quality_markers=parsing_raw.get('quality_markers', []),
        title_format=parsing_raw.get('title_format', '{title} - {grand_prix}'),
        title_cleanup=parsing_raw.get('title_cleanup', {})
    )

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

    result = ' '.join(result.split())

    return result.strip()


def _normalize_key(text: str) -> str:
    """Normalize a string for dictionary lookups / comparisons."""
    t = text.lower()
    t = re.sub(r'[._\-]+', ' ', t)
    t = ' '.join(t.split())
    return t


def extract_round_info(torrent_name: str, config: Config) -> Tuple[str, str]:
    """
    Extract round number and grand prix name from torrent name.

    Returns (round_number, grand_prix_name). The grand_prix_name is cosmetic
    only; 'Unknown' is acceptable and does NOT affect deduplication.
    """
    round_number = 'Unknown'
    grand_prix_name = 'Unknown'

    if config._round_regex:
        match = config._round_regex.search(torrent_name)
        if match:
            groups = match.groups()
            round_number = groups[-1]  # Last group is always the round number

    if config._gp_regex:
        match = config._gp_regex.search(torrent_name)
        if match:
            grand_prix_name = match.group(1)

    if grand_prix_name == 'Unknown' and config._fallback_gp_regex:
        match = config._fallback_gp_regex.search(torrent_name)
        if match:
            grand_prix_name = match.group(1)

    grand_prix_name = clean_title(grand_prix_name, config.parsing.title_cleanup)

    return round_number, grand_prix_name


def normalize_round(round_number: str) -> str:
    """
    Normalize a round number to a canonical integer-string form.
    'R05' -> '5', '05' -> '5', '5' -> '5'. Returns 'Unknown' if not numeric.
    """
    if round_number is None:
        return 'Unknown'
    m = re.search(r'(\d+)', str(round_number))
    if not m:
        return 'Unknown'
    try:
        return str(int(m.group(1)))
    except ValueError:
        return 'Unknown'


def extract_title(filename: str, config: Config) -> str:
    """Extract the title (session name) portion from a filename for display."""
    title = 'Unknown'

    if config._title_regex:
        match = config._title_regex.search(filename)
        if match:
            groups = match.groups()
            if len(groups) >= 2:
                title = groups[1]
            else:
                title = groups[0]

    if title == 'Unknown' and config._fallback_title_regex:
        match = config._fallback_title_regex.search(filename)
        if match:
            title = match.group(1)

    title = clean_title(title, config.parsing.title_cleanup)

    return title


# Source/quality tail tokens we strip when deriving the session key. These are
# matched as whole, dot/space separated tokens. Combined tags like "SkyF1HD" or
# "SkySports" are handled by the regex tail-strip in derive_session_key, but we
# also list common standalone tokens here for the token-popping pass.
_SESSION_TAIL_TOKENS = {
    'sky', 'skysports', 'skyf1hd', 'skyf1uhd', 'skyuhd', 'sports', 'f1',
    'f1tv', 'tv', 'uhd', 'hd', 'sd', 'fhd', 'uk', 'world', 'feed', 'multi',
    'web', 'webrip', 'webdl', '2160p', '1080p', '720p', '480p', '2160',
    '1080', '720', '480', '4k', 'h264', 'h265', 'x264', 'x265', 'hevc',
    'avc', 'aac', 'ac3', 'ddp', 'dd', 'mp4', 'mkv', 'ts', 'avi',
}

# Regex that strips a trailing run of source/quality junk after the session
# name. It targets the common F1 tail forms regardless of how the source tag is
# glued together (Sky.Sports.F1.UHD, SkyF1HD.1080P, F1TV.2160p, etc.).
_TAIL_JUNK_RE = re.compile(
    r'[.\s_\-]+'
    r'(?:sky\w*|f1\w*|sports?|uhd|hd|sd|fhd|tv|uk|world|feed|multi|web\w*|'
    r'\d{3,4}p|\d{3,4}|4k|h26[45]|x26[45]|hevc|avc|aac|ac3|ddp?)'
    r'(?:[.\s_\-]+(?:sky\w*|f1\w*|sports?|uhd|hd|sd|fhd|tv|uk|world|feed|'
    r'multi|web\w*|\d{3,4}p|\d{3,4}|4k|h26[45]|x26[45]|hevc|avc|aac|ac3|ddp?))*'
    r'$',
    flags=re.IGNORECASE
)


def derive_session_key(filename: str, config: Config) -> str:
    """
    Derive a stable, GP-name-independent session identity from a filename.

    Strategy (all generic, no per-source config required):
      1. Drop the file extension.
      2. Drop a leading episode-number prefix ("08.").
      3. Drop everything up to and including a "...Grand Prix" marker, or
         failing that, everything up to and including the round token (Rxx /
         Round xx).
      4. Strip trailing source/quality junk via regex (handles SkyF1HD,
         Sky.Sports.F1.UHD, F1TV.2160p, etc.).
      5. Pop any residual standalone source/quality tokens.
      6. Normalize the remaining session words.

    If an explicit session_key_pattern is configured, that is used instead.
    Falls back to the cleaned display title if nothing else works.
    """
    if config._session_key_regex:
        m = config._session_key_regex.search(filename)
        if m and m.groups():
            return _normalize_key(m.groups()[-1])

    name = filename

    # 1. strip extension
    name = re.sub(r'\.(mkv|mp4|avi|ts)$', '', name, flags=re.IGNORECASE)

    # 2. strip leading episode number ("08.")
    name = re.sub(r'^\d+[.\s]+', '', name)

    # 3a. Prefer cutting after "...Grand Prix"
    gp_cut = re.search(r'grand[.\s]prix[.\s]+(.*)$', name, flags=re.IGNORECASE)
    if gp_cut:
        session_part = gp_cut.group(1)
    else:
        # 3b. Fallback: cut after the round token if present.
        rcut = re.search(r'r\d+[.\s]+(.*)$', name, flags=re.IGNORECASE)
        session_part = rcut.group(1) if rcut else name

    # 4. strip trailing source/quality junk (handles glued tags like SkyF1HD)
    session_part = _TAIL_JUNK_RE.sub('', session_part)

    # 5. pop any residual standalone source/quality tokens
    tokens = re.split(r'[.\s_\-]+', session_part)
    while tokens and tokens[-1].lower() in _SESSION_TAIL_TOKENS:
        tokens.pop()

    session = ' '.join(tokens).strip()

    # 6. normalize, with a sensible fallback
    key = _normalize_key(session)
    if not key:
        key = _normalize_key(extract_title(filename, config))
    return key


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
    """
    Build a mapping from normalized round number to thumbnail URL.
    Calendar keys are normalized so 'R05'/'05'/'5' all resolve.
    """
    countries = sport_config.get('countries', {})
    calendar = sport_config.get('calendar', {})

    out: Dict[str, str] = {}
    for round_num, country in calendar.items():
        out[normalize_round(round_num)] = countries.get(country, '')
    return out


# ============================================================================
# Row parsing (pass 1)
# ============================================================================

def parse_row(
    row: List[str],
    col_indices: Dict[str, int],
    config: Config
) -> Optional[Dict]:
    """
    Parse a single CSV row into an intermediate record dict WITHOUT assigning
    episode numbers or doing dedup. Returns None if the row should be skipped.
    """
    required_cols = ['torrent_name', 'filename', 'infohash', 'file_index']
    for col in required_cols:
        if col not in col_indices:
            return None
        if col_indices[col] >= len(row):
            return None

    torrent_name = row[col_indices['torrent_name']].strip()
    filename = row[col_indices['filename']].strip()
    infohash = row[col_indices['infohash']].strip()

    try:
        file_index = int(row[col_indices['file_index']])
    except (ValueError, TypeError):
        return None

    filesize = ''
    if 'filesize' in col_indices and col_indices['filesize'] < len(row):
        filesize = row[col_indices['filesize']].strip()

    actual_filename = extract_filename(filename)

    # Skip non-video files
    file_ext = os.path.splitext(actual_filename)[1].lower()
    if file_ext not in {'.mkv', '.mp4', '.avi', '.ts'}:
        if config.debug.get('log_skipped_rows'):
            print(f"  Skipped (non-video): {actual_filename}")
        return None

    # Round (the source of truth) + cosmetic GP name.
    round_number_raw, grand_prix_name = extract_round_info(torrent_name, config)
    round_number = normalize_round(round_number_raw)

    if round_number == 'Unknown' and config.output.get('deduplicate', True):
        if config.debug.get('log_skipped_rows'):
            print(f"  Skipped (unknown round): {torrent_name}")
        return None

    # Display title from the filename.
    title = extract_title(actual_filename, config)
    if title == 'Unknown':
        if config.debug.get('log_skipped_rows'):
            print(f"  Skipped (no title match): {actual_filename}")
        return None

    # Session identity for dedup — independent of the GP name.
    session_key = derive_session_key(actual_filename, config)

    formatted_title = format_title(title, grand_prix_name, config)

    return {
        'torrent_name': torrent_name,
        'round_number': round_number,
        'grand_prix': grand_prix_name,
        'session_key': session_key,
        'title': title,
        'formatted_title': formatted_title,
        'infohash': infohash,
        'file_index': file_index,
        'filename': actual_filename,
        'filesize': filesize,
    }


# ============================================================================
# Torrent completeness scoring (optional, only used when prefer_complete)
# ============================================================================

def score_torrents(records: List[Dict]) -> Dict[str, Dict]:
    """
    Group records by infohash and score each torrent. Only used when
    output.prefer_complete_torrents is enabled (default: disabled, so the
    first occurrence in input order wins instead).
    """
    torrents: Dict[str, Dict] = {}
    for rec in records:
        h = rec['infohash']
        t = torrents.setdefault(h, {
            'file_count': 0,
            'has_known_gp': False,
            'total_size': 0.0,
        })
        t['file_count'] += 1
        if rec['grand_prix'] and rec['grand_prix'] != 'Unknown':
            t['has_known_gp'] = True
        try:
            t['total_size'] += float(rec['filesize']) if rec['filesize'] else 0.0
        except (ValueError, TypeError):
            pass
    return torrents


def torrent_rank(infohash: str, torrent_stats: Dict[str, Dict]) -> Tuple:
    """Return a sortable rank tuple for a torrent. Higher tuples are better."""
    stats = torrent_stats.get(infohash, {})
    return (
        stats.get('file_count', 0),
        1 if stats.get('has_known_gp') else 0,
        stats.get('total_size', 0.0),
    )


# ============================================================================
# Main Processing
# ============================================================================

def process_csv(config: Config, sport_config: Dict, dry_run: bool = False) -> int:
    """
    Process the input CSV and generate output CSV.

    Pipeline:
      1. Parse every video row into an intermediate record.
      2. Deduplicate by (round, session). By default the FIRST occurrence in
         input order wins (first-come-first-served). The GP name plays NO part
         in the dedup key, so a round never contains two of the same session.
      3. Number episodes deterministically within each round.

    Returns the number of entries processed.
    """
    input_path = resolve_path(config.data_paths.get('input_csv', 'content.csv'))
    output_path = resolve_path(config.data_paths.get('output_csv', '6processed.csv'))

    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        return 0

    column_mapping = config.column_mapping or get_default_column_mapping()
    thumbnail_map = build_thumbnail_map(sport_config)

    # ---- Pass 1: parse all rows (preserving input order) ----
    records: List[Dict] = []
    skipped_count = 0

    with open(input_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)

        try:
            header = next(reader)
        except StopIteration:
            print("Error: Input CSV is empty")
            return 0

        col_indices = detect_columns(header, column_mapping)

        if config.debug.get('verbose'):
            print(f"Detected columns: {col_indices}")

        required = ['torrent_name', 'filename', 'infohash', 'file_index']
        missing = [col for col in required if col not in col_indices]
        if missing:
            print(f"Error: Missing required columns: {missing}")
            print(f"Available columns: {header}")
            return 0

        for row_num, row in enumerate(reader, start=2):
            if not row or all(cell.strip() == '' for cell in row):
                continue
            rec = parse_row(row, col_indices, config)
            if rec:
                records.append(rec)
            else:
                skipped_count += 1

    if config.debug.get('verbose'):
        print(f"Parsed {len(records)} candidate rows, skipped {skipped_count}")

    # ---- Pass 2: deduplicate by (round, session) ----
    deduplicate = config.output.get('deduplicate', True)
    dedup_mode = config.output.get('dedup_by', 'session')  # 'session' (default) or 'title'
    # Default is FIRST-OCCURRENCE-WINS. Set prefer_complete_torrents:true to
    # instead pick the most complete torrent for each session.
    prefer_complete = config.output.get('prefer_complete_torrents', False)

    torrent_stats = score_torrents(records) if prefer_complete else {}

    best_by_key: Dict[Tuple[str, str], Dict] = {}

    for rec in records:
        if deduplicate:
            if dedup_mode == 'title':
                # Legacy behaviour (NOT recommended): GP name via title.
                key = (rec['round_number'], _normalize_key(rec['formatted_title']))
            else:
                # Default, recommended: round + session only.
                key = (rec['round_number'], rec['session_key'])
        else:
            key = (rec['round_number'], f"{rec['infohash']}:{rec['file_index']}")

        existing = best_by_key.get(key)
        if existing is None:
            best_by_key[key] = rec
            continue

        if not deduplicate:
            continue

        if prefer_complete:
            new_rank = torrent_rank(rec['infohash'], torrent_stats)
            old_rank = torrent_rank(existing['infohash'], torrent_stats)
            if new_rank > old_rank:
                if config.debug.get('verbose'):
                    print(f"  Replacing source for {key}: "
                          f"{existing['infohash'][:8]} -> {rec['infohash'][:8]} "
                          f"(rank {old_rank} -> {new_rank})")
                best_by_key[key] = rec
            else:
                if config.debug.get('verbose'):
                    print(f"  Skipped duplicate session {key}: keeping "
                          f"{existing['infohash'][:8]}")
        else:
            # First-occurrence-wins: keep what we already have, drop this one.
            if config.debug.get('verbose'):
                print(f"  Skipped duplicate session {key}: keeping first "
                      f"({existing['infohash'][:8]}, dropped {rec['infohash'][:8]})")

    chosen = list(best_by_key.values())

    # ---- Pass 3: episode numbering ----
    def file_prefix_num(rec: Dict) -> int:
        m = re.match(r'^(\d+)[.\s]', rec['filename'])
        return int(m.group(1)) if m else 9999

    rounds: Dict[str, List[Dict]] = {}
    for rec in chosen:
        rounds.setdefault(rec['round_number'], []).append(rec)

    output_data: List[Dict] = []
    for round_number, recs in rounds.items():
        recs.sort(key=lambda r: (file_prefix_num(r), r['formatted_title']))

        try:
            season = int(round_number)
        except ValueError:
            season = 0

        thumbnail = thumbnail_map.get(round_number, '')

        for episode_idx, rec in enumerate(recs, start=1):
            output = {
                'series_id': config.series_id,
                'season': season,
                'episode': episode_idx,
                'title': rec['formatted_title'],
                'thumbnail': thumbnail,
                'infoHash': rec['infohash'],
                'fileIdx': rec['file_index'],
                'filename': rec['filename'],
            }
            if config.output.get('include_filesize', True):
                output['filesize'] = rec['filesize']
            if config.output.get('include_quality', False):
                output['quality'] = config.quality
            output_data.append(output)

    if config.debug.get('verbose'):
        print(f"After dedup: {len(output_data)} unique sessions across "
              f"{len(rounds)} rounds")

    # ---- Sort output ----
    sort_keys = config.output.get('sort_by', ['season', 'episode'])
    output_data.sort(key=lambda x: tuple(x.get(k, 0) for k in sort_keys))

    if dry_run:
        print(f"Dry run: would write {len(output_data)} entries to {output_path}")
        return len(output_data)

    # ---- Compare with existing & write if changed ----
    existing_data = []
    if output_path.exists():
        try:
            with open(output_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                existing_data = list(reader)
        except Exception:
            pass

    fieldnames = ['series_id', 'season', 'episode', 'title', 'thumbnail', 'infoHash', 'fileIdx']
    if config.output.get('include_filesize', True):
        fieldnames.append('filesize')
    if config.output.get('include_quality', False):
        fieldnames.append('quality')
    fieldnames.append('filename')

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

    config, raw_config = load_config()

    if args.verbose:
        config.debug['verbose'] = True

    if config.debug.get('verbose'):
        print(f"Series ID: {config.series_id}")
        print(f"Year: {config.year}")
        print(f"Sport: {config.sport}")
        print(f"Quality: {config.quality}")

    sport_config = load_sport_config(config)

    if config.debug.get('verbose'):
        calendar_count = len(sport_config.get('calendar', {}))
        print(f"Loaded {calendar_count} rounds from sport config")

    count = process_csv(config, sport_config, dry_run=args.dry_run)

    if count == 0:
        sys.exit(1)


if __name__ == '__main__':
    main()
