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
from datetime import datetime, timezone
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

    # Optional explicit session-key pattern
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
    """Load sport-specific configuration (countries, calendar, round_dates, etc.)."""
    config_file = config.data_paths.get('config_file')

    if not config_file:
        return {'countries': {}, 'calendar': {}, 'round_dates': {}}

    config_path = resolve_path(config_file)

    if not config_path.exists():
        if config.debug.get('verbose'):
            print(f"Warning: Sport config not found at {config_path}")
        return {'countries': {}, 'calendar': {}, 'round_dates': {}}

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"Warning: Invalid JSON in sport config: {e}")
        return {'countries': {}, 'calendar': {}, 'round_dates': {}}


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
        'filesize': ['filesize_gb', 'filesize', 'size', 'file_size', 'size_gb'],
        'timestamp': ['timestamp', 'time', 'date', 'release', 'released']
    }


# ============================================================================
# Timestamp Normalization & Date-Range Round Matching
# ============================================================================

def normalize_timestamp(ts_str: str, default_year: int = 2026) -> str:
    """
    Normalize any input timestamp string into Stremio/Nuvio ISO-8601 UTC format:
    YYYY-MM-DDTHH:MM:SS.000Z
    """
    if not ts_str or not ts_str.strip():
        return f"{default_year}-01-01T00:00:00.000Z"

    s = ts_str.strip()

    # Handle Unix epoch timestamps (seconds or milliseconds)
    if re.fullmatch(r'\d+(\.\d+)?', s):
        try:
            val = float(s)
            if val > 1e12:
                val /= 1000.0
            dt = datetime.fromtimestamp(val, tz=timezone.utc)
            return dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        except Exception:
            pass

    # Handle ISO-8601 strings (e.g., 2026-10-08T14:03:38+00:00 or ...Z)
    try:
        iso_candidate = s.replace('Z', '+00:00') if s.endswith('Z') else s
        dt = datetime.fromisoformat(iso_candidate)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc)
        else:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    except ValueError:
        pass

    # Fallback common date formats
    for fmt in (
        "%d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc)
            else:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        except ValueError:
            continue

    return f"{default_year}-01-01T00:00:00.000Z"


def lookup_round_by_date(ts_iso: str, sport_config: Dict) -> Optional[str]:
    """
    Match a normalized ISO-8601 timestamp ('YYYY-MM-DDTHH:MM:SS.000Z') against
    'round_dates' in sport_config (e.g., f1_config.json).
    """
    round_dates = sport_config.get('round_dates', {})
    if not round_dates or not ts_iso or ts_iso.endswith('-01-01T00:00:00.000Z'):
        return None

    try:
        file_date = datetime.strptime(ts_iso[:10], "%Y-%m-%d").date()
    except ValueError:
        return None

    for r_num, info in round_dates.items():
        if not isinstance(info, dict):
            continue
        start_s = info.get('start')
        end_s = info.get('end')
        if not start_s or not end_s:
            continue
        try:
            start_d = datetime.strptime(start_s, "%Y-%m-%d").date()
            end_d = datetime.strptime(end_s, "%Y-%m-%d").date()
            if start_d <= file_date <= end_d:
                return normalize_round(r_num)
        except ValueError:
            continue

    return None


# ============================================================================
# Text Processing & Session Ordering
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


def _strip_formula1_prefix(text: str) -> str:
    """
    Remove leading file index (e.g., '01.') and normalize 'Formula.1' / 'Formula 1'
    to 'F1' so regexes like `[x.](?:R)?(\\d+)\\.` NEVER accidentally match the `.1.`
    in `Formula.1.2026x84...`!
    """
    cleaned = re.sub(r'^\d+[.\s_]+', '', text)
    cleaned = re.sub(r'\bFormula[.\s_]+1\b', 'F1', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bFormula[.\s_]+One\b', 'F1', cleaned, flags=re.IGNORECASE)
    return cleaned


def extract_explicit_r_round(text: str) -> Optional[str]:
    """
    Return the round number string ONLY if text has an explicit R<num> or Round <num>
    marker (not YYYYxNN).
    """
    cleaned = _strip_formula1_prefix(text)
    m = re.search(r'(?:^|[.\s_])(?:R|Round[.\s_]*)(\d{1,2})(?=[.\s_]|$)', cleaned, re.IGNORECASE)
    if m:
        return m.group(1)
    return None


def extract_round_info(text: str, config: Config) -> Tuple[str, str]:
    """
    Extract round number and grand prix name from text (torrent name or filename).
    """
    round_number = 'Unknown'
    grand_prix_name = 'Unknown'

    cleaned_for_round = _strip_formula1_prefix(text)

    # 1. Explicit R<num> or Round <num> (e.g., R15, Round 15) takes highest priority
    explicit_r = extract_explicit_r_round(text)
    if explicit_r is not None:
        round_number = explicit_r
    else:
        # 2. Explicit <year>x<num> (e.g., 2026x84)
        m_explicit_x = re.search(r'\b\d{4}x(\d{1,3})\b', cleaned_for_round, re.IGNORECASE)
        if m_explicit_x:
            round_number = m_explicit_x.group(1)
        elif config._round_regex:
            # 3. Configured round_pattern on cleaned string (without Formula.1.)
            match = config._round_regex.search(cleaned_for_round)
            if match and match.groups():
                round_number = match.groups()[-1]

    if config._gp_regex:
        match = config._gp_regex.search(cleaned_for_round)
        if not match:
            match = config._gp_regex.search(text)
        if match:
            grand_prix_name = match.group(1)

    if grand_prix_name == 'Unknown' and config._fallback_gp_regex:
        match = config._fallback_gp_regex.search(cleaned_for_round)
        if not match:
            match = config._fallback_gp_regex.search(text)
        if match:
            grand_prix_name = match.group(1)

    # Also allow title_regex group(1) to supply GP name if title_regex has 2 groups (like in ss/ssf4)
    if grand_prix_name == 'Unknown' and config._title_regex:
        t_match = config._title_regex.search(text)
        if t_match and len(t_match.groups()) >= 2:
            grand_prix_name = t_match.group(1)

    grand_prix_name = clean_title(grand_prix_name, config.parsing.title_cleanup)

    return round_number, grand_prix_name


def normalize_round(round_number: str) -> str:
    """
    Normalize a round number to a canonical integer-string form.
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


_SESSION_TAIL_TOKENS = {
    'sky', 'skysports', 'skyf1hd', 'skyf1uhd', 'skyuhd', 'sports', 'f1',
    'f1tv', 'tv', 'uhd', 'hd', 'sd', 'fhd', 'uk', 'world', 'feed', 'multi',
    'web', 'webrip', 'webdl', '2160p', '1080p', '720p', '480p', '2160',
    '1080', '720', '480', '4k', 'h264', 'h265', 'x264', 'x265', 'hevc',
    'avc', 'aac', 'ac3', 'ddp', 'dd', 'mp4', 'mkv', 'ts', 'avi', 'ss',
}

_TAIL_JUNK_RE = re.compile(
    r'[.\s_\-]+'
    r'(?:sky\w*|f1\w*|sports?|uhd|hd|sd|fhd|tv|uk|world|feed|multi|web\w*|'
    r'\d{3,4}p|\d{3,4}|4k|h26[45]|x26[45]|hevc|avc|aac|ac3|ddp?|ss)'
    r'(?:[.\s_\-]+(?:sky\w*|f1\w*|sports?|uhd|hd|sd|fhd|tv|uk|world|feed|'
    r'multi|web\w*|\d{3,4}p|\d{3,4}|4k|h26[45]|x26[45]|hevc|avc|aac|ac3|ddp?|ss))*'
    r'$',
    flags=re.IGNORECASE
)


def derive_session_key(filename: str, config: Config, extracted_title: str = '') -> str:
    """Derive a stable, GP-name-independent session identity from a filename."""
    if config._session_key_regex:
        m = config._session_key_regex.search(filename)
        if m and m.groups():
            return _normalize_key(m.groups()[-1])

    name = filename
    name = re.sub(r'\.(mkv|mp4|avi|ts)$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'^\d+[.\s]+', '', name)

    gp_cut = re.search(r'grand[.\s]prix[.\s]+(.*)$', name, flags=re.IGNORECASE)
    if gp_cut:
        session_part = gp_cut.group(1)
    elif extracted_title and extracted_title != 'Unknown':
        session_part = extracted_title
    else:
        rcut = re.search(r'(?:^|[.\s])r\d+[.\s]+[a-z]+[.\s]+(.*)$', name, flags=re.IGNORECASE)
        if rcut:
            session_part = rcut.group(1)
        else:
            xcut = re.search(r'\d{4}x\d+[.\s]+(?:r\d+[.\s]+)?[a-z]+[.\s]+(.*)$', name, flags=re.IGNORECASE)
            session_part = xcut.group(1) if xcut else name

    session_part = _TAIL_JUNK_RE.sub('', session_part)

    tokens = re.split(r'[.\s_\-]+', session_part)
    while tokens and tokens[-1].lower() in _SESSION_TAIL_TOKENS:
        tokens.pop()

    session = ' '.join(tokens).strip()

    key = _normalize_key(session)
    if not key:
        key = _normalize_key(extracted_title or extract_title(filename, config))

    # Normalize common uploader typos so they deduplicate cleanly
    key = key.replace('team pricipal ', 'team principals ')
    key = key.replace('team principal ', 'team principals ')
    return key


# Canonical chronological order for a motorsport weekend
_CANONICAL_SESSION_ORDER = [
    ('weekend warm up', 10),
    ('drivers press conference', 15),
    ('driver press conference', 15),
    ('press conference', 18),
    ('paddock uncut', 20),
    ('free practice one', 30),
    ('free practice 1', 30),
    ('practice one', 30),
    ('practice 1', 30),
    ('fp1', 30),
    ('team principals press conference', 40),
    ('team principal press conference', 40),
    ('team pricipal press conference', 40),
    ('free practice two', 50),
    ('free practice 2', 50),
    ('practice two', 50),
    ('practice 2', 50),
    ('fp2', 50),
    ('pre sprint qualifying', 52),
    ('sprint qualifying pre', 52),
    ('sprint qualifying', 55),
    ('sprint shootout', 55),
    ('post sprint qualifying', 57),
    ('sprint qualifying post', 57),
    ('f1 show', 60),
    ('free practice three', 70),
    ('free practice 3', 70),
    ('practice three', 70),
    ('practice 3', 70),
    ('fp3', 70),
    ('pre sprint', 72),
    ('sprint pre', 72),
    ('sprint', 75),
    ('post sprint', 77),
    ('sprint post', 77),
    ('teds sprint notebook', 78),
    ('sprint notebook', 78),
    ('pre qualifying', 82),
    ('qualifying pre', 82),
    ('qualifying', 85),
    ('post qualifying', 88),
    ('qualifying post', 88),
    ('teds qualifying notebook', 90),
    ('qualifying notebook', 90),
    ('grand prix sunday', 95),
    ('pre race', 95),
    ('race pre', 95),
    ('build up', 95),
    ('race', 100),
    ('grand prix', 100),
    ('chequered flag', 105),
    ('post race', 105),
    ('race post', 105),
    ('teds notebook', 110),
    ('notebook', 110),
]


def canonical_session_rank(session_key: str) -> int:
    """Return a canonical weekend ordering rank for a session key."""
    s = session_key.lower().strip()
    for pattern, rank in _CANONICAL_SESSION_ORDER:
        if s == pattern:
            return rank
    for pattern, rank in sorted(_CANONICAL_SESSION_ORDER, key=lambda x: len(x[0]), reverse=True):
        if pattern in s:
            return rank
    return 500


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
    """Build a mapping from normalized round number to thumbnail URL."""
    countries = sport_config.get('countries', {})
    calendar = sport_config.get('calendar', {})

    out: Dict[str, str] = {}
    for round_num, country in calendar.items():
        out[normalize_round(round_num)] = countries.get(country, '')
    return out


# ============================================================================
# Smart Inference Mapping (Aliases + Calendar + Date Ranges)
# ============================================================================

# Built-in fallback aliases in case a sport config doesn't list round_dates aliases
_BUILTIN_F1_ALIASES: Dict[str, List[str]] = {
    'Australia': ['Australian', 'Melbourne', 'Albert Park'],
    'China': ['Chinese', 'Shanghai'],
    'Japan': ['Japanese', 'Suzuka'],
    'Canada': ['Canadian', 'Montreal'],
    'Monaco': ['Monte Carlo'],
    'Austria': ['Austrian', 'Spielberg', 'Red Bull Ring'],
    'United Kingdom': ['British', 'Great Britain', 'Silverstone', 'UK'],
    'Belgium': ['Belgian', 'Spa', 'Spa-Francorchamps'],
    'Hungary': ['Hungarian', 'Budapest', 'Hungaroring'],
    'Netherlands': ['Dutch', 'Zandvoort'],
    'Italy': ['Italian', 'Monza'],
    'Azerbaijan': ['Azerbaijani', 'Baku'],
    'Malaysia': ['Bahrain', 'Bahraini', 'Malaysian', 'Sepang', 'Sakhir'],
    'Singapore': ['Marina Bay'],
    'Mexico': ['Mexican', 'Mexico City'],
    'Brazil': ['Brazilian', 'Sao Paulo', 'Interlagos'],
    'Qatar': ['Qatari', 'Lusail', 'Losail'],
    'United Arab Emirates': ['Abu Dhabi', 'UAE', 'Yas Marina'],
}


def _gp_lookup_variants(gp: str) -> List[str]:
    """Generate normalized lookup variants for a Grand Prix / country / track name."""
    if not gp or gp == 'Unknown':
        return []
    # Split CamelCase (e.g. SaudiArabiaGP -> Saudi Arabia GP, DutchGP -> Dutch GP)
    s = re.sub(r'([a-z])([A-Z])', r'\1 \2', gp)
    s = s.lower()
    s = re.sub(r'[._\-]+', ' ', s)
    s = ' '.join(s.split()).strip()

    core = re.sub(r'\s*(?:grand\s+prix|gp)$', '', s).strip()
    core_nospace = core.replace(' ', '')

    variants = [
        s,
        core,
        core_nospace,
        f"{core} gp",
        f"{core}gp",
        f"{core_nospace}gp",
        f"{core} grand prix",
    ]
    return [v for v in dict.fromkeys(variants) if v]


def build_gp_to_round_map(
    sport_config: Dict,
    raw_rows: List[List[str]],
    col_indices: Dict[str, int],
    config: Config
) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    """
    Builds a mapping of Grand Prix names/aliases to Round numbers:
    Returns:
      - unambiguous_map: variant -> round_norm (when a name maps to a single round, or latest round)
      - multi_map: variant -> list of round_norms (tracks ambiguous names like 'Spain' or 'United States')
    """
    multi_map: Dict[str, List[str]] = {}

    def register_name(gp: str, r_norm: str):
        if not gp or gp == 'Unknown' or not r_norm or r_norm in ('Unknown', '0'):
            return
        for variant in _gp_lookup_variants(gp):
            rounds_list = multi_map.setdefault(variant, [])
            if r_norm not in rounds_list:
                rounds_list.append(r_norm)

    # 1. Seed from sport_config['round_dates'] aliases (most specific)
    round_dates = sport_config.get('round_dates', {})
    for r_num, info in round_dates.items():
        r_norm = normalize_round(r_num)
        if isinstance(info, dict):
            for alias in info.get('aliases', []):
                register_name(alias, r_norm)

    # 2. Seed from sport_config['calendar'] + built-in demonym aliases
    calendar = sport_config.get('calendar', {})
    for r_num, country_name in calendar.items():
        r_norm = normalize_round(r_num)
        if r_norm == '0':
            continue
        gp_clean = clean_title(country_name, config.parsing.title_cleanup)
        register_name(gp_clean, r_norm)
        for builtin_alias in _BUILTIN_F1_ALIASES.get(country_name, []):
            register_name(builtin_alias, r_norm)

    # 3. Seed from current CSV data ONLY when the row has an explicit R<num>
    for row in raw_rows:
        if 'filename' not in col_indices or 'torrent_name' not in col_indices:
            continue
        if col_indices['filename'] >= len(row) or col_indices['torrent_name'] >= len(row):
            continue

        filename = row[col_indices['filename']].strip()
        torrent_name = row[col_indices['torrent_name']].strip()
        actual_filename = extract_filename(filename)

        explicit_r = extract_explicit_r_round(actual_filename) or extract_explicit_r_round(torrent_name)
        if not explicit_r:
            continue

        _, gp_name = extract_round_info(actual_filename, config)
        if gp_name == 'Unknown':
            _, gp_name = extract_round_info(torrent_name, config)

        r_norm = normalize_round(explicit_r)
        if r_norm != 'Unknown':
            try:
                if 0 < int(r_norm) <= 30:
                    register_name(gp_name, r_norm)
            except ValueError:
                pass

    unambiguous_map: Dict[str, str] = {}
    for variant, rounds_list in multi_map.items():
        if len(rounds_list) == 1:
            unambiguous_map[variant] = rounds_list[0]

    return unambiguous_map, multi_map


def resolve_round_number(
    actual_filename: str,
    torrent_name: str,
    grand_prix_name: str,
    timestamp_iso: str,
    extracted_round_norm: str,
    unambiguous_gp_map: Dict[str, str],
    multi_gp_map: Dict[str, List[str]],
    sport_config: Dict,
) -> str:
    """
    Resolve the true motorsport Round number using a 4-step priority hierarchy:
      1. Explicit R<num> / Round <num> in the inner filename (e.g. R15) -> authoritative.
      2. Unambiguous GP name/alias match in f1_config (e.g. 'Bahrain' -> 16, 'DutchGP' -> 12).
      3. Date-range match in f1_config['round_dates'] using the file's timestamp
         (resolves missing round numbers, YYYYxNN files, and multi-race countries like Spain/USA).
      4. Explicit R<num> in the outer torrent name, or fallback to multi_gp_map / extracted round.
    """
    # Step 1: Explicit R<num> in the inner video filename itself
    file_explicit_r = extract_explicit_r_round(actual_filename)
    if file_explicit_r is not None:
        r_norm = normalize_round(file_explicit_r)
        if r_norm != 'Unknown' and int(r_norm) <= 30:
            return r_norm

    # Step 2: Check GP name against unambiguous aliases & check timestamp against round_dates
    date_round = lookup_round_by_date(timestamp_iso, sport_config)

    if grand_prix_name and grand_prix_name != 'Unknown':
        for variant in _gp_lookup_variants(grand_prix_name):
            # If the GP name maps to a single unique round in the season (e.g. Bahrain -> 16, Singapore -> 17)
            if variant in unambiguous_gp_map:
                return unambiguous_gp_map[variant]
            # If the GP name maps to multiple rounds (e.g. Spain -> 7 or 14, United States -> 4, 18, 21)
            if variant in multi_gp_map:
                candidate_rounds = multi_gp_map[variant]
                if date_round and date_round in candidate_rounds:
                    return date_round
                return candidate_rounds[-1]

    # Step 3: If no explicit R<num> in filename and GP name didn't uniquely resolve it, use date range!
    if date_round:
        return date_round

    # Step 4: Fallback to explicit R<num> in torrent_name (if reasonable)
    torrent_explicit_r = extract_explicit_r_round(torrent_name)
    if torrent_explicit_r is not None:
        r_norm = normalize_round(torrent_explicit_r)
        if r_norm != 'Unknown' and int(r_norm) <= 30:
            return r_norm

    # Final fallback: only use extracted_round_norm if it's <= 30
    if extracted_round_norm != 'Unknown':
        try:
            if 0 < int(extracted_round_norm) <= 30:
                return extracted_round_norm
        except ValueError:
            pass

    return 'Unknown'


# ============================================================================
# Row parsing (pass 1)
# ============================================================================

def parse_row(
    row: List[str],
    col_indices: Dict[str, int],
    config: Config,
    unambiguous_gp_map: Dict[str, str],
    multi_gp_map: Dict[str, List[str]],
    sport_config: Dict,
) -> Optional[Dict]:
    """Parse a single CSV row into an intermediate record dict."""
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

    raw_timestamp = ''
    if 'timestamp' in col_indices and col_indices['timestamp'] < len(row):
        raw_timestamp = row[col_indices['timestamp']].strip()
    timestamp = normalize_timestamp(raw_timestamp, default_year=config.year)

    actual_filename = extract_filename(filename)

    file_ext = os.path.splitext(actual_filename)[1].lower()
    if file_ext not in {'.mkv', '.mp4', '.avi', '.ts'}:
        if config.debug.get('log_skipped_rows'):
            print(f"  Skipped (non-video): {actual_filename}")
        return None

    # Extract raw round/GP from filename first, then fallback to torrent_name
    round_number_raw, grand_prix_name = extract_round_info(actual_filename, config)
    if round_number_raw == 'Unknown' or grand_prix_name == 'Unknown':
        t_round, t_gp = extract_round_info(torrent_name, config)
        if round_number_raw == 'Unknown':
            round_number_raw = t_round
        if grand_prix_name == 'Unknown':
            grand_prix_name = t_gp

    extracted_round_norm = normalize_round(round_number_raw)

    # Resolve authoritative round number via Explicit R<num> -> GP Alias -> Date Range
    round_number = resolve_round_number(
        actual_filename=actual_filename,
        torrent_name=torrent_name,
        grand_prix_name=grand_prix_name,
        timestamp_iso=timestamp,
        extracted_round_norm=extracted_round_norm,
        unambiguous_gp_map=unambiguous_gp_map,
        multi_gp_map=multi_gp_map,
        sport_config=sport_config,
    )

    if round_number == 'Unknown' and config.output.get('deduplicate', True):
        if config.debug.get('log_skipped_rows'):
            print(f"  Skipped (unknown round): {torrent_name}")
        return None

    title = extract_title(actual_filename, config)
    if title == 'Unknown':
        if config.debug.get('log_skipped_rows'):
            print(f"  Skipped (no title match): {actual_filename}")
        return None

    session_key = derive_session_key(actual_filename, config, extracted_title=title)
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
        'timestamp': timestamp,
    }


# ============================================================================
# Torrent completeness scoring
# ============================================================================

def score_torrents(records: List[Dict]) -> Dict[str, Dict]:
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
    input_path = resolve_path(config.data_paths.get('input_csv', 'content.csv'))
    output_path = resolve_path(config.data_paths.get('output_csv', '6processed.csv'))

    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        return 0

    column_mapping = get_default_column_mapping()
    if config.column_mapping:
        column_mapping.update(config.column_mapping)

    thumbnail_map = build_thumbnail_map(sport_config)

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
            return 0

        raw_rows = []
        for row_num, row in enumerate(reader, start=2):
            if not row or all(cell.strip() == '' for cell in row):
                continue
            raw_rows.append(row)

        unambiguous_gp_map, multi_gp_map = build_gp_to_round_map(
            sport_config, raw_rows, col_indices, config
        )

        for row in raw_rows:
            rec = parse_row(
                row, col_indices, config, unambiguous_gp_map, multi_gp_map, sport_config
            )
            if rec:
                records.append(rec)
            else:
                skipped_count += 1

    if config.debug.get('verbose'):
        print(f"Parsed {len(records)} candidate rows, skipped {skipped_count}")

    deduplicate = config.output.get('deduplicate', True)
    dedup_mode = config.output.get('dedup_by', 'session')
    prefer_complete = config.output.get('prefer_complete_torrents', True)

    torrent_stats = score_torrents(records) if prefer_complete else {}
    best_by_key: Dict[Tuple[str, str], Dict] = {}

    for rec in records:
        if deduplicate:
            if dedup_mode == 'title':
                key = (rec['round_number'], _normalize_key(rec['formatted_title']))
            else:
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
                          f"{existing['infohash'][:8]} (dropped {rec['infohash'][:8]})")
        else:
            if config.debug.get('verbose'):
                print(f"  Skipped duplicate session {key}: keeping first "
                      f"({existing['infohash'][:8]}, dropped {rec['infohash'][:8]})")

    chosen = list(best_by_key.values())

    # ---- Pass 3: episode numbering ----
    def leading_file_num(rec: Dict) -> int:
        """Match leading file number (e.g., '01.F1...'), else 9999."""
        m = re.match(r'^(\d+)[.\s_\-]', rec['filename'])
        return int(m.group(1)) if m else 9999

    def x_seq_num(rec: Dict) -> int:
        """Match ShowSplitter-style '2026x91' sequence counter as tie-breaker."""
        mx = re.search(r'\b\d{4}x(\d+)\b', rec['filename'], re.IGNORECASE)
        return int(mx.group(1)) if mx else 9999

    rounds: Dict[str, List[Dict]] = {}
    for rec in chosen:
        rounds.setdefault(rec['round_number'], []).append(rec)

    output_data: List[Dict] = []
    for round_number, recs in rounds.items():
        # Sort order within each round:
        # 1. Leading file index prefix (01..11) if present
        # 2. Canonical motorsport weekend order (Pre Quali -> Quali -> Post Quali -> Pre Race -> Race -> Post Race)
        # 3. YYYYxNN sequence number (if present)
        # 4. Timestamp & file_index
        recs.sort(key=lambda r: (
            leading_file_num(r),
            canonical_session_rank(r['session_key']),
            x_seq_num(r),
            r.get('timestamp', ''),
            r.get('file_index', 0),
            r['formatted_title']
        ))

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
                'timestamp': rec['timestamp']
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

    fieldnames = ['series_id', 'season', 'episode', 'title', 'thumbnail', 'infoHash', 'fileIdx', 'timestamp']
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