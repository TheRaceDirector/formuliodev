import csv
import logging
import os
import subprocess
import sys
import time
import json
import base64
import requests
import signal
import urllib.parse
import hashlib
import threading
from threading import Thread
from flask import Flask, jsonify, abort, send_from_directory, request, redirect
from werkzeug.middleware.proxy_fix import ProxyFix
from time import time as now


# ═══════════════════════════════════════════════════════════════════════════
# Logging — console only, no file
# ═══════════════════════════════════════════════════════════════════════════

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


# ═══════════════════════════════════════════════════════════════════════════
# Detect Python executable (fixes WinError 2 on Windows)
# ═══════════════════════════════════════════════════════════════════════════

PYTHON_EXE = sys.executable  # Use the same Python that's running this script


# ═══════════════════════════════════════════════════════════════════════════
# Simple TTL cache for RD resolutions
# ═══════════════════════════════════════════════════════════════════════════

_rd_cache = {}
_rd_cache_lock = threading.Lock()


def rd_cache_get(cache_key):
    """Get cached RD result if still valid. Success: 5 min, Failure: 60s."""
    with _rd_cache_lock:
        entry = _rd_cache.get(cache_key)
        if not entry:
            return None
        ttl = 60 if entry['result'] == '__UNAVAILABLE__' else 300
        if now() - entry['time'] < ttl:
            return entry['result']
        del _rd_cache[cache_key]
        return None


def rd_cache_set(cache_key, result):
    """Cache an RD result."""
    with _rd_cache_lock:
        # Evict expired entries if cache is large
        if len(_rd_cache) > 1000:
            cutoff = now() - 300
            expired = [k for k, v in _rd_cache.items() if v['time'] < cutoff]
            for k in expired:
                del _rd_cache[k]
        _rd_cache[cache_key] = {'result': result, 'time': now()}


# ═══════════════════════════════════════════════════════════════════════════
# Config
# ═══════════════════════════════════════════════════════════════════════════

class Config:
    SCRIPT_INTERVAL = 909  # seconds between script runs (~15 min)
    TORBOX_API_BASE = 'https://api.torbox.app'
    RD_API_BASE = 'https://api.real-debrid.com/rest/1.0'


config = Config()

app = Flask(__name__, static_folder='static')
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)


# ═══════════════════════════════════════════════════════════════════════════
# TorBox Helper Functions
# ═══════════════════════════════════════════════════════════════════════════

def torbox_check_cached(api_key, info_hashes):
    """Check which hashes are cached on TorBox. Returns dict {hash: bool}."""
    if not info_hashes:
        return {}
    url = f"{config.TORBOX_API_BASE}/v1/api/torrents/checkcached"
    headers = {'Authorization': f'Bearer {api_key}'}
    unique_hashes = list(set(info_hashes))
    params = [('hash', h) for h in unique_hashes]
    params.append(('format', 'object'))
    params.append(('list_files', 'true'))
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get('success') and data.get('data'):
            cached_data = data['data']
            return {h: bool(cached_data.get(h) and len(cached_data[h]) > 0) for h in unique_hashes}
        return {h: False for h in unique_hashes}
    except Exception as e:
        logger.error(f"TorBox check cached error: {e}")
        return {h: False for h in unique_hashes}


def torbox_create_torrent(api_key, info_hash):
    """Create a torrent on TorBox. Returns torrent_id or None."""
    url = f"{config.TORBOX_API_BASE}/v1/api/torrents/createtorrent"
    headers = {'Authorization': f'Bearer {api_key}'}
    magnet = f"magnet:?xt=urn:btih:{info_hash}"
    try:
        resp = requests.post(url, headers=headers, data={
            'magnet': magnet, 'seed': 1, 'allow_zip': 'true', 'as_queued': 'false'
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get('success') and data.get('data'):
            return data['data'].get('torrent_id')
        return None
    except Exception as e:
        logger.error(f"TorBox create torrent error: {e}")
        return None


def torbox_find_torrent_by_hash(api_key, info_hash):
    """Find torrent in user's list by hash. Returns torrent_id or None."""
    url = f"{config.TORBOX_API_BASE}/v1/api/torrents/mylist"
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        resp = requests.get(url, headers=headers, params={'bypass_cache': 'true'}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get('success') and data.get('data'):
            for torrent in data['data']:
                if torrent.get('hash', '').lower() == info_hash.lower():
                    return torrent.get('id')
        return None
    except Exception as e:
        logger.error(f"TorBox find torrent error: {e}")
        return None


def torbox_get_download_link(api_key, torrent_id, file_idx=None, user_ip=None):
    """Get download link for a torrent file. Returns URL string or None."""
    url = f"{config.TORBOX_API_BASE}/v1/api/torrents/requestdl"
    params = {
        'token': api_key, 'torrent_id': torrent_id,
        'file_id': file_idx if file_idx is not None else 0,
        'zip_link': 'false', 'redirect': 'false'
    }
    if user_ip:
        params['user_ip'] = user_ip
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get('success') and data.get('data'):
            return data['data']
        return None
    except Exception as e:
        logger.error(f"TorBox request download error: {e}")
        return None


def torbox_get_torrent_info(api_key, torrent_id):
    """Get torrent info including file list. Returns dict or None."""
    url = f"{config.TORBOX_API_BASE}/v1/api/torrents/mylist"
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        resp = requests.get(url, headers=headers, params={
            'id': torrent_id, 'bypass_cache': 'true'
        }, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get('success') and data.get('data'):
            return data['data']
        return None
    except Exception as e:
        logger.error(f"TorBox get torrent info error: {e}")
        return None


def torbox_get_stream_url(api_key, info_hash, file_idx, filename, user_ip=None):
    """Full flow: find/create torrent -> get download URL. Returns URL or None."""
    try:
        torrent_id = torbox_find_torrent_by_hash(api_key, info_hash)
        if not torrent_id:
            torrent_id = torbox_create_torrent(api_key, info_hash)
        if not torrent_id:
            return None

        tb_file_id = 0
        torrent_data = torbox_get_torrent_info(api_key, torrent_id)
        if torrent_data and 'files' in torrent_data:
            for f in torrent_data['files']:
                if filename and filename in f.get('name', ''):
                    tb_file_id = f.get('id', 0)
                    break
            else:
                if file_idx is not None and file_idx < len(torrent_data['files']):
                    tb_file_id = torrent_data['files'][file_idx].get('id', 0)

        return torbox_get_download_link(api_key, torrent_id, tb_file_id, user_ip=user_ip)
    except Exception as e:
        logger.error(f"TorBox stream URL error for {info_hash}: {e}")
        return None


def torbox_validate_key(api_key):
    """Validate a TorBox API key. Returns user info dict or None."""
    url = f"{config.TORBOX_API_BASE}/v1/api/user/me"
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        resp = requests.get(url, headers=headers, timeout=8)
        resp.raise_for_status()
        data = resp.json()
        if data.get('success') and data.get('data'):
            return data['data']
        return None
    except Exception as e:
        logger.error(f"TorBox validate key error: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════
# Real-Debrid Helper Functions
# ═══════════════════════════════════════════════════════════════════════════

def rd_add_magnet(api_key, info_hash):
    """Add magnet to RD. Returns torrent ID or None."""
    url = f"{config.RD_API_BASE}/torrents/addMagnet"
    headers = {'Authorization': f'Bearer {api_key}'}
    magnet = f"magnet:?xt=urn:btih:{info_hash}"
    try:
        resp = requests.post(url, headers=headers, data={'magnet': magnet}, timeout=15)
        resp.raise_for_status()
        return resp.json().get('id')
    except Exception as e:
        logger.error(f"RD add magnet error: {e}")
        return None


def rd_get_torrent_info(api_key, torrent_id):
    """Get torrent info from RD. Returns dict or None."""
    url = f"{config.RD_API_BASE}/torrents/info/{torrent_id}"
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"RD get torrent info error: {e}")
        return None


def rd_select_files(api_key, torrent_id, file_ids="all"):
    """Select files for a torrent on RD. Returns True on success."""
    url = f"{config.RD_API_BASE}/torrents/selectFiles/{torrent_id}"
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        resp = requests.post(url, headers=headers, data={'files': file_ids}, timeout=10)
        return resp.status_code in (200, 202, 204)
    except Exception as e:
        logger.error(f"RD select files error: {e}")
        return False


def rd_unrestrict_link(api_key, link, user_ip=None):
    """Unrestrict a link on RD. Returns download URL or None."""
    url = f"{config.RD_API_BASE}/unrestrict/link"
    headers = {'Authorization': f'Bearer {api_key}'}
    data = {'link': link, 'remote': 0}
    try:
        resp = requests.post(url, headers=headers, data=data, timeout=15)
        if resp.status_code == 403:
            logger.error(f"RD unrestrict 403 — link: {link}, response: {resp.text}")
            return None
        resp.raise_for_status()
        return resp.json().get('download')
    except Exception as e:
        logger.error(f"RD unrestrict link error: {e}")
        return None


def rd_find_torrent_by_hash(api_key, info_hash):
    """Find existing torrent in user's RD list by hash. Returns torrent dict or None."""
    url = f"{config.RD_API_BASE}/torrents"
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        resp = requests.get(url, headers=headers, params={'limit': 2500}, timeout=10)
        resp.raise_for_status()
        for torrent in resp.json():
            if torrent.get('hash', '').lower() == info_hash.lower():
                return torrent
        return None
    except Exception as e:
        logger.error(f"RD find torrent error: {e}")
        return None


def rd_get_stream_url(api_key, info_hash, file_idx, filename, user_ip=None):
    """Full RD flow. Returns a direct download URL string, or None if not ready."""
    try:
        existing = rd_find_torrent_by_hash(api_key, info_hash)

        if existing:
            torrent_id = existing['id']
            status = existing.get('status', '')
            logger.info(f"RD torrent {info_hash[:8]} found, status={status}")

            if status == 'waiting_files_selection':
                rd_select_files(api_key, torrent_id, 'all')
                return None

            elif status == 'downloaded':
                info = rd_get_torrent_info(api_key, torrent_id)
                if not info:
                    return None
                links = info.get('links', [])
                if not links:
                    logger.info(f"RD torrent {info_hash[:8]} downloaded but no links yet")
                    return None
                return _pick_and_unrestrict(api_key, info, links, file_idx, filename, user_ip)

            elif status in ('downloading', 'queued', 'magnet_conversion', 'compressing', 'uploading'):
                logger.info(f"RD torrent {info_hash[:8]} in progress ({status}), not ready")
                return None

            else:
                logger.warning(f"RD torrent {info_hash[:8]} unusable status={status}")
                return None

        else:
            logger.info(f"RD torrent {info_hash[:8]} not in library, adding...")
            torrent_id = rd_add_magnet(api_key, info_hash)
            if not torrent_id:
                logger.error(f"RD failed to add magnet for {info_hash[:8]}")
                return None

            time.sleep(2)

            info = rd_get_torrent_info(api_key, torrent_id)
            if info and info.get('status') == 'waiting_files_selection':
                rd_select_files(api_key, torrent_id, 'all')
            elif info and info.get('status') == 'downloaded':
                links = info.get('links', [])
                if links:
                    return _pick_and_unrestrict(api_key, info, links, file_idx, filename, user_ip)
            time.sleep(2)

            info = rd_get_torrent_info(api_key, torrent_id)
            if info and info.get('status') == 'waiting_files_selection':
                rd_select_files(api_key, torrent_id, 'all')
            elif info and info.get('status') == 'downloaded':
                links = info.get('links', [])
                if links:
                    return _pick_and_unrestrict(api_key, info, links, file_idx, filename, user_ip)

            logger.info(f"RD torrent {info_hash[:8]} added and queued, not ready yet")
            return None

    except Exception as e:
        logger.error(f"RD stream URL error for {info_hash}: {e}")
        return None


def _pick_and_unrestrict(api_key, info, links, file_idx, filename, user_ip):
    """Pick the right link from a downloaded torrent and unrestrict it."""
    files = info.get('files', [])
    selected_files = [f for f in files if f.get('selected') == 1]

    link_to_use = links[0]

    if selected_files and len(links) == len(selected_files):
        if filename:
            for i, f in enumerate(selected_files):
                if filename in f.get('path', ''):
                    link_to_use = links[i]
                    logger.info(f"RD matched file by name: {f.get('path')}")
                    break
        elif file_idx is not None:
            for i, f in enumerate(selected_files):
                if f.get('id') == file_idx + 1:
                    link_to_use = links[i]
                    logger.info(f"RD matched file by idx {file_idx}")
                    break
    elif file_idx is not None and file_idx < len(links):
        link_to_use = links[file_idx]

    return rd_unrestrict_link(api_key, link_to_use, user_ip=user_ip)


def rd_validate_key(api_key):
    """Validate an RD API token. Returns user info dict or None."""
    url = f"{config.RD_API_BASE}/user"
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        resp = requests.get(url, headers=headers, timeout=8)
        if resp.status_code in (401, 403):
            return None
        resp.raise_for_status()
        data = resp.json()
        return data if data.get('username') else None
    except Exception as e:
        logger.error(f"RD validate key error: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════
# Config Parsing & User IP
# ═══════════════════════════════════════════════════════════════════════════

def parse_config(config_str):
    """Parse base64-encoded config from URL path."""
    if not config_str:
        return {'debrid': {}, 'enableP2P': True}
    try:
        padding = 4 - len(config_str) % 4
        if padding != 4:
            config_str += '=' * padding
        decoded = base64.b64decode(config_str).decode('utf-8')
        cfg = json.loads(decoded)
        return {
            'debrid': cfg.get('debrid', {}),
            'enableP2P': cfg.get('enableP2P', True)
        }
    except Exception as e:
        logger.error(f"Config parse error: {e}")
        return {'debrid': {}, 'enableP2P': True}


def get_user_ip():
    """Get the user's real IP from the request (behind proxy)."""
    return request.remote_addr


# ═══════════════════════════════════════════════════════════════════════════
# Manifest & Catalog Data
# ═══════════════════════════════════════════════════════════════════════════

MANIFEST = {
    'id': 'org.stremio.formulio',
    'version': '3.0.0',
    'name': 'Formulio',
    'description': (
        'An Addon for Motor Racing Replay Content with Debrid support. '
        '(This addon only displays content from external sources. '
        'Users are responsible for complying with all applicable laws in their jurisdiction)'
    ),
    'logo': 'https://i.postimg.cc/5tTmz4jb/formulio1.png',
    'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
    'behaviorHints': {
        'configurable': True,
        'configurationRequired': False,
    },
    'types': ['series'],
    'catalogs': [
        {
            'type': 'series',
            'id': 'formulio-series',
            'name': 'Formulio',
            'extra': [
                {'name': 'search', 'isRequired': False},
                {'name': 'genre', 'options': ['Formula Racing', 'Moto Racing'], 'isRequired': False}
            ]
        }
    ],
    'resources': [
        'catalog',
        {'name': 'meta', 'types': ['series'], 'idPrefixes': ['hpy']},
        {'name': 'stream', 'types': ['series'], 'idPrefixes': ['hpy']}
    ]
}

# Each catalog entry: id, name, metadata, and the CSV file that holds its videos
CATALOG = {
    'series': [
            {
            'id': 'hpytt0202605', 'name': 'Sky F1 - 4K',
            'description': 'If you get a StremThru error, use the P2P source\nformulio@tuta.io',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/zfPNXN1H/sky14k.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'], 'videos': [],
            'videoFile': './egor/eg4/6processed.csv'
        },
        {
            'id': 'hpytt0202601', 'name': 'Sky F1 - FHD',
            'description': 'If you get a StremThru error, use the P2P source\nformulio@tuta.io',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/g2d9tyXS/sky1.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'], 'videos': [],
            'videoFile': './egor/ego/6processed.csv'
        },
        {
            'id': 'hpytt0202603', 'name': 'F1TV - English',
            'description': 'If you get a StremThru error, use the P2P source\nformulio@tuta.io',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/pXf4j9GD/f1tveng.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'], 'videos': [],
            'videoFile': './ss/ssf/6processed.csv'
        },
        {
            'id': 'hpytt0202604', 'name': 'F1TV - International',
            'description': 'If you get a StremThru error, use the P2P source\nformulio@tuta.io',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/1zjjSDXZ/f1tvint.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'], 'videos': [],
            'videoFile': './ss/ssm/6processed.csv'
        },
        {
            'id': 'hpytt0202606', 'name': 'Sky F1 - 4K (2)',
            'description': 'If you get a StremThru error, use the P2P source\nformulio@tuta.io',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/ry4Tc7Zz/sky24k.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'], 'videos': [],
            'videoFile': './smcg/sm4/6processed.csv'
        },
        {
            'id': 'hpytt0202602', 'name': 'Sky F1 - FHD (2)',
            'description': 'If you get a StremThru error, use the P2P source\nformulio@tuta.io',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/KYMnKTQb/sky2.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'], 'videos': [],
            'videoFile': './smcg/smc/6processed.csv'
        },
        {
            'id': 'hpytt0202612', 'name': 'MotoGP - 4K',
            'description': 'If you get a StremThru error, use the P2P source\nformulio@tuta.io',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/MHmvsGDg/motogp4k.jpg',
            'logo': 'https://i.postimg.cc/nh8PKc5n/moto.png',
            'background': 'https://i.postimg.cc/fR252zq3/motobackground.jpg',
            'genres': ['Moto Racing'], 'videos': [],
            'videoFile': './smcm/sm4/6processed.csv'
        },
        {
            'id': 'hpytt0202611', 'name': 'MotoGP - FHD',
            'description': 'If you get a StremThru error, use the P2P source\nformulio@tuta.io',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/3Rpyv1D8/motogphd.jpg',
            'logo': 'https://i.postimg.cc/nh8PKc5n/moto.png',
            'background': 'https://i.postimg.cc/fR252zq3/motobackground.jpg',
            'genres': ['Moto Racing'], 'videos': [],
            'videoFile': './smcm/smc/6processed.csv'
        },
        {
            'id': 'hpytt0202607', 'name': 'Sky F1 - SD',
            'description': 'This is Low Quality SD\nformulio@tuta.io',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/Pqcn5Vvx/sky2sd.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'], 'videos': [],
            'videoFile': './smcg/sms/6processed.csv'
        }
    ]
}

OPTIONAL_META = [
    "posterShape", "background", "logo", "videos", "description",
    "releaseInfo", "imdbRating", "director", "cast", "dvdRelease",
    "released", "inTheaters", "certification", "runtime", "language",
    "country", "awards", "website", "isPeered"
]


# ═══════════════════════════════════════════════════════════════════════════
# Video / CSV Loading
# ═══════════════════════════════════════════════════════════════════════════

def respond_with(data):
    resp = jsonify(data)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Headers'] = '*'
    return resp


def load_videos(filepath):
    """Load video entries from a processed CSV file."""
    videos = []
    try:
        with open(filepath, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                video_obj = {
                    'id': row['series_id'].strip(),
                    'season': int(row['season'].strip()),
                    'episode': int(row['episode'].strip()),
                    'title': row['title'].strip(),
                    'thumbnail': row['thumbnail'].strip(),
                    'infoHash': row['infoHash'].strip(),
                }
                file_idx = row.get('fileIdx', '').strip()
                if file_idx:
                    video_obj['fileIdx'] = int(file_idx)
                filesize = row.get('filesize', '').strip()
                if filesize:
                    video_obj['filesize'] = filesize
                quality = row.get('quality', '').strip()
                if quality:
                    video_obj['quality'] = quality
                filename = row.get('filename', '').strip()
                if filename:
                    video_obj['filename'] = filename
                videos.append(video_obj)
        videos.sort(key=lambda v: (v['season'], v['episode']))
    except FileNotFoundError:
        logger.warning(f"CSV not found: {filepath}")
    except (ValueError, KeyError) as e:
        logger.error(f"Error parsing {filepath}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error reading {filepath}: {e}")
    return videos


def load_all_videos():
    """Reload all video data from CSV files into the catalog."""
    for series in CATALOG['series']:
        video_file = series.get('videoFile')
        if video_file:
            series['videos'] = load_videos(video_file)
            logger.info(f"Loaded {len(series['videos'])} videos for '{series['name']}' from {video_file}")


# ═══════════════════════════════════════════════════════════════════════════
# Background Script Runner
#
# Two types of directories:
#   1. FEED_DIRECTORIES  — contain 1formationlap.py that fetches RSS → f1db.csv
#   2. PIPELINE_DIRECTORIES — contain 1formationlap.py that processes → 6processed.csv
#
# On startup: run ALL scripts once (feeds first, then pipelines).
# On loop: repeat every SCRIPT_INTERVAL seconds.
# ═══════════════════════════════════════════════════════════════════════════

# Parent dirs: RSS feed fetchers (produce/update f1db.csv)
FEED_DIRECTORIES = [
    'egor',
    'smcg',
    'ss',
    'smcm',
]

# Child dirs: processing pipelines (produce 6processed.csv)
PIPELINE_DIRECTORIES = [
    'egor/ego',
    'egor/eg4',
    'smcg/smc',
    'smcg/sm4',
    'smcg/sms',
    'ss/ssf',
    'ss/ssm',
    'smcm/smc',
    'smcm/sm4',
]


def run_script(directory):
    """
    Run 1formationlap.py in the given directory.
    Returns True if it ran successfully.
    """
    script_path = os.path.join(directory, '1formationlap.py')
    if not os.path.isfile(script_path):
        logger.warning(f"Script not found, skipping: {script_path}")
        return False
    
    # Determine timeout: feed scripts are fast (300s), pipeline scripts need more time
    # because 5torrenttocontent.py resolves magnet links from the network
    is_pipeline = '/' in directory or '\\' in directory  # child dirs have path separators
    timeout = 1200 if is_pipeline else 300  # 20 min for pipelines, 5 min for feeds
    
    try:
        logger.info(f"Running: {script_path} (timeout: {timeout}s)")
        result = subprocess.run(
            [PYTHON_EXE, script_path],
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.stdout.strip():
            last_lines = result.stdout.strip().split('\n')[-3:]
            for line in last_lines:
                logger.info(f"  [{directory}] {line}")
        return True
    except subprocess.TimeoutExpired:
        logger.error(f"Script {script_path} timed out after {timeout}s")
        return False
    except subprocess.CalledProcessError as e:
        stderr_text = (e.stderr or '').strip()
        if stderr_text:
            logger.error(f"Script {script_path} failed (exit {e.returncode}):\n{stderr_text[:1000]}")
        else:
            logger.error(f"Script {script_path} failed (exit {e.returncode}), no stderr output")
        return False
    except Exception as e:
        logger.error(f"Error launching {script_path}: {e}")
        return False


def run_pipeline_and_reload(directory):
    """
    Run a single pipeline script and immediately reload videos if its CSV changed.
    Returns True if the CSV was updated.
    """
    csv_path = os.path.join(directory, '6processed.csv')
    mtime_before = os.path.getmtime(csv_path) if os.path.exists(csv_path) else 0

    success = run_script(directory)

    if success:
        mtime_after = os.path.getmtime(csv_path) if os.path.exists(csv_path) else 0
        if mtime_after != mtime_before:
            logger.info(f"CSV updated: {csv_path} — reloading videos immediately")
            load_all_videos()
            return True
    return False


def run_scripts_in_loop():
    """
    Background thread:
      1. Run feed scripts (RSS fetchers) in parallel — updates f1db.csv in parent dirs
      2. Run pipeline scripts in parallel — updates 6processed.csv in child dirs,
         reloading video data immediately after each pipeline that produces a change
      3. Sleep and repeat
    """
    logger.info(f"Script loop starting (Python: {PYTHON_EXE})")

    while True:
        # Step 1: Run all feed fetchers in parallel and wait for all to finish
        # before starting pipelines (pipelines depend on the feed output)
        logger.info("--- Running feed fetchers (parallel) ---")
        feed_threads = []
        for directory in FEED_DIRECTORIES:
            t = Thread(target=run_script, args=(directory,), daemon=True)
            t.start()
            feed_threads.append(t)
        for t in feed_threads:
            t.join()

        # Step 2: Run all pipelines in parallel — each one reloads videos
        # immediately if its CSV changed, so new content is live as fast as possible
        logger.info("--- Running pipelines (parallel) ---")
        pipeline_threads = []
        for directory in PIPELINE_DIRECTORIES:
            t = Thread(target=run_pipeline_and_reload, args=(directory,), daemon=True)
            t.start()
            pipeline_threads.append(t)
        for t in pipeline_threads:
            t.join()

        logger.info(f"--- Script loop complete. Sleeping {config.SCRIPT_INTERVAL}s ---")
        time.sleep(config.SCRIPT_INTERVAL)


def csv_watcher_loop():
    """
    Independent background thread: poll all CSV mod times every 30 seconds.
    Catches any external changes to CSVs without waiting for the script loop,
    including new files being created for the first time.
    """
    csv_mod_times = {}

    # Seed initial mod times for any CSVs that already exist
    for series in CATALOG['series']:
        path = series.get('videoFile')
        if path and os.path.exists(path):
            csv_mod_times[path] = os.path.getmtime(path)

    logger.info("CSV watcher started")

    while True:
        time.sleep(30)
        try:
            changed = False
            for series in CATALOG['series']:
                path = series.get('videoFile')
                if not path:
                    continue
                if not os.path.exists(path):
                    continue
                mtime = os.path.getmtime(path)
                if mtime != csv_mod_times.get(path, 0):
                    csv_mod_times[path] = mtime
                    changed = True
                    logger.info(f"CSV watcher detected change: {path}")

            if changed:
                logger.info("CSV watcher reloading all videos...")
                load_all_videos()
        except Exception as e:
            logger.error(f"CSV watcher error: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# Stream Building
# ═══════════════════════════════════════════════════════════════════════════

def build_stream_title(video, provider_tag):
    """Build display title for a stream entry."""
    quality = video.get('quality', '')
    filesize = video.get('filesize', '')
    filename = video.get('filename', '')

    parts = [provider_tag]
    if quality:
        parts.append(f"📺 {quality}")
    if filesize:
        parts.append(f"💾 {filesize} GB")
    header = '  '.join(parts)

    if filename:
        return f"{header}\n📁 {filename}"
    return header


def build_streams_for_video(video, series, season, debrid_cfg, enable_p2p, tb_cached, user_ip):
    """Build stream entries for a single video. Returns list of stream dicts."""
    streams = []
    info_hash = video['infoHash']
    filename = video.get('filename', '')
    file_idx = video.get('fileIdx', 0)

    # ── TorBox Debrid Stream ──
    tb_key = debrid_cfg.get('tb', {}).get('apiKey')
    if tb_key and tb_cached.get(info_hash):
        try:
            download_url = torbox_get_stream_url(tb_key, info_hash, file_idx, filename, user_ip=user_ip)
            if download_url:
                stream = {
                    'title': build_stream_title(video, '⚡ [TB]'),
                    'url': download_url,
                    'behaviorHints': {
                        'bingeGroup': f"tb-{series['id']}-{season}",
                        'notWebReady': False
                    }
                }
                if filename:
                    stream['behaviorHints']['filename'] = filename
                streams.append(stream)
        except Exception as e:
            logger.error(f"TB stream error for {info_hash}: {e}")

    # ── Real-Debrid Stream (lazy — resolved on playback via /rd/play/) ──
    rd_key = debrid_cfg.get('rd', {}).get('apiKey')
    if rd_key:
        config_data = json.dumps({'debrid': debrid_cfg, 'enableP2P': enable_p2p})
        config_b64 = base64.b64encode(config_data.encode('utf-8')).decode('utf-8').rstrip('=')

        encoded_filename = urllib.parse.quote(filename, safe='') if filename else ''
        proxy_url = f"{request.host_url.rstrip('/')}/rd/play/{config_b64}/{info_hash}/{file_idx}/{encoded_filename}"

        stream = {
            'title': build_stream_title(video, '⚡ [RD]'),
            'url': proxy_url,
            'behaviorHints': {
                'bingeGroup': f"rd-{series['id']}-{season}",
                'notWebReady': False
            }
        }
        if filename:
            stream['behaviorHints']['filename'] = filename
        streams.append(stream)

    # ── P2P Stream ──
    if enable_p2p:
        stream = {
            'title': build_stream_title(video, '🔗 [P2P]'),
            'infoHash': info_hash,
            'behaviorHints': {
                'bingeGroup': f"p2p-{series['id']}-{season}"
            }
        }
        if file_idx is not None:
            stream['fileIdx'] = file_idx
        if filename:
            stream['behaviorHints']['filename'] = filename
        streams.append(stream)

    return streams


# ═══════════════════════════════════════════════════════════════════════════
# RD Proxy Endpoint — resolves on playback, not on browse
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/rd/play/<config_str>/<info_hash>/<int:file_idx>/<path:filename>')
@app.route('/rd/play/<config_str>/<info_hash>/<int:file_idx>/')
@app.route('/rd/play/<config_str>/<info_hash>/<int:file_idx>')
def rd_play(config_str, info_hash, file_idx, filename=None):
    """On-demand RD resolver. Redirects to real file URL, or serves placeholder."""
    # HEAD probes from the player — don't do any RD work
    if request.method == 'HEAD':
        return '', 200

    cfg = parse_config(config_str)
    rd_key = cfg.get('debrid', {}).get('rd', {}).get('apiKey')
    if not rd_key:
        return send_from_directory(app.static_folder, 'rd_downloading.mp4')

    user_ip = get_user_ip()
    if filename:
        filename = urllib.parse.unquote(filename)

    key_hash = hashlib.md5(rd_key.encode()).hexdigest()[:8]
    cache_key = f"{key_hash}:{info_hash}:{file_idx}:{filename or ''}"

    cached = rd_cache_get(cache_key)
    if cached == '__UNAVAILABLE__':
        return send_from_directory(app.static_folder, 'rd_downloading.mp4')
    if cached:
        logger.info(f"RD cache hit for {info_hash[:8]}")
        return redirect(cached)

    download_url = rd_get_stream_url(rd_key, info_hash, file_idx, filename, user_ip=user_ip)

    if download_url:
        rd_cache_set(cache_key, download_url)
        logger.info(f"RD resolved {info_hash[:8]}")
        return redirect(download_url)
    else:
        rd_cache_set(cache_key, '__UNAVAILABLE__')
        logger.info(f"RD not ready for {info_hash[:8]}, serving placeholder")
        return send_from_directory(app.static_folder, 'rd_downloading.mp4')


# ═══════════════════════════════════════════════════════════════════════════
# API Validation Proxy Endpoints (for frontend CORS bypass)
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/api/validate/tb', methods=['POST'])
def validate_torbox():
    """Proxy endpoint for TorBox API key validation."""
    try:
        data = request.get_json(silent=True) or {}
        api_key = data.get('apiKey', '').strip()
        if not api_key:
            return respond_with({'success': False, 'error': 'No API key provided'})

        user_data = torbox_validate_key(api_key)
        if user_data:
            return respond_with({
                'success': True,
                'plan': user_data.get('plan', 'Unknown'),
                'email': user_data.get('email', '')
            })
        return respond_with({'success': False, 'error': 'Invalid API key'})
    except Exception as e:
        logger.error(f"TB validation proxy error: {e}")
        return respond_with({'success': False, 'error': 'Validation failed'})


@app.route('/api/validate/rd', methods=['POST'])
def validate_realdebrid():
    """Proxy endpoint for Real-Debrid API token validation."""
    try:
        data = request.get_json(silent=True) or {}
        api_key = data.get('apiKey', '').strip()
        if not api_key:
            return respond_with({'success': False, 'error': 'No API key provided'})

        user_data = rd_validate_key(api_key)
        if user_data:
            return respond_with({
                'success': True,
                'username': user_data.get('username', ''),
                'type': user_data.get('type', 'free'),
                'expiration': user_data.get('expiration', '')
            })
        return respond_with({'success': False, 'error': 'Invalid API token'})
    except Exception as e:
        logger.error(f"RD validation proxy error: {e}")
        return respond_with({'success': False, 'error': 'Validation failed'})


# ═══════════════════════════════════════════════════════════════════════════
# Routes: Static Pages
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/configure')
def configure():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/images/<path:filename>')
def static_files(filename):
    return send_from_directory('images', filename)


# ═══════════════════════════════════════════════════════════════════════════
# Routes: Unconfigured (no config in path)
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/manifest.json')
def manifest_default():
    return respond_with(MANIFEST)

@app.route('/catalog/<type>/<id>.json')
def catalog_default(type, id):
    return _handle_catalog(type)

@app.route('/catalog/<type>/<id>/genre=<genre>.json')
def catalog_genre_default(type, id, genre):
    return _handle_catalog_genre(type, genre)

@app.route('/catalog/<type>/<id>/search=<query>.json')
def catalog_search_default(type, id, query):
    return _handle_catalog_search(type, query)

@app.route('/meta/<type>/<id>.json')
def meta_default(type, id):
    return _handle_meta(type, id)

@app.route('/stream/<type>/<id>.json')
def stream_default(type, id):
    return _handle_stream(type, id, None)


# ═══════════════════════════════════════════════════════════════════════════
# Routes: Configured (config in path)
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/<config_str>/manifest.json')
def manifest_configured(config_str):
    cfg = parse_config(config_str)
    manifest = dict(MANIFEST)
    providers = []
    if cfg['debrid'].get('tb', {}).get('apiKey'):
        providers.append('TB')
    if cfg['debrid'].get('rd', {}).get('apiKey'):
        providers.append('RD')
    if cfg.get('enableP2P'):
        providers.append('P2P')
    if providers:
        tag = ' + '.join(providers)
        manifest['name'] = f'Formulio ({tag})'
        manifest['id'] = 'org.stremio.formulio.configured'
    return respond_with(manifest)

@app.route('/<config_str>/catalog/<type>/<id>.json')
def catalog_configured(config_str, type, id):
    return _handle_catalog(type)

@app.route('/<config_str>/catalog/<type>/<id>/genre=<genre>.json')
def catalog_genre_configured(config_str, type, id, genre):
    return _handle_catalog_genre(type, genre)

@app.route('/<config_str>/catalog/<type>/<id>/search=<query>.json')
def catalog_search_configured(config_str, type, id, query):
    return _handle_catalog_search(type, query)

@app.route('/<config_str>/meta/<type>/<id>.json')
def meta_configured(config_str, type, id):
    return _handle_meta(type, id)

@app.route('/<config_str>/stream/<type>/<id>.json')
def stream_configured(config_str, type, id):
    return _handle_stream(type, id, config_str)


# ═══════════════════════════════════════════════════════════════════════════
# Route Handlers (shared logic)
# ═══════════════════════════════════════════════════════════════════════════

def _handle_catalog(type):
    if type not in MANIFEST['types']:
        abort(404)
    catalog = CATALOG.get(type, [])
    return respond_with({
        'metas': [{
            'id': item['id'], 'type': type, 'name': item['name'],
            'genres': item.get('genres', []), 'poster': item['poster']
        } for item in catalog]
    })


def _handle_catalog_genre(type, genre):
    if type not in MANIFEST['types']:
        abort(404)
    genre = urllib.parse.unquote(genre)
    catalog = CATALOG.get(type, [])
    filtered = [item for item in catalog if genre in item.get('genres', [])]
    return respond_with({
        'metas': [{
            'id': item['id'], 'type': type, 'name': item['name'],
            'genres': item.get('genres', []), 'poster': item['poster']
        } for item in filtered]
    })


def _handle_catalog_search(type, query):
    if type not in MANIFEST['types']:
        abort(404)
    query = urllib.parse.unquote(query).lower()
    catalog = CATALOG.get(type, [])
    results = []
    for item in catalog:
        if query in item['name'].lower():
            results.append(item)
        elif item.get('description') and query in item['description'].lower():
            results.append(item)
        elif any(query in v.get('title', '').lower() for v in item.get('videos', [])):
            results.append(item)
    return respond_with({
        'metas': [{
            'id': item['id'], 'type': type, 'name': item['name'],
            'genres': item.get('genres', []), 'poster': item['poster']
        } for item in results]
    })


def _handle_meta(type, id):
    if type not in MANIFEST['types']:
        abort(404)
    item = next((i for i in CATALOG.get(type, []) if i['id'] == id), None)
    if not item:
        abort(404)
    meta = {k: item[k] for k in item if k in OPTIONAL_META}
    meta.update({
        'id': item['id'], 'type': type, 'name': item['name'],
        'genres': item['genres'], 'poster': item['poster'],
        'logo': item['logo'], 'background': item['background'],
        'videos': [{
            'id': f"{item['id']}:{v['season']}:{v['episode']}",
            'title': v['title'], 'thumbnail': v['thumbnail'],
            'season': v['season'], 'episode': v['episode']
        } for v in item['videos']]
    })
    return respond_with({'meta': meta})


def _handle_stream(type, id, config_str):
    if type not in MANIFEST['types']:
        abort(404)

    cfg = parse_config(config_str) if config_str else {'debrid': {}, 'enableP2P': True}
    debrid_cfg = cfg.get('debrid', {})
    enable_p2p = cfg.get('enableP2P', True)

    if not debrid_cfg and not enable_p2p:
        enable_p2p = True

    user_ip = get_user_ip()

    if ':' in id:
        try:
            series_id, season, episode = id.split(':')
            season, episode = int(season), int(episode)
        except ValueError:
            abort(400)
    else:
        series_id, season, episode = id, 1, 1

    series = next((s for s in CATALOG.get(type, []) if s['id'] == series_id), None)
    if not series:
        abort(404)

    if ':' in id:
        videos = [v for v in series['videos'] if v['season'] == season and v['episode'] == episode]
    else:
        videos = series['videos']

    if not videos:
        abort(404)

    hashes = list(set(v['infoHash'] for v in videos))

    # Check TorBox cache if configured
    tb_cached = {}
    tb_key = debrid_cfg.get('tb', {}).get('apiKey')
    if tb_key:
        tb_cached = torbox_check_cached(tb_key, hashes)
        logger.info(f"TB cache: {sum(1 for v in tb_cached.values() if v)}/{len(hashes)} cached")

    all_streams = []
    for video in videos:
        all_streams.extend(
            build_streams_for_video(video, series, season, debrid_cfg, enable_p2p, tb_cached, user_ip)
        )

    return respond_with({'streams': all_streams})


# ═══════════════════════════════════════════════════════════════════════════
# Startup
# ═══════════════════════════════════════════════════════════════════════════

def graceful_shutdown(signum, frame):
    logger.info("Shutting down gracefully...")
    exit(0)


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)

    # Load existing video data immediately on startup before any scripts run
    try:
        load_all_videos()
    except Exception as e:
        logger.error(f"Error loading initial video data: {e}")

    # Start background script runner — runs feeds then pipelines in parallel,
    # reloading video data immediately after each pipeline that produces a CSV change
    Thread(target=run_scripts_in_loop, daemon=True).start()

    # Start independent CSV watcher — polls every 30s to catch any changes
    # including externally written files or new CSVs being created
    Thread(target=csv_watcher_loop, daemon=True).start()

    logger.info(f"Formulio addon starting (Python: {PYTHON_EXE})")
    app.run(host='0.0.0.0', port=8000)
