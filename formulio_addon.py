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
import shutil
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
# Simple TTL cache for RD + AD resolutions
# ═══════════════════════════════════════════════════════════════════════════

_rd_cache = {}
_rd_cache_lock = threading.Lock()

_ad_cache = {}
_ad_cache_lock = threading.Lock()


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
        if len(_rd_cache) > 1000:
            cutoff = now() - 300
            expired = [k for k, v in _rd_cache.items() if v['time'] < cutoff]
            for k in expired:
                del _rd_cache[k]
        _rd_cache[cache_key] = {'result': result, 'time': now()}


def ad_cache_get(cache_key):
    """Get cached AD result if still valid. Success: 5 min, Failure: 60s."""
    with _ad_cache_lock:
        entry = _ad_cache.get(cache_key)
        if not entry:
            return None
        ttl = 60 if entry['result'] == '__UNAVAILABLE__' else 300
        if now() - entry['time'] < ttl:
            return entry['result']
        del _ad_cache[cache_key]
        return None


def ad_cache_set(cache_key, result):
    """Cache an AD result."""
    with _ad_cache_lock:
        if len(_ad_cache) > 1000:
            cutoff = now() - 300
            expired = [k for k, v in _ad_cache.items() if v['time'] < cutoff]
            for k in expired:
                del _ad_cache[k]
        _ad_cache[cache_key] = {'result': result, 'time': now()}


# ═══════════════════════════════════════════════════════════════════════════
# Config
# ═══════════════════════════════════════════════════════════════════════════

class Config:
    SCRIPT_INTERVAL = 909  # seconds between script runs (~15 min)
    TORBOX_API_BASE = 'https://api.torbox.app'
    RD_API_BASE = 'https://api.real-debrid.com/rest/1.0'
    AD_API_BASE = 'https://api.alldebrid.com/v4'
    AD_API_BASE_V41 = 'https://api.alldebrid.com/v4.1'


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
                return _rd_pick_and_unrestrict(api_key, info, links, file_idx, filename, user_ip)

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
                    return _rd_pick_and_unrestrict(api_key, info, links, file_idx, filename, user_ip)
            time.sleep(4)

            info = rd_get_torrent_info(api_key, torrent_id)
            if info and info.get('status') == 'waiting_files_selection':
                rd_select_files(api_key, torrent_id, 'all')
            elif info and info.get('status') == 'downloaded':
                links = info.get('links', [])
                if links:
                    return _rd_pick_and_unrestrict(api_key, info, links, file_idx, filename, user_ip)

            logger.info(f"RD torrent {info_hash[:8]} added and queued, not ready yet")
            return None

    except Exception as e:
        logger.error(f"RD stream URL error for {info_hash}: {e}")
        return None


def _rd_pick_and_unrestrict(api_key, info, links, file_idx, filename, user_ip):
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
# AllDebrid Helper Functions
# ═══════════════════════════════════════════════════════════════════════════

def ad_upload_magnet(api_key, info_hash):
    """Upload magnet to AllDebrid. Returns magnet id or None."""
    url = f"{config.AD_API_BASE}/magnet/upload"
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        resp = requests.post(url, headers=headers, data={'magnets[]': info_hash}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get('status') == 'success':
            magnets = data.get('data', {}).get('magnets', [])
            if magnets:
                m = magnets[0]
                if 'error' in m:
                    logger.error(f"AD upload magnet error: {m['error']}")
                    return None
                return m.get('id')
        return None
    except Exception as e:
        logger.error(f"AD upload magnet error: {e}")
        return None


def ad_get_magnet_status(api_key, magnet_id):
    """Get status of a specific AllDebrid magnet. Returns magnet dict or None."""
    url = f"{config.AD_API_BASE_V41}/magnet/status"
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        resp = requests.post(url, headers=headers, data={'id': magnet_id}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get('status') == 'success':
            magnets = data.get('data', {}).get('magnets', [])
            # v4.1 returns a list; sometimes an object when a specific id is requested
            if isinstance(magnets, list):
                for m in magnets:
                    if str(m.get('id')) == str(magnet_id):
                        return m
                if magnets:
                    return magnets[0]
            elif isinstance(magnets, dict):
                return magnets
        return None
    except Exception as e:
        logger.error(f"AD get magnet status error: {e}")
        return None


def ad_find_magnet_by_hash(api_key, info_hash):
    """Find existing magnet in user's AllDebrid list by hash. Returns magnet dict or None."""
    url = f"{config.AD_API_BASE_V41}/magnet/status"
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        resp = requests.post(url, headers=headers, data={}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get('status') == 'success':
            magnets = data.get('data', {}).get('magnets', [])
            if not isinstance(magnets, list):
                return None
            target_hash = info_hash.lower()
            for m in magnets:
                m_hash = (m.get('hash') or '').lower()
                if m_hash == target_hash:
                    return m
        return None
    except Exception as e:
        logger.error(f"AD find magnet error: {e}")
        return None


def ad_get_magnet_files(api_key, magnet_id):
    """Get files (with download links) for an AllDebrid magnet. Returns list or None."""
    url = f"{config.AD_API_BASE}/magnet/files"
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        resp = requests.post(url, headers=headers, data={'id[]': magnet_id}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get('status') == 'success':
            magnets = data.get('data', {}).get('magnets', [])
            if magnets and isinstance(magnets, list):
                m = magnets[0]
                if 'error' in m:
                    logger.error(f"AD get files error: {m['error']}")
                    return None
                return m.get('files', [])
        return None
    except Exception as e:
        logger.error(f"AD get magnet files error: {e}")
        return None


def ad_unlock_link(api_key, link, user_ip=None):
    """Unlock an AllDebrid link. Returns direct download URL or None."""
    url = f"{config.AD_API_BASE}/link/unlock"
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        resp = requests.post(url, headers=headers, data={'link': link}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get('status') == 'success':
            return data.get('data', {}).get('link')
        else:
            err = data.get('error', {})
            logger.error(f"AD unlock error: {err.get('code')} - {err.get('message')}")
            return None
    except Exception as e:
        logger.error(f"AD unlock link error: {e}")
        return None


def _ad_flatten_files(files_tree):
    """Flatten AllDebrid nested files tree into a list of {path, size, link} entries."""
    results = []

    def walk(nodes, prefix=""):
        if not isinstance(nodes, list):
            return
        for node in nodes:
            name = node.get('n', '')
            current_path = f"{prefix}/{name}" if prefix else name
            if 'e' in node and isinstance(node['e'], list):
                # folder
                walk(node['e'], current_path)
            elif 'l' in node:
                # file with link
                results.append({
                    'path': current_path,
                    'name': name,
                    'size': node.get('s', 0),
                    'link': node['l']
                })

    walk(files_tree or [])
    return results


def _ad_pick_file(flat_files, file_idx, filename):
    """Pick the correct file from a flattened AllDebrid file list."""
    if not flat_files:
        return None

    # Try match by filename
    if filename:
        for f in flat_files:
            if filename in f['path'] or filename == f['name']:
                logger.info(f"AD matched file by name: {f['path']}")
                return f

    # Filter only video-like files for idx matching (similar to how RD orders them)
    video_exts = ('.mkv', '.mp4', '.avi', '.mov', '.m4v', '.ts', '.flv', '.wmv', '.webm')
    videos = [f for f in flat_files if f['name'].lower().endswith(video_exts)]

    candidates = videos if videos else flat_files

    if file_idx is not None and 0 <= file_idx < len(candidates):
        logger.info(f"AD matched file by idx {file_idx}: {candidates[file_idx]['path']}")
        return candidates[file_idx]

    # Fallback: largest file
    largest = max(candidates, key=lambda f: f.get('size', 0))
    logger.info(f"AD fallback to largest file: {largest['path']}")
    return largest


def ad_get_stream_url(api_key, info_hash, file_idx, filename, user_ip=None):
    """Full AllDebrid flow. Returns a direct download URL or None if not ready."""
    try:
        existing = ad_find_magnet_by_hash(api_key, info_hash)

        if existing:
            magnet_id = existing['id']
            status_code = existing.get('statusCode', -1)
            status = existing.get('status', '')
            logger.info(f"AD magnet {info_hash[:8]} found, status={status} ({status_code})")

            if status_code == 4:
                # Ready - get files and unlock
                files_tree = ad_get_magnet_files(api_key, magnet_id)
                if not files_tree:
                    logger.info(f"AD magnet {info_hash[:8]} ready but no files yet")
                    return None

                flat = _ad_flatten_files(files_tree)
                picked = _ad_pick_file(flat, file_idx, filename)
                if not picked:
                    logger.error(f"AD no file picked for {info_hash[:8]}")
                    return None

                return ad_unlock_link(api_key, picked['link'], user_ip=user_ip)

            elif status_code in (0, 1, 2, 3):
                # Processing: queued / downloading / compressing / uploading
                logger.info(f"AD magnet {info_hash[:8]} in progress ({status}), not ready")
                return None

            else:
                # Error states (5+)
                logger.warning(f"AD magnet {info_hash[:8]} error status={status} ({status_code})")
                return None

        else:
            logger.info(f"AD magnet {info_hash[:8]} not in library, uploading...")
            magnet_id = ad_upload_magnet(api_key, info_hash)
            if not magnet_id:
                logger.error(f"AD failed to upload magnet for {info_hash[:8]}")
                return None

            time.sleep(2)

            # Check if instantly ready
            info = ad_get_magnet_status(api_key, magnet_id)
            if info and info.get('statusCode') == 4:
                files_tree = ad_get_magnet_files(api_key, magnet_id)
                if files_tree:
                    flat = _ad_flatten_files(files_tree)
                    picked = _ad_pick_file(flat, file_idx, filename)
                    if picked:
                        return ad_unlock_link(api_key, picked['link'], user_ip=user_ip)

            logger.info(f"AD magnet {info_hash[:8]} uploaded and queued, not ready yet")
            return None

    except Exception as e:
        logger.error(f"AD stream URL error for {info_hash}: {e}")
        return None


def ad_validate_key(api_key):
    """Validate an AllDebrid API key. Returns user info dict or None."""
    url = f"{config.AD_API_BASE}/user"
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        resp = requests.get(url, headers=headers, timeout=8)
        resp.raise_for_status()
        data = resp.json()
        if data.get('status') == 'success':
            user = data.get('data', {}).get('user')
            if user and user.get('username'):
                return user
        return None
    except Exception as e:
        logger.error(f"AD validate key error: {e}")
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
    'version': '3.0.1',
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
            'id': 'hpytt0202605', 'name': 'Sky F1 UHD',
            'description': 'IMPORTANT MESSAGE\nThe addon now supports TorBox, RealDebrid & AllDebrid\nRemove addon, reinstall from formulio.hayd.uk',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/c4CjHMNS/sf1uhd.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'], 'videos': [],
            'videoFile': './egor/eg4/6processed.csv'
        },
        {
            'id': 'hpytt0202615', 'name': 'F1TV UHD (English)',
            'description': 'IMPORTANT MESSAGE\nThe addon now supports TorBox, RealDebrid & AllDebrid\nRemove addon, reinstall from formulio.hayd.uk',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/43xW3VMN/f1tenglishuhd.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'], 'videos': [],
            'videoFile': './ss/ssf4/6processed.csv'
        },
        {
            'id': 'hpytt0202614', 'name': 'F1TV UHD (Global)',
            'description': 'The addon now supports TorBox, RealDebrid & AllDebrid\nRemove addon, reinstall at formulio.hayd.uk\nLanguages: 🇬🇧 🇩🇪 🇪🇸 🇫🇷 🇳🇱 🇵🇹 🇯🇵 🔇',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/1zBcw2pr/f1tenguhd.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'], 'videos': [],
            'videoFile': './ss/ssm4/6processed.csv'
        },
        {
            'id': 'hpytt0202606', 'name': 'Sky F1 UHD (alt)',
            'description': 'IMPORTANT MESSAGE\nThe addon now supports TorBox, RealDebrid & AllDebrid\nRemove addon, reinstall from formulio.hayd.uk',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/QC73nRky/sf12uhd.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'], 'videos': [],
            'videoFile': './smcg/sm4/6processed.csv'
        },
        {
            'id': 'hpytt0202601', 'name': 'Sky F1',
            'description': 'IMPORTANT MESSAGE\nThe addon now supports TorBox, RealDebrid & AllDebrid\nRemove addon, reinstall from formulio.hayd.uk',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/QM30pcw2/sf1.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'], 'videos': [],
            'videoFile': './egor/ego/6processed.csv'
        },
        {
            'id': 'hpytt0202603', 'name': 'F1TV (English)',
            'description': 'IMPORTANT MESSAGE\nThe addon now supports TorBox, RealDebrid & AllDebrid\nRemove addon, reinstall from formulio.hayd.uk',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/pXf4j9GD/f1tveng.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'], 'videos': [],
            'videoFile': './ss/ssf/6processed.csv'
        },
        {
            'id': 'hpytt0202604', 'name': 'F1TV (Global)',
            'description': 'The addon now supports TorBox, RealDebrid & AllDebrid\nRemove addon, reinstall at formulio.hayd.uk\nLanguages: 🇬🇧 🇩🇪 🇪🇸 🇫🇷 🇳🇱 🇵🇹 🇯🇵 🔇',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/1zjjSDXZ/f1tvint.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'], 'videos': [],
            'videoFile': './ss/ssm/6processed.csv'
        },
        {
            'id': 'hpytt0202602', 'name': 'Sky F1 (alternative)',
            'description': 'IMPORTANT MESSAGE\nThe addon now supports TorBox, RealDebrid & AllDebrid\nRemove addon, reinstall from formulio.hayd.uk',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/KYMnKTQb/sky2.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'], 'videos': [],
            'videoFile': './smcg/smc/6processed.csv'
        },
        {
            'id': 'hpytt0202612', 'name': 'MotoGP 4K',
            'description': 'IMPORTANT MESSAGE\nThe addon now supports TorBox, RealDebrid & AllDebrid\nRemove addon, reinstall from formulio.hayd.uk',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/MHmvsGDg/motogp4k.jpg',
            'logo': 'https://i.postimg.cc/nh8PKc5n/moto.png',
            'background': 'https://i.postimg.cc/fR252zq3/motobackground.jpg',
            'genres': ['Moto Racing'], 'videos': [],
            'videoFile': './smcm/sm4/6processed.csv'
        },
        {
            'id': 'hpytt0202611', 'name': 'MotoGP',
            'description': 'IMPORTANT MESSAGE\nThe addon now supports TorBox, RealDebrid & AllDebrid\nRemove addon, reinstall from formulio.hayd.uk',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/3Rpyv1D8/motogphd.jpg',
            'logo': 'https://i.postimg.cc/nh8PKc5n/moto.png',
            'background': 'https://i.postimg.cc/fR252zq3/motobackground.jpg',
            'genres': ['Moto Racing'], 'videos': [],
            'videoFile': './smcm/smc/6processed.csv'
        },
        {
            'id': 'hpytt0202607', 'name': 'Sky F1 SD',
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
# CSV Health Monitoring
# ═══════════════════════════════════════════════════════════════════════════

def check_csv_health():
    """Check that all expected CSV files exist and are valid."""
    missing = []
    invalid = []
    
    for series in CATALOG['series']:
        path = series.get('videoFile')
        if not path:
            continue
        
        if not os.path.exists(path):
            missing.append(path)
            logger.error(f"🚨 MISSING CSV: {path}")
        else:
            try:
                size = os.path.getsize(path)
                if size == 0:
                    invalid.append(path)
                    logger.error(f"🚨 EMPTY CSV: {path}")
                else:
                    with open(path, 'r') as f:
                        lines = f.readlines()
                        if len(lines) < 1:
                            invalid.append(path)
                            logger.error(f"🚨 INVALID CSV (no headers): {path}")
            except Exception as e:
                invalid.append(path)
                logger.error(f"🚨 CORRUPTED CSV {path}: {e}")
    
    return missing, invalid


# ═══════════════════════════════════════════════════════════════════════════
# Background Script Runner — with graceful shutdown and backup protection
# ═══════════════════════════════════════════════════════════════════════════

FEED_DIRECTORIES = [
    'egor',
    'smcg',
    'ss',
    'smcm',
]

PIPELINE_DIRECTORIES = [
    'egor/ego',
    'egor/eg4',
    'smcg/smc',
    'smcg/sm4',
    'smcg/sms',
    'ss/ssf',
    'ss/ssm',    
    'ss/ssf4',
    'ss/ssm4',
    'smcm/smc',
    'smcm/sm4',
]


def run_script(directory):
    script_path = os.path.join(directory, '1formationlap.py')
    if not os.path.isfile(script_path):
        logger.warning(f"Script not found, skipping: {script_path}")
        return False
    
    is_pipeline = '/' in directory or '\\' in directory
    timeout = 1200 if is_pipeline else 300
    
    try:
        logger.info(f"Running: {script_path} (timeout: {timeout}s)")
        
        process = subprocess.Popen(
            [PYTHON_EXE, '1formationlap.py'],  # ← FIXED: just the filename, not the full path
            cwd=directory,                       # ← cwd already navigates into the folder
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        try:
            stdout, stderr = process.communicate(timeout=timeout)
            
            if process.returncode == 0:
                if stdout.strip():
                    last_lines = stdout.strip().split('\n')[-3:]
                    for line in last_lines:
                        logger.info(f"  [{directory}] {line}")
                return True
            else:
                logger.error(f"Script {script_path} failed with exit code {process.returncode}")
                if stderr:
                    logger.error(f"stderr: {stderr[:1000]}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.warning(f"Script {script_path} timed out after {timeout}s, attempting graceful shutdown...")
            process.terminate()
            try:
                stdout, stderr = process.communicate(timeout=30)
                logger.info(f"Script {script_path} terminated gracefully")
                if stdout:
                    logger.debug(f"stdout: {stdout[-500:]}")
                return False
            except subprocess.TimeoutExpired:
                logger.error(f"Script {script_path} did not respond to SIGTERM, force killing")
                process.kill()
                stdout, stderr = process.communicate()
                logger.error(f"Script {script_path} was force-killed")
                return False
                
    except Exception as e:
        logger.error(f"Error running {script_path}: {e}")
        return False


def run_pipeline_and_reload(directory):
    csv_path = os.path.join(directory, '6processed.csv')
    backup_path = os.path.join(directory, '6processed.csv.backup')
    
    if os.path.exists(csv_path):
        try:
            shutil.copy2(csv_path, backup_path)
            logger.info(f"Created backup: {backup_path}")
        except Exception as e:
            logger.error(f"Failed to create backup for {csv_path}: {e}")
    
    mtime_before = os.path.getmtime(csv_path) if os.path.exists(csv_path) else 0

    success = run_script(directory)

    if success:
        if os.path.exists(csv_path):
            try:
                with open(csv_path, 'r') as f:
                    lines = f.readlines()
                    if len(lines) < 2:
                        logger.error(f"CSV {csv_path} is empty or invalid, restoring backup")
                        if os.path.exists(backup_path):
                            shutil.copy2(backup_path, csv_path)
                            return False
            except Exception as e:
                logger.error(f"CSV validation failed for {csv_path}: {e}, restoring backup")
                if os.path.exists(backup_path):
                    shutil.copy2(backup_path, csv_path)
                return False
        
        mtime_after = os.path.getmtime(csv_path) if os.path.exists(csv_path) else 0
        if mtime_after != mtime_before:
            logger.info(f"CSV updated: {csv_path} — reloading videos immediately")
            load_all_videos()
            return True
    else:
        if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
            if os.path.exists(backup_path):
                logger.error(f"CSV {csv_path} missing/empty after failed run, restoring backup")
                shutil.copy2(backup_path, csv_path)
    
    return False


def run_scripts_in_loop():
    logger.info(f"Script loop starting (Python: {PYTHON_EXE})")

    while True:
        logger.info("--- Running feed fetchers (parallel) ---")
        feed_threads = []
        for directory in FEED_DIRECTORIES:
            t = Thread(target=run_script, args=(directory,), daemon=True)
            t.start()
            feed_threads.append(t)
        for t in feed_threads:
            t.join()

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
    csv_mod_times = {}

    for series in CATALOG['series']:
        path = series.get('videoFile')
        if path and os.path.exists(path):
            csv_mod_times[path] = os.path.getmtime(path)

    logger.info("CSV watcher started with health monitoring")

    while True:
        time.sleep(30)
        try:
            missing, invalid = check_csv_health()
            
            if missing or invalid:
                logger.error(f"CSV health check failed: {len(missing)} missing, {len(invalid)} invalid")
            
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

def build_stream_name(video, provider_tag):
    """Build the short stream 'name' shown as a label/badge in Stremio."""
    # Kept for provider identification in the stream list
    return provider_tag


def build_stream_title(video):
    """Build main stream title: 'Episode Title - Quality' (e.g. 'Race - JapaneseGP - 4K')."""
    title = video.get('title', '').strip()
    quality = video.get('quality', '').strip()
    if title and quality:
        return f"{title} - {quality}"
    return title or quality or 'Stream'


def build_stream_description(video, provider_tag):
    """Build the detailed description (previously the title) with provider + quality + size + filename."""
    quality = video.get('quality', '')
    filesize = video.get('filesize', '')
    filename = video.get('filename', '')

    parts = [provider_tag]
    if quality:
        parts.append(f"\n🎥 {quality}")
    if filesize:
        parts.append(f"\n📦 {filesize} GB")
    header = '  '.join(parts)

    if filename:
        return f"{header}\n\n{filename}"
    return header

def build_streams_for_video(video, series, season, debrid_cfg, enable_p2p, tb_cached, user_ip):
    """Build stream entries for a single video. Returns list of stream dicts."""
    streams = []
    info_hash = video['infoHash']
    filename = video.get('filename', '')
    file_idx = video.get('fileIdx', 0)

    stream_title = build_stream_title(video)

    # ── TorBox Debrid Stream ──
    tb_key = debrid_cfg.get('tb', {}).get('apiKey')
    if tb_key and tb_cached.get(info_hash):
        try:
            download_url = torbox_get_stream_url(tb_key, info_hash, file_idx, filename, user_ip=user_ip)
            if download_url:
                stream = {
                    'name': 'Formulio\n⚡ TorBox',
                    'title': stream_title,
                    'description': build_stream_description(video, '⚡ [TorBox]'),
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
            'name': 'Formulio\nRealDebrid',
            'title': stream_title,
            'description': build_stream_description(video, '[RealDebrid]'),
            'url': proxy_url,
            'behaviorHints': {
                'bingeGroup': f"rd-{series['id']}-{season}",
                'notWebReady': False
            }
        }
        if filename:
            stream['behaviorHints']['filename'] = filename
        streams.append(stream)

    # ── AllDebrid Stream (lazy — resolved on playback via /ad/play/) ──
    ad_key = debrid_cfg.get('ad', {}).get('apiKey')
    if ad_key:
        config_data = json.dumps({'debrid': debrid_cfg, 'enableP2P': enable_p2p})
        config_b64 = base64.b64encode(config_data.encode('utf-8')).decode('utf-8').rstrip('=')

        encoded_filename = urllib.parse.quote(filename, safe='') if filename else ''
        proxy_url = f"{request.host_url.rstrip('/')}/ad/play/{config_b64}/{info_hash}/{file_idx}/{encoded_filename}"

        stream = {
            'name': 'Formulio\nAllDebrid',
            'title': stream_title,
            'description': build_stream_description(video, '[AllDebrid]'),
            'url': proxy_url,
            'behaviorHints': {
                'bingeGroup': f"ad-{series['id']}-{season}",
                'notWebReady': False
            }
        }
        if filename:
            stream['behaviorHints']['filename'] = filename
        streams.append(stream)

    # ── P2P Stream ──
    if enable_p2p:
        stream = {
            'name': 'Formulio\n🔗 P2P',
            'title': stream_title,
            'description': build_stream_description(video, '🔗 [P2P]'),
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
# AD Proxy Endpoint — resolves on playback, not on browse
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/ad/play/<config_str>/<info_hash>/<int:file_idx>/<path:filename>')
@app.route('/ad/play/<config_str>/<info_hash>/<int:file_idx>/')
@app.route('/ad/play/<config_str>/<info_hash>/<int:file_idx>')
def ad_play(config_str, info_hash, file_idx, filename=None):
    """On-demand AllDebrid resolver. Redirects to real file URL, or serves placeholder."""
    if request.method == 'HEAD':
        return '', 200

    cfg = parse_config(config_str)
    ad_key = cfg.get('debrid', {}).get('ad', {}).get('apiKey')
    if not ad_key:
        return send_from_directory(app.static_folder, 'rd_downloading.mp4')

    user_ip = get_user_ip()
    if filename:
        filename = urllib.parse.unquote(filename)

    key_hash = hashlib.md5(ad_key.encode()).hexdigest()[:8]
    cache_key = f"{key_hash}:{info_hash}:{file_idx}:{filename or ''}"

    cached = ad_cache_get(cache_key)
    if cached == '__UNAVAILABLE__':
        return send_from_directory(app.static_folder, 'rd_downloading.mp4')
    if cached:
        logger.info(f"AD cache hit for {info_hash[:8]}")
        return redirect(cached)

    download_url = ad_get_stream_url(ad_key, info_hash, file_idx, filename, user_ip=user_ip)

    if download_url:
        ad_cache_set(cache_key, download_url)
        logger.info(f"AD resolved {info_hash[:8]}")
        return redirect(download_url)
    else:
        ad_cache_set(cache_key, '__UNAVAILABLE__')
        logger.info(f"AD not ready for {info_hash[:8]}, serving placeholder")
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


@app.route('/api/validate/ad', methods=['POST'])
def validate_alldebrid():
    """Proxy endpoint for AllDebrid API key validation."""
    try:
        data = request.get_json(silent=True) or {}
        api_key = data.get('apiKey', '').strip()
        if not api_key:
            return respond_with({'success': False, 'error': 'No API key provided'})

        user_data = ad_validate_key(api_key)
        if user_data:
            is_premium = user_data.get('isPremium', False)
            is_trial = user_data.get('isTrial', False)
            account_type = 'premium' if is_premium else ('trial' if is_trial else 'free')
            return respond_with({
                'success': True,
                'username': user_data.get('username', ''),
                'email': user_data.get('email', ''),
                'type': account_type,
                'isPremium': is_premium
            })
        return respond_with({'success': False, 'error': 'Invalid API key'})
    except Exception as e:
        logger.error(f"AD validation proxy error: {e}")
        return respond_with({'success': False, 'error': 'Validation failed'})


# ═══════════════════════════════════════════════════════════════════════════
# Routes: Static Pages
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/<config_str>/configure')
def configure_with_config(config_str):
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
    # Parse the config just to validate it, but don't alter the manifest name.
    parse_config(config_str)
    manifest = dict(MANIFEST)
    # Give configured installs a distinct id so Stremio treats it as a separate addon,
    # but keep the display name as just "Formulio".
    manifest['id'] = 'org.stremio.formulio.configured'
    manifest['name'] = 'Formulio'
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

    try:
        load_all_videos()
    except Exception as e:
        logger.error(f"Error loading initial video data: {e}")

    Thread(target=run_scripts_in_loop, daemon=True).start()
    Thread(target=csv_watcher_loop, daemon=True).start()

    logger.info(f"Formulio addon starting (Python: {PYTHON_EXE})")
    app.run(host='0.0.0.0', port=8000)
