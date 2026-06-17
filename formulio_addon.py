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
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify, abort, send_from_directory, request, redirect
from werkzeug.middleware.proxy_fix import ProxyFix
from time import time as now


# ═══════════════════════════════════════════════════════════════════════════
# Logging
# ═══════════════════════════════════════════════════════════════════════════

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

PYTHON_EXE = sys.executable


# ═══════════════════════════════════════════════════════════════════════════
# TTL caches
# ═══════════════════════════════════════════════════════════════════════════

_rd_cache: dict = {}
_rd_cache_lock = threading.Lock()

_ad_cache: dict = {}
_ad_cache_lock = threading.Lock()

_pm_cache: dict = {}
_pm_cache_lock = threading.Lock()

_tb_cache: dict = {}
_tb_cache_lock = threading.Lock()

_oc_cache: dict = {}
_oc_cache_lock = threading.Lock()

_oc_requestid_cache: dict = {}
_oc_requestid_lock = threading.Lock()

_oc_inflight_locks: dict = {}
_oc_inflight_locks_guard = threading.Lock()

# ═══════════════════════════════════════════════════════════════════════════
# Sentinels
# ═══════════════════════════════════════════════════════════════════════════

OC_PENDING = '__PENDING__'
OC_ERROR = '__ERROR__'
OC_CANCELED = '__CANCELED__'
OC_UNAVAILABLE = '__UNAVAILABLE__'
OC_REQ_NONE = '__NONE__'
OC_CACHED = "__CACHED__"

def rd_cache_get(cache_key: str):
    with _rd_cache_lock:
        entry = _rd_cache.get(cache_key)
        if not entry:
            return None
        ttl = 60 if entry['result'] == '__UNAVAILABLE__' else 300
        if now() - entry['time'] < ttl:
            return entry['result']
        del _rd_cache[cache_key]
        return None


def rd_cache_set(cache_key: str, result):
    with _rd_cache_lock:
        if len(_rd_cache) > 1000:
            cutoff = now() - 300
            for k in [k for k, v in _rd_cache.items() if v['time'] < cutoff]:
                del _rd_cache[k]
        _rd_cache[cache_key] = {'result': result, 'time': now()}


def ad_cache_get(cache_key: str):
    with _ad_cache_lock:
        entry = _ad_cache.get(cache_key)
        if not entry:
            return None
        ttl = 60 if entry['result'] == '__UNAVAILABLE__' else 300
        if now() - entry['time'] < ttl:
            return entry['result']
        del _ad_cache[cache_key]
        return None


def ad_cache_set(cache_key: str, result):
    with _ad_cache_lock:
        if len(_ad_cache) > 1000:
            cutoff = now() - 300
            for k in [k for k, v in _ad_cache.items() if v['time'] < cutoff]:
                del _ad_cache[k]
        _ad_cache[cache_key] = {'result': result, 'time': now()}


def pm_cache_get(cache_key: str):
    with _pm_cache_lock:
        entry = _pm_cache.get(cache_key)
        if not entry:
            return None
        ttl = 60 if entry['result'] == '__UNAVAILABLE__' else 300
        if now() - entry['time'] < ttl:
            return entry['result']
        del _pm_cache[cache_key]
        return None


def pm_cache_set(cache_key: str, result):
    with _pm_cache_lock:
        if len(_pm_cache) > 1000:
            cutoff = now() - 300
            for k in [k for k, v in _pm_cache.items() if v['time'] < cutoff]:
                del _pm_cache[k]
        _pm_cache[cache_key] = {'result': result, 'time': now()}


def tb_cache_get(cache_key: str):
    with _tb_cache_lock:
        entry = _tb_cache.get(cache_key)
        if not entry:
            return None
        ttl = 60 if entry['result'] == '__UNAVAILABLE__' else 300
        if now() - entry['time'] < ttl:
            return entry['result']
        del _tb_cache[cache_key]
        return None


def tb_cache_set(cache_key: str, result):
    with _tb_cache_lock:
        if len(_tb_cache) > 1000:
            cutoff = now() - 300
            for k in [k for k, v in _tb_cache.items() if v['time'] < cutoff]:
                del _tb_cache[k]
        _tb_cache[cache_key] = {'result': result, 'time': now()}

def oc_cache_get(cache_key: str):
    with _oc_cache_lock:
        entry = _oc_cache.get(cache_key)
        if not entry:
            return None

        result = entry['result']
        age = now() - entry['time']

        if result == '__PENDING__':
            ttl = 15
        elif result in ('__ERROR__', '__CANCELED__'):
            ttl = 20
        elif result == '__UNAVAILABLE__':
            ttl = 60
        else:
            ttl = 300

        if age < ttl:
            return result

        del _oc_cache[cache_key]
        return None

def oc_cache_set(cache_key: str, result):
    with _oc_cache_lock:
        if len(_oc_cache) > 1000:
            cutoff = now() - 300
            for k in [k for k, v in _oc_cache.items() if v['time'] < cutoff]:
                del _oc_cache[k]
        _oc_cache[cache_key] = {'result': result, 'time': now()}

def oc_requestid_cache_get(key: str):
    with _oc_requestid_lock:
        entry = _oc_requestid_cache.get(key)
        if not entry:
            return None

        result = entry['result']
        age = now() - entry['time']

        if result == OC_REQ_NONE:
            ttl = 20
        else:
            ttl = 3600

        if age < ttl:
            return result

        del _oc_requestid_cache[key]
        return None

def oc_requestid_cache_set(key: str, result):
    with _oc_requestid_lock:
        if len(_oc_requestid_cache) > 1000:
            cutoff = now() - 3600
            for k in [k for k, v in _oc_requestid_cache.items() if v['time'] < cutoff]:
                del _oc_requestid_cache[k]
        _oc_requestid_cache[key] = {'result': result, 'time': now()}

def oc_requestid_cache_delete(key: str):
    with _oc_requestid_lock:
        _oc_requestid_cache.pop(key, None)

# ═══════════════════════════════════════════════════════════════════════════
# Config
# ═══════════════════════════════════════════════════════════════════════════

class Config:
    SCRIPT_INTERVAL = 909
    TORBOX_API_BASE = 'https://api.torbox.app'
    RD_API_BASE = 'https://api.real-debrid.com/rest/1.0'
    AD_API_BASE = 'https://api.alldebrid.com/v4'
    AD_API_BASE_V41 = 'https://api.alldebrid.com/v4.1'
    PM_API_BASE = 'https://www.premiumize.me/api'
    OC_API_BASE = 'https://offcloud.com/api'


config = Config()

app = Flask(__name__, static_folder='static')
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

# ═══════════════════════════════════════════════════════════════════════════
# Offcloud Helper Functions
# ═══════════════════════════════════════════════════════════════════════════

def oc_add_magnet(api_key: str, info_hash: str):
    url = f"{config.OC_API_BASE}/cloud"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {"url": f"magnet:?xt=urn:btih:{info_hash}"}
    
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        if "error" in data:
            logger.error(f"OC add magnet error: {data['error']}")
            return None
            
        return data
    except Exception as e:
        logger.error(f"OC add magnet error: {e}")
        return None

def oc_get_magnet_status(api_key: str, request_id: str):
    url = f"{config.OC_API_BASE}/cloud/status"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {"requestId": request_id}
    
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        if isinstance(data, dict) and 'error' in data:
            logger.error(f"OC status error: {data['error']}")
            return None
            
        if isinstance(data, dict) and isinstance(data.get('status'), dict):
            return data['status']
            
        return data
    except Exception as e:
        logger.error(f"OC get magnet status error: {e}")
        return None

def oc_explore_archive(api_key: str, request_id: str):
    url = f"{config.OC_API_BASE}/cloud/explore/{request_id}"
    headers = {"Authorization": f"Bearer {api_key}"}
    params = {"format": "detailed"}
    
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        if isinstance(data, dict) and "error" in data:
            logger.error(f"OC explore archive error: {data['error']}")
            return None
            
        if isinstance(data, dict) and "files" in data:
            return data["files"]
            
        return data
    except Exception as e:
        logger.error(f"OC explore archive error: {e}")
        return None

def _oc_pick_file_link(items: list, file_idx, filename):
    if not isinstance(items, list) or not items:
        return None

    normalized_filename = urllib.parse.unquote(filename).strip().lower() if filename else None

    def item_url(item):
        value = item.get("url")
        return value if isinstance(value, str) and value else None

    def item_name(item):
        return str(item.get("filename") or item.get("fileName") or item.get("serverFileName") or item.get("name") or "").strip().lower()

    def item_path(item):
            path = item.get("path")
            if isinstance(path, str) and path:
                return path.strip("/").lower()
                
            folder = item.get("folder") or []
            if isinstance(folder, list):
                folder_path = "/".join(str(p).strip() for p in folder if p)
            else:
                folder_path = str(folder).strip()
            name = item_name(item)
            return f"{folder_path}/{name}".strip("/").lower()

    if normalized_filename:
        for item in items:
            if not isinstance(item, dict):
                continue
            name = item_name(item)
            path = item_path(item)
            if normalized_filename in name or normalized_filename in path:
                picked = item_url(item)
                if picked:
                    return picked

    if file_idx is not None and 0 <= file_idx < len(items):
        item = items[file_idx]
        if isinstance(item, dict):
            picked = item_url(item)
            if picked:
                return picked

    video_exts = ('.mkv', '.mp4', '.avi', '.mov', '.m4v', '.ts', '.flv', '.wmv', '.webm')
    video_items = []
    for item in items:
        if not isinstance(item, dict):
            continue
        name = item_name(item)
        if name.endswith(video_exts):
            video_items.append(item)

    candidates = video_items if video_items else [item for item in items if isinstance(item, dict)]

    if not candidates:
        return None

    biggest = max(candidates, key=lambda item: int(item.get("size") or 0))
    return item_url(biggest)

def oc_cache_info(api_key: str, info_hash: str, include_files: bool = False):
    url = f"{config.OC_API_BASE}/cache/info"
    magnet = f"magnet:?xt=urn:btih:{(info_hash or '').strip().lower()}"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "urls": [magnet],
        "includeFiles": bool(include_files),
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if isinstance(data, list) and data:
            item = data[0]
            if isinstance(item, dict):
                cached = item.get("cached")
                if isinstance(cached, str):
                    item["cached"] = cached.strip().lower() in ("true", "1", "yes")
                return item

        return None
    except Exception as e:
        logger.warning(f"OC cache info error for {info_hash[:8]}: {e}")
        return None

def oc_cache_download(api_key: str, info_hash: str):
    url = f"{config.OC_API_BASE}/cache/download"
    magnet = f"magnet:?xt=urn:btih:{(info_hash or '').strip().lower()}"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "url": magnet,
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.warning(f"OC cache download error for {info_hash[:8]}: {e}")
        return []

def _oc_resolve_downloaded_response(api_key: str, info_hash: str, request_id: str, file_idx, filename):
    status_data = oc_get_magnet_status(api_key, request_id)
    if not status_data:
        return OC_ERROR, None

    status = (status_data.get("status") or "").lower()

    if status == "downloaded":
        cached_files = oc_cache_download(api_key, info_hash)
        picked = _oc_pick_file_link(cached_files, file_idx, filename)
        if isinstance(picked, str) and picked:
            return picked, status

        explored = oc_explore_archive(api_key, request_id)
        picked = _oc_pick_file_link(explored, file_idx, filename)
        if isinstance(picked, str) and picked:
            return picked, status

        direct_url = status_data.get("url")
        if isinstance(direct_url, str) and direct_url:
            return direct_url, status

        logger.warning(f"OC downloaded but no playable link found for request_id={request_id}")
        return OC_UNAVAILABLE, status

    if status in ("created", "downloading", "queued", "processing", "uploading"):
        return OC_PENDING, status

    if status == "error":
        return OC_ERROR, status

    if status == "canceled":
        return OC_CANCELED, status

    return OC_UNAVAILABLE, status

def _oc_get_request_id(api_key: str, info_hash: str, requestid_key: str, allow_lookup: bool):
    request_id = None
    info_hash = (info_hash or '').strip().lower()

    cached_request_id = oc_requestid_cache_get(requestid_key)
    if cached_request_id == OC_REQ_NONE:
        logger.info(f"OC requestId none cache hit for {info_hash[:8]}")
        return None
    elif cached_request_id:
        logger.info(f"OC requestId cache hit for {info_hash[:8]}")
        return cached_request_id

    cache_info = oc_cache_info(api_key, info_hash, include_files=True)
    if cache_info:
        logger.info(f"OC cache/info {info_hash[:8]} cached={cache_info.get('cached')}")

    if isinstance(cache_info, dict) and cache_info.get("cached") is False:
        oc_requestid_cache_set(requestid_key, OC_REQ_NONE)
        return None

    if isinstance(cache_info, dict):
        request_id = cache_info.get("requestId") or cache_info.get("request_id")
        if request_id:
            oc_requestid_cache_set(requestid_key, request_id)
            return request_id
        elif cache_info.get("cached") is True:
            oc_requestid_cache_set(requestid_key, OC_CACHED)
            return OC_CACHED

    if not allow_lookup:
        return None

    oc_requestid_cache_set(requestid_key, OC_REQ_NONE)
    return None

def _oc_handle_add_response(api_key: str, info_hash: str, requestid_key: str,
                            added: dict, file_idx, filename, log_prefix: str = "OC added magnet"):
    if not added:
        return OC_ERROR, None

    request_id = added.get("requestId")
    status = (added.get("status") or "").lower()

    if not request_id:
        return OC_ERROR, None

    oc_requestid_cache_set(requestid_key, request_id)
    logger.info(f"{log_prefix} {info_hash[:8]} status={status}")

    if status in ("created", "downloading", "queued"):
        return OC_PENDING, status

    if status == "error":
        return OC_ERROR, status

    if status == "canceled":
        return OC_CANCELED, status

    return _oc_resolve_downloaded_response(api_key, info_hash, request_id, file_idx, filename)

def oc_get_stream_url(api_key: str, info_hash: str, file_idx, filename, user_ip=None):
    hash_lock = None
    created_lock = False
    requestid_key = None

    key_hash = hashlib.md5(api_key.encode()).hexdigest()[:8]
    cache_key = f"{key_hash}:{info_hash}:{file_idx}:{filename}"
    requestid_key = f"{key_hash}:{info_hash}:request_id"

    with _oc_inflight_locks_guard:
        hash_lock = _oc_inflight_locks.get(requestid_key)
        if hash_lock is None:
            hash_lock = threading.Lock()
            _oc_inflight_locks[requestid_key] = hash_lock
            created_lock = True

    try:
        with hash_lock:
            cached = oc_cache_get(cache_key)
            if cached:
                if cached in (OC_PENDING, OC_ERROR, OC_CANCELED, OC_UNAVAILABLE):
                    return None
                return cached

            request_id = _oc_get_request_id(api_key, info_hash, requestid_key, allow_lookup=False)

            if request_id == OC_CACHED:
                logger.info(f"OC magnet {info_hash[:8]} is cached (no requestId), using cache/download directly")
                cached_files = oc_cache_download(api_key, info_hash)
                picked = _oc_pick_file_link(cached_files, file_idx, filename)
                if isinstance(picked, str) and picked:
                    oc_cache_set(cache_key, picked)
                    return picked
                
                request_id = None

            if request_id and request_id != OC_REQ_NONE:
                resolved, status = _oc_resolve_downloaded_response(
                    api_key, info_hash, request_id, file_idx, filename
                )

                logger.info(f"OC magnet {info_hash[:8]} status={status}")

                if isinstance(resolved, str) and resolved not in (OC_PENDING, OC_ERROR, OC_CANCELED, OC_UNAVAILABLE):
                    oc_cache_set(cache_key, resolved)
                    return resolved

                if resolved in (OC_CANCELED, OC_ERROR):
                    logger.info(
                        f"OC magnet {info_hash[:8]} not recoverable from current request; clearing requestId and re-adding"
                    )
                    oc_requestid_cache_delete(requestid_key)

                    resolved, status = _oc_handle_add_response(
                        api_key,
                        info_hash,
                        requestid_key,
                        oc_add_magnet(api_key, info_hash),
                        file_idx,
                        filename,
                        log_prefix="OC re-added magnet"
                    )

                    logger.info(f"OC magnet {info_hash[:8]} post-readd status={status}")

                    if isinstance(resolved, str) and resolved not in (OC_PENDING, OC_ERROR, OC_CANCELED, OC_UNAVAILABLE):
                        oc_cache_set(cache_key, resolved)
                        return resolved

                if resolved in (OC_ERROR, OC_UNAVAILABLE, OC_CANCELED):
                    oc_requestid_cache_delete(requestid_key)

                if not resolved:
                    resolved = OC_PENDING

                oc_cache_set(cache_key, resolved)
                return None

        resolved, status = _oc_handle_add_response(
            api_key,
            info_hash,
            requestid_key,
            oc_add_magnet(api_key, info_hash),
            file_idx,
            filename,
            log_prefix="OC added magnet"
        )

        logger.info(f"OC magnet {info_hash[:8]} status={status}")

        if resolved in (OC_ERROR, OC_UNAVAILABLE, OC_CANCELED):
            oc_requestid_cache_delete(requestid_key)

        oc_cache_set(cache_key, resolved)
        if resolved not in (OC_PENDING, OC_ERROR, OC_CANCELED, OC_UNAVAILABLE):
            return resolved
        return None

    except Exception as e:
        logger.error(f"OC stream URL error for {info_hash}: {e}")
        return None

    finally:
        if created_lock and hash_lock is not None and requestid_key is not None:
            with _oc_inflight_locks_guard:
                if _oc_inflight_locks.get(requestid_key) is hash_lock:
                    _oc_inflight_locks.pop(requestid_key, None)

def oc_validate_key(api_key: str):
    url = f"{config.OC_API_BASE}/account/info"
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return {
                "email": data.get("email"),
                "is_premium": data.get("is_premium"),
                "can_download": data.get("can_download")
            }
        return False
    except Exception as e:
        logger.error(f"OC validate key error: {e}")
        return False
        
# ═══════════════════════════════════════════════════════════════════════════
# TorBox Helper Functions
# ═══════════════════════════════════════════════════════════════════════════

def torbox_create_torrent(api_key: str, info_hash: str):
    url = f"{config.TORBOX_API_BASE}/v1/api/torrents/createtorrent"
    headers = {'Authorization': f'Bearer {api_key}'}
    magnet = f"magnet:?xt=urn:btih:{info_hash}"
    try:
        resp = requests.post(url, headers=headers, data={
            'magnet': magnet, 'seed': 1, 'allow_zip': 'true', 'as_queued': 'false'
        }, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get('success') and data.get('data'):
            return data['data'].get('torrent_id')
        return None
    except Exception as e:
        logger.error(f"TorBox create torrent error: {e}")
        return None


def torbox_find_torrent_by_hash(api_key: str, info_hash: str):
    url = f"{config.TORBOX_API_BASE}/v1/api/torrents/mylist"
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        resp = requests.get(url, headers=headers, params={'bypass_cache': 'true'}, timeout=8)
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


def torbox_get_download_link(api_key: str, torrent_id, file_idx=None, user_ip=None):
    url = f"{config.TORBOX_API_BASE}/v1/api/torrents/requestdl"
    params = {
        'token': api_key,
        'torrent_id': torrent_id,
        'file_id': file_idx if file_idx is not None else 0,
        'zip_link': 'false',
        'redirect': 'false'
    }
    if user_ip:
        params['user_ip'] = user_ip
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get('success') and data.get('data'):
            return data['data']
        return None
    except Exception as e:
        logger.error(f"TorBox request download error: {e}")
        return None


def torbox_get_torrent_info(api_key: str, torrent_id):
    url = f"{config.TORBOX_API_BASE}/v1/api/torrents/mylist"
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        resp = requests.get(url, headers=headers, params={
            'id': torrent_id, 'bypass_cache': 'true'
        }, timeout=8)
        resp.raise_for_status()
        data = resp.json()
        if data.get('success') and data.get('data'):
            return data['data']
        return None
    except Exception as e:
        logger.error(f"TorBox get torrent info error: {e}")
        return None


def torbox_get_stream_url(api_key: str, info_hash: str, file_idx, filename, user_ip=None):
    """Full flow: find/create torrent -> get download URL."""
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


def torbox_validate_key(api_key: str):
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

def rd_add_magnet(api_key: str, info_hash: str):
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


def rd_get_torrent_info(api_key: str, torrent_id):
    url = f"{config.RD_API_BASE}/torrents/info/{torrent_id}"
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"RD get torrent info error: {e}")
        return None


def rd_select_files(api_key: str, torrent_id, file_ids="all") -> bool:
    url = f"{config.RD_API_BASE}/torrents/selectFiles/{torrent_id}"
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        resp = requests.post(url, headers=headers, data={'files': file_ids}, timeout=10)
        return resp.status_code in (200, 202, 204)
    except Exception as e:
        logger.error(f"RD select files error: {e}")
        return False


def rd_unrestrict_link(api_key: str, link: str, user_ip=None):
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


def rd_find_torrent_by_hash(api_key: str, info_hash: str):
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


def rd_get_stream_url(api_key: str, info_hash: str, file_idx, filename, user_ip=None):
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
                    return None
                return _rd_pick_and_unrestrict(api_key, info, links, file_idx, filename, user_ip)
            elif status in ('downloading', 'queued', 'magnet_conversion', 'compressing', 'uploading'):
                logger.info(f"RD torrent {info_hash[:8]} in progress ({status})")
                return None
            else:
                logger.warning(f"RD torrent {info_hash[:8]} unusable status={status}")
                return None
        else:
            logger.info(f"RD torrent {info_hash[:8]} not in library, adding...")
            torrent_id = rd_add_magnet(api_key, info_hash)
            if not torrent_id:
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

            logger.info(f"RD torrent {info_hash[:8]} queued, not ready yet")
            return None

    except Exception as e:
        logger.error(f"RD stream URL error for {info_hash}: {e}")
        return None


def _rd_pick_and_unrestrict(api_key: str, info: dict, links: list, file_idx, filename, user_ip):
    files = info.get('files', [])
    selected_files = [f for f in files if f.get('selected') == 1]
    link_to_use = links[0]

    if selected_files and len(links) == len(selected_files):
        if filename:
            for i, f in enumerate(selected_files):
                if filename in f.get('path', ''):
                    link_to_use = links[i]
                    break
        elif file_idx is not None:
            for i, f in enumerate(selected_files):
                if f.get('id') == file_idx + 1:
                    link_to_use = links[i]
                    break
    elif file_idx is not None and file_idx < len(links):
        link_to_use = links[file_idx]

    return rd_unrestrict_link(api_key, link_to_use, user_ip=user_ip)


def rd_validate_key(api_key: str):
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

def ad_upload_magnet(api_key: str, info_hash: str):
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


def ad_get_magnet_status(api_key: str, magnet_id):
    url = f"{config.AD_API_BASE_V41}/magnet/status"
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        resp = requests.post(url, headers=headers, data={'id': magnet_id}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get('status') == 'success':
            magnets = data.get('data', {}).get('magnets', [])
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


def ad_find_magnet_by_hash(api_key: str, info_hash: str):
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
                if (m.get('hash') or '').lower() == target_hash:
                    return m
        return None
    except Exception as e:
        logger.error(f"AD find magnet error: {e}")
        return None


def ad_get_magnet_files(api_key: str, magnet_id):
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


def ad_unlock_link(api_key: str, link: str, user_ip=None):
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


def _ad_flatten_files(files_tree: list) -> list:
    results: list = []

    def walk(nodes, prefix=""):
        if not isinstance(nodes, list):
            return
        for node in nodes:
            name = node.get('n', '')
            current_path = f"{prefix}/{name}" if prefix else name
            if 'e' in node and isinstance(node['e'], list):
                walk(node['e'], current_path)
            elif 'l' in node:
                results.append({
                    'path': current_path,
                    'name': name,
                    'size': node.get('s', 0),
                    'link': node['l']
                })

    walk(files_tree or [])
    return results


def _ad_pick_file(flat_files: list, file_idx, filename):
    if not flat_files:
        return None

    if filename:
        for f in flat_files:
            if filename in f['path'] or filename == f['name']:
                return f

    video_exts = ('.mkv', '.mp4', '.avi', '.mov', '.m4v', '.ts', '.flv', '.wmv', '.webm')
    videos = [f for f in flat_files if f['name'].lower().endswith(video_exts)]
    candidates = videos if videos else flat_files

    if file_idx is not None and 0 <= file_idx < len(candidates):
        return candidates[file_idx]

    return max(candidates, key=lambda f: f.get('size', 0))


def ad_get_stream_url(api_key: str, info_hash: str, file_idx, filename, user_ip=None):
    try:
        existing = ad_find_magnet_by_hash(api_key, info_hash)

        if existing:
            magnet_id = existing['id']
            status_code = existing.get('statusCode', -1)
            status = existing.get('status', '')
            logger.info(f"AD magnet {info_hash[:8]} found, status={status} ({status_code})")

            if status_code == 4:
                files_tree = ad_get_magnet_files(api_key, magnet_id)
                if not files_tree:
                    return None
                flat = _ad_flatten_files(files_tree)
                picked = _ad_pick_file(flat, file_idx, filename)
                if not picked:
                    return None
                return ad_unlock_link(api_key, picked['link'], user_ip=user_ip)
            elif status_code in (0, 1, 2, 3):
                logger.info(f"AD magnet {info_hash[:8]} in progress ({status})")
                return None
            else:
                logger.warning(f"AD magnet {info_hash[:8]} error status={status} ({status_code})")
                return None
        else:
            logger.info(f"AD magnet {info_hash[:8]} not in library, uploading...")
            magnet_id = ad_upload_magnet(api_key, info_hash)
            if not magnet_id:
                return None

            time.sleep(2)
            info = ad_get_magnet_status(api_key, magnet_id)
            if info and info.get('statusCode') == 4:
                files_tree = ad_get_magnet_files(api_key, magnet_id)
                if files_tree:
                    flat = _ad_flatten_files(files_tree)
                    picked = _ad_pick_file(flat, file_idx, filename)
                    if picked:
                        return ad_unlock_link(api_key, picked['link'], user_ip=user_ip)

            logger.info(f"AD magnet {info_hash[:8]} uploaded and queued")
            return None

    except Exception as e:
        logger.error(f"AD stream URL error for {info_hash}: {e}")
        return None


def ad_validate_key(api_key: str):
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
# Premiumize Helper Functions
# ═══════════════════════════════════════════════════════════════════════════

def _pm_pick_content(content: list, file_idx, filename):
    """Pick the best matching file entry from a directdl content[] array."""
    if not content:
        return None

    if filename:
        for c in content:
            path = c.get('path', '')
            if filename in path or filename == path.split('/')[-1]:
                return c

    video_exts = ('.mkv', '.mp4', '.avi', '.mov', '.m4v', '.ts', '.flv', '.wmv', '.webm')
    videos = [c for c in content if c.get('path', '').lower().endswith(video_exts)]
    candidates = videos if videos else content

    if file_idx is not None and 0 <= file_idx < len(candidates):
        return candidates[file_idx]

    return max(candidates, key=lambda c: c.get('size', 0))


def pm_get_stream_url(api_key: str, info_hash: str, file_idx, filename, user_ip=None):
    """
    Resolve a magnet to a direct download link via /api/transfer/directdl.
    This generates instant links without storing the transfer in the cloud.
    """
    url = f"{config.PM_API_BASE}/transfer/directdl"
    headers = {'Authorization': f'Bearer {api_key}'}
    magnet = f"magnet:?xt=urn:btih:{info_hash}"
    try:
        resp = requests.post(url, headers=headers, data={'src': magnet}, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        if data.get('status') != 'success':
            logger.info(
                f"PM directdl {info_hash[:8]} not ready: "
                f"{data.get('code')} - {data.get('message')}"
            )
            return None
        content = data.get('content', [])
        picked = _pm_pick_content(content, file_idx, filename)
        if not picked:
            return None
        return picked.get('link')
    except Exception as e:
        logger.error(f"PM stream URL error for {info_hash}: {e}")
        return None


def pm_validate_key(api_key: str):
    url = f"{config.PM_API_BASE}/account/info"
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        resp = requests.get(url, headers=headers, timeout=8)
        resp.raise_for_status()
        data = resp.json()
        if data.get('status') == 'success':
            return data
        return None
    except Exception as e:
        logger.error(f"PM validate key error: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════
# Config Parsing & User IP
# ═══════════════════════════════════════════════════════════════════════════

def parse_config(config_str: str) -> dict:
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


def get_user_ip() -> str:
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

CATALOG = {
    'series': [
        {
            'id': 'hpytt0202605',
            'name': 'Sky F1 UHD',
            'type': 'series',
            'description': 'SkyF1 UK UHD\n🇬🇧 David Croft, Martin Brundle, Bernie Collins, Anthony Davidson',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/c4CjHMNS/sf1uhd.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing', 'Motorsport'],
            'videos': [],
            'videoFile': './egor/eg4/6processed.csv',
            'behaviorHints': {
            'hasScheduledVideos': True,
            },
        },
        {
            'id': 'hpytt0202615',
            'name': 'F1TV UHD (English)',
            'type': 'series',
            'description': 'F1TV Live UHD\n🇬🇧 Alex Jacques, Jolyon Palmer, David Coulthard, Alex Brundle\n🇪🇸 Chacho López, Diego Mejía, Giselle Zarur.',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/43xW3VMN/f1tenglishuhd.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing', 'Motorsport'],
            'videos': [],
            'videoFile': './ss/ssf4/6processed.csv',
            'behaviorHints': {
            'hasScheduledVideos': True,
            },
        },
        {
            'id': 'hpytt0202614', 
            'name': 'F1TV UHD (Global)',
            'type': 'series',
            'description': 'F1TV World Feed UHD\n🇩🇪 🇫🇷 🇳🇱 🇵🇹 🇯🇵 🇪🇸 🇬🇧 🔇',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/1zBcw2pr/f1tenguhd.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing', 'Motorsport'],
            'videos': [],            
            'videoFile': './ss/ssm4/6processed.csv',
            'behaviorHints': {
            'hasScheduledVideos': True,
            },
        },
        {
            'id': 'hpytt0202606', 
            'name': 'Sky F1 UHD (Alt)',
            'type': 'series',
            'description': 'SkyF1 UK UHD\n🇬🇧 David Croft, Martin Brundle, Bernie Collins, Anthony Davidson',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/QC73nRky/sf12uhd.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing', 'Motorsport'],
            'videos': [],            
            'videoFile': './smcg/sm4/6processed.csv',
            'behaviorHints': {
            'hasScheduledVideos': True,
            },
        },
        {
            'id': 'hpytt0202601', 
            'name': 'Sky F1',
            'type': 'series',
            'description': 'SkyF1 UK\n🇬🇧 David Croft, Martin Brundle, Bernie Collins, Anthony Davidson',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/QM30pcw2/sf1.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing', 'Motorsport'],
            'videos': [],            
            'videoFile': './egor/ego/6processed.csv',
            'behaviorHints': {
            'hasScheduledVideos': True,
            },
        },
        {
            'id': 'hpytt0202603', 
            'name': 'F1TV (English)',
            'type': 'series',
            'description': 'F1TV Live\n🇬🇧 Alex Jacques, Jolyon Palmer, David Coulthard, Alex Brundle\n🇪🇸 Chacho López, Diego Mejía, Giselle Zarur.',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/pXf4j9GD/f1tveng.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing', 'Motorsport'],
            'videos': [],            
            'videoFile': './ss/ssf/6processed.csv',
            'behaviorHints': {
            'hasScheduledVideos': True,
            },
        },
        {
            'id': 'hpytt0202604', 
            'name': 'F1TV (Global)',
            'type': 'series',
            'description': 'F1TV World Feed\n🇩🇪 🇫🇷 🇳🇱 🇵🇹 🇯🇵 🇪🇸 🇬🇧 🔇',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/1zjjSDXZ/f1tvint.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing', 'Motorsport'],
            'videos': [],            
            'videoFile': './ss/ssm/6processed.csv',
            'behaviorHints': {
            'hasScheduledVideos': True,
            },
        },
        {
            'id': 'hpytt0202602', 
            'name': 'Sky F1 (Alternative)',
            'type': 'series',
            'description': 'SkyF1 UK\n🇬🇧 David Croft, Martin Brundle, Bernie Collins, Anthony Davidson',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/KYMnKTQb/sky2.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing', 'Motorsport'],
            'videos': [],            
            'videoFile': './smcg/smc/6processed.csv',
            'behaviorHints': {
            'hasScheduledVideos': True,
            },
        },
        {
            'id': 'hpytt0202612', 
            'name': 'MotoGP 4K',
            'type': 'series',
            'description': 'TNT MotoGP 4K\n🇬🇧 Gavin Emmett, Steve Day, Neil Hodgson, Sylvain Guintoli',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/MHmvsGDg/motogp4k.jpg',
            'logo': 'https://i.postimg.cc/nh8PKc5n/moto.png',
            'background': 'https://i.postimg.cc/fR252zq3/motobackground.jpg',
            'genres': ['Moto Racing', 'Motorsport'],
            'videos': [],            
            'videoFile': './smcm/sm4/6processed.csv',
            'behaviorHints': {
            'hasScheduledVideos': True,
            },
        },
        {
            'id': 'hpytt0202611', 
            'name': 'MotoGP',
            'type': 'series',
            'description': 'TNT MotoGP\n🇬🇧 Gavin Emmett, Steve Day, Neil Hodgson, Sylvain Guintoli',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/3Rpyv1D8/motogphd.jpg',
            'logo': 'https://i.postimg.cc/nh8PKc5n/moto.png',
            'background': 'https://i.postimg.cc/fR252zq3/motobackground.jpg',
            'genres': ['Moto Racing', 'Motorsport'],
            'videos': [],            
            'videoFile': './smcm/smc/6processed.csv',
            'behaviorHints': {
            'hasScheduledVideos': True,
            },
        },
        {
            'id': 'hpytt0202607', 
            'name': 'Sky F1 SD',
            'type': 'series',
            'description': 'Low Quality SD\nformulio@tuta.io',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/Pqcn5Vvx/sky2sd.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing', 'Motorsport'],
            'videos': [],            
            'videoFile': './smcg/sms/6processed.csv',
            'behaviorHints': {
            'hasScheduledVideos': True,
            },
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

def respond_with(data: dict):
    resp = jsonify(data)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Headers'] = '*'

    # Cache Control Headers to fix Nuvio's caching lag
    # max-age=3600 (1 hour cache), stale-while-revalidate=1800 (allows 30 mins background refreshing)
    resp.headers['Cache-Control'] = 'max-age=3600, stale-while-revalidate=1800'
    return resp

def load_videos(filepath: str) -> list:
    """
    Load videos from a processed CSV.

    Defensive guarantee: each (season, episode) slot is unique. If a malformed
    CSV (e.g. a partial write, or a merger regression) contains two rows with
    the same (season, episode), the FIRST one wins and the duplicate is logged
    and dropped. This prevents Stremio from showing one episode in the meta
    while resolving the stream of a different file, which would make the
    displayed episode and the played content disagree.
    """
    videos: list = []
    seen_slots: dict = {}          # (season, episode) -> title (for logging)
    duplicate_slots = 0

    try:
        with open(filepath, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    season = int(row['season'].strip())
                    episode = int(row['episode'].strip())
                except (ValueError, KeyError):
                    logger.error(f"Bad season/episode in {filepath}: {row}")
                    continue

                slot = (season, episode)
                if slot in seen_slots:
                    duplicate_slots += 1
                    logger.error(
                        f"Duplicate (S{season}E{episode}) in {filepath}: "
                        f"'{row.get('title', '').strip()}' clashes with "
                        f"'{seen_slots[slot]}' — keeping first, dropping this one"
                    )
                    continue

                video_obj: dict = {
                    'id': row['series_id'].strip(),
                    'season': season,
                    'episode': episode,
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

                seen_slots[slot] = video_obj['title']
                videos.append(video_obj)

        videos.sort(key=lambda v: (v['season'], v['episode']))

        if duplicate_slots:
            logger.warning(
                f"{filepath}: dropped {duplicate_slots} duplicate "
                f"(season, episode) row(s) during load"
            )
    except FileNotFoundError:
        logger.warning(f"CSV not found: {filepath}")
    except (ValueError, KeyError) as e:
        logger.error(f"Error parsing {filepath}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error reading {filepath}: {e}")
    return videos


def load_all_videos():
    """Atomically reload videos. Never replaces a populated list with an empty
    one (prevents Stremio caching a 'movie-like' meta with zero videos)."""
    for series in CATALOG['series']:
        video_file = series.get('videoFile')
        if not video_file:
            continue
        new_videos = load_videos(video_file)
        if new_videos:
            series['videos'] = new_videos
            logger.info(f"Loaded {len(new_videos)} videos for '{series['name']}' from {video_file}")
        else:
            existing = series.get('videos') or []
            if existing:
                logger.warning(
                    f"Reload for '{series['name']}' returned 0 videos — "
                    f"KEEPING previous {len(existing)} videos (file: {video_file})"
                )
            else:
                logger.warning(f"No videos for '{series['name']}' and none cached ({video_file})")


# ═══════════════════════════════════════════════════════════════════════════
# CSV Health Monitoring
# ═══════════════════════════════════════════════════════════════════════════

def check_csv_health():
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
# Background Script Runner
# ═══════════════════════════════════════════════════════════════════════════

FEED_DIRECTORIES = ['egor', 'smcg', 'ss', 'smcm']

PIPELINE_BATCH_1 = [
    'egor/ego', 'egor/eg4', 'smcg/smc', 'smcg/sm4', 'smcg/sms', 'ss/ssf',
]

PIPELINE_BATCH_2 = [
    'ss/ssm', 'ss/ssf4', 'ss/ssm4', 'smcm/smc', 'smcm/sm4',
]


def run_script(directory: str) -> bool:
    script_path = os.path.join(directory, '1formationlap.py')
    if not os.path.isfile(script_path):
        logger.warning(f"Script not found, skipping: {script_path}")
        return False

    is_pipeline = '/' in directory or '\\' in directory
    timeout = 1200 if is_pipeline else 300

    try:
        logger.info(f"Running: {script_path} (timeout: {timeout}s)")
        process = subprocess.Popen(
            [PYTHON_EXE, '1formationlap.py'],
            cwd=directory,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        try:
            stdout, stderr = process.communicate(timeout=timeout)
            if process.returncode == 0:
                if stdout.strip():
                    for line in stdout.strip().split('\n')[-3:]:
                        logger.info(f"  [{directory}] {line}")
                return True
            else:
                logger.error(f"Script {script_path} failed (exit {process.returncode})")
                if stderr:
                    logger.error(f"stderr: {stderr[:1000]}")
                return False
        except subprocess.TimeoutExpired:
            logger.warning(f"Script {script_path} timed out, terminating...")
            process.terminate()
            try:
                process.communicate(timeout=30)
                return False
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()
                return False
    except Exception as e:
        logger.error(f"Error running {script_path}: {e}")
        return False


def run_pipeline_and_reload(directory: str) -> bool:
    csv_path = os.path.join(directory, '6processed.csv')
    backup_path = os.path.join(directory, '6processed.csv.backup')

    if os.path.exists(csv_path):
        try:
            shutil.copy2(csv_path, backup_path)
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
                        logger.error(f"CSV {csv_path} empty after run, restoring backup")
                        if os.path.exists(backup_path):
                            shutil.copy2(backup_path, csv_path)
                        return False
            except Exception as e:
                logger.error(f"CSV validation failed for {csv_path}: {e}")
                if os.path.exists(backup_path):
                    shutil.copy2(backup_path, csv_path)
                return False

        mtime_after = os.path.getmtime(csv_path) if os.path.exists(csv_path) else 0
        if mtime_after != mtime_before:
            parsed = load_videos(csv_path)
            if parsed:
                logger.info(f"CSV updated: {csv_path} ({len(parsed)} videos) — reloading")
                load_all_videos()
                return True
            else:
                logger.error(f"New CSV {csv_path} parsed to 0 videos — restoring backup")
                if os.path.exists(backup_path):
                    shutil.copy2(backup_path, csv_path)
                return False
    else:
        if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
            if os.path.exists(backup_path):
                logger.error(f"CSV missing/empty after failed run, restoring backup: {csv_path}")
                shutil.copy2(backup_path, csv_path)

    return False


def run_pipeline_batch(batch: list, batch_name: str):
    logger.info(f"--- Running pipeline batch '{batch_name}' ({len(batch)} scripts) ---")
    threads = []
    for directory in batch:
        t = Thread(target=run_pipeline_and_reload, args=(directory,), daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    logger.info(f"--- Pipeline batch '{batch_name}' complete ---")


def run_scripts_in_loop():
    logger.info(f"Script loop starting (Python: {PYTHON_EXE})")
    while True:
        logger.info("--- Running feed fetchers (sequential) ---")
        for directory in FEED_DIRECTORIES:
            run_script(directory)
        run_pipeline_batch(PIPELINE_BATCH_1, "Batch 1")
        run_pipeline_batch(PIPELINE_BATCH_2, "Batch 2")
        logger.info(f"--- Script loop complete. Sleeping {config.SCRIPT_INTERVAL}s ---")
        time.sleep(config.SCRIPT_INTERVAL)


def csv_watcher_loop():
    csv_mod_times: dict = {}
    for series in CATALOG['series']:
        path = series.get('videoFile')
        if path and os.path.exists(path):
            csv_mod_times[path] = os.path.getmtime(path)

    logger.info("CSV watcher started")
    while True:
        time.sleep(30)
        try:
            missing, invalid = check_csv_health()
            if missing or invalid:
                logger.error(f"CSV health check failed: {len(missing)} missing, {len(invalid)} invalid")

            changed = False
            for series in CATALOG['series']:
                path = series.get('videoFile')
                if not path or not os.path.exists(path):
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

def build_stream_title(video: dict, provider_tag: str) -> str:
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


def _build_debrid_proxy_stream(provider: str, tag: str, video: dict, series: dict,
                               season: int, debrid_cfg: dict, enable_p2p: bool) -> dict:
    """Build a lazy proxy stream entry for a debrid provider (tb/rd/ad/pm)."""
    info_hash = video['infoHash']
    filename = video.get('filename', '')
    file_idx = video.get('fileIdx', 0)

    config_data = json.dumps({'debrid': debrid_cfg, 'enableP2P': enable_p2p})
    config_b64 = base64.b64encode(config_data.encode('utf-8')).decode('utf-8').rstrip('=')
    encoded_filename = urllib.parse.quote(filename, safe='') if filename else ''
    proxy_url = (
        f"{request.host_url.rstrip('/')}/{provider}/play"
        f"/{config_b64}/{info_hash}/{file_idx}/{encoded_filename}"
    )
    stream = {
        'title': build_stream_title(video, tag),
        'url': proxy_url,
        'behaviorHints': {
            'bingeGroup': f"{provider}-{series['id']}-{season}",
            'notWebReady': False
        }
    }
    if filename:
        stream['behaviorHints']['filename'] = filename
    return stream


def build_streams_for_video(video: dict, series: dict, season: int, debrid_cfg: dict,
                            enable_p2p: bool) -> list:
    """
    Build stream entries for a single video.

    All debrid providers (TorBox, Real-Debrid, AllDebrid, Premiumize) are lazy:
    they point at a proxy /<provider>/play endpoint that resolves the link when
    the user presses play. P2P is shown when enabled.
    """
    streams: list = []
    info_hash: str = video['infoHash']
    filename: str = video.get('filename', '')
    file_idx: int = video.get('fileIdx', 0)
                                       
    # ── TorBox ──────────────────────────────────────────────────────────────
    if debrid_cfg.get('tb', {}).get('apiKey', ''):
        streams.append(_build_debrid_proxy_stream(
            'tb', '⚡ [TorBox]', video, series, season, debrid_cfg, enable_p2p))

    # ── Real-Debrid ──────────────────────────────────────────────────────────
    if debrid_cfg.get('rd', {}).get('apiKey', ''):
        streams.append(_build_debrid_proxy_stream(
            'rd', '⚡ [RealDebrid]', video, series, season, debrid_cfg, enable_p2p))

    # ── AllDebrid ────────────────────────────────────────────────────────────
    if debrid_cfg.get('ad', {}).get('apiKey', ''):
        streams.append(_build_debrid_proxy_stream(
            'ad', '⚡ [AllDebrid]', video, series, season, debrid_cfg, enable_p2p))

    # ── Premiumize ───────────────────────────────────────────────────────────
    if debrid_cfg.get('pm', {}).get('apiKey', ''):
        streams.append(_build_debrid_proxy_stream(
            'pm', '⚡ [Premiumize]', video, series, season, debrid_cfg, enable_p2p))

    # ── Offcloud ────────────────────────────────────────────────────────────
    oc_key: str = debrid_cfg.get('oc', {}).get('apiKey', '')
    if oc_key:
        config_data = json.dumps({'debrid': debrid_cfg, 'enableP2P': enable_p2p})
        config_b64 = base64.b64encode(config_data.encode('utf-8')).decode('utf-8').rstrip('=')
        encoded_filename = urllib.parse.quote(filename, safe='') if filename else ''
        proxy_url = (
            f"{request.host_url.rstrip('/')}/oc/play"
            f"/{config_b64}/{info_hash}/{file_idx}/{encoded_filename}"
        )
        stream = {
            'title': build_stream_title(video, '⚡ [Offcloud]'),
            'url': proxy_url,
            'behaviorHints': {
                'bingeGroup': f"oc-{series['id']}-{season}",
                'notWebReady': False
            }
        }
        if filename:
            stream['behaviorHints']['filename'] = filename
        streams.append(stream)

    # ── P2P ─────────────────────────────────────────────────────────────────
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
# Proxy Endpoints — RD
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/rd/play/<config_str>/<info_hash>/<int:file_idx>/<path:filename>')
@app.route('/rd/play/<config_str>/<info_hash>/<int:file_idx>/')
@app.route('/rd/play/<config_str>/<info_hash>/<int:file_idx>')
def rd_play(config_str: str, info_hash: str, file_idx: int, filename: str = ''):
    if request.method == 'HEAD':
        return '', 200

    cfg = parse_config(config_str)
    rd_key: str = cfg.get('debrid', {}).get('rd', {}).get('apiKey', '')
    if not rd_key:
        return send_from_directory(app.static_folder, 'rd_downloading.mp4')

    user_ip = get_user_ip()
    if filename:
        filename = urllib.parse.unquote(filename)

    key_hash = hashlib.md5(rd_key.encode()).hexdigest()[:8]
    cache_key = f"{key_hash}:{info_hash}:{file_idx}:{filename}"

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
# Proxy Endpoints — AD
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/ad/play/<config_str>/<info_hash>/<int:file_idx>/<path:filename>')
@app.route('/ad/play/<config_str>/<info_hash>/<int:file_idx>/')
@app.route('/ad/play/<config_str>/<info_hash>/<int:file_idx>')
def ad_play(config_str: str, info_hash: str, file_idx: int, filename: str = ''):
    if request.method == 'HEAD':
        return '', 200

    cfg = parse_config(config_str)
    ad_key: str = cfg.get('debrid', {}).get('ad', {}).get('apiKey', '')
    if not ad_key:
        return send_from_directory(app.static_folder, 'rd_downloading.mp4')

    user_ip = get_user_ip()
    if filename:
        filename = urllib.parse.unquote(filename)

    key_hash = hashlib.md5(ad_key.encode()).hexdigest()[:8]
    cache_key = f"{key_hash}:{info_hash}:{file_idx}:{filename}"

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
# Proxy Endpoints — TB
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/tb/play/<config_str>/<info_hash>/<int:file_idx>/<path:filename>')
@app.route('/tb/play/<config_str>/<info_hash>/<int:file_idx>/')
@app.route('/tb/play/<config_str>/<info_hash>/<int:file_idx>')
def tb_play(config_str: str, info_hash: str, file_idx: int, filename: str = ''):
    if request.method == 'HEAD':
        return '', 200

    cfg = parse_config(config_str)
    tb_key: str = cfg.get('debrid', {}).get('tb', {}).get('apiKey', '')
    if not tb_key:
        return send_from_directory(app.static_folder, 'rd_downloading.mp4')

    user_ip = get_user_ip()
    if filename:
        filename = urllib.parse.unquote(filename)

    key_hash = hashlib.md5(tb_key.encode()).hexdigest()[:8]
    cache_key = f"{key_hash}:{info_hash}:{file_idx}:{filename}"

    cached = tb_cache_get(cache_key)
    if cached == '__UNAVAILABLE__':
        return send_from_directory(app.static_folder, 'rd_downloading.mp4')
    if cached:
        logger.info(f"TB cache hit for {info_hash[:8]}")
        return redirect(cached)

    download_url = torbox_get_stream_url(tb_key, info_hash, file_idx, filename or None,
                                         user_ip=user_ip)
    if download_url:
        tb_cache_set(cache_key, download_url)
        logger.info(f"TB resolved {info_hash[:8]}")
        return redirect(download_url)
    else:
        tb_cache_set(cache_key, '__UNAVAILABLE__')
        logger.info(f"TB not ready for {info_hash[:8]}, serving placeholder")
        return send_from_directory(app.static_folder, 'rd_downloading.mp4')


# ═══════════════════════════════════════════════════════════════════════════
# Proxy Endpoints — PM
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/pm/play/<config_str>/<info_hash>/<int:file_idx>/<path:filename>')
@app.route('/pm/play/<config_str>/<info_hash>/<int:file_idx>/')
@app.route('/pm/play/<config_str>/<info_hash>/<int:file_idx>')
def pm_play(config_str: str, info_hash: str, file_idx: int, filename: str = ''):
    if request.method == 'HEAD':
        return '', 200

    cfg = parse_config(config_str)
    pm_key: str = cfg.get('debrid', {}).get('pm', {}).get('apiKey', '')
    if not pm_key:
        return send_from_directory(app.static_folder, 'rd_downloading.mp4')

    user_ip = get_user_ip()
    if filename:
        filename = urllib.parse.unquote(filename)

    key_hash = hashlib.md5(pm_key.encode()).hexdigest()[:8]
    cache_key = f"{key_hash}:{info_hash}:{file_idx}:{filename}"

    cached = pm_cache_get(cache_key)
    if cached == '__UNAVAILABLE__':
        return send_from_directory(app.static_folder, 'rd_downloading.mp4')
    if cached:
        logger.info(f"PM cache hit for {info_hash[:8]}")
        return redirect(cached)

    download_url = pm_get_stream_url(pm_key, info_hash, file_idx, filename, user_ip=user_ip)
    if download_url:
        pm_cache_set(cache_key, download_url)
        logger.info(f"PM resolved {info_hash[:8]}")
        return redirect(download_url)
    else:
        pm_cache_set(cache_key, '__UNAVAILABLE__')
        logger.info(f"PM not ready for {info_hash[:8]}, serving placeholder")
        return send_from_directory(app.static_folder, 'rd_downloading.mp4')

# ═══════════════════════════════════════════════════════════════════════════
# Proxy Endpoints — OC
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/oc/play/<config_str>/<info_hash>/<int:file_idx>/<path:filename>')
@app.route('/oc/play/<config_str>/<info_hash>/<int:file_idx>/')
@app.route('/oc/play/<config_str>/<info_hash>/<int:file_idx>')
def oc_play(config_str: str, info_hash: str, file_idx: int, filename: str = ''):
    if request.method == 'HEAD':
        return '', 200

    cfg = parse_config(config_str)
    oc_key: str = cfg.get('debrid', {}).get('oc', {}).get('apiKey', '')
    if not oc_key:
        return send_from_directory(app.static_folder, 'rd_downloading.mp4')

    user_ip = get_user_ip()
    info_hash = (info_hash or '').strip().lower()
    filename = urllib.parse.unquote(filename).strip().lower() if filename else ''

    key_hash = hashlib.md5(oc_key.encode()).hexdigest()[:8]
    cache_key = f"{key_hash}:{info_hash}:{file_idx}:{filename}"

    cached = oc_cache_get(cache_key)
    if cached in (OC_PENDING, OC_ERROR, OC_CANCELED, OC_UNAVAILABLE):
        logger.info(f"OC cached state for {info_hash[:8]}: {cached}")
        return send_from_directory(app.static_folder, 'rd_downloading.mp4')

    if cached:
        logger.info(f"OC cache hit for {info_hash[:8]}")
        return redirect(cached)

    download_url = oc_get_stream_url(oc_key, info_hash, file_idx, filename, user_ip=user_ip)
    if download_url:
        oc_cache_set(cache_key, download_url)
        logger.info(f"OC resolved {info_hash[:8]}")
        return redirect(download_url)

    cached_state = oc_cache_get(cache_key)
    if cached_state in (OC_PENDING, OC_ERROR, OC_CANCELED, OC_UNAVAILABLE):
        logger.info(f"OC not ready for {info_hash[:8]}, state={cached_state}, serving placeholder")
        return send_from_directory(app.static_folder, 'rd_downloading.mp4')

    # FIX: We deleted the oc_cache_set line here.
    # Now it just serves the video without writing a 60-second block to the cache!
    logger.info(f"OC not ready for {info_hash[:8]}, no explicit state set, serving placeholder")
    return send_from_directory(app.static_folder, 'rd_downloading.mp4')

# ═══════════════════════════════════════════════════════════════════════════
# API Validation Proxy Endpoints
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/api/validate/tb', methods=['POST'])
def validate_torbox():
    try:
        data = request.get_json(silent=True) or {}
        api_key: str = data.get('apiKey', '').strip()
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
    try:
        data = request.get_json(silent=True) or {}
        api_key: str = data.get('apiKey', '').strip()
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
    try:
        data = request.get_json(silent=True) or {}
        api_key: str = data.get('apiKey', '').strip()
        if not api_key:
            return respond_with({'success': False, 'error': 'No API key provided'})
        user_data = ad_validate_key(api_key)
        if user_data:
            is_premium: bool = user_data.get('isPremium', False)
            is_trial: bool = user_data.get('isTrial', False)
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

@app.route('/api/validate/oc', methods=['POST'])
def validate_offcloud():
    try:
        data = request.get_json(silent=True) or {}
        apikey = str(data.get("apiKey", "")).strip()
        if not apikey:
            return respond_with({"success": False, "error": "No API key provided"})
            
        userdata = oc_validate_key(apikey)
        if userdata:
            return respond_with({
                "success": True,
                "email": userdata.get("email", ""),
                "is_premium": userdata.get("is_premium"),
                "can_download": userdata.get("can_download")
            })
        return respond_with({"success": False, "error": "Invalid API key"})
    except Exception as e:
        logger.error(f"OC validation proxy error: {e}")
        return respond_with({"success": False, "error": "Validation failed"})

@app.route('/api/validate/pm', methods=['POST'])
def validate_premiumize():
    try:
        data = request.get_json(silent=True) or {}
        api_key: str = data.get('apiKey', '').strip()
        if not api_key:
            return respond_with({'success': False, 'error': 'No API key provided'})
        info = pm_validate_key(api_key)
        if info:
            premium_until = info.get('premium_until')
            is_premium = bool(premium_until) and premium_until > now()
            account_type = 'premium' if is_premium else 'free'
            return respond_with({
                'success': True,
                'username': info.get('customer_id', ''),
                'type': account_type,
                'isPremium': is_premium
            })
        return respond_with({'success': False, 'error': 'Invalid API key'})
    except Exception as e:
        logger.error(f"PM validation proxy error: {e}")
        return respond_with({'success': False, 'error': 'Validation failed'})


# ═══════════════════════════════════════════════════════════════════════════
# Routes: Static Pages
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')


@app.route('/<config_str>/configure')
def configure_with_config(config_str: str):
    return send_from_directory(app.static_folder, 'index.html')


@app.route('/images/<path:filename>')
def static_files(filename: str):
    return send_from_directory('images', filename)


# ═══════════════════════════════════════════════════════════════════════════
# Routes: Unconfigured
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/manifest.json')
def manifest_default():
    return respond_with(MANIFEST)


@app.route('/catalog/<type>/<id>.json')
def catalog_default(type: str, id: str):
    return _handle_catalog(type)


@app.route('/catalog/<type>/<id>/genre=<genre>.json')
def catalog_genre_default(type: str, id: str, genre: str):
    return _handle_catalog_genre(type, genre)


@app.route('/catalog/<type>/<id>/search=<query>.json')
def catalog_search_default(type: str, id: str, query: str):
    return _handle_catalog_search(type, query)


@app.route('/meta/<type>/<id>.json')
def meta_default(type: str, id: str):
    return _handle_meta(type, id)


@app.route('/stream/<type>/<id>.json')
def stream_default(type: str, id: str):
    return _handle_stream(type, id, None)


# ═══════════════════════════════════════════════════════════════════════════
# Routes: Configured
# ═══════════════════════════════════════════════════════════════════════════

@app.route('/<config_str>/manifest.json')
def manifest_configured(config_str: str):
    parse_config(config_str)
    manifest = dict(MANIFEST)
    manifest['id'] = 'org.stremio.formulio.configured'
    manifest['name'] = 'Formulio'
    return respond_with(manifest)


@app.route('/<config_str>/catalog/<type>/<id>.json')
def catalog_configured(config_str: str, type: str, id: str):
    return _handle_catalog(type)


@app.route('/<config_str>/catalog/<type>/<id>/genre=<genre>.json')
def catalog_genre_configured(config_str: str, type: str, id: str, genre: str):
    return _handle_catalog_genre(type, genre)


@app.route('/<config_str>/catalog/<type>/<id>/search=<query>.json')
def catalog_search_configured(config_str: str, type: str, id: str, query: str):
    return _handle_catalog_search(type, query)


@app.route('/<config_str>/meta/<type>/<id>.json')
def meta_configured(config_str: str, type: str, id: str):
    return _handle_meta(type, id)


@app.route('/<config_str>/stream/<type>/<id>.json')
def stream_configured(config_str: str, type: str, id: str):
    return _handle_stream(type, id, config_str)


# ═══════════════════════════════════════════════════════════════════════════
# Route Handlers
# ═══════════════════════════════════════════════════════════════════════════

def _handle_catalog(type: str):
    if type not in MANIFEST['types']:
        abort(404)
    catalog = CATALOG.get(type, [])
    return respond_with({
        'metas': [{
            'id': item['id'], 'type': type, 'name': item['name'],
            'genres': item.get('genres', []), 'poster': item['poster']
        } for item in catalog]
    })


def _handle_catalog_genre(type: str, genre: str):
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


def _handle_catalog_search(type: str, query: str):
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


def _handle_meta(type: str, id: str):
    if type not in MANIFEST['types']:
        abort(404)
    item = next((i for i in CATALOG.get(type, []) if i['id'] == id), None)
    if not item:
        abort(404)

    videos = item.get('videos') or []

    if not videos:
        logger.error(f"Meta for {id} has 0 videos — returning 503 to avoid bad cache")
        resp = jsonify({'meta': {}, 'error': 'Catalog temporarily unavailable'})
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        return resp, 503

    meta = {k: item[k] for k in item if k in OPTIONAL_META}
    meta.update({
        'id': item['id'], 'type': type, 'name': item['name'],
        'genres': item['genres'], 'poster': item['poster'],
        'logo': item['logo'], 'background': item['background'],
        'videos': [{
            'id': f"{item['id']}:{v['season']}:{v['episode']}",
            'title': v['title'], 'thumbnail': v['thumbnail'],
            'season': v['season'], 'episode': v['episode'],
            'released': v.get('released', '2026-01-01T00:00:00.000Z'),
        } for v in videos]
    })

    resp = jsonify({'meta': meta})
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Headers'] = '*'
    resp.headers['Cache-Control'] = 'public, max-age=300'
    return resp


def _handle_stream(type: str, id: str, config_str):
    if type not in MANIFEST['types']:
        abort(404)

    cfg: dict = parse_config(config_str) if config_str else {'debrid': {}, 'enableP2P': True}
    debrid_cfg: dict = cfg.get('debrid', {})
    enable_p2p: bool = cfg.get('enableP2P', True)

    if not debrid_cfg and not enable_p2p:
        enable_p2p = True

    if ':' in id:
        try:
            series_id, season_s, episode_s = id.split(':')
            season = int(season_s)
            episode = int(episode_s)
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

    all_streams: list = []
    for video in videos:
        try:
            all_streams.extend(
                build_streams_for_video(video, series, season, debrid_cfg, enable_p2p)
            )
        except Exception as e:
            logger.error(f"Error building streams for {video.get('infoHash', '?')[:8]}: {e}")

    # Safety net: if nothing was built (no debrid AND p2p disabled),
    # still offer P2P so Stremio never shows a bare "no streams found".
    if not all_streams:
        logger.warning(f"No streams built for {id} — emitting P2P fallback")
        for video in videos:
            fallback = {
                'title': build_stream_title(video, '🔗 [P2P]'),
                'infoHash': video['infoHash'],
                'behaviorHints': {'bingeGroup': f"p2p-{series['id']}-{season}"}
            }
            if video.get('fileIdx') is not None:
                fallback['fileIdx'] = video.get('fileIdx', 0)
            if video.get('filename'):
                fallback['behaviorHints']['filename'] = video['filename']
            all_streams.append(fallback)

    resp = jsonify({'streams': all_streams})
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Headers'] = '*'
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    return resp


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
