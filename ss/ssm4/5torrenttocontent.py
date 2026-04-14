import time
import os
import csv
import glob
import hashlib
import struct
import socket
import random
import signal
import sys
import json
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

# ============================================================
# CONFIGURATION LOADING
# ============================================================

def load_config():
    """Load configuration from info.json (REQUIRED)"""
    config_file = 'info.json'
    
    print(f"DEBUG: Current working directory: {os.getcwd()}")
    print(f"DEBUG: Looking for config at: {os.path.abspath(config_file)}")
    
    if not os.path.isfile(config_file):
        print(f"[ERROR]: {config_file} not found!")
        print(f"   This file is required for the script to run.")
        print(f"   Please create {config_file} with the necessary configuration.")
        sys.exit(1)
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
            print(f"[LOADED] Configuration from {config_file}")
            if config.get('description'):
                print(f"  Description: {config['description']}")
            return config
    except json.JSONDecodeError as e:
        print(f"[ERROR]: Invalid JSON in {config_file}")
        print(f"   {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR]: Could not read {config_file}")
        print(f"   {e}")
        sys.exit(1)

# Load configuration at startup
CONFIG = load_config()

# Extract configuration values with clear error messages
try:
    quality = CONFIG['quality'].strip().upper()
    
    # Validate quality
    VALID_QUALITIES = ['SD', 'FHD', '4K']
    if quality not in VALID_QUALITIES:
        print(f"[ERROR]: Invalid quality '{quality}'")
        print(f"   Must be one of: {', '.join(VALID_QUALITIES)}")
        sys.exit(1)
    
    PEER_CONNECT_TIMEOUT = CONFIG['peer_settings']['peer_connect_timeout']
    PEER_METADATA_TIMEOUT = CONFIG['peer_settings']['peer_metadata_timeout']
    MAX_PEERS_TO_TRY = CONFIG['peer_settings']['max_peers_to_try']
    TRACKER_TIMEOUT = CONFIG['tracker_settings']['tracker_timeout']
    DHT_TIMEOUT = CONFIG['tracker_settings']['dht_timeout']
    VERBOSE = CONFIG['debug']['verbose']
    DEBUG_PEERS = CONFIG['debug']['debug_peers']
    
    # Per-magnet timeout: max seconds to spend resolving a single magnet link
    # Falls back to 90s if not specified in config
    PER_MAGNET_TIMEOUT = CONFIG.get('per_magnet_timeout', 90)
    
    # Overall script timeout: max total seconds this script will run
    # Falls back to 900s (15 min) if not specified in config
    OVERALL_SCRIPT_TIMEOUT = CONFIG.get('overall_script_timeout', 900)
    
except KeyError as e:
    print(f"[ERROR]: Missing required configuration key: {e}")
    print(f"   Please check your info.json file")
    sys.exit(1)

print(f"[QUALITY] Filter: {quality}")
print(f"  Will process directories ending with: {quality}")
print(f"[TIMEOUTS] Per-magnet: {PER_MAGNET_TIMEOUT}s | Overall script: {OVERALL_SCRIPT_TIMEOUT}s")
print()

# DHT Bootstrap nodes (kept for DHT discovery)
DHT_BOOTSTRAP_NODES = [
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881),
    ("dht.libtorrent.org", 25401),
]

peer_stats = {
    'connect_failed': 0,
    'connect_timeout': 0,
    'handshake_failed': 0,
    'no_extension': 0,
    'no_metadata_size': 0,
    'rejected': 0,
    'timeout_waiting': 0,
    'success': 0,
}

shutdown_requested = False
script_start_time = None  # Track when the script started

def signal_handler(sig, frame):
    global shutdown_requested
    print('\n\n[WARNING]: Ctrl+C detected - shutting down gracefully...')
    shutdown_requested = True

signal.signal(signal.SIGINT, signal_handler)

def check_overall_timeout():
    """Check if the overall script timeout has been exceeded."""
    global shutdown_requested, script_start_time
    if script_start_time and time.time() - script_start_time > OVERALL_SCRIPT_TIMEOUT:
        print(f"\n[TIMEOUT] Overall script timeout ({OVERALL_SCRIPT_TIMEOUT}s) exceeded - finishing up")
        shutdown_requested = True
        return True
    return False

def log(msg, level=1):
    if VERBOSE or level == 0:
        print(msg)

def debug(msg):
    if DEBUG_PEERS:
        print(f"      {msg}")

def is_public_ip(ip):
    try:
        parts = [int(p) for p in ip.split('.')]
        if parts[0] == 10:
            return False
        if parts[0] == 172 and 16 <= parts[1] <= 31:
            return False
        if parts[0] == 192 and parts[1] == 168:
            return False
        if parts[0] == 127:
            return False
        if parts[0] == 0:
            return False
        if parts[0] >= 224:
            return False
        return True
    except:
        return False

def extract_info_hash(magnet_uri):
    import re
    import base64
    
    match = re.search(r'btih:([a-fA-F0-9]{40})', magnet_uri)
    if match:
        return bytes.fromhex(match.group(1))
    
    match = re.search(r'btih:([A-Za-z2-7]{32})', magnet_uri)
    if match:
        return base64.b32decode(match.group(1).upper())
    
    return None

def extract_trackers(magnet_uri):
    import re
    from urllib.parse import unquote
    trackers = re.findall(r'tr=([^&]+)', magnet_uri)
    return [unquote(t) for t in trackers]

def format_size(size_bytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"

def size_to_gb(size_bytes):
    """Convert bytes to GB with 2 decimal places"""
    return round(size_bytes / (1024**3), 2)

# ============================================================
# BENCODE
# ============================================================

def bencode(obj):
    if isinstance(obj, int):
        return b'i' + str(obj).encode() + b'e'
    elif isinstance(obj, bytes):
        return str(len(obj)).encode() + b':' + obj
    elif isinstance(obj, str):
        encoded = obj.encode('utf-8')
        return str(len(encoded)).encode() + b':' + encoded
    elif isinstance(obj, list):
        return b'l' + b''.join(bencode(i) for i in obj) + b'e'
    elif isinstance(obj, dict):
        result = b'd'
        for k, v in sorted(obj.items()):
            if isinstance(k, str):
                k = k.encode('utf-8')
            result += bencode(k) + bencode(v)
        return result + b'e'
    else:
        raise TypeError(f"Cannot bencode {type(obj)}")

def bencode_decode(data):
    result, _ = bencode_decode_with_pos(data)
    return result

def bencode_decode_with_pos(data, pos=0):
    if data[pos:pos+1] == b'd':
        result = {}
        pos += 1
        while data[pos:pos+1] != b'e':
            key, pos = bencode_decode_with_pos(data, pos)
            value, pos = bencode_decode_with_pos(data, pos)
            result[key] = value
        return result, pos + 1
    elif data[pos:pos+1] == b'l':
        result = []
        pos += 1
        while data[pos:pos+1] != b'e':
            value, pos = bencode_decode_with_pos(data, pos)
            result.append(value)
        return result, pos + 1
    elif data[pos:pos+1] == b'i':
        end = data.index(b'e', pos)
        return int(data[pos+1:end]), end + 1
    elif data[pos:pos+1].isdigit():
        colon = data.index(b':', pos)
        length = int(data[pos:colon])
        start = colon + 1
        return data[start:start+length], start + length
    else:
        raise ValueError(f"Invalid bencode at position {pos}")

# ============================================================
# UDP TRACKER
# ============================================================

def get_peers_from_tracker(tracker_url, info_hash):
    global shutdown_requested
    if shutdown_requested:
        return []
    
    import re
    
    match = re.match(r'udp://([^:/]+):(\d+)', tracker_url)
    if not match:
        return []
    
    host, port = match.group(1), int(match.group(2))
    
    try:
        ip = socket.gethostbyname(host)
    except:
        return []
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(TRACKER_TIMEOUT)
    
    try:
        transaction_id = random.randint(0, 0xFFFFFFFF)
        connect_packet = struct.pack('>QII', 0x41727101980, 0, transaction_id)
        sock.sendto(connect_packet, (ip, port))
        
        response = sock.recv(16)
        action, trans_id, connection_id = struct.unpack('>IIQ', response)
        
        if action != 0 or trans_id != transaction_id:
            return []
        
        transaction_id = random.randint(0, 0xFFFFFFFF)
        announce_packet = struct.pack(
            '>QII20s20sQQQIIIiH',
            connection_id, 1, transaction_id,
            info_hash, os.urandom(20),
            0, 0, 0, 0, 0,
            random.randint(0, 0xFFFFFFFF),
            -1, 6881
        )
        
        sock.sendto(announce_packet, (ip, port))
        response = sock.recv(65535)
        
        if len(response) < 20:
            return []
        
        action = struct.unpack('>I', response[:4])[0]
        if action != 1:
            return []
        
        peers = []
        peer_data = response[20:]
        for i in range(0, len(peer_data), 6):
            if shutdown_requested:
                break
            if i + 6 <= len(peer_data):
                peer_ip = '.'.join(str(b) for b in peer_data[i:i+4])
                peer_port = struct.unpack('>H', peer_data[i+4:i+6])[0]
                if peer_port > 0 and is_public_ip(peer_ip):
                    peers.append((peer_ip, peer_port))
        
        return peers
        
    except:
        return []
    finally:
        sock.close()

# ============================================================
# DHT
# ============================================================

def get_peers_from_dht(info_hash):
    global shutdown_requested
    if shutdown_requested:
        return []
    
    log(f"    [DHT] Querying...")
    
    node_id = os.urandom(20)
    peers = set()
    queried = set()
    to_query = []
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(3)
    
    try:
        for host, port in DHT_BOOTSTRAP_NODES:
            if shutdown_requested:
                break
            try:
                ip = socket.gethostbyname(host)
                to_query.append((ip, port))
            except:
                continue
        
        tid = 0
        start_time = time.time()
        
        while to_query and time.time() - start_time < DHT_TIMEOUT and not shutdown_requested:
            addr = to_query.pop(0)
            if addr in queried:
                continue
            queried.add(addr)
            
            tid = (tid + 1) % 65536
            msg = bencode({
                b't': struct.pack('>H', tid),
                b'y': b'q',
                b'q': b'get_peers',
                b'a': {b'id': node_id, b'info_hash': info_hash}
            })
            
            try:
                sock.sendto(msg, addr)
                data, _ = sock.recvfrom(65536)
                response = bencode_decode(data)
                
                if b'r' in response:
                    r = response[b'r']
                    
                    if b'values' in r:
                        for peer_data in r[b'values']:
                            if shutdown_requested:
                                break
                            for i in range(0, len(peer_data), 6):
                                if i + 6 <= len(peer_data):
                                    ip = '.'.join(str(b) for b in peer_data[i:i+4])
                                    port = struct.unpack('>H', peer_data[i+4:i+6])[0]
                                    if port > 0 and is_public_ip(ip):
                                        peers.add((ip, port))
                    
                    if b'nodes' in r:
                        nodes_data = r[b'nodes']
                        for i in range(0, len(nodes_data), 26):
                            if shutdown_requested:
                                break
                            if i + 26 <= len(nodes_data):
                                ip = '.'.join(str(b) for b in nodes_data[i+20:i+24])
                                port = struct.unpack('>H', nodes_data[i+24:i+26])[0]
                                if port > 0 and is_public_ip(ip):
                                    new_addr = (ip, port)
                                    if new_addr not in queried:
                                        to_query.append(new_addr)
            except socket.timeout:
                continue
            except:
                continue
            
            if len(peers) >= 50:
                break
        
        log(f"    [DHT] Found {len(peers)} peers (queried {len(queried)} nodes)")
        return list(peers)
        
    finally:
        sock.close()

# ============================================================
# BEP 9 METADATA FETCH
# ============================================================

metadata_result = None

def fetch_metadata_from_peer_sync(info_hash, ip, port):
    global metadata_result, peer_stats, shutdown_requested
    
    if metadata_result is not None or shutdown_requested:
        return None
    
    peer_id_str = f"{ip}:{port}"
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(PEER_CONNECT_TIMEOUT)
        sock.connect((ip, port))
        sock.settimeout(PEER_METADATA_TIMEOUT)
        debug(f"{peer_id_str} - connected")
    except socket.timeout:
        peer_stats['connect_timeout'] += 1
        return None
    except Exception as e:
        peer_stats['connect_failed'] += 1
        return None
    
    try:
        # Handshake
        pstr = b'BitTorrent protocol'
        reserved = b'\x00\x00\x00\x00\x00\x10\x00\x01'
        peer_id = b'-PY0001-' + os.urandom(12)
        
        handshake = struct.pack('>B', len(pstr)) + pstr + reserved + info_hash + peer_id
        sock.sendall(handshake)
        
        try:
            response = sock.recv(68)
        except:
            peer_stats['handshake_failed'] += 1
            return None
        
        if len(response) < 68:
            peer_stats['handshake_failed'] += 1
            return None
        
        if not (response[25] & 0x10):
            debug(f"{peer_id_str} - no extension support")
            peer_stats['no_extension'] += 1
            return None
        
        debug(f"{peer_id_str} - supports extensions")
        
        # Extension handshake
        ext_hs = bencode({b'm': {b'ut_metadata': 1}, b'metadata_size': 0})
        ext_msg = b'\x00' + ext_hs
        sock.sendall(struct.pack('>IB', len(ext_msg) + 1, 20) + ext_msg)
        
        metadata_size = 0
        peer_ut_metadata = 0
        metadata_pieces = {}
        pieces_requested = False
        
        start_time = time.time()
        buffer = b''
        
        while time.time() - start_time < PEER_METADATA_TIMEOUT and not shutdown_requested:
            if metadata_result is not None:
                return None
            
            try:
                sock.settimeout(2)
                data = sock.recv(16384)
                if not data:
                    debug(f"{peer_id_str} - connection closed")
                    break
                buffer += data
            except socket.timeout:
                continue
            except Exception as e:
                debug(f"{peer_id_str} - recv error: {type(e).__name__}")
                break
            
            # Process messages
            while len(buffer) >= 4:
                length = struct.unpack('>I', buffer[:4])[0]
                if length == 0:
                    buffer = buffer[4:]
                    continue
                if length > 1000000:
                    buffer = b''
                    break
                if len(buffer) < 4 + length:
                    break
                
                msg = buffer[4:4+length]
                buffer = buffer[4+length:]
                
                msg_type = msg[0]
                
                if msg_type != 20:
                    continue
                
                ext_id = msg[1]
                payload = msg[2:]
                
                if ext_id == 0:  # Extension handshake from peer
                    try:
                        decoded = bencode_decode(payload)
                        
                        client = decoded.get(b'v', b'unknown')
                        if isinstance(client, bytes):
                            client = client.decode('utf-8', errors='replace')
                        debug(f"{peer_id_str} - client: {client}")
                        
                        metadata_size = decoded.get(b'metadata_size', 0)
                        debug(f"{peer_id_str} - metadata_size: {metadata_size}")
                        
                        if b'm' in decoded:
                            peer_ut_metadata = decoded[b'm'].get(b'ut_metadata', 0)
                            debug(f"{peer_id_str} - peer's ut_metadata ID: {peer_ut_metadata}")
                        
                        if metadata_size == 0:
                            debug(f"{peer_id_str} - peer has no metadata!")
                            peer_stats['no_metadata_size'] += 1
                            return None
                        
                        if metadata_size > 0 and peer_ut_metadata > 0 and not pieces_requested:
                            num_pieces = (metadata_size + 16383) // 16384
                            debug(f"{peer_id_str} - requesting {num_pieces} pieces (using ext_id={peer_ut_metadata})")
                            
                            for piece in range(num_pieces):
                                req_payload = bencode({b'msg_type': 0, b'piece': piece})
                                full_msg = struct.pack('>IB', len(req_payload) + 2, 20) + bytes([peer_ut_metadata]) + req_payload
                                sock.sendall(full_msg)
                            
                            pieces_requested = True
                            debug(f"{peer_id_str} - sent all piece requests")
                            
                    except Exception as e:
                        debug(f"{peer_id_str} - ext handshake error: {e}")
                
                elif ext_id == 1:
                    try:
                        decoded, end_pos = bencode_decode_with_pos(payload)
                        msg_type_inner = decoded.get(b'msg_type', -1)
                        
                        if msg_type_inner == 1:  # Data
                            piece_idx = decoded.get(b'piece', 0)
                            piece_data = payload[end_pos:]
                            metadata_pieces[piece_idx] = piece_data
                            
                            num_pieces = (metadata_size + 16383) // 16384
                            debug(f"{peer_id_str} - got piece {piece_idx + 1}/{num_pieces} ({len(piece_data)} bytes)")
                            
                            if len(metadata_pieces) == num_pieces:
                                full = b''.join(metadata_pieces[i] for i in range(num_pieces))
                                
                                if hashlib.sha1(full).digest() == info_hash:
                                    debug(f"{peer_id_str} - [VERIFIED] METADATA!")
                                    peer_stats['success'] += 1
                                    return bencode_decode(full)
                                else:
                                    debug(f"{peer_id_str} - hash mismatch!")
                        
                        elif msg_type_inner == 2:  # Reject
                            debug(f"{peer_id_str} - REJECTED")
                            peer_stats['rejected'] += 1
                            return None
                            
                    except Exception as e:
                        debug(f"{peer_id_str} - metadata parse error: {e}")
        
        debug(f"{peer_id_str} - timeout (got {len(metadata_pieces)}/{(metadata_size + 16383) // 16384 if metadata_size else '?'} pieces)")
        peer_stats['timeout_waiting'] += 1
        return None
        
    except Exception as e:
        debug(f"{peer_id_str} - error: {e}")
        return None
    finally:
        sock.close()

def fetch_metadata_parallel(info_hash, peers):
    global metadata_result, peer_stats, shutdown_requested
    
    if shutdown_requested:
        return None
    
    metadata_result = None
    
    for key in peer_stats:
        peer_stats[key] = 0
    
    if not peers:
        return None
    
    peers = list(peers)
    random.shuffle(peers)
    peers = peers[:MAX_PEERS_TO_TRY]
    
    log(f"    Trying {len(peers)} peers (20 parallel)...")
    
    def worker(peer):
        global metadata_result, shutdown_requested
        if metadata_result is not None or shutdown_requested:
            return None
        result = fetch_metadata_from_peer_sync(info_hash, peer[0], peer[1])
        if result is not None:
            metadata_result = result
        return result
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(worker, peer) for peer in peers]
        
        for future in futures:
            if shutdown_requested:
                break
            try:
                future.result(timeout=PEER_METADATA_TIMEOUT + 5)
            except:
                pass
            if metadata_result is not None:
                break
    
    log(f"\n    === Stats ===")
    log(f"    Connect fail/timeout: {peer_stats['connect_failed']}/{peer_stats['connect_timeout']}")
    log(f"    No extension/metadata: {peer_stats['no_extension']}/{peer_stats['no_metadata_size']}")
    log(f"    Rejected/Timeout: {peer_stats['rejected']}/{peer_stats['timeout_waiting']}")
    log(f"    Success: {peer_stats['success']}")
    
    return metadata_result

# ============================================================
# MAIN
# ============================================================

def get_all_peers(info_hash, trackers):
    global shutdown_requested
    if shutdown_requested:
        return []
    
    all_peers = set()
    
    udp_trackers = [t for t in trackers if 'udp://' in t]
    log(f"    [TRACKERS] Querying {len(udp_trackers)} trackers...")
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_peers_from_tracker, t, info_hash) for t in udp_trackers]
        for future in futures:
            if shutdown_requested:
                break
            try:
                result = future.result(timeout=TRACKER_TIMEOUT + 2)
                if result:
                    all_peers.update(result)
            except:
                pass
    
    log(f"    [TRACKERS] Found {len(all_peers)} peers")
    
    if not shutdown_requested:
        dht_peers = get_peers_from_dht(info_hash)
        all_peers.update(dht_peers)
    
    return list(all_peers)

def magnet_to_torrent_info(magnet_uri):
    global shutdown_requested
    if shutdown_requested:
        return None
    
    info_hash = extract_info_hash(magnet_uri)
    if not info_hash:
        print(f"  [FAILED] Could not extract info hash")
        return None
    
    print(f"  Info hash: {info_hash.hex().upper()}")
    
    trackers = extract_trackers(magnet_uri)
    
    if not trackers:
        print("  [WARNING] No trackers found in magnet link")
        return None
    
    peers = get_all_peers(info_hash, trackers)
    
    if shutdown_requested:
        return None
    
    log(f"    Total: {len(peers)} unique peers")
    
    if not peers:
        return None
    
    metadata = fetch_metadata_parallel(info_hash, peers)
    return metadata

def resolve_magnet_with_timeout(magnet_link, timeout_seconds):
    """
    Resolve a single magnet link with a hard timeout.
    Uses a thread pool to enforce the timeout — if the resolution
    takes longer than timeout_seconds, we give up on this magnet.
    Returns metadata dict or None.
    """
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(magnet_to_torrent_info, magnet_link)
        try:
            return future.result(timeout=timeout_seconds)
        except FuturesTimeoutError:
            print(f"  [SKIPPED] Magnet timed out after {timeout_seconds}s — moving on")
            return None
        except Exception as e:
            print(f"  [SKIPPED] Magnet error: {e}")
            return None

def extract_files_with_sizes(metadata):
    files = []
    
    if b'files' in metadata:
        for file_info in metadata[b'files']:
            path_parts = file_info.get(b'path', [])
            filepath = '/'.join(p.decode('utf-8', errors='replace') for p in path_parts)
            size = file_info.get(b'length', 0)
            files.append((filepath, size))
    else:
        name = metadata.get(b'name', b'unknown').decode('utf-8', errors='replace')
        size = metadata.get(b'length', 0)
        files.append((name, size))
    
    return files

def process_csv_files():
    global shutdown_requested, script_start_time
    
    script_start_time = time.time()
    
    content_file_path = 'content.csv'

    if not os.path.isfile(content_file_path):
        with open(content_file_path, 'w', newline='', encoding='utf-8') as content_file:
            content_writer = csv.writer(content_file)
            content_writer.writerow(['torrent file name', 'filename within torrent', 'infohash', 'file index', 'filesize_gb'])

    # Get all directories
    all_dirs = [d for d in os.listdir('.') if os.path.isdir(d)]
    
    # Filter directories by quality
    matching_dirs = []
    skipped_dirs = []
    
    for subdir in all_dirs:
        if not subdir.startswith('2'):
            continue
        
        if subdir.endswith(quality):
            matching_dirs.append(subdir)
        else:
            skipped_dirs.append(subdir)
    
    # Sort for consistent processing
    matching_dirs.sort()
    
    if matching_dirs:
        print(f"\n[DIRS] Found {len(matching_dirs)} directories matching quality '{quality}':")
        for d in matching_dirs:
            print(f"   • {d}")
    else:
        print(f"\n[WARNING] No directories found matching quality '{quality}'")
    
    if skipped_dirs:
        print(f"\n[JUMPED] Skipping {len(skipped_dirs)} directories (wrong quality):")
        for d in skipped_dirs[:5]:  # Show first 5
            print(f"   • {d}")
        if len(skipped_dirs) > 5:
            print(f"   ... and {len(skipped_dirs) - 5} more")
    
    print()
    
    magnets_skipped = 0
    magnets_resolved = 0
    magnets_failed = 0
    
    for subdir in matching_dirs:
        if shutdown_requested or check_overall_timeout():
            print("\n[WARNING] Shutdown/timeout requested - stopping processing")
            break
        
        csv_files = sorted(glob.glob(os.path.join(subdir, '*.csv')))
        
        for csv_file in csv_files:
            if shutdown_requested or check_overall_timeout():
                print("\n[WARNING] Shutdown/timeout requested - stopping current file")
                break
            
            print(f"\n{'='*60}")
            print(f"Processing: {csv_file}")
            print(f"Directory quality: {subdir.split(quality)[0]}...{quality}")
            elapsed_total = time.time() - script_start_time
            remaining = OVERALL_SCRIPT_TIMEOUT - elapsed_total
            print(f"Script time: {elapsed_total:.0f}s elapsed, ~{remaining:.0f}s remaining")
            print('='*60)
            
            all_magnets_successful = True
            
            with open(csv_file, 'r', encoding='utf-8') as file, \
                 open(content_file_path, 'a', newline='', encoding='utf-8') as content_file:
                
                content_writer = csv.writer(content_file)
                
                for line_num, line in enumerate(file, 1):
                    if shutdown_requested or check_overall_timeout():
                        print("\n[WARNING] Shutdown/timeout requested - stopping current file")
                        all_magnets_successful = False
                        break
                    
                    line = line.strip()
                    if not line:
                        continue
                    
                    parts = line.rsplit(',', 2)
                    if len(parts) != 3:
                        print(f"  [FAILED] Line {line_num}: Incorrect format")
                        continue
                    
                    torrent_name, infohash, magnet_link = parts
                    torrent_name = torrent_name.strip()
                    infohash = infohash.strip()
                    magnet_link = magnet_link.strip()

                    print(f"\n[{line_num}] {torrent_name[:55]}...")
                    
                    start_time = time.time()
                    
                    # Use per-magnet timeout to skip slow magnets
                    metadata = resolve_magnet_with_timeout(magnet_link, PER_MAGNET_TIMEOUT)
                    
                    elapsed = time.time() - start_time
                    
                    if shutdown_requested or check_overall_timeout():
                        all_magnets_successful = False
                        break
                    
                    if metadata is None:
                        all_magnets_successful = False
                        if elapsed >= PER_MAGNET_TIMEOUT - 1:
                            magnets_skipped += 1
                            print(f"  [SKIPPED] Timed out after {elapsed:.1f}s (limit: {PER_MAGNET_TIMEOUT}s)")
                        else:
                            magnets_failed += 1
                            print(f"  [FAILED] ({elapsed:.1f}s)")
                        continue

                    magnets_resolved += 1
                    files = extract_files_with_sizes(metadata)
                    total_size = sum(size for _, size in files)
                    
                    print(f"  [SUCCESS] Got metadata in {elapsed:.1f}s - {format_size(total_size)} in {len(files)} file(s)")
                    
                    for file_index, (filepath, size) in enumerate(files):
                        size_gb = size_to_gb(size)
                        content_writer.writerow([torrent_name, filepath, infohash, file_index, size_gb])
                        print(f'    [{file_index}] {filepath} ({format_size(size)}, {size_gb} GB)')

            if shutdown_requested or check_overall_timeout():
                print(f'\n[WARNING] Interrupted - not archiving {csv_file}')
                break
            
            if all_magnets_successful:
                archive_file_path = csv_file.rsplit('.', 1)[0] + '.archive'
                os.rename(csv_file, archive_file_path)
                print(f'\n[SUCCESS] Archived: {csv_file}')
            else:
                print(f'\n[PARTIAL] Not archiving {csv_file} - some magnets failed/skipped')
    
    # Print summary
    total_time = time.time() - script_start_time
    print(f"\n{'='*60}")
    print(f"[SUMMARY]")
    print(f"  Total time: {total_time:.1f}s")
    print(f"  Resolved:   {magnets_resolved}")
    print(f"  Failed:     {magnets_failed}")
    print(f"  Skipped:    {magnets_skipped} (per-magnet timeout)")
    if shutdown_requested:
        print(f"  Status:     Stopped early (shutdown/timeout)")
    else:
        print(f"  Status:     Completed normally")
    print(f"{'='*60}")

if __name__ == '__main__':
    try:
        process_csv_files()
    except KeyboardInterrupt:
        print('\n\n[WARNING] Force interrupted')
        sys.exit(1)