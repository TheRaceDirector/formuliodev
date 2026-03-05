import ast
import logging
import os
import subprocess
import time
from threading import Thread
from flask import Flask, jsonify, abort, send_from_directory, request
from werkzeug.middleware.proxy_fix import ProxyFix
from functools import wraps
import signal
from logging.handlers import RotatingFileHandler
import urllib.parse

# Enhanced logging setup
log_dir = os.path.expanduser('~/.formulio_logs')
log_file = os.path.join(log_dir, 'app.log')

# Ensure the log directory exists
if not os.path.exists(log_dir):
    try:
        os.makedirs(log_dir, exist_ok=True)
    except PermissionError:
        log_dir = '/tmp/formulio_logs'
        log_file = os.path.join(log_dir, 'app.log')
        os.makedirs(log_dir, exist_ok=True)

handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Configuration management
class Config:
    SCRIPT_INTERVAL = 909  # 15 minutes & 9 seconds
    MAX_REQUESTS_PER_MINUTE = 60

config = Config()

# Flask app setup with static folder specified
app = Flask(__name__, static_folder='static')
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

MANIFEST = {
    'id': 'org.stremio.formulio',
    'version': '2.0.4',
    'name': 'Formulio',
    'description': 'An Addon for Motor Racing Replay Content. (This addon only displays content from external sources. Use this Stremio torrent addon only where legally permitted. Users are responsible for complying with all applicable laws in their jurisdiction)',
    'logo': 'https://i.postimg.cc/5tTmz4jb/formulio1.png',
    'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
    'behaviorHints': {
        'configurable': True,
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

# P2P version of the manifest
MANIFEST_P2P = MANIFEST.copy()
MANIFEST_P2P.update({
    'id': 'org.stremio.formulio.p2p',  # Different ID
    'name': 'Formulio P2P',            # Different name
    'description': 'An Addon for Motor Racing Replay Content (P2P Version with exclusive content). (This addon only displays content from external sources. Use this Stremio torrent addon only where legally permitted. Users are responsible for complying with all applicable laws in their jurisdiction)',
})

# P2P exclusive series list
P2P_EXCLUSIVE_SERIES_LIST = [
    # Add your exclusive series here if needed
]

CATALOG = {
    'series': [
        {
            'id': 'hpytt0202601',
            'name': 'Sky F1 - FHD',
            'description': 'If you receive a StremThru playback error\nUse >Formulio P2P< source\nbig addon update happening for 2026 formulio@tuta.io',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/g2d9tyXS/sky1.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'type': 'movie',
            'genres': ['Formula Racing'],
            'videos': []
        },
        {
            'id': 'hpytt0202602',
            'name': 'Sky F1 - FHD-2',
            'description': 'If you receive a StremThru playback error\nUse >Formulio P2P< source\nbig addon update happening for 2026 formulio@tuta.io',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/KYMnKTQb/sky2.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'],
            'videos': []
        },
        {
            'id': 'hpytt0202603',
            'name': 'F1TV - English',
            'description': 'If you receive a StremThru playback error\nUse >Formulio P2P< source\nbig addon update happening for 2026 formulio@tuta.io',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/pXf4j9GD/f1tveng.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'],
            'videos': []
        },
        {
            'id': 'hpytt0202604',
            'name': 'F1TV - International',
            'description': 'If you receive a StremThru playback error\nUse >Formulio P2P< source\nbig addon update happening for 2026 formulio@tuta.io',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/1zjjSDXZ/f1tvint.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'],
            'videos': []
        },
        {
            'id': 'hpytt0202605',
            'name': 'Sky F1 - 4K',
            'description': 'If you receive a StremThru playback error\nUse >Formulio P2P< source\nbig addon update happening for 2026 formulio@tuta.io',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/zfPNXN1H/sky14k.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'],
            'videos': []
        },
        {
            'id': 'hpytt0202606',
            'name': 'Sky F1 - 4K-2',
            'description': 'If you receive a StremThru playback error\nUse >Formulio P2P< source\nbig addon update happening for 2026 formulio@tuta.io',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/ry4Tc7Zz/sky24k.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'],
            'videos': []
        },
        {
            'id': 'hpytt0202607',
            'name': 'Sky F1 - SD',
            'description': 'If you receive a StremThru playback error\nUse >Formulio P2P< source\nbig addon update happening for 2026 formulio@tuta.io',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/Pqcn5Vvx/sky2sd.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'],
            'videos': []
        },
        {
            'id': 'hpytt0202611',
            'name': 'MotoGP - FHD',
            'description': 'If you receive a StremThru playback error\nUse >Formulio P2P< source\nbig addon update happening for 2026 formulio@tuta.io',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/3Rpyv1D8/motogphd.jpg',
            'logo': 'https://i.postimg.cc/nh8PKc5n/moto.png',
            'background': 'https://i.postimg.cc/fR252zq3/motobackground.jpg',
            'genres': ['Moto Racing'],
            'videos': []
        },
        {
            'id': 'hpytt0202612',
            'name': 'MotoGP - 4K',
            'description': 'If you receive a StremThru playback error\nUse >Formulio P2P< source\nbig addon update happening for 2026 formulio@tuta.io',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/MHmvsGDg/motogp4k.jpg',
            'logo': 'https://i.postimg.cc/nh8PKc5n/moto.png',
            'background': 'https://i.postimg.cc/fR252zq3/motobackground.jpg',
            'genres': ['Moto Racing'],
            'videos': []
        },
        {
            'id': 'hpytt0202613',
            'name': 'MotoGP - SD',
            'description': 'If you receive a StremThru playback error\nUse >Formulio P2P< source\nbig addon update happening for 2026 formulio@tuta.io',
            'releaseInfo': '2026',
            'poster': 'https://i.postimg.cc/qqTNXK88/motogpsd.jpg',
            'logo': 'https://i.postimg.cc/nh8PKc5n/moto.png',
            'background': 'https://i.postimg.cc/fR252zq3/motobackground.jpg',
            'genres': ['Moto Racing'],
            'videos': []
#        },
#        {
#            'id': 'hpytt0202614',
#            'name': 'WSBK - FHD',
#            'description': 'Configure backup & faster playback via P2P > formulio.hayd.uk\nAusGP ticket or email me\nformulio@tuta.io',
#            'releaseInfo': '2026',
#            'poster': '',
#            'logo': '',
#            'background': '',
#            'genres': ['Moto Racing'],
#            'videos': []
        }
    ]
}

# Keep this as it's used in the addon_meta function
OPTIONAL_META = ["posterShape", "background", "logo", "videos", "description", "releaseInfo", "imdbRating", "director", "cast",
                 "dvdRelease", "released", "inTheaters", "certification", "runtime", "language", "country", "awards", "website", "isPeered"]

def respond_with(data):
    resp = jsonify(data)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Headers'] = '*'
    return resp

def load_videos(filepath):
    videos = []
    try:
        with open(filepath, 'r') as file:
            content = file.read()
            content = f"{{{content}}}"
            videos_dict = ast.literal_eval(content)
            
            # Sort by the full_id to ensure proper ordering
            sorted_items = sorted(videos_dict.items(), key=lambda x: x[0])
            
            for full_id, video_info in sorted_items:
                series_id, season, episode = full_id.split(':')
                
                video_obj = {
                    'id': series_id,
                    'season': int(season),
                    'episode': int(episode),
                    'title': video_info[0]['title'],
                    'thumbnail': video_info[0]['thumbnail'],
                    'infoHash': video_info[0]['infoHash']
                }
                
                # Add fileIdx if it exists
                if 'fileIdx' in video_info[0]:
                    video_obj['fileIdx'] = video_info[0]['fileIdx']
                
                # Add filename if it exists and is not empty
                if 'filename' in video_info[0] and video_info[0]['filename']:
                    video_obj['filename'] = video_info[0]['filename']
                
                videos.append(video_obj)
    except FileNotFoundError:
        logger.error(f"File {filepath} not found.")
    except (ValueError, KeyError, IndexError) as e:
        logger.error(f"Error parsing {filepath}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error reading {filepath}: {str(e)}")
    
    return videos

def run_scripts_in_loop():
    directories = ['egor', 'egor/ego', 'smcg', 'smcg/smc', 'ss', 'ss/ssf', 'ss/ssm', 'egor/eg4', 'smcg/sm4', 'smcg/sms', 'smcm', 'smcm/sm4', 'smcm/smc', 'smcm/sms', 'sam', 'sam/wsbk']
    file_mod_times = {}

    logger.info("Starting run_scripts_in_loop")

    for directory in directories:
        filepath = os.path.join(directory, '6processed.txt')
        if os.path.exists(filepath):
            file_mod_times[directory] = os.path.getmtime(filepath)
            logger.info(f"Existing file found: {filepath}")
        else:
            logger.warning(f"{filepath} not found. Attempting to run script to generate it.")
            script_path = os.path.join(directory, '1formationlap.py')
            try:
                result = subprocess.run(['python3', script_path], check=True, capture_output=True, text=True)
                logger.info(f"Initial run of {script_path}:")
                logger.info(f"STDOUT: {result.stdout}")
                logger.info(f"STDERR: {result.stderr}")
                if os.path.exists(filepath):
                    file_mod_times[directory] = os.path.getmtime(filepath)
                    logger.info(f"File generated successfully: {filepath}")
                else:
                    logger.error(f"Script at {script_path} did not generate the expected file.")
            except subprocess.CalledProcessError as e:
                logger.error(f"Error running {script_path}: {e.stderr}")

    while True:
        logger.info("Starting new iteration of script execution loop")
        for directory in directories:
            script_path = os.path.join(directory, '1formationlap.py')
            logger.info(f"Running {script_path}")
            try:
                result = subprocess.run(['python3', script_path], check=True, capture_output=True, text=True)
                logger.info(f"Execution of {script_path} completed")
                logger.debug(f"STDOUT from {script_path}:")
                for line in result.stdout.splitlines():
                    logger.debug(line)
                if result.stderr:
                    logger.warning(f"STDERR from {script_path}:")
                    for line in result.stderr.splitlines():
                        logger.warning(line)

                filepath = os.path.join(directory, '6processed.txt')
                if os.path.exists(filepath):
                    new_mod_time = os.path.getmtime(filepath)
                    if new_mod_time != file_mod_times.get(directory, 0):
                        file_mod_times[directory] = new_mod_time
                        logger.info(f"File {filepath} has been updated. Triggering server restart.")
                        restart_server()
                    else:
                        logger.info(f"No changes detected in {filepath}")
                else:
                    logger.error(f"Expected output file not found: {filepath}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Error running {script_path}: {e.stderr}")
            except FileNotFoundError:
                logger.error(f"Script not found: {script_path}")
            except Exception as e:
                logger.error(f"Unexpected error while processing {script_path}: {str(e)}")

        logger.info(f"Sleeping for {config.SCRIPT_INTERVAL} seconds before next iteration")
        time.sleep(config.SCRIPT_INTERVAL)

def restart_server():
    with app.app_context():
        try:
            CATALOG['series'][0]['videos'] = load_videos('./egor/ego/6processed.txt')
            CATALOG['series'][1]['videos'] = load_videos('./smcg/smc/6processed.txt')
            CATALOG['series'][2]['videos'] = load_videos('./ss/ssf/6processed.txt')
            CATALOG['series'][3]['videos'] = load_videos('./ss/ssm/6processed.txt')
            CATALOG['series'][4]['videos'] = load_videos('./egor/eg4/6processed.txt')
            CATALOG['series'][5]['videos'] = load_videos('./smcg/sm4/6processed.txt')
            CATALOG['series'][6]['videos'] = load_videos('./smcg/sms/6processed.txt')
            CATALOG['series'][7]['videos'] = load_videos('./smcm/smc/6processed.txt')
            CATALOG['series'][8]['videos'] = load_videos('./smcm/sm4/6processed.txt')
            CATALOG['series'][9]['videos'] = load_videos('./smcm/sms/6processed.txt')
            CATALOG['series'][10]['videos'] = load_videos('./sam/wsbk/6processed.txt')
            
            # Uncomment if you add P2P exclusive content
            # for i, series in enumerate(P2P_EXCLUSIVE_SERIES_LIST):
            #    P2P_EXCLUSIVE_SERIES_LIST[i]['videos'] = load_videos('./path/to/exclusive/content.txt')
            
            logger.info("Server restarted with new content.")
        except Exception as e:
            logger.error(f"Error during server restart: {e}")


@app.route('/manifest.json')
def addon_manifest():
    version = request.args.get('v', 'default')
    
    if version == 'p2p':
        return respond_with(MANIFEST_P2P)
    else:
        return respond_with(MANIFEST)

@app.route('/catalog/<type>/<id>.json')
def addon_catalog(type, id):
    if type not in MANIFEST['types']:
        abort(404)
    
    # Get the version from query parameters
    version = request.args.get('v', 'default')
    genre = request.args.get('genre')
    
    # Start with the regular catalog
    catalog = CATALOG.get(type, []).copy()
    
    # Add P2P exclusive content if requesting the P2P version
    if version == 'p2p':
        catalog.extend(P2P_EXCLUSIVE_SERIES_LIST)
    
    # Filter by genre if provided
    if genre:
        catalog = [item for item in catalog if genre in item.get('genres', [])]
    
    metaPreviews = {
        'metas': [
            {
                'id': item['id'],
                'type': type,
                'name': item['name'],
                'genres': item.get('genres', []),
                'poster': item['poster']
            } for item in catalog
        ]
    }
    return respond_with(metaPreviews)

# This route needs to be restored to handle Stremio's direct genre filtering paths
@app.route('/catalog/<type>/<id>/genre=<genre>.json')
def addon_catalog_filtered(type, id, genre):
    if type not in MANIFEST['types']:
        abort(404)
    
    # URL decode the genre parameter
    genre = urllib.parse.unquote(genre)
    version = request.args.get('v', 'default')
    
    # Start with the regular catalog
    catalog = CATALOG.get(type, []).copy()
    
    # Add P2P exclusive content if requesting the P2P version
    if version == 'p2p':
        catalog.extend(P2P_EXCLUSIVE_SERIES_LIST)
    
    # Filter catalog items by the requested genre
    filtered_items = [item for item in catalog if genre in item.get('genres', [])]
    
    metaPreviews = {
        'metas': [
            {
                'id': item['id'],
                'type': type,
                'name': item['name'],
                'genres': item.get('genres', []),
                'poster': item['poster']
            } for item in filtered_items
        ]
    }
    return respond_with(metaPreviews)

@app.route('/meta/<type>/<id>.json')
def addon_meta(type, id):
    if type not in MANIFEST['types']:
        abort(404)
    
    version = request.args.get('v', 'default')
    
    def mk_item(item):
        meta = {key: item[key] for key in item.keys() if key in OPTIONAL_META}
        meta['id'] = item['id']
        meta['type'] = type
        meta['name'] = item['name']
        meta['genres'] = item['genres']
        meta['poster'] = item['poster']
        meta['logo'] = item['logo']
        meta['background'] = item['background']
        meta['videos'] = [{'id': f"{item['id']}:{video['season']}:{video['episode']}",
                           'title': video['title'],
                           'thumbnail': video['thumbnail'],
                           'season': video['season'],
                           'episode': video['episode']} for video in item['videos']]
        return meta
    
    # Look in regular catalog
    item = next((item for item in CATALOG[type] if item['id'] == id), None)
    
    # If not found and P2P version, check if it's one of the exclusive series
    if item is None and version == 'p2p':
        item = next((series for series in P2P_EXCLUSIVE_SERIES_LIST if series['id'] == id), None)
    
    if item is None:
        abort(404)
        
    meta = {'meta': mk_item(item)}
    return respond_with(meta)

@app.route('/stream/<type>/<id>.json')
def addon_stream(type, id):
    if type not in MANIFEST['types']:
        abort(404)
    
    version = request.args.get('v', 'default')
    
    # Handle both formats: with or without season/episode
    if ':' in id:
        try:
            series_id, season, episode = id.split(':')
            season, episode = int(season), int(episode)
        except ValueError:
            logger.error(f"Invalid ID format: {id}")
            abort(400)
    else:
        series_id = id
        season = 1
        episode = 1
    
    streams = {'streams': []}
    
    # Look in regular catalog
    series_list = CATALOG.get(type, [])
    
    # Add exclusive P2P series if appropriate
    if version == 'p2p':
        p2p_exclusive_ids = [series['id'] for series in P2P_EXCLUSIVE_SERIES_LIST]
        if series_id in p2p_exclusive_ids:
            series_list = list(series_list) + P2P_EXCLUSIVE_SERIES_LIST
    
    for series in series_list:
        if series['id'] == series_id:
            if ':' in id:
                for video in series['videos']:
                    if video['season'] == season and video['episode'] == episode:
                        stream = {
                            'title': video['title'],
                            'thumbnail': video['thumbnail'],
                            'infoHash': video['infoHash'],
                            'behaviorHints': {
                                'bingeGroup': f"{series['id']}-{season}"
                            }
                        }
                        
                        # Add fileIdx if it exists
                        if 'fileIdx' in video:
                            stream['fileIdx'] = video['fileIdx']
                        
                        # Add filename both at root and in behaviorHints for maximum compatibility
                        if 'filename' in video and video['filename']:
                            stream['filename'] = video['filename']
                            stream['behaviorHints']['filename'] = video['filename']
                        
                        streams['streams'].append(stream)
                        break
            else:
                for video in series['videos']:
                    stream = {
                        'title': video['title'],
                        'thumbnail': video['thumbnail'],
                        'infoHash': video['infoHash'],
                        'behaviorHints': {
                            'bingeGroup': f"{series['id']}-{video['season']}"
                        }
                    }
                    
                    # Add fileIdx if it exists
                    if 'fileIdx' in video:
                        stream['fileIdx'] = video['fileIdx']
                    
                    # Add filename both at root and in behaviorHints
                    if 'filename' in video and video['filename']:
                        stream['filename'] = video['filename']
                        stream['behaviorHints']['filename'] = video['filename']
                    
                    streams['streams'].append(stream)
    
    if not streams['streams']:
        abort(404)
    
    return respond_with(streams)

@app.route('/catalog/<type>/<id>/search=<query>.json')
def addon_catalog_search(type, id, query):
    if type not in MANIFEST['types']:
        abort(404)
    
    # URL decode the search query
    query = urllib.parse.unquote(query).lower()
    version = request.args.get('v', 'default')
    
    # Start with the regular catalog
    catalog = CATALOG.get(type, []).copy()
    
    # Add P2P exclusive content if requesting the P2P version
    if version == 'p2p':
        catalog.extend(P2P_EXCLUSIVE_SERIES_LIST)
    
    # Find items that match the search query in name, description, or other relevant fields
    search_results = []
    for item in catalog:
        # Check if query appears in the name or description
        if (query in item['name'].lower() or 
            (item.get('description') and query in item['description'].lower())):
            search_results.append(item)
        
        # Also search in video titles for series content
        elif 'videos' in item:
            for video in item['videos']:
                if 'title' in video and query in video['title'].lower():
                    search_results.append(item)
                    break
    
    metaPreviews = {
        'metas': [
            {
                'id': item['id'],
                'type': type,
                'name': item['name'],
                'genres': item.get('genres', []),
                'poster': item['poster']
            } for item in search_results
        ]
    }
    return respond_with(metaPreviews)

@app.route('/images/<path:filename>')
def static_files(filename):
    return send_from_directory('images', filename)

@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')

def graceful_shutdown(signum, frame):
    logger.info("Received shutdown signal. Shutting down gracefully...")
    # Perform any cleanup operations here
    exit(0)

if __name__ == '__main__':
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)

    # Start the script-running loop in a separate thread
    Thread(target=run_scripts_in_loop, daemon=True).start()

    # Load initial video data
    try:
        CATALOG['series'][0]['videos'] = load_videos('./egor/ego/6processed.txt')
        CATALOG['series'][1]['videos'] = load_videos('./smcg/smc/6processed.txt')
        CATALOG['series'][2]['videos'] = load_videos('./ss/ssf/6processed.txt')
        CATALOG['series'][3]['videos'] = load_videos('./ss/ssm/6processed.txt')
        CATALOG['series'][4]['videos'] = load_videos('./egor/eg4/6processed.txt')
        CATALOG['series'][5]['videos'] = load_videos('./smcg/sm4/6processed.txt')
        CATALOG['series'][6]['videos'] = load_videos('./smcg/sms/6processed.txt')
        CATALOG['series'][7]['videos'] = load_videos('./smcm/smc/6processed.txt')
        CATALOG['series'][8]['videos'] = load_videos('./smcm/sm4/6processed.txt')
        CATALOG['series'][9]['videos'] = load_videos('./smcm/sms/6processed.txt')
        CATALOG['series'][10]['videos'] = load_videos('./sam/wsbk/6processed.txt')
        
        # Uncomment if you add P2P exclusive content
        # for i, series in enumerate(P2P_EXCLUSIVE_SERIES_LIST):
        #    P2P_EXCLUSIVE_SERIES_LIST[i]['videos'] = load_videos('./path/to/exclusive/content.txt')
    except Exception as e:
        logger.error(f"Error loading initial video data: {e}")

    # Start the Flask server
    app.run(host='0.0.0.0', port=8000)
