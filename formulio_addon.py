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

# Enhanced logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration management
class Config:
    SCRIPT_INTERVAL = 909  # 15 minutes 9 seconds
    MAX_REQUESTS_PER_MINUTE = 60

config = Config()

# Flask app setup with static folder specified
app = Flask(__name__, static_folder='static')
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

MANIFEST = {
    'id': 'org.stremio.formulio',
    'version': '1.0.0',
    'name': 'Formulio',
    'description': 'An Addon for Formula One Replay Content.  (This addon only displays content from external sources. Use this Stremio torrent addon only where legally permitted. Users are responsible for complying with all applicable laws in their jurisdiction)',
    'types': ['series'],
    'catalogs': [
        {'type': 'series', 'id': 'formulio-series'}
    ],
    'resources': [
        'catalog',
        {'name': "meta", 'types': ["series"], 'idPrefixes': ["hpy"]},
        {'name': 'stream', 'types': ['series'], 'idPrefixes': ['tt', 'hpy']}
    ]
}

CATALOG = {
    'series': [
        {
            'id': 'hpytt0202401',
            'name': 'SkyF1 UK1',
            'description': 'SkyF1 2024 F1 Season - 1080p 50fps (egortech) \n \nTip: for optimal playback, ensure your TV streaming device is in 50Hz mode',
            'releaseInfo': '2024',
            'poster': 'https://i.postimg.cc/g2d9tyXS/sky1.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'],
            'videos': []
        },
        {
            'id': 'hpytt0202402',
            'name': 'SkyF1 UK2',
            'description': 'SkyF1 2024 F1 Season - 1080p 50fps (smcgill1969) \n \nTip: for optimal playback, ensure your TV streaming device is in 50Hz mode',
            'releaseInfo': '2024',
            'poster': 'https://i.postimg.cc/KYMnKTQb/sky2.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'],
            'videos': []
        },
        {
            'id': 'hpytt0202403',
            'name': 'F1TV English',
            'description': 'F1TV 2024 F1 Season - English - 1080p 50fps (showstopper) \n \nTip: for optimal playback, ensure your TV streaming device is in 50Hz mode',
            'releaseInfo': '2024',
            'poster': 'https://i.postimg.cc/pXf4j9GD/f1tveng.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'],
            'videos': []
        },
        {
            'id': 'hpytt0202404',
            'name': 'F1TV International',
            'description': 'F1TV 2024 F1 Season - Multiple languages selectable - 1080p 50fps (showstopper) \nAudio track can be changed during playback \nTip: for optimal playback, ensure your TV streaming device is in 50Hz mode',
            'releaseInfo': '2024',
            'poster': 'https://i.postimg.cc/1zjjSDXZ/f1tvint.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'],
            'videos': []
        },
        {
            'id': 'hpytt0202405',
            'name': 'SkyF1 4K1',
            'description': 'SkyF1 2024 F1 Season - 2160p 50fps (egortech) \n \nTip: for optimal playback, ensure your TV streaming device is in 50Hz mode & supports 4K',
            'releaseInfo': '2024',
            'poster': 'https://i.postimg.cc/zfPNXN1H/sky14k.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'],
            'videos': []
        },
        {
            'id': 'hpytt0202406',
            'name': 'SkyF1 4K2',
            'description': 'SkyF1 2024 F1 Season - 2160p 50fps (smcgill1969) \n \nTip: for optimal playback, ensure your TV streaming device is in 50Hz mode & supports 4K',
            'releaseInfo': '2024',
            'poster': 'https://i.postimg.cc/ry4Tc7Zz/sky24k.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'],
            'videos': []
        },
        {
            'id': 'hpytt0202407',
            'name': 'SkyF1 SD',
            'description': 'SkyF1 2024 F1 Season - 576p 25fps (smcgill1969) \n \nNote: This is not high definition, please use the standard series or the 4K one',
            'releaseInfo': '2024',
            'poster': 'https://i.postimg.cc/WbXTfgf7/sky2sd.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'],
            'videos': []
        }
    ]
}

# Remove this line if it's not used elsewhere in your project
# METAHUB_URL = 'https://images.metahub.space/poster/medium/{}/img'

# Keep this as it's used in the addon_meta function
OPTIONAL_META = ["posterShape", "background", "logo", "videos", "description", "releaseInfo", "imdbRating", "director", "cast",
                 "dvdRelease", "released", "inTheaters", "certification", "runtime", "language", "country", "awards", "website", "isPeered"]

# Remove this line as it's redundant
# app = Flask(__name__)

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
            for full_id, video_info in videos_dict.items():
                series_id, season, episode = full_id.split(':')
                videos.append({
                    'id': series_id,
                    'season': int(season),
                    'episode': int(episode),
                    'title': video_info[0]['title'],
                    'infoHash': video_info[0]['infoHash'],
                    'fileIdx': video_info[0]['fileIdx']
                })
    except FileNotFoundError:
        logger.error(f"File {filepath} not found.")
    except (ValueError, KeyError, IndexError) as e:
        logger.error(f"Error parsing {filepath}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error reading {filepath}: {e}")
    return videos

def run_scripts_in_loop():
    directories = ['egor', 'egor/ego', 'smcg', 'smcg/smc', 'ss', 'ss/ssf', 'ss/ssm', 'egor/eg4', 'smcg/sm4', 'smcg/sms']
    file_mod_times = {}

    logger.info("Starting run_scripts_in_loop")

    for directory in directories:
        filepath = os.path.join(directory, '5processed.txt')
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

                filepath = os.path.join(directory, '5processed.txt')
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
            CATALOG['series'][0]['videos'] = load_videos('./egor/ego/5processed.txt')
            CATALOG['series'][1]['videos'] = load_videos('./smcg/smc/5processed.txt')
            CATALOG['series'][2]['videos'] = load_videos('./ss/ssf/5processed.txt')
            CATALOG['series'][3]['videos'] = load_videos('./ss/ssm/5processed.txt')
            CATALOG['series'][4]['videos'] = load_videos('./egor/eg4/5processed.txt')
            CATALOG['series'][5]['videos'] = load_videos('./smcg/sm4/5processed.txt')
            CATALOG['series'][6]['videos'] = load_videos('./smcg/sms/5processed.txt')
            logger.info("Server restarted with new content.")
        except Exception as e:
            logger.error(f"Error during server restart: {e}")

# Rate limiting decorator
def rate_limit(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if request.headers.get('X-Forwarded-For'):
            remote_addr = request.headers.get('X-Forwarded-For').split(',')[0].strip()
        else:
            remote_addr = request.remote_addr or '127.0.0.1'
        
        key = f"{remote_addr}:{int(time.time()) // 60}"
        current = cache.get(key, 0)
        
        if current >= config.MAX_REQUESTS_PER_MINUTE:
            abort(429)
        
        cache.set(key, current + 1, 60)
        return f(*args, **kwargs)
    return decorated_function

@app.route('/manifest.json')
@rate_limit
def addon_manifest():
    return respond_with(MANIFEST)

@app.route('/catalog/<type>/<id>.json')
@rate_limit
def addon_catalog(type, id):
    if type not in MANIFEST['types']:
        abort(404)
    catalog = CATALOG.get(type, [])
    metaPreviews = {
        'metas': [
            {
                'id': item['id'],
                'type': type,
                'name': item['name'],
                'genres': item['genres'],
                'poster': item['poster']
            } for item in catalog
        ]
    }
    return respond_with(metaPreviews)

@app.route('/meta/<type>/<id>.json')
@rate_limit
def addon_meta(type, id):
    if type not in MANIFEST['types']:
        abort(404)
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
                           'season': video['season'],
                           'episode': video['episode']} for video in item['videos']]
        return meta
    meta = {'meta': next((mk_item(item) for item in CATALOG[type] if item['id'] == id), None)}
    if meta['meta'] is None:
        abort(404)
    return respond_with(meta)

@app.route('/stream/<type>/<id>.json')
@rate_limit
def addon_stream(type, id):
    if type not in MANIFEST['types']:
        abort(404)
    try:
        series_id, season, episode = id.split(':')
        season, episode = int(season), int(episode)
    except ValueError:
        abort(400)
    streams = {'streams': []}
    for series in CATALOG.get(type, []):
        if series['id'] == series_id:
            for video in series['videos']:
                if video['season'] == season and video['episode'] == episode:
                    streams['streams'].append({
                        'title': video['title'],
                        'infoHash': video['infoHash'],
                        'fileIdx': video['fileIdx']
                    })
                    break
    if not streams['streams']:
        abort(404)
    return respond_with(streams)

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
        CATALOG['series'][0]['videos'] = load_videos('./egor/ego/5processed.txt')
        CATALOG['series'][1]['videos'] = load_videos('./smcg/smc/5processed.txt')
        CATALOG['series'][2]['videos'] = load_videos('./ss/ssf/5processed.txt')
        CATALOG['series'][3]['videos'] = load_videos('./ss/ssm/5processed.txt')
        CATALOG['series'][4]['videos'] = load_videos('./egor/eg4/5processed.txt')
        CATALOG['series'][5]['videos'] = load_videos('./smcg/sm4/5processed.txt')
        CATALOG['series'][6]['videos'] = load_videos('./smcg/sms/5processed.txt')
    except Exception as e:
        logger.error(f"Error loading initial video data: {e}")

    # Start the Flask server
    app.run(host='0.0.0.0', port=8000)