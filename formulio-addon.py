import ast
import logging
import os
import subprocess
import time
from threading import Thread
from flask import Flask, jsonify, abort, send_from_directory

# Set up logging
logging.basicConfig(level=logging.DEBUG)

# Flask app setup with static folder specified
app = Flask(__name__, static_folder='static')

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
            'description': 'SkyF1 2024 F1 Season - 1080p 50fps (egortech) \n \n Tip: for optimal playback, ensure your TV streaming device is in 50Hz mode',
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
            'description': 'SkyF1 2024 F1 Season - 1080p 50fps (smcgill1969) \n \n Tip: for optimal playback, ensure your TV streaming device is in 50Hz mode',
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
            'description': 'F1TV 2024 F1 Season - English - 1080p 50fps (showstopper) \n \n Tip: for optimal playback, ensure your TV streaming device is in 50Hz mode',
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
            'description': 'F1TV 2024 F1 Season - Multiple languages selectable - 1080p 50fps (showstopper) \n \n Audio track can be changed during playback \n \n Tip: for optimal playback, ensure your TV streaming device is in 50Hz mode',
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
            'description': 'SkyF1 2024 F1 Season - 2160p 50fps (egortech) \n \n Tip: for optimal playback, ensure your TV streaming device is in 50Hz mode & supports 4K',
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
            'description': 'SkyF1 2024 F1 Season - 2160p 50fps (smcgill1969) \n \n Tip: for optimal playback, ensure your TV streaming device is in 50Hz mode & supports 4K',
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
            'description': 'SkyF1 2024 F1 Season - 576p 25fps (smcgill1969) \n \n Note: This is not high definition, please use the standard series or the 4K one',
            'releaseInfo': '2024',
            'poster': 'https://i.postimg.cc/WbXTfgf7/sky2sd.jpg',
            'logo': 'https://i.postimg.cc/Vs0MNnGk/f1logo.png',
            'background': 'https://i.postimg.cc/TPThqWJg/background1.jpg',
            'genres': ['Formula Racing'],
            'videos': []
        }
    ]
}

METAHUB_URL = 'https://images.metahub.space/poster/medium/{}/img'
OPTIONAL_META = ["posterShape", "background", "logo", "videos", "description", "releaseInfo", "imdbRating", "director", "cast",
                 "dvdRelease", "released", "inTheaters", "certification", "runtime", "language", "country", "awards", "website", "isPeered"]

app = Flask(__name__)

def respond_with(data):
    resp = jsonify(data)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Headers'] = '*'
    return resp

def load_videos(filepath):
    videos = []
    try:
        with open(filepath, 'r') as file:
            content = file.readlines()
            content = ''.join([line.strip() for line in content])
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
        logging.error(f"File {filepath} not found.")
    except Exception as e:
        logging.error(f"Error reading {filepath}: {e}")
    return videos

def run_scripts_in_loop():
    directories = ['ego', 'smc', 'ssf', 'ssm', 'eg4', 'sm4', 'sms']
    file_mod_times = {}
    for directory in directories:
        filepath = os.path.join(directory, '5processed.txt')
        if os.path.exists(filepath):
            file_mod_times[directory] = os.path.getmtime(filepath)
        else:
            logging.warning(f"{filepath} not found. Attempting to run script to generate it.")
            script_path = os.path.join(directory, '0run_scripts.py')
            try:
                subprocess.run(['python3', script_path], check=True)
                if os.path.exists(filepath):
                    file_mod_times[directory] = os.path.getmtime(filepath)
                else:
                    logging.error(f"Script at {script_path} did not generate the expected file.")
            except subprocess.CalledProcessError as e:
                logging.error(f"Error running {script_path}: {e}")

    while True:
        for directory in directories:
            script_path = os.path.join(directory, '0run_scripts.py')
            logging.info(f"Running {script_path}")
            try:
                subprocess.run(['python3', script_path], check=True)
                new_mod_time = os.path.getmtime(os.path.join(directory, '5processed.txt'))
                if new_mod_time != file_mod_times.get(directory, 0):
                    file_mod_times[directory] = new_mod_time
                    restart_server()
            except subprocess.CalledProcessError as e:
                logging.error(f"Error running {script_path}: {e}")
            except FileNotFoundError:
                logging.error(f"File not found: {os.path.join(directory, '5processed.txt')}")
        time.sleep(40)  # Wait for 9 minutes before running the scripts again

def restart_server():
    global app
    with app.app_context():
        ego_videos = load_videos('./ego/5processed.txt')
        smc_videos = load_videos('./smc/5processed.txt')
        ssf_videos = load_videos('./ssf/5processed.txt')
        ssm_videos = load_videos('./ssm/5processed.txt')
        eg4_videos = load_videos('./eg4/5processed.txt')
        sm4_videos = load_videos('./sm4/5processed.txt')
        sms_videos = load_videos('./sms/5processed.txt')
        CATALOG['series'][0]['videos'] = ego_videos
        CATALOG['series'][1]['videos'] = smc_videos
        CATALOG['series'][2]['videos'] = ssf_videos
        CATALOG['series'][3]['videos'] = ssm_videos
        CATALOG['series'][4]['videos'] = eg4_videos
        CATALOG['series'][5]['videos'] = sm4_videos
        CATALOG['series'][6]['videos'] = sms_videos
        logging.info("Server restarted with new content.")

@app.route('/manifest.json')
def addon_manifest():
    return respond_with(MANIFEST)

@app.route('/catalog/<type>/<id>.json')
def addon_catalog(type, id):
    if type not in MANIFEST['types']:
        abort(404)
    catalog = CATALOG[type] if type in CATALOG else []
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
    return respond_with(meta)

@app.route('/stream/<type>/<id>.json')
def addon_stream(type, id):
    if type not in MANIFEST['types']:
        abort(404)
    series_id, season, episode = id.split(':')
    streams = {'streams': []}
    for series in CATALOG.get(type, []):
        if series['id'] == series_id:
            for video in series['videos']:
                if video['season'] == int(season) and video['episode'] == int(episode):
                    streams['streams'].append({
                        'title': video['title'],
                        'infoHash': video['infoHash'],
                        'fileIdx': video['fileIdx']
                    })
                    break
    return respond_with(streams)

@app.route('/images/<path:filename>')
def static_files(filename):
    return send_from_directory('images', filename)

@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')

if __name__ == '__main__':
    # Start the script-running loop in a separate thread
    Thread(target=run_scripts_in_loop).start()

    # Load initial video data and start the Flask server
    ego_videos = load_videos('./ego/5processed.txt')
    smc_videos = load_videos('./smc/5processed.txt')
    ssf_videos = load_videos('./ssf/5processed.txt')
    ssm_videos = load_videos('./ssm/5processed.txt')
    eg4_videos = load_videos('./eg4/5processed.txt')
    sm4_videos = load_videos('./sm4/5processed.txt')
    sms_videos = load_videos('./sms/5processed.txt')
    CATALOG['series'][0]['videos'] = ego_videos
    CATALOG['series'][1]['videos'] = smc_videos
    CATALOG['series'][2]['videos'] = ssf_videos
    CATALOG['series'][3]['videos'] = ssm_videos
    CATALOG['series'][4]['videos'] = eg4_videos
    CATALOG['series'][5]['videos'] = sm4_videos
    CATALOG['series'][6]['videos'] = sms_videos
    app.run(host='0.0.0.0', port=7000)
