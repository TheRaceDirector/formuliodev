import multiprocessing
import os
from threading import Thread
import logging
from formulio_addon import run_scripts_in_loop

# Gunicorn config variables
bind = "0.0.0.0:8000"
#timeout = 120
#keepalive = 5
#max_requests = 1000
#max_requests_jitter = 50

# Logging
loglevel = 'warning'
accesslog = '/var/log/gunicorn/access.log'
errorlog = '/var/log/gunicorn/error.log'

# Configure logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def on_starting(server):
    logger.warning("Gunicorn server is starting")

def on_reload(server):
    logger.warning("Gunicorn server is reloading")

def pre_request(worker, req):
    pass  # Removed debug logging

def post_request(worker, req, environ, resp):
    pass  # Removed debug logging

def worker_abort(worker):
    logger.error(f"Worker {worker.pid} aborted")

def post_fork(server, worker):
    logger.warning(f"Worker {worker.pid} forked")
    try:
        thread = Thread(target=run_scripts_in_loop, daemon=True)
        thread.start()
        logger.warning(f"Background script thread started in worker {worker.pid}")
    except Exception as e:
        logger.error(f"Failed to start background script in worker {worker.pid}: {str(e)}")

def worker_exit(server, worker):
    logger.warning(f"Worker {worker.pid} exited")

# Ensure log directory exists
os.makedirs('/var/log/gunicorn', exist_ok=True)
