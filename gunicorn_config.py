import multiprocessing
import os
from threading import Thread
import logging
from formulio_addon import run_scripts_in_loop

# Gunicorn config variables
bind = "0.0.0.0:8000"
timeout = 120
keepalive = 5
max_requests = 1000
max_requests_jitter = 50

# Logging
loglevel = 'info'
accesslog = '/var/log/gunicorn/access.log'
errorlog = '/var/log/gunicorn/error.log'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def on_starting(server):
    logger.info("Gunicorn server is starting")

def on_reload(server):
    logger.info("Gunicorn server is reloading")

def pre_request(worker, req):
    worker.log.debug(f"Received request: {req.method} {req.path}")

def post_request(worker, req, environ, resp):
    worker.log.debug(f"Completed request: {req.method} {req.path} - Status: {resp.status}")

def worker_abort(worker):
    logger.error(f"Worker {worker.pid} aborted")

def post_fork(server, worker):
    logger.info(f"Worker {worker.pid} forked")
    try:
        thread = Thread(target=run_scripts_in_loop, daemon=True)
        thread.start()
        logger.info(f"Background script thread started in worker {worker.pid}")
    except Exception as e:
        logger.error(f"Failed to start background script in worker {worker.pid}: {str(e)}")

def worker_exit(server, worker):
    logger.info(f"Worker {worker.pid} exited")

# Ensure log directory exists
os.makedirs('/var/log/gunicorn', exist_ok=True)
