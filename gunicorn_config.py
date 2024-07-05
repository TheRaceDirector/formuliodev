from threading import Thread
from formulio_addon import run_scripts_in_loop

def post_fork(server, worker):
    Thread(target=run_scripts_in_loop).start()
