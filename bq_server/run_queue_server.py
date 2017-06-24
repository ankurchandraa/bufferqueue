import os

import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from bq_server.bqueue_server import BQueueServer

# run this file to start buffer queue server
if __name__ == "__main__":

    server = BQueueServer()
    server.listen()