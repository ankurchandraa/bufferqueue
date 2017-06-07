import socket

from bqueue_server import BQueueServer
from buffer_map import BufferQueueMap

if __name__ == "__main__":

    server = BQueueServer()
    server.listen()