import socket

from common_logger import logger
from constants import BUFFER_SIZE, LOCALHOST, PRINT_DATA_LOG


# Base class for socket clicnt

class BQueueSocketClient(object):
    def __init__(self, sock=None, hostname=None, port=None):
        if sock is None:
            self.sock = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            if not hostname:
                self.hostname = LOCALHOST
            if not port:
                self.port = 10000
        else:
            self.sock = sock

        self.sock.connect((hostname, port))

    def send_data(self, data):
        message = data
        logger.info("sending data from client")
        if PRINT_DATA_LOG:
            logger.info("{}".format(message))
        self.sock.send(data)
        data = self.sock.recv(BUFFER_SIZE)
        logger.info("receive from server")
        if PRINT_DATA_LOG:
            logger.info("{}".format(data))

    def close_connection(self):
        self.sock.close()
