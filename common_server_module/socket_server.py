import socket

from common_util.common_logger import logger
from common_util.constants import LOCALHOST


class BQueueSocketServer(object):
    def __init__(self, **kwargs):
        self.sock = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM)
        if 'hostname' not in kwargs or not kwargs['hostname']:
            self.hostname = LOCALHOST
        else:
            self.hostname = kwargs['hostname']
        if 'port' not in kwargs or not kwargs['port']:
            self.port = 10000
        else:
            self.port = kwargs['port']
        self.args = kwargs

    def listen(self):
        self.sock.bind((self.hostname, self.port))

        self.sock.listen(5)
        while True:
            try:
                clientsocket, address = self.sock.accept()
                self.bind_thread(clientsocket)
            except KeyboardInterrupt as e:
                logger.exception('------------Keyboard termination-------------Disconnecting')
            finally:
                self.sock.shutdown()
                self.sock.close()
