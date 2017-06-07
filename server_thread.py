import json
from threading import Thread

from common_logger import logger
from constants import BUFFER_SIZE


class ThreadedServer(Thread):
    def __init__(self, socket, **kwargs):
        Thread.__init__(self)
        self.sock = socket
        self.args = kwargs
        self.start()

    def run(self):
        while 1:
            logger.info('Server thread waiting for data')
            data = self.sock.recv(BUFFER_SIZE)
            logger.info(data)
            if data:
                data = json.loads(data)
                response = self.process_data(data)
                self.sock.send(response.get_message())
            else:
                break
