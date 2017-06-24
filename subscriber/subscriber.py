import SocketServer
import json
import socket

from common_server_module.server_thread import ThreadedServer
from common_server_module.socket_server import BQueueSocketServer
from common_util.api_response import ApiResponse
from common_util.common_logger import logger
from common_util.constants import LOCALHOST, BUFFER_SIZE


# Subscriber class send request to queue server to subscribe the ip:port.
# If queue server having anything for this subscriber, It sends to this subscriber on its server port

class BQueueSubscriber(object):
    def __init__(self, sock=None, hostname=None, port=None, callback=None):
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

        self.server_port = 11000
        self.consumer_listener = BQueueSocketServer(**{'port': self.server_port})
        self.consumer_listener.bind_thread = self.bind_thread
        self.callback = callback

    def bind_thread(self, clientsocket):
        ConsumerListenerThread(clientsocket, **{'callback': self.callback})

    def send_data(self, data):
        message = data
        logger.info("sending data {} from subscriber".format(message))
        self.sock.send(data)
        data = self.sock.recv(BUFFER_SIZE)
        logger.info("{} receive from bufferqueue in subscriber".format(data))

    def close_connection(self):
        self.sock.close()

    def listen(self):
        try:
            self.consumer_listener.listen()
        except SocketServer.socket.error:
            logger.exception("subscriber: port already in use")

    def subscribe_queue(self, queue_name):
        data = {
            'queue': queue_name,
            'operation': 'subscribe',
            'host': LOCALHOST + ':' + str(self.server_port)
        }
        self.send_data(json.dumps(data))


class ConsumerListenerThread(ThreadedServer):
    def process_data(self, data):
        # callback method call. If will invoke callback method in subscriber
        self.args['callback'](data)
        return ApiResponse(True, 'subscriber received data')
