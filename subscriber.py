import json
import socket

import sys

import cPickle
from threading import Thread

from api_response import ApiResponse
from server_thread import ThreadedServer
from socket_server import BQueueSocketServer


class BQueueSubscriber(object):
    def __init__(self, sock=None, hostname=None, port=None):
        if sock is None:
            self.sock = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            if not hostname:
                self.hostname = socket.gethostname()
            if not port:
                self.port = 10000
        else:
            self.sock = sock

        self.sock.connect((hostname, port))

        self.server_port = 11000
        self.consumer_listener = BQueueSocketServer(**{'port': self.server_port})
        self.consumer_listener.bind_thread = self.bind_thread

    def bind_thread(self, clientsocket):
        ConsumerListenerThread(clientsocket)

    def send_data(self, data):
        message = data
        print >> sys.stderr, 'sending "%s"' % message
        self.sock.send(data)
        data = self.sock.recv(1024)
        print >> sys.stderr, 'received "%s"' % data

    def close_connection(self):
        self.sock.close()

    def subscribe_queue(self, queue_name):
        data = {
            'queue': queue_name,
            'operation': 'subscribe',
            'host': socket.gethostname() + ':' + str(self.server_port)
        }
        self.send_data(json.dumps(data))

        self.consumer_listener.listen()


class ConsumerListenerThread(ThreadedServer):

    def process_data(self, data):
        return ApiResponse(True, 'subscriber received data')