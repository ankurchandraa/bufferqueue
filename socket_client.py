import socket

import sys


class BQueueSocketClient(object):
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

    def send_data(self, data):
        message = data
        print >> sys.stderr, 'sending "%s"' % message
        self.sock.send(data)
        data = self.sock.recv(1024)
        print >> sys.stderr, 'received "%s"' % data

    def close_connection(self):
        self.sock.close()
