from buffer_map import BufferQueueMap
from server_thread import ThreadedServer
from socket_server import BQueueSocketServer

MSGLEN = 1024


class BQueueServer(BQueueSocketServer):
    def __init__(self, hostname=None, port=None):
        self.buffer_map = BufferQueueMap()
        args = {'hostname': hostname, 'port': port, 'buffer_map': self.buffer_map}
        super(self.__class__, self).__init__(**args)

    def bind_thread(self, clientsocket):
        BQueueServerThread(clientsocket, **self.args)


class BQueueServerThread(ThreadedServer):
    def process_data(self, data):
        buffer_map = self.args['buffer_map']
        if data.get('operation') == 'create_queue':
            return buffer_map.add_queue(data.get('queue'), data.get('buffer_size'))
        elif data.get('operation') == 'subscribe':
            return buffer_map.subscribe_to_queue(data.get('host'), data.get('queue'))
        elif data.get('queue'):
            return buffer_map.append_data_to_queue(data.get('queue'), data.get('data'))
