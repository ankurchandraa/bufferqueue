import json

from buffer_map import BufferQueueMap, logger
from common_server_module.server_thread import ThreadedServer
from common_server_module.socket_server import BQueueSocketServer


class BQueueServer(BQueueSocketServer):
    def __init__(self, hostname=None, port=None):
        self.buffer_map = BufferQueueMap()
        args = {'hostname': hostname, 'port': port, 'buffer_map': self.buffer_map}
        super(self.__class__, self).__init__(**args)
        logger.info("Starting Bqueue server on port {}".format(str(self.port)))

    def bind_thread(self, clientsocket):
        BQueueServerThread(clientsocket, **self.args)
        logger.info("BQueueServerThread bounded with server")


class BQueueServerThread(ThreadedServer):
    def process_data(self, data):
        data = json.loads(data)
        buffer_map = self.args['buffer_map']
        if data.get('operation') == 'create_queue':
            logger.info("create queue request received")
            return buffer_map.add_queue(data.get('queue'), data.get('buffer_size'))
        elif data.get('operation') == 'subscribe':
            logger.info("queue subscribe request received")
            return buffer_map.subscribe_to_queue(data.get('host'), data.get('queue'))
        elif data.get('queue'):
            logger.info("add data to queue request received")
            return buffer_map.append_data_to_queue(data.get('queue'), data.get('data'))
