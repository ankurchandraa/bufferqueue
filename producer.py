import json
import socket

import sys

import cPickle

from api_response import ApiResponse
from socket_client import BQueueSocketClient


class BQueueProducer(BQueueSocketClient):

    def publish_data(self, queue, data):
        if not queue:
            return ApiResponse(False, "queue name is mandatory")
        data_dict = {'queue': queue, 'data': data}
        self.send_data(json.dumps(data_dict))

    def create_queue(self, queue_name, buffer_size):
        data = {
            'queue': queue_name,
            'operation': 'create_queue',
            'buffer_size': buffer_size
        }
        self.send_data(json.dumps(data))
