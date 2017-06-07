import json
from collections import defaultdict

from api_response import ApiResponse
from common_logger import logger
from socket_client import BQueueSocketClient

"""
BufferQueueMap is the main backbone for bufferqueue.
queue_map param will hold the multiple queues with different names
subscriber_map will keep the subsciber machine host and port
queue_size_definition holds the size of each queue
queue_size_counter holds the current size of the each queue in queue_map
"""

class BufferQueueMap(object):
    def __init__(self):
        self.queue_map = defaultdict(list)
        self.subscriber_map = defaultdict(list)
        self.queue_size_definition = defaultdict(int)
        self.queue_size_counter = defaultdict(int)

    def get_queue(self, queue_name):
        if self.is_valid_queue(queue_name):
            return self.queue_map[queue_name]
        return None

    def add_queue(self, queue_name, buffer_size):
        self.queue_size_definition[queue_name] = buffer_size
        return ApiResponse(True, 'queue created')

    def subscribe_to_queue(self, subscriber_id, queue):
        response = ApiResponse(True, "Succesfully subscribed to {}".format(queue))
        self.subscriber_map[queue].append(subscriber_id)
        return response

    def append_data_to_queue(self, queue, data):
        if self.is_valid_queue(queue):
            self.queue_map[queue].append(data)
            self.queue_size_counter[queue] += 1
            self.is_queue_full(queue)
            return ApiResponse(True, "Data published on queue {}".format(queue))
        else:
            return ApiResponse(False, "Queue not present")

    def is_valid_queue(self, queue):
        if (isinstance(queue, unicode) or isinstance(queue, str)) \
                and queue in self.queue_size_definition:
            return True
        return False

    def is_queue_full(self, queue_name):
        if self.queue_size_counter[queue_name] == self.queue_size_definition[queue_name]:
            logger.info('{} queue is full, broadcasting data to subscribers'.format(queue_name))
            subscriber_list = self.subscriber_map[queue_name]
            data = self.queue_map[queue_name]
            self.publish_data_to_consumer(subscriber_list, data)
            self.clear_queue(queue_name)

    def clear_queue(self, queue_name):
        self.queue_map[queue_name] = []
        self.queue_size_counter[queue_name] = 0

    def publish_data_to_consumer(self, subscriber_list, data):
        if isinstance(subscriber_list, list):
            for subscriber in subscriber_list:
                self.send_data_to_host(subscriber, data)
        elif isinstance(subscriber_list, unicode) or isinstance(subscriber_list, str):
            self.send_data_to_host(subscriber_list, data)

    def send_data_to_host(self, subscriber, data):
        host, port = self.get_subscriber_host(subscriber)
        client = BQueueSocketClient(hostname=host, port=port)
        client.send_data(json.dumps(data))
        logger.info("sending data to host {}".format(host))

    def get_subscriber_host(self, host_string):
        temp_list = host_string.split(':')
        host = temp_list[0]
        port = int(temp_list[1])
        return host, port
