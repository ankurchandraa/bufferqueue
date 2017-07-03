from collections import defaultdict

import redis

from common_util.constants import REDIS_SERVER, REDIS_PORT


class PersistenceWrapper(object):

    REDIS_QUEUE_SIZE_MAP_KEY = 'key|queue_map'
    REDIS_QUEUE_DATA_KEY = 'key|data|queue|%s'
    REDIS_QUEUE_COUNTER_KEY = 'key|counter|queue|%s'
    REDIS_QUEUE_SUBSCRIBER_MAP = 'key|subscriber|queue|%s'

    def __init__(self):
        self.redis_connection = redis.Redis(host=REDIS_SERVER,port=REDIS_PORT)

    def add_queue(self, queue_name, buffer_size):
        self.redis_connection.hset(self.REDIS_QUEUE_SIZE_MAP_KEY, queue_name, buffer_size)

    def delete_queue(self, queue_name):
        self.redis_connection.hdel(self.REDIS_QUEUE_SIZE_MAP_KEY, queue_name)

    def restore_queue(self):
        queue_map = self.redis_connection.hgetall(self.REDIS_QUEUE_SIZE_MAP_KEY)
        return queue_map

    def persist_queue_data(self, queue, data):
        self.redis_connection.rpush(self.REDIS_QUEUE_DATA_KEY%queue, data)

    def increament_queue_counter(self, queue):
        self.redis_connection.incr(self.REDIS_QUEUE_COUNTER_KEY%queue)

    def reset_queue(self, queue, count):
        # new_data = self.redis_connection.lrange(self.REDIS_QUEUE_DATA_KEY%queue, count, -1)
        pipeline = self.redis_connection.pipeline()
        pipeline.delete(self.REDIS_QUEUE_COUNTER_KEY%queue)
        pipeline.delete(self.REDIS_QUEUE_DATA_KEY % queue)
        # pipeline.rpush(self.REDIS_QUEUE_DATA_KEY%queue, new_data)
        pipeline.execute()

    def persist_subscriber(self, queue, subscriber):
        self.redis_connection.sadd(self.REDIS_QUEUE_SUBSCRIBER_MAP%queue, subscriber)

    def restore_subscriber_map(self, queue_list):
        subscriber_map = defaultdict(set)
        for queue in queue_list:
            subscriber_map[queue] = self.redis_connection.smembers(self.REDIS_QUEUE_SUBSCRIBER_MAP%queue)
        return subscriber_map

    def restore_queue_data(self, queue_list):
        queue_data_map = defaultdict(set)
        for queue in queue_list:
            queue_data_map[queue] = self.redis_connection.lrange(self.REDIS_QUEUE_DATA_KEY % queue, 0, -1)
        return queue_data_map

    def restore_queue_size_counter(self, queue_list):
        queue_size_counter = defaultdict(set)
        for queue in queue_list:
            counter_value = self.redis_connection.get(self.REDIS_QUEUE_COUNTER_KEY % queue)
            if not counter_value:
                counter_value = 0
            queue_size_counter[queue] = int(counter_value)
        return queue_size_counter

