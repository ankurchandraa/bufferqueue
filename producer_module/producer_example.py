import os

import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print BASE_DIR
sys.path.insert(0, BASE_DIR)

from common_util.constants import LOCALHOST
from producer import BQueueProducer

if __name__ == "__main__":
    producer = BQueueProducer(hostname=LOCALHOST, port=10000)
    producer.create_queue('test1', 10)
    producer.create_queue('test2', 10)
    producer.create_queue('test3', 10)
    producer.create_queue('test4', 10)
    producer.create_queue('test5', 10)

    for i in range(5):
        producer.publish_data('test' + str(1), {'sample': 'data'})
        producer.publish_data('test' + str(2), 134343)
        producer.publish_data('test' + str(3), 'test string')
        producer.publish_data('test' + str(4), ('tuple', 'test'))
        producer.publish_data('test' + str(5), [12, 'test data in list', 333333])
