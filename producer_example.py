import socket
from random import randint

from constants import LOCALHOST
from producer import BQueueProducer

if __name__ == "__main__":
    producer = BQueueProducer(hostname=LOCALHOST, port=10000)
    producer.create_queue('test1', 1000)
    producer.create_queue('test2', 1000)
    producer.create_queue('test3', 1000)
    producer.create_queue('test4', 1000)
    producer.create_queue('test5', 1000)

    for i in range(50000):
        producer.publish_data('test' + str(1), {'sample': 'data'})
        producer.publish_data('test' + str(2), 134343)
        producer.publish_data('test' + str(3), 'test string')
        producer.publish_data('test' + str(4), ('tuple', 'test'))
        producer.publish_data('test' + str(5), [12, 'test data in list', 333333])
