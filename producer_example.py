import socket
from random import randint

from producer import BQueueProducer

if __name__ == "__main__":
    producer = BQueueProducer(hostname=socket.gethostname(), port=10000)
    producer.create_queue('test1', 5000)
    producer.create_queue('test2', 2000)
    producer.create_queue('test3', 250)
    producer.create_queue('test4', 200)
    producer.create_queue('test5', 2000)

    for i in range(50000):
        rand_no = randint(1, 5)
        producer.publish_data('test' + str(rand_no), {'sample': 'data'})
