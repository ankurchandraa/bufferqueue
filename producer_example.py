import socket

from producer import BQueueProducer

if __name__ == "__main__":
    producer = BQueueProducer(hostname=socket.gethostname(), port=10000)
    producer.create_queue('test', 2)
    producer.publish_data('test', 'this is test data i am ascending')
