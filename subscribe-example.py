import socket

from producer import BQueueProducer
from subscriber import BQueueSubscriber

if __name__ == "__main__":
    subscriber = BQueueSubscriber(hostname=socket.gethostname(), port=10000)
    subscriber.subscribe_queue('test')
