import socket

from common_logger import logger
from subscriber import BQueueSubscriber


"""
If any data is published from bufferqueue, it will be received in callback_method for this subscriber
"""
def callback_method(data):
    logger.info("data in callback subscriber received \n{}".format(data))

if __name__ == "__main__":
    subscriber = BQueueSubscriber(hostname=socket.gethostname(), port=10000, callback=callback_method)
    subscriber.subscribe_queue('test1')
    subscriber.subscribe_queue('test2')
    subscriber.subscribe_queue('test3')
    subscriber.subscribe_queue('test4')
    subscriber.subscribe_queue('test5')
    subscriber.listen()
