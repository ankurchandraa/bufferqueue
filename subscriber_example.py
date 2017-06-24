import json

from common_logger import logger
from constants import LOCALHOST, PRINT_DATA_LOG
from subscriber import BQueueSubscriber


"""
If any data is published from bufferqueue, it will be received in callback_method for this subscriber
"""
received_messages = 0


def callback_method(data):
    jsoned_data = json.loads(data)
    queue_name = jsoned_data['queue']
    actual_data = jsoned_data['data']
    if PRINT_DATA_LOG:
        logger.info("data in callback subscriber for queue {} received \n{}".format(queue_name, actual_data))
    increment_counter(len(actual_data))


def increment_counter(count):
    global received_messages
    received_messages += count
    logger.info("received messages count \n{}".format(str(received_messages)))


if __name__ == "__main__":
    subscriber = BQueueSubscriber(hostname=LOCALHOST, port=10000, callback=callback_method)
    subscriber.subscribe_queue('test1')
    subscriber.subscribe_queue('test2')
    subscriber.subscribe_queue('test3')
    subscriber.subscribe_queue('test4')
    subscriber.subscribe_queue('test5')
    logger.info("received messages count \n{}".format(str(received_messages)))
    subscriber.listen()
