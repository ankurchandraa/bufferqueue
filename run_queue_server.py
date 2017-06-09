from bqueue_server import BQueueServer

# run this file to start buffer queue server
if __name__ == "__main__":

    server = BQueueServer()
    server.listen()