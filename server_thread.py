import json
from threading import Thread


class ThreadedServer(Thread):
    def __init__(self, socket, **kwargs):
        Thread.__init__(self)
        self.sock = socket
        self.args = kwargs
        self.start()

    def run(self):
        while 1:
            print 'Client Sent'
            data = self.sock.recv(1024)
            print data
            if data:
                data = json.loads(data)
                response = self.process_data(data)
                self.sock.send(response.get_message())
            else:
                break
