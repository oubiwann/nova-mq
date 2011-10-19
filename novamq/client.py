from eventlet.green import zmq
import pickle
from novamq.util import QueueSocket



class RoundRobinClient(object):

    def __init__(self, ctx, addr, ident):
        self.outq = QueueSocket(ctx, addr, zmq.XREQ,
                                bind=False, ident=ident)

    def send(self, style, target, data):
        self.outq.send([style, target, pickle.dumps(data)])

    def recv(self):
        data = self.outq.recv()
        data[-1] = pickle.loads(data[-1])
        return data


    def call(self, target, data):
        self.send('call', target, data)
        return self.recv()

    def cast(self, target, data):
        self.send('cast', target, data)

    def close(self):
        self.outq.close()



class P2PConnection(object):

    def __init__(self, ctx, addr, ident, sock_type=zmq.XREQ):
        self.queue = QueueSocket(ctx, addr, sock_type, bind=False,
                                   ident=ident)

    def send(self, style, target, data):
        self.queue.send([style, target, pickle.dumps(data)])

    def recv(self):
        data = self.queue.recv()
        data[-1] = pickle.loads(data[-1])
        return data

    def call(self, target, data):
        self.send('call', target, data)
        return self.recv()

    def cast(self, target, data):
        self.send('cast', target, data)

    def reply(self, target, style, client_id, data):
        self.queue.send([target, style, client_id, pickle.dumps(data)])

    def close(self):
        self.queue.close()


class PubSubClient(object):

    def __init__(self, ctx, addr, ident):
        self.outq = QueueSocket(ctx, addr, zmq.XREQ,
                                recv=False, bind=False, ident=ident)

    def cast(self, target, data):
        self.outq.send(['cast', target, pickle.dumps(data)])

    def close(self):
        self.outq.close()

