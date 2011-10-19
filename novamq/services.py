from eventlet.green import zmq
import pickle
from novamq import client
from novamq.util import QueueSocket


class PubSubService(object):

    def __init__(self, ctx, addr, subscribe):
        self.inqueue = QueueSocket(ctx, addr, zmq.SUB, bind=False,
                                   send=False, subscribe=subscribe)

    def pull(self):
        data = self.inqueue.recv()
        data[-1] = pickle.loads(data[-1])
        return data

    def close(self):
        self.inqueue.close()


class RoundRobinService(client.RoundRobinClient):

    def __init__(self, ctx, addr, ident=None):
        self.recvq = None
        self.ctx = ctx
        super(RoundRobinService, self).__init__(ctx, addr, ident)


    def register(self, target):
        self.send('register', target, {})
        style, topic, resp = self.recv()
        self.recv_addr = resp['address']
        self.recvq = QueueSocket(self.ctx, self.recv_addr, zmq.PULL, send=False, bind=False)


    def pull(self):
        assert self.recvq, "You need to register first."
        data = self.recvq.recv()
        data[-1] = pickle.loads(data[-1])
        return data

    def reply(self, client_id, style, target, data):
        self.outq.send([client_id, style, target, pickle.dumps(data)])

    def close(self):
        self.outq.close()
        self.recvq.close()

class P2PService(client.P2PConnection):

    def __init__(self, ctx, addr, ident):
        super(P2PService, self).__init__(ctx, addr, ident, sock_type=zmq.XREP)

