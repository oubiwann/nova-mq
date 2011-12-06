import pickle

from eventlet.green import zmq
from eventlet.queue import LightQueue
import eventlet

from novamq.util import QueueSocket


class Router(object):
    """
    Base class that encapsulates most of what they all do, which is move stuff
    from clients to services and back.
    """
    def __init__(self, client_side, service_side):
        """
        Give it two sockets for the client and service side of communication,
        then it will create internal queues for the shuttling between
        them.
        """
        self.client_side = client_side
        self.service_side = service_side
        self.responses = LightQueue()
        self.requests = LightQueue()

    def run(self):
        """
        Kicks off the four eventlets that make the router work
        async from each side without crushing eventlet and zeromq.
        """
        eventlet.spawn_n(self.client_receiver)
        eventlet.spawn_n(self.service_receiver)
        eventlet.spawn_n(self.client_sender)
        eventlet.spawn_n(self.service_sender)

    def client_receiver(self):
        pass

    def service_receiver(self):
        pass

    def client_sender(self):
        pass

    def service_sender(self):
        pass

    def clean_message(self, data):
        data[-1] = pickle.loads(data[-1])
        if not data[1]:
            del data[1]

        return data

    def from_client(self):
        data = self.client_side.recv()
        return self.clean_message(data)

    def to_client(self, ident, style, topic, msg):
        self.client_side.send([ident, style, topic, pickle.dumps(msg)])

    def from_service(self):
        data = self.service_side.recv()
        return self.clean_message(data)

    def to_service(self, ident, style, topic, msg):
        self.service_side.send([ident, style, topic, pickle.dumps(msg)])


class RoundRobinRouter(Router):
    """
    A router that takes requests from clients and then routes them to
    a requested queue, BUT listeners on that queue receive messages in
    round-robin patterns (using a ZMQ_PULL socket).  Services have to
    register the queue they want to receive on.  Currently clients will
    get an error message if they try to send and the queue isn't there,
    which will get fixed up soon.
    """
    def __init__(self, ctx, client_side_addr, service_side_pattern, start_port):
        self.service_side_pattern = service_side_pattern
        self.start_port = start_port
        self.client_side_addr = client_side_addr

        client_side = QueueSocket(ctx, client_side_addr, zmq.XREP)

        # in the case of RR, we use this socket to give the service somewhere to
        # register their receiving end rather than as a way to talk to them
        service_side_addr = service_side_pattern % start_port
        service_side = QueueSocket(ctx, service_side_addr, zmq.XREP)

        super(RoundRobinRouter, self).__init__(client_side, service_side)

        # finally this is where services will have their addresses setup
        self.send_sockets = {}
        self.ctx = ctx

    def client_sender(self):
        while True:
            ident, style, topic, msg = self.responses.get()
            print "RR CLIENT SEND", ident, style, topic, msg
            self.to_client(ident, style, topic, msg)

    def service_sender(self):
        while True:
            sock, ident, style, topic, msg = self.requests.get()
            print "RR SERVICE SEND", sock, ident, style, topic, msg
            sock.send([ident, style, topic, pickle.dumps(msg)])

    def client_receiver(self):
        while True:
            ident, style, topic, msg = self.from_client()
            print "RR CLIENT RECV", ident, style, topic, msg
            topic_sock = self.send_sockets.get(topic, None)

            if topic_sock:
                self.requests.put([topic_sock, ident, style, topic, msg])
            else:
                # need to queue up at this point and wait until we can send
                self.register_queue(ident, topic)
                self.responses.put([ident, style, topic,
                                   pickle.dumps({"error": "Invalid topic, not registered."})])

    def service_receiver(self):
        while True:
            req = self.from_service()
            print "RR CLIENT RECV", req

            if len(req) == 4:
                ident, style, topic, msg = req
                assert style == "register", "Invalid register message: %r" % req
                addr = self.register_queue(ident, topic)
                self.to_service(ident, style, topic, {"address": addr})
            else:
                _, client_id, style, topic, msg = req
                self.responses.put([client_id, style, topic, msg])

    def register_queue(self, ident, topic):
        print "REGISTERED QUEUES", len(self.send_sockets)

        if topic not in self.send_sockets:
            print "RR REGISTERED QUEUE", topic
            self.start_port += 1
            addr = self.service_side_pattern % self.start_port

            self.send_sockets[topic] = QueueSocket(self.ctx, addr, zmq.PUSH,
                                                   recv=False)
            return addr
        else:
            return self.send_sockets[topic].addr


class PubSubRouter(Router):
    """
    A pubsub router (aka fanout) takes messages coming in from the XREP
    socket and then pushes it out on the PUB socket using the topic as
    the front part of the message so it gets routed to the subscribers.
    """
    def __init__(self, ctx, client_side_addr, service_side_addr):
        self.client_side_addr = client_side_addr

        self.service_side_addr = service_side_addr

        client_side = QueueSocket(ctx, client_side_addr, zmq.XREP)
        service_side = QueueSocket(ctx, service_side_addr, zmq.PUB, recv=False)

        super(PubSubRouter, self).__init__(client_side, service_side)

    def client_receiver(self):
        # this is all onesided so we don't need to handle from services
        while True:
            ident, style, topic, msg = self.from_client()
            print "PUB SUB CLIENT RECV", ident, style, topic, msg
            self.requests.put([ident, style, topic, msg])

    def service_sender(self):
        while True:
            ident, style, topic, msg = self.requests.get()
            # have to do this raw so we can put the topic in for pub
            print "PUB SUB SERVICE SEND", ident, style, topic, msg
            self.service_side.send([topic, style, pickle.dumps(msg)])


class PointToPointRouter(Router):
    """
    P2P is the classic router that receives requests on one side
    and sends them out on the other.  It uses some "odd" tricks
    in ZeroMQ to make sure things are messaged right.
    """
    def __init__(self, ctx, client_side_addr, service_side_addr):
        self.client_side_addr = client_side_addr
        self.service_side_addr = service_side_addr

        client_side = QueueSocket(ctx, client_side_addr, zmq.XREP)
        service_side = QueueSocket(ctx, service_side_addr, zmq.XREP)

        super(PointToPointRouter, self).__init__(client_side, service_side)

    def client_sender(self):
        while True:
            topic, style, ident, msg = self.responses.get()
            self.to_client(ident, style, topic, msg)

    def service_sender(self):
        while True:
            topic, style, ident, msg = self.requests.get()
            self.to_service(ident, style, topic, msg)

    def client_receiver(self):
        while True:
            ident, style, topic, msg = self.from_client()
            self.requests.put([ident, style, topic, msg])

    def service_receiver(self):
        while True:
            topic, style, ident, msg = self.from_service()
            self.responses.put([topic, style, ident, msg])
