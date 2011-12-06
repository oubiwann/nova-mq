from eventlet.green import zmq


class QueueSocket(object):
    """
    A tiny wrapper around ZeroMQ to simplify the send/recv protocol
    and connection management.
    """
    def __init__(self, ctx, addr, zmq_type, bind=True, recv=True, send=True,
                 ident=None, subscribe=None):
        print "QUEUESOCKET MAKE", addr
        self.sock = ctx.socket(zmq_type)
        self.can_send = send
        self.can_recv = recv
        self.ident = ident
        self.subscribe = subscribe
        self.addr = addr

        if self.ident:
            self.sock.setsockopt(zmq.IDENTITY, ident)

        if self.subscribe:
            self.sock.setsockopt(zmq.SUBSCRIBE, subscribe)

        if bind:
            self.sock.bind(addr)
        else:
            self.sock.connect(addr)

    def close(self):
        print "QUEUESOCKET CLOSE", self.addr
        self.sock.close(linger=0)
        self.sock = None

    def recv(self):
        assert self.can_recv, "You cannot recv on this socket."
        return self.sock.recv_multipart()

    def send(self, data):
        self.sock.send_multipart(data)


