import eventlet
from nose.tools import *
import random
import sys
from eventlet.green import zmq
from novamq import services, routers, client
from eventlet.greenpool import GreenPool
from eventlet.timeout import Timeout
import os

CTX = zmq.Context(1)
TOPIC = "testtopic:%d" % os.getpid()

p2p = routers.PointToPointRouter(CTX, "tcp://127.0.0.1:6804", "tcp://127.0.0.1:6805")
p2p.run()

def fake_service(service_addr):
    p2p_service = services.P2PService(CTX, service_addr, TOPIC)

    while True:
        ident, style, client_id, data = p2p_service.recv()
        print "FAKE SERVICE GOT", repr(ident), style, client_id, data

        if style == "call":
            p2p_service.reply(ident, style, client_id, data)
            print "SENT FAKE REPLY"


def fake_client(client_addr, ident):
    p2p_client = client.P2PConnection(CTX, client_addr, ident)
    i = 0


    for i in xrange(0, 10):
        style, topic, data = p2p_client.call(TOPIC,
                               {"say": "anything", "ident": ident,
                                "count": 100+i})

        print "CLIENT", ident, "SENT", topic, data
        i += 1
        assert_equal(data['ident'], ident)


def test_high_client_load():
    eventlet.spawn_n(fake_service, "tcp://127.0.0.1:6805")
    clients = GreenPool()

    for i in xrange(0, 100):
        clients.spawn(fake_client, "tcp://127.0.0.1:6804",
                      "%s:%s" % (os.getpid(), i))

    clients.waitall()
