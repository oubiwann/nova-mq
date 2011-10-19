from nose.tools import *
from novamq import services, routers, client
import random
import sys
from eventlet.green import zmq
import eventlet
from eventlet.greenpool import GreenPool

CTX = zmq.Context(1)

fanout = routers.PubSubRouter(CTX, "tcp://127.0.0.1:6800", "tcp://127.0.0.1:6801")
fanout.run()

def fake_service(service_addr, test_context):
    pub_service = services.PubSubService(CTX, service_addr, "testpubs")
    count = 0

    for i in xrange(0, 100):
        print "PULLING..."
        req = pub_service.pull()
        print "SUB SERVER GOT: ", req
        count += 1

    test_context['services'] = count

def fake_client(client_addr, ident, test_context):
    conn = client.PubSubClient(CTX, client_addr, ident)
    count = 0

    for i in xrange(0,10):
        count += 1
        conn.cast('testpubs', {"say": "anything",
                               "ident": ident,
                               "count": count})
    test_context['clients'] = count

def test_high_client_load():
    test_context = {'clients': 0, 'services': 0}
    pool = GreenPool()

    pool.spawn(fake_service,
                     "tcp://127.0.0.1:6801", test_context)

    for i in xrange(0, 10):
        pool.spawn(fake_client, "tcp://127.0.0.1:6800",
                      "%s" % i, test_context)

    pool.waitall()

    assert_equal(test_context['clients'], 10)
    assert_equal(test_context['services'], 100)
