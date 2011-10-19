from nose.tools import *
from novamq import services, routers, client
import random
import sys
from eventlet.green import zmq
import eventlet
from eventlet.greenpool import GreenPool
import os

CTX = zmq.Context(1)

rrobin = routers.RoundRobinRouter(CTX, "tcp://127.0.0.1:6802",
                                  "tcp://127.0.0.1:%d", 6900)
rrobin.run()

# a fake service that registers fro the rrtest service
def fake_service(service_addr, index):
    rr_service = services.RoundRobinService(CTX, service_addr)
    rr_service.register('rrtest')

    while True:
        client_id, style, target, data = rr_service.pull()
        assert_equal(data['test'], True)

        print "SERVICE SENT", data, "FROM CLIENT", client_id
        data['address'] = rr_service.recv_addr
        data['index'] = index
        data['test'] = False
        rr_service.reply(client_id, style, target, data)

# a simple client that just asks for stuff from one of the
# above using call operations
def fake_client(client_addr, ident):
    rr = client.RoundRobinClient(CTX, client_addr, ident)
    eventlet.sleep(1)

    for i in xrange(0, random.randint(1, 10)):
        style, target, data = rr.call('rrtest', {'test': True, "ident": ident})
        print "CLIENT RESP", data
        assert_equal(data['ident'], ident)
        assert_equal(data['test'], False)
        assert_equal(data['address'], "tcp://127.0.0.1:6901")


def test_high_workload():
    # fire up three services to receive in roundrobin style, giving
    # each an ident so we can make sure they're working that way
    eventlet.spawn_n(fake_service, "tcp://127.0.0.1:6900", 1)
    eventlet.spawn_n(fake_service, "tcp://127.0.0.1:6900", 2)
    eventlet.spawn_n(fake_service, "tcp://127.0.0.1:6900", 3)

    clients = GreenPool()

    # fire up a bunch of clients to thrash it at random
    for i in xrange(0, 100):
        clients.spawn(fake_client, "tcp://127.0.0.1:6802", "%s:%s" % (os.getpid(), i))

    clients.waitall()

