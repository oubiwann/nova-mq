from nova import flags
from nova import context
from nova import utils
import uuid
import traceback
from eventlet.green import zmq
from eventlet.timeout import Timeout
from eventlet import pools
import eventlet
import types
import logging
from nova.rpc.common import RemoteError
import pickle
from greenlet import GreenletExit


CTX = zmq.Context(1)
LOG = logging.getLogger('nova.rpc')

def socket_maker_maker(t, s, a):
    def make_socket():
        sock = CTX.socket(t)
        if s:
            sock.setsockopt(zmq.SUBSCRIBE, s)
        sock.connect(a)
        return sock
    return make_socket

PUB_POOL = pools.Pool(min_size=5, create=socket_maker_maker(zmq.XREQ, None, "tcp://*:3998"))


class Connection(object):

    @classmethod
    def instance(cls, new=True):
        return Connection(new=new)

    def __init__(self, new=True):
        self.new = new

class RpcContext(context.RequestContext):
    def __init__(self, *args, **kwargs):
        self.replies = []
        super(RpcContext, self).__init__(*args, **kwargs)

    def reply(self, *args):
        for i in args:
            self.replies.append(i)



class SimpleAdapterConsumer(object):

    def __init__(self, connection=None, proxy=None, topic=None):
        LOG.info("ADAPTER INIT: connection=%r, proxy=%r, topic=%r" %
                 (connection, proxy, topic))
        self.connection = connection
        self.proxy = proxy
        self.topic = topic
        self.runners = []

        self._connect(connection, proxy, topic)

    def _connect(self, connection, proxy, topic):
        raise NotImplemented("This needs to be implemented.")

    def close(self):
        for runner in self.runners:
            runner.kill()

        eventlet.sleep(0.3)
        self.sub.close()

    def get_data(self):
        LOG.info("FETCHING.., fanout: %r" % self.is_fanout)
        topic, _, style, context, msg = self.sub.recv_multipart()
        LOG.info("GOT TOPIC %r to %r" % (topic, self.topic))
        return topic, style, context, msg

    def send_data(self, context, data):
        if self.pub_pool.free() == 0:
            send = self.pub_pool.create()
            self.pub_pool.put(send)

        with self.pub_pool.item() as send:
            send.send_multipart([context, pickle.dumps(data, pickle.HIGHEST_PROTOCOL)])

    def _message_for_us(self, topic, style):
        if self.is_fanout:
            return style == "fanout_cast" and topic == self.topic
        else:
            return style != "fanout_cast" and topic == self.topic


    def fetch(self, enable_callbacks=True):
        topic, style, context, msg = self.get_data()

        if not self._message_for_us(topic, style):
            LOG.info("RPCIMPL: ignore message for %r and %r" % (topic, style))
            return

        LOG.info("GOT MSG %r CONTEXT %r" % (msg, context))
        data = pickle.loads(msg)
        reqctx = RpcContext.from_dict(pickle.loads(context))

        LOG.info("PROXY OBJECT %r" % self.proxy)
        func = getattr(self.proxy, data['method'])

        if style == "cast":
            eventlet.spawn_n(func, reqctx, **data['args'])
        else:
            try:
                LOG.info("CALLING FUNCTION: %r with %r" % (func, data))
                result = func(reqctx, **data['args']) or reqctx.replies

                if isinstance(result, types.GeneratorType):
                    result = list(result)
                if isinstance(result, types.ListType):
                    result = result
                else:
                    result = [result]

                LOG.info("SENDING RESPONSE: %r" % result)
                self.send_data(context, result)
            except GreenletExit:
                pass
            except Exception, e:
                LOG.exception("FETCH EXCEPTION")
                exc = {'exc_type': 'Exception', 'value': data['args'].get('value',
                                                                          None), 'traceback': ()}
                self.send_data(context, [{'error': True, 'data': exc}])

    def attach_to_eventlet(self):
        def looper(adapter):
            try:
                while True:
                    LOG.info("LOOPER RUNNING FOR OBJECT: %r" % adapter)
                    adapter.fetch(enable_callbacks=True)
                    eventlet.sleep(1)
            except zmq.ZMQError:
                pass # we get this when the eventlet is killed

        self.runners.append(eventlet.spawn(looper, self))


class FanoutAdapterConsumer(SimpleAdapterConsumer):
    def _connect(self, connection, proxy, topic):
        self.is_fanout = True
        self.sub = CTX.socket(zmq.SUB)
        self.sub.setsockopt(zmq.SUBSCRIBE, self.topic)
        self.sub.connect("tcp://*:3997")
        self.pub_pool = PUB_POOL


class TopicAdapterConsumer(SimpleAdapterConsumer):
    def _connect(self, connection, proxy, topic):
        self.is_fanout = False
        self.sub = CTX.socket(zmq.XREQ)
        self.sub.setsockopt(zmq.IDENTITY, self.topic)
        self.sub.connect("tcp://*:3999")
        self.pub_pool = PUB_POOL

class PointToPointConsumer(SimpleAdapterConsumer):
    def _connect(self, connection, proxy, topic):
        self.is_fanout = False
        sef.sub = CTX.socket(zmq.XREQ)
        self.sub.setsockopt(zmq.IDENTITY, self.topic)
        self.sub.connect("tcp://*:3999")
        self.pub_pool = PUB_POOL

        # send a message to the server to indicate that we are a call queue


class ConsumerSet(object):

    def __init__(self, connection, consumer_list):
        self.connection = connection
        self.consumer_list = consumer_list
        self.running = True

    def init(self, conn):
        LOG.info("INIT")

    def reconnect(self):
        LOG.info("RECONNECT")

    def wait(self, limit=None):
        LOG.info("WAITING FOR REQUESTS")

        for i in self.consumer_list:
            i.attach_to_eventlet()

        while self.running:
            eventlet.sleep(0.1)

    def close(self):
        LOG.info("CLOSE")
        self.running = False

        for i in self.consumer_list:
            i.close()

def _base_request(style, context, topic, msg):
    SEND = CTX.socket(zmq.REQ)
    SEND.connect("tcp://127.0.0.1:3999")

    try:
        with Timeout(30) as t:
            LOG.info("CONTEXT: %r, MESSAGE: %r" % (context.to_dict(), msg))

            context_dict = context.to_dict()
            context_data = pickle.dumps(context_dict, pickle.HIGHEST_PROTOCOL)
            msg_data = pickle.dumps(msg, pickle.HIGHEST_PROTOCOL)

            payload = [str(topic), str(style), context_data, msg_data]

            LOG.info("SENDING THE CLIENT REQUEST: %r" % payload)
            SEND.send_multipart(payload)

            LOG.info("WAITING FOR RESPONSE")
            context, reply = SEND.recv_multipart()

            data = pickle.loads(reply)
            LOG.info("GOT RESPONSE: %r" % data)

            return data
    except GreenletExit, e:
        # ignored, this happens when we kill while waiting
        return [None]
    except:
        LOG.exception("ERROR IN SEND WITH: context: %r, topic: %r, msg: %r" %
                      (context, topic, msg))
        return [None]
    finally:
        SEND.close()


def _base_send(style, context, topic, msg):
    data = _base_request(style, context, topic, msg)
    first = data[0]

    if isinstance(first, types.DictType) and 'error' in first:
        LOG.info("!!!!!!! BOOM")
        exc = first['data']
        raise RemoteError(exc['exc_type'], exc['value'], exc['traceback'])
    else:
        return data

# required methods for the rpc module

def create_connection(new=True):
    return Connection()

def call(context, topic, msg):
    return _base_send("call", context, topic, msg)[-1]

def cast(context, topic, msg):
    _base_send("cast", context, topic, msg)

def fanout_cast(context, topic, msg):
    _base_send("fanout_cast", context, topic, msg)

def multicall(context, topic, msg):
    return _base_send("multicall", context, topic, msg)



