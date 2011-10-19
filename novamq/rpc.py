from novamq import client, services
from nova.rpc.common import RemoteError
from nova import context
from eventlet.green import zmq
from eventlet.greenpool import GreenPool
from eventlet.timeout import Timeout
from eventlet.pool import Pool
from uuid import uuid4
import eventlet
import os
import pickle
import logging
import types
import hashlib
from greenlet import GreenletExit
import traceback

CTX = zmq.Context(1)

# TODO: make this configurable
RR_CLIENT = "tcp://127.0.0.1:6802"
RR_SERVICE = "tcp://127.0.0.1:6900"
PUBSUB_SERVICE = "tcp://127.0.0.1:6800"
PUBSUB_CLIENT = "tcp://127.0.0.1:6801"

class RpcContext(context.RequestContext):
    def __init__(self, *args, **kwargs):
        self.replies = []
        super(RpcContext, self).__init__(*args, **kwargs)

    @classmethod
    def marshal(self, ctx):
        ctx_data = ctx.to_dict()
        return pickle.dumps(ctx_data, pickle.HIGHEST_PROTOCOL)

    @classmethod
    def unmarshal(self, data):
        return RpcContext.from_dict(pickle.loads(data))

    def reply(self, *args):
        for i in args:
            self.replies.append(i)


class ConsumerBase(object):

    def __init__(self, topic, proxy):
        self.topic = topic
        self.proxy = proxy

    def normalize_reply(self, result, replies):
        if isinstance(result, types.GeneratorType):
            return list(result)
        elif replies:
            return replies
        else:
            return [result]

    def build_exception(self, data, trace):
        """
        A list is always returned, but an exception is 
        a dict so that the caller can differentiate exception
        responses from data responses.
        """
        return {'exc_type': 'Exception',
                'value': data['args'].get('value', None),
                'traceback': trace}

    def process(self, style, target, ctx, data):
        print "PROCESS", style, target, ctx, data
        try:
            func = getattr(self.proxy, data['method'])
        except AttributeError:
            return self.build_exception(data, traceback.format_exc())

        if style == "cast":
            eventlet.spawn_n(func, ctx, **data['args'])
        else:
            try:
                result = func(ctx, **data['args'])
                return self.normalize_reply(result, ctx.replies)
            except GreenletExit:
                # ignore these since they are just from shutdowns
                pass
            except Exception:
                return self.build_exception(data, traceback.format_exc())

class FanoutConsumer(ConsumerBase):
    def __init__(self, topic, proxy):
        super(FanoutConsumer, self).__init__(topic, proxy)
        self.service = services.PubSubService(CTX, PUBSUB_SERVICE, self.topic)

    def consume(self):
        print "!!!!! FANOUT RUNNING", self.topic

        try:
            while True:
                print "FANOUT", self.topic, "PULL"
                request = self.service.pull()
                print "FANOUT GOT", request
                client_id, style, target, data = request
                ctx, request = data
                ctx = RpcContext.unmarshal(ctx)

                self.process(style, target, ctx, request)
        except:
            print "DIE THE SOCKET", self.topic
            self.service.close()


class TopicConsumer(ConsumerBase):
    def __init__(self, topic, proxy):
        super(TopicConsumer, self).__init__(topic, proxy)
        self.service = services.RoundRobinService(CTX, RR_SERVICE)
        self.service.register(self.topic)

    def consume(self):
        print "!!!! TOPIC RUNNING", self.topic

        try:
            while True:
                client_id, style, target, data = self.service.pull()
                ctx, request = data
                ctx = RpcContext.unmarshal(ctx)

                reply = self.process(style, target, ctx, request)

                ctx_reply = RpcContext.marshal(ctx)
                payload = [ctx_reply, reply]
                self.service.reply(client_id, style, target, reply)
        except:
            print "DIE THE SOCKET", self.topic
            self.service.close()


class Connection(object):

    def __init__(self):
        self.consumers = []
        self.consumer_threads = []

    def create_consumer(self, topic, proxy, fanout):
        if fanout:
            self.consumers.append(FanoutConsumer(topic, proxy))
        else:
            self.consumers.append(TopicConsumer(topic, proxy))

    def close(self):
        for thread in self.consumer_threads:
            eventlet.spawn_after(1, thread.throw)

    def consume_in_thread(self):
        for consumer in self.consumers:
            self.consumer_threads.append(eventlet.spawn(consumer.consume))
            # TODO: this sucks
            eventlet.sleep(0.1)

    def consume(self, limit=None):
        for consumer in self.consumers:
            self.consumer_threads.spawn(consumer.consume)

def genident(context):
    # might want to make this a hash of a compound id
    return uuid4().hex

def _send(style, context, topic, msg, method=client.RoundRobinClient,
         addr=RR_CLIENT):

    if topic.endswith(".None"): topic = topic.split(".")[0]

    print "SEND", style, context, topic, msg, method
    ident = genident(context)
    conn = method(CTX, addr, ident)

    try:
        with Timeout(5) as t:
            payload = [RpcContext.marshal(context), msg]

            if style == "cast":
                # assumes cast can't return an exception
                return conn.cast(topic, payload)
            elif style == "call":
                style, target, data = conn.call(topic, payload)

                if isinstance(data, types.DictType) and 'exc_type' in data:
                    raise RemoteError(data['exc_type'], data['value'], data['traceback'])
                else:
                    return style, target, data
            else:
                assert False, "Invalid call style: %s" % style
    finally:
        print "DIE CLIENT", topic
        conn.close()

def create_connection(new=True):
    return Connection()

def multicall(context, topic, msg):
    print "RR MULTICALL", topic, msg
    style, target, data = _send("call", context, str(topic), msg)
    return data

def call(context, topic, msg):
    print "RR CALL", topic, msg
    style, target, data = _send("call", context, str(topic), msg)
    return data[-1]

def cast(context, topic, msg):
    print "RR CAST", topic, msg
    _send("cast", context, str(topic), msg)

def fanout_cast(context, topic, msg):
    print "FANOUT CAST", topic, msg

    _send("cast", context, str(topic), msg, method=client.PubSubClient,
         addr=PUBSUB_CLIENT)


