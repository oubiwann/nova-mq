This code is a first attempt at a ZeroMQ router for Nova.
It is a prototype of the new Queue Abstraction Layer in
nova that lets you point nova at any queueing system,
not just RabbitMQ.  The semantics of Rabbit and ZeroMQ
are different enough that this proves out the QAL will
work with just about anything.  It also lays the foundation
for future work in alternative RPC mechanisms for Nova
using anything:  Thrift, Protocol Buffers, etc.

Warning
=======

This code is going to change.  The direction it went in 
turned out to be a dead end because of technical limitations
and bugs in Eventlet, ZeroMQ, and pyzmq.  Expect it to
change in the internal design, and maybe the API a little
bit.


Current Design
==============

In the novamq module there's APIs for clients, 
services, and the router.  In each of these is
the concept of a PubSub, RoundRobin, or P2P connection
that creates the semantics Nova expects to exist
for its RPC mechanisms.  These are then tied together
into the bin/novamq script, which simply starts up
a router for each kind of communication.

Read the novamq/client.py, novamq/service.py, and
novamq/router.py for the implementations of each
of these.

After this, the actual RPC implementation is in
novamq/rpc.py and exposes the Queue Abstraction Layer
that nova needs to talk to all of this.  This file
is probably the one that needs the most rework
but it passes all but about 200 tests, and most of
those fail because of Eventlet and ZeroMQ running
out of open file descriptors or random epoll/kqueue
errors.

The Types Of Queues
===================

The kinds of queues and their semantics are:

Peer2Peer
---------

This is a simple send a message to a receiver through the queue.  You
can easily just point this directly at the target service rather than
go through the novamq server.  Nova doesn't actually do much with
this, but it will come into play in decentralized designs.

This was created mostly to round out the models for future
work.

RoundRobin
----------

Nova uses this semantic to send a message to one of X listeners on a
specific queue, and sometimes get a response.  A problem with nova is
they have a call vs. cast concept which makes implementing this
difficult.  In the current implementation queues are created on the
fly as services register them, and then they can be used.  An
alternative decentralized design would be to have the queue server
just keep track of addresses where the actual receiver lives, and
send those back for direct communication.

One design difference between this code and the normal Rabbit code is how
replies are handled.  In the Rabbit code temporary queues are created for the
replies and then destroyed when the reply is received.  In the novamq code it
just uses the native zeromq random ident string and routes entirely through
zeromq without making extra queues inside the server.

PubSub
------

This is the model when you want one message to go to all listeners that are on
a certain queue and don't expect a reply.

Message Format
==============

The messages are sent as Python pickles because certain parts of Nova code
send out raw datetime objects and other things that can't be turned into
JSON.  It'd be nice if they quit doing that so everyone can use JSON and
you can work with Nova from any language.


Design Problems
===============

The current design relies on ZeroMQ's own identity for sockets
to maintain the identity of everyone sending messages throug
the system.  This turns out to be a fatal flaw when combined
with eventlet and pyzmq.  First, eventlet has a race condition
(which I have a branch to fix) that cause it to pause while
getting events from ZeroMQ.  Second, ZeroMQ can't seem to handle
large numbers of temporary sockets and dies with random asserts
in kqueue, epoll, file descriptor limits, and just about everything.
Finally, pyzmq has errors in memory handling that make it not
close sockets quick enough and instead wait for garbage 
collection to clean up.

After weeks of trying to use multiple zeromq sockets and the
socket zeromq idents to coordinate messages, it's clear this
won't work.  Instead, the design has to change so that there's
a zeromq socket per queue type per process.  Then have the
server manage the identities and don't use zeromq's internal
idents for message identity.


Installing
==========

To install novamq in its current state do this:

1. Setup nova, and its .nova-venv virtualenv and get all the tests to run
3. Get my eventlet fork https://bitbucket.org/zedshaw/eventlet
4. pip uninstall the other eventlet from the .nova-venv
5. Install my fork of eventlet into the .nova-venv
6. Install the latest ZeroMQ and pyzmq
2. Grab the source https://github.com/cloudscaling/nova-mq
7. Go into nova-mq and run the test suite.

At that point it should be installed and ready for hacking.

Hacking
=======

Nova uses the novamq.rpc module to talk to the server as either a client or
service.  This is set as the 'rpc_backend' flag.  Easiest way to hack on this
code is to simply do this:

  ln ../nova-mq/novamq .
  export PYTHONPATH=.
  vi nova/rpc/__init__.py

And then change the source in nova/rpc/__init__.py to use novamq.rpc on line
26.  You can then just run unit tests all you want with no problems an no
configurations needed.


