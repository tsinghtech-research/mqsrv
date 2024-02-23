#!/usr/bin/env python
import sys
import traceback
import os
import signal
import atexit
from functools import partial
import inspect
import socket

from loguru import logger
from kombu import Connection, Queue, Exchange
from kombu.mixins import ConsumerProducerMixin
from greenthread.green import *

from .rpc_utils import pack_funcs, rpc_decode_req, rpc_encode_rep
from .base import get_connection, get_rpc_exchange, get_event_exchange
from .exc import BaseException, MethodNotFound

def format_function_name(fn):
    if hasattr(fn, '__rpc_name__'):
        return fn.__rpc_name__

    fn_name = fn.__name__
    if inspect.isfunction(fn):
        return fn_name

    if inspect.ismethod(fn):
        obj = fn.__self__
        if not hasattr(obj, 'rpc_prefix'):
            rpc_prefix = ''
        else:
            rpc_prefix = obj.rpc_prefix

        if rpc_prefix:
            return f'{rpc_prefix}_{fn_name}'
        return fn_name

    else:
        raise

class MessageQueueServer(ConsumerProducerMixin):

    def __init__(self,
                 connection,
                 rpc_queue,
                 event_queues,
                 serializer=None,
                 pool_size=10000):

        self.connection = connection
        self.rpc_queue = rpc_queue
        self.event_queues = event_queues
        self.serializer = serializer

        self.pool = GreenPool(pool_size)
        self.ctx_pool = GreenPool(pool_size)

        self.rpcs = {}
        self.event_handlers = {}
        self.exc_handlers = {}
        self.ctxs = {}
        self.evt_cb_id = 0
        self.exc_handler_id = 0

        self.is_setuped = False

    def register_rpc(self, fn, name=''):
        if not name:
            name = format_function_name(fn)

        assert name not in self.rpcs
        self.rpcs[name] = fn
        logger.info(f'register_rpc: {name} {fn.__name__}')
        return name

    def unregister_rpc(self, name):
        if name in self.rpcs:
            self.rpcs.pop(name)
            logger.info(f'unregister_rpc: {name} {fn.__name__}')

    def register_event_handler(self, evt_type, cb):
        if evt_type not in self.event_handlers:
            self.event_handlers[evt_type] = {}

        handlers = self.event_handlers[evt_type]

        self.evt_cb_id += 1
        cb_id = self.evt_cb_id

        assert cb_id not in handlers
        handlers[cb_id] = cb
        logger.info(f'register_event_handler: {evt_type} {cb.__name__}')
        return cb_id

    def unregister_event_handler(self, cb_id):
        evt_type = None
        for k, v in self.event_handlers.items():
            if cb_id in v:
                evt_type = k
                break

        if not evt_type:
            return

        self.event_handlers[evt_type].pop(cb_id)
        if not self.event_handlers[evt_type]:
            self.event_handlers.pop(evt_type)

        logger.info(f'unregister_event_handler: {evt_type} {cb_id}')

    def register_exc_handler(self, h):
        self.exc_handler_id += 1
        exc_h_id = self.exc_handler_id
        assert h not in self.exc_handlers.values()
        self.exc_handlers[exc_h_id] = h
        logger.info(f'register_exc_handler: {h.__name__}')
        return exc_h_id

    def unregister_exc_handler(self, exc_id):
        if exc_id in self.exc_handlers:
            self.exc_handlers.pop(exc_id)
            logger.info(f'unregister_exc_handler: {exc_id}')

    def register_context(self, ctx, name=''):
        if not name:
            if hasattr(ctx, 'name'):
                name = ctx.name
            else:
                name = id(ctx)

        self.ctxs[name] = ctx

    def get_consumers(self, Consumer, channel):
        out = []
        if self.rpc_queue:
            out.append(Consumer(
                on_message=self._on_rpc_message,
                queues=[self.rpc_queue],
                prefetch_count=1,
                accept=['pickle', 'json']
            ))

        if self.event_queues:
            out.append(Consumer(
                on_message=self._on_event_message,
                queues=self.event_queues,
                accept=['pickle', 'json'],
                no_ack=True
            ))

        return out


    def send_reply(self, message, result=None, error=None):
        req_id = message.properties['correlation_id']
        routing_key = message.properties['reply_to']
        logger.debug(f"sending reply [{routing_key}, {req_id}])")

        self.producer.publish(
            rpc_encode_rep(req_id, result=result, error=error),
            exchange='',
            routing_key=routing_key,
            correlation_id=req_id,
            serializer=self.serializer,
            retry=True,
        )
        message.ack()

    def handle_exception(self, e):
        logger.exception(e)
        for h in self.exc_handlers.values():
            self.pool.spawn(h, e)

    def _rpc_worker(self, message):
        req_id = message.properties['correlation_id']
        _, meth, args, kws = rpc_decode_req(message.payload)
        logger.debug(f"reciving request [{self.rpc_queue.name}, {req_id}] {meth}")
        send_reply = partial(self.send_reply, message)

        try:
            if meth not in self.rpcs:
                raise MethodNotFound(f"method {meth} not found!")

            result = self.rpcs[meth](*args, **kws)

        except BaseException as e:
            logger.error(f"BaseException for request id {req_id}")
            send_reply(error=sys.exc_info())
            self.handle_exception(e)

        except Exception as e:
            # we need to handle generic Exception BUG here
            logger.error(f"BUG for request id {req_id}")
            logger.exception(e)
            raise

        else:
            send_reply(result=result)

    def _on_rpc_message(self, message):
        self.pool.spawn(self._rpc_worker, message)

    def _event_worker(self, cb, evt_type, evt_data):
        logger.debug(f"reciving event [{evt_type}])")
        try:
            cb(evt_type, evt_data)

        except BaseException as e:
            logger(f"BaseException for event {evt_type}")
            self.handle_exception(e)

        except Exception as e:
            logger.error(f"BUG for event {evt_type}")
            logger.exception(e)

    def _on_event_message(self, message):
        evt_type, evt_data = message.payload
        if evt_type not in self.event_handlers:
            return

        for cb in self.event_handlers[evt_type].values():
            self.pool.spawn(self._event_worker, cb, evt_type, evt_data)

    def apply_to_ctx(self, meth, *args, **kws):
        def worker(ctx):
            getattr(ctx, meth)(*args, **kws)

        ctxs = [i for i in self.ctxs.values() if hasattr(i, meth)]
        self.ctx_pool.imap(worker, ctxs)
        green_pool_join(self.ctx_pool)

        green_pool_join(self.pool)

    def setup(self):
        if self.is_setuped:
            return

        logger.info("server setting up...")
        self.apply_to_ctx('setup')
        self.is_setuped = True
        logger.info("server setuped.")

    def teardown(self):
        if not self.is_setuped:
            return

        logger.info("server tearing down...")
        self.apply_to_ctx('teardown')
        self.is_setuped = False
        logger.info("server teared down.")

def to_pair(v):
    if isinstance(v, str):
        return (v, v)
    assert isinstance(v, tuple) and len(v) == 2
    return v

def make_server(conn=None, rpc_routing_key=None, event_routing_keys=[], rpc_exchange=None, event_exchange=None, rpc_queue=None, event_queues=[], **kws):
    if isinstance(event_routing_keys, str):
        event_routing_keys = [event_routing_keys]

    conn = get_connection(conn)
    if not isinstance(rpc_exchange, Exchange):
        rpc_exchange = get_rpc_exchange(rpc_exchange)

    if rpc_queue or rpc_routing_key:
        if rpc_queue is None:
            q_name, routing_key = to_pair(rpc_routing_key)
            rpc_queue = Queue(q_name, routing_key=routing_key, exchange=rpc_exchange, exclusive=True, auto_delete=True)

    if not isinstance(event_exchange, Exchange):
        event_exchange = get_event_exchange(event_exchange)

    if event_queues or event_routing_keys:
        if not event_queues:
            event_queues = []
            for i in event_routing_keys:
                q_name, routing_key = to_pair(i)
                event_queues.append(Queue(q_name, routing_key=routing_key, exchange=event_exchange))

    return MessageQueueServer(conn, rpc_queue, event_queues, **kws)

def wait_for_connection(addr, max_tries):
    host, port = addr
    if not port:
        port = 5672

    for i in range(max_tries):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            sock.close()
        except ConnectionRefusedError as e:
            logger.info(f"connection refused, retry {i}")
            green_sleep(2**i)
            continue
        else:
            break

def run_server(server, block=True, max_tries=10):
    conn = server.connection
    logger.info(f'starting at {conn.as_uri()}')

    wait_for_connection((conn.hostname, conn.port), max_tries)

    def shutdown():
        server.teardown()
        logger.info("server teardown")

    atexit.register(shutdown)
    server.setup()

    runlet = green_spawn(server.run)

    if not block:
        return

    while True:
        try:
            green_thread_join(runlet)
        except OSError as exc:
            import errno
            if exc.errno == errno.EINTR:
                # this is the OSError(4) caused by the signalhandler.
                # ignore and go back to waiting on the runner
                continue
            raise

        except KeyboardInterrupt:
            break
