#!/usr/bin/env python
import sys
import traceback
import os
import signal
from functools import partial
import inspect

import eventlet
from eventlet import StopServe
from kombu import Connection, Queue, Exchange
from kombu.mixins import ConsumerProducerMixin

from .rpc_utils import pack_funcs, rpc_decode_req, rpc_encode_rep
from .base import get_connection, get_rpc_exchange, get_event_exchange
from .logger import get_logger, set_logger_level
from .exc import BaseException, MethodNotFound

def format_function_name(fn):
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
                 logger=None,
                 pool_size=10000):

        self.connection = connection
        self.rpc_queue = rpc_queue
        self.event_queues = event_queues

        self.pool = eventlet.GreenPool(pool_size)
        self.ctx_pool = eventlet.GreenPool(pool_size)

        if not logger:
            self.logger = get_logger('mqserver')

        self.rpcs = {}
        self.event_handlers = {}
        self.exc_handlers = []
        self.ctxs = {}

    set_logger_level = set_logger_level

    def register_rpc(self, fn, name=''):
        if not name:
            name = format_function_name(fn)

        self.rpcs[name] = fn
        self.logger.info(f'register_rpc: {name} {fn.__name__}')

    def register_event_handler(self, evt_type, cb):
        if evt_type not in self.event_handlers:
            self.event_handlers[evt_type] = []

        self.event_handlers[evt_type].append(cb)
        self.logger.info(f'register_event_handler: {evt_type} {cb.__name__}')

    def register_exc_handler(self, h):
        self.exc_handlers.append(h)
        self.logger.info(f'register_exc_handler: {h.__name__}')

    def register_context(self, ctx, name=''):
        if not name:
            if hasattr(ctx, 'name'):
                name = ctx.name
            else:
                name = id(ctx)

        self.ctxs[name] = ctx

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(
                on_message=self.on_rpc_message,
                queues=[self.rpc_queue],
                prefetch_count=1,
                no_ack=True,
            ),
            Consumer(
                on_message=self.on_event_message,
                queues=self.event_queues,
                no_ack=True
            ),
        ]

    def send_reply(self, message, result=None, error=None):
        req_id = message.properties['correlation_id']
        routing_key = message.properties['reply_to']
        self.logger.debug(f"sending reply [{routing_key}, {req_id}])")

        self.producer.publish(
            rpc_encode_rep(req_id, result=result, error=error),
            exchange='',
            routing_key=routing_key,
            correlation_id=req_id,
            retry=True,
        )

    def handle_exception(self, e):
        self.logger.exception(e)
        for h in self.exc_handlers:
            self.pool.spawn_n(h, e)

    def rpc_worker(self, message):
        req_id = message.properties['correlation_id']
        _, meth, args, kws = rpc_decode_req(message.payload)
        self.logger.debug(f"reciving request [{self.rpc_queue.name}, {req_id}] {meth}")
        send_reply = partial(self.send_reply, message)

        try:
            if meth not in self.rpcs:
                raise MethodNotFound(f"method {meth} not found!")

            result = self.rpcs[meth](*args, **kws)

        except BaseException as e:
            self.logger.error(f"BaseException for request id {req_id}")
            send_reply(error=sys.exc_info())
            self.handle_exception(e)

        except Exception as e:
            # we need to handle generic Exception BUG here
            self.logger.error(f"BUG for request id {req_id}")
            self.logger.exception(e)
            raise

        else:
            send_reply(result=result)

    def on_rpc_message(self, message):
        self.pool.spawn_n(self.rpc_worker, message)

    def event_worker(self, cb, evt_type, evt_data):
        self.logger.debug(f"reciving event [{evt_type}])")
        try:
            cb(evt_type, evt_data)

        except BaseException as e:
            self.logger(f"BaseException for event {evt_type}")
            self.handle_exception(e)

        except Exception as e:
            self.logger.error(f"BUG for event {evt_type}")
            self.logger.exception(e)

    def on_event_message(self, message):
        evt_type, evt_data = message.payload
        if evt_type not in self.event_handlers:
            return

        for cb in self.event_handlers[evt_type]:
            self.pool.spawn_n(self.event_worker, cb, evt_type, evt_data)

    def apply_to_ctx(self, meth, *args, **kws):
        def worker(ctx):
            getattr(ctx, meth)(*args, **kws)

        ctxs = [i for i in self.ctxs.values() if hasattr(i, meth)]
        self.ctx_pool.imap(worker, ctxs)
        self.ctx_pool.waitall()

    def setup(self):
        self.logger.info("server setting up...")
        self.apply_to_ctx('setup')
        self.logger.info("server setuped.")

    def teardown(self):
        self.logger.info("server tearing down...")
        self.apply_to_ctx('teardown')
        self.logger.info("server teared down.")
        raise StopServe

def make_server(conn=None, rpc_routing_key=None, event_routing_keys=[], rpc_exchange=None, event_exchange=None, rpc_queue=None, event_queues=[], **kws):
    if isinstance(event_routing_keys, str):
        event_routing_keys = [event_routing_keys]

    conn = get_connection(conn)
    if not isinstance(rpc_exchange, Exchange):
        rpc_exchange = get_rpc_exchange(rpc_exchange)

    if not isinstance(event_exchange, Exchange):
        event_exchange = get_event_exchange()

    if rpc_queue is None:
        rpc_queue = Queue(rpc_routing_key, routing_key=rpc_routing_key, exchange=rpc_exchange)

    if not event_queues:
        event_queues = [Queue(i, routing_key=i, exchange=event_exchange) for i in event_routing_keys]

    return MessageQueueServer(conn, rpc_queue, event_queues, **kws)

def run_server(server, block=True):
    logger = server.logger
    logger.info(f'starting at {server.connection.as_uri()}')

    def shutdown(sig_no, frame):
        eventlet.spawn_n(server.teardown)

    signal.signal(signal.SIGTERM, shutdown)

    server.setup()

    runlet = eventlet.spawn(server.run)

    if not block:
        return

    while True:
        try:
            runlet.wait()
        except OSError as exc:
            if exc.errno == errno.EINTR:
                # this is the OSError(4) caused by the signalhandler.
                # ignore and go back to waiting on the runner
                continue
            raise

        except KeyboardInterrupt:
            try:
                server.teardown()
            except StopServe:
                break

        except StopServe:
            break

    logger.info("server stopped")
