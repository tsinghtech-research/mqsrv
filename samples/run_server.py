#!/usr/bin/env python
import sys
import traceback
import os
import os.path as osp
cur_d = osp.dirname(__file__)
sys.path.insert(0, cur_d+'/../')

from kombu import Connection, Exchange

import eventlet
from mqsrv.logger import set_logger_level
from mqsrv.base import get_rpc_exchange
from mqsrv.server import MessageQueueServer, run_server, make_server

eventlet.monkey_patch()

def fib_fn(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib_fn(n - 1) + fib_fn(n - 2)

def handle_event(evt_type, evt_data):
    print ("handle event", evt_type, evt_data)

class FibClass:
    def setup(self):
        print ("fib setuped")

    def teardown(self):
        print ("fib teared down")

    def fib(self, n):
        return fib_fn(n)

if __name__ == '__main__':
    fib_obj = FibClass()

    server = make_server(
        rpc_routing_key='server_rpc_queue',
        event_routing_keys=['server_event_queue'],
    )
    server.register_rpc(fib_obj.fib)
    server.register_rpc(fib_fn)
    server.register_event_handler('new', handle_event)
    server.register_context(fib_obj)

    set_logger_level(server, 'debug')
    run_server(server)
