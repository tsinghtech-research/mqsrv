#!/usr/bin/env python
import eventlet
eventlet.monkey_patch()

import sys
import traceback
import os
import os.path as osp
cur_d = osp.dirname(__file__)
sys.path.insert(0, cur_d+'/../')

from kombu import Connection, Exchange
from mqsrv.logger import set_logger_level
from mqsrv.base import get_rpc_exchange
from mqsrv.server import MessageQueueServer, run_server, make_server

def echo(a):
    return a

class EventServer:
    rpc_prefix = 'event_server'
    def __init__(self, name):
        self.name = name

    def setup(self):
        print (f"server {self.name} setuped")

    def teardown(self):
        print (f"server {self.name} teared down")

    def handle_event(self, evt_type, evt_data):
        print (f"server {self.name} handle event", evt_type, evt_data)

def main(name):
    srv = EventServer(name)

    server = make_server(
        rpc_routing_key=f'server_rpc_queue_{name}',
        event_routing_keys=[(f'server_event_queue_{name}', 'server_event_key')],
    )
    server.register_rpc(echo)
    server.register_event_handler('new', srv.handle_event)
    server.register_context(srv)

    set_logger_level(server, 'debug')
    run_server(server)

if __name__ == "__main__":
    import fire
    fire.Fire(main)
