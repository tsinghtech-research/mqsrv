#!/usr/bin/env python
import eventlet
eventlet.monkey_patch()

import click
import json
from common import parse_addr
from eventlet.queue import Queue
from mqsrv.logger import set_logger_level
from mqsrv.client import make_client
from mqsrv.server import make_server

class SocketHandler:
    def __init__(self, sock, pubber, caller, timeout=1):
        self.queue = Queue()
        self.sock = sock
        self.fd = sock.makefile('rw')
        self.pubber = pubber
        self.caller = caller
        self.timeout = timeout
        self.is_disconnected = False

    def handle_message(self):
        while True:
            s = self.fd.readline()
            if not s:
                self.is_disconnected = True
                break
            data = json.loads(s)
            assert data
            self.queue.put(data)

    def run(self):
        while not self.is_disconnected:
            data = self.queue.get()
            cmd = data['Command']
            if cmd == 'get_device_status':
                exc, device_status = self.caller.get_device_status()
                msg = json.dumps(device_status) + '\n'
                self.fd.write(msg)

            elif cmd == 'shutdown':
                self.fd.write('shutdown\n')

            elif cmd =='end_scan':
                exc, ret = self.caller.end_scan()
                self.fd.write('end_scan\n')

            elif cmd == 'start_scan':
                exc, ret = self.caller.start_scan()
                self.fd.write('start_scan\n')

            else:
                print ("unsupported command", data)

            self.fd.flush()

    def handle_event(self, evt_type, evt_data):
        print ("recv event", evt_type, evt_data)
        self.fd.write(json.dumps(evt_data)+'\n')
        self.fd.flush()

@click.command()
@click.option('--addr', default='0.0.0.0:8081')
def main(addr):
    print (f"server started at addr {addr}")

    evt_q = 'mars_event_queue'

    ctrl_client = make_client()
    set_logger_level(ctrl_client, 'debug')
    ctrl_caller = ctrl_client.get_caller('mars_rpc_queue')
    ctrl_pubber = ctrl_client.get_pubber(evt_q)

    ctrl_evt_server = make_server(
        event_routing_keys=[evt_q],
    )
    ctrl_evt_server.setup()
    eventlet.spawn_n(ctrl_evt_server.run)

    front_server = eventlet.listen(parse_addr(addr))
    pool = eventlet.GreenPool()
    while True:
        try:
            client_sock, client_addr = front_server.accept()
            print (f"accept socket({client_addr})")
            handler = SocketHandler(client_sock, ctrl_pubber, ctrl_caller, timeout=1)
            pool.spawn_n(handler.run)
            pool.spawn_n(handler.handle_message)

            ctrl_evt_server.register_event_handler(
                'new_result', handler.handle_event)

        except (SystemExit, KeyboardInterrupt):
            break

if __name__ == "__main__":
    main()
