#!/usr/bin/env python
import eventlet
eventlet.monkey_patch()

import click
import json
from common import parse_addr, sock_send
from eventlet.queue import Queue
from mqsrv.logger import set_logger_level
from mqsrv.client import make_client
from mqsrv.server import make_server, run_server

class SocketHandler:
    def __init__(self, sock, pubber, caller, timeout=1, max_size=512):
        self.cmd_queue = Queue()
        self.evt_queue = Queue()
        self.sock = sock
        self.fd = sock.makefile('rw')
        self.pubber = pubber
        self.caller = caller
        self.timeout = timeout
        self.max_size = max_size
        self.is_connected = True

    def __del__(self):
        self.close()

    def close(self):
        self.is_connected = False
        self.caller.end_scan()

    def loop_put_command(self):
        while True:
            s = self.sock.recv(self.max_size)
            if not s:
                self.close()
                break
            data = json.loads(s)
            assert data
            self.cmd_queue.put(data)

    def sock_send(self, msg):
        if not self.is_connected:
            return False

        try:
            sock_send(self.sock, msg)
        except socket.error:
            self.close()
            return False

        return True

    def loop_process_command(self):
        while self.is_connected:
            data = self.cmd_queue.get()
            cmd = data['Command']
            if cmd == 'get_device_status':
                exc, device_status = self.caller.get_device_status()
                msg = json.dumps(device_status) + '\n'
                self.sock_send(msg)

            elif cmd == 'shutdown':
                self.sock_send('shutdown\n')

            elif cmd =='end_scan':
                exc, ret = self.caller.end_scan()
                self.sock_send('end_scan\n')

            elif cmd == 'start_scan':
                exc, ret = self.caller.start_scan()
                self.sock_send('start_scan\n')

            else:
                print ("unsupported command", data)

    def put_event(self, evt_type, evt_data):
        self.evt_queue.put((evt_type, evt_data))

    def loop_process_event(self):
        while self.is_connected:
            evt_type, evt_data = self.evt_queue.get()
            print ("recv event", evt_type, evt_data)
            self.sock_send('\x02'+json.dumps(evt_data)+'\n')

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
    run_server(ctrl_evt_server, block=False)

    front_server = eventlet.listen(parse_addr(addr))
    pool = eventlet.GreenPool()
    while True:
        try:
            client_sock, client_addr = front_server.accept()
            print (f"accept socket({client_addr})")
            handler = SocketHandler(client_sock, ctrl_pubber, ctrl_caller, timeout=1)
            pool.spawn_n(handler.loop_process_event)
            pool.spawn_n(handler.loop_process_command)
            pool.spawn_n(handler.loop_put_command)

            ctrl_evt_server.register_event_handler(
                'new_result', handler.put_event)

        except (SystemExit, KeyboardInterrupt):
            break

if __name__ == "__main__":
    main()
