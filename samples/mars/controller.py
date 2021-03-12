#!/usr/bin/env python
import eventlet
eventlet.monkey_patch()

from eventlet import sleep, event
from mqsrv.logger import set_logger_level
from mqsrv.base import get_rpc_exchange
from mqsrv.client import make_client
from mqsrv.server import MessageQueueServer, run_server, make_server

class Controller:
    def __init__(self, evt_q, interval=0.1):
        self.client = make_client()
        self.pubber = self.client.get_pubber(evt_q)
        self.is_running = False
        self.prev_is_running = False
        self.stop_evt = event.Event()
        self.interval = interval

    def get_device_status(self):
        return {f'camera{i}': True for i in range(6)}

    def end_scan(self, timeout=10):
        self.is_running = False
        self.stop_evt.wait(timeout=timeout)
        return True

    def start_scan(self):
        self.is_running = True
        # do initialization job
        return True

    def run(self):
        while True:
            if self.prev_is_running and not self.is_running:
                self.stop_evt.send()
                self.prev_is_running = self.is_running

            if not self.is_running:
                sleep(self.interval)
                self.prev_is_running = self.is_running
                continue

            sleep(0.5) # do your job
            print ("publising")
            self.pubber("new_result", {'code': '12345'})

            self.prev_is_running = self.is_running

def main():
    evt_q = 'mars_event_queue'
    obj = Controller(evt_q)

    eventlet.spawn_n(obj.run)

    server = make_server(
        rpc_routing_key='mars_rpc_queue',
        event_routing_keys=[evt_q],
    )

    server.register_rpc(obj.get_device_status, "get_device_status")
    server.register_rpc(obj.start_scan, "start_scan")
    server.register_rpc(obj.end_scan, "end_scan")

    set_logger_level(server, 'debug')
    print ("controller started")
    run_server(server)

if __name__ == '__main__':
    main()
