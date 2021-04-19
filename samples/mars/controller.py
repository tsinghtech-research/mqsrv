#!/usr/bin/env python
import eventlet
eventlet.monkey_patch()

from eventlet import sleep, event
import time
from mqsrv.logger import set_logger_level
from mqsrv.base import get_rpc_exchange
from mqsrv.client import make_client
from mqsrv.server import MessageQueueServer, run_server, make_server

class Controller:
    def __init__(self, evt_q, interval=0.1):
        self.client = make_client()
        self.pubber = self.client.get_pubber(evt_q)
        self.is_running = False
        self.has_send_stop_evt = False
        self.stop_evt = event.Event()
        self.interval = interval

    def get_device_status(self):
        return {f'camera{i}': True for i in range(6)}

    def end_scan(self, timeout=10):
        if not self.is_running:
            return True

        self.is_running = False
        print ("end scan")
        start_t = time.time()
        self.stop_evt.wait(timeout=timeout)
        print ("finish end scan", time.time() - start_t)
        return True

    def start_scan(self):
        self.is_running = True
        self.has_send_stop_evt = False
        self.stop_evt = event.Event()
        # do initialization job
        return True

    def run(self):
        while True:
            if not self.is_running and not self.has_send_stop_evt:
                self.stop_evt.send()
                self.has_send_stop_evt = True

            if not self.is_running:
                sleep(self.interval)
                continue

            sleep(0.5) # do your job
            print ("publising")
            self.pubber("new_result", {'code': '12345'})

def main():
    evt_q = 'mars_event_queue'
    obj = Controller(evt_q)

    eventlet.spawn_n(obj.run)

    server = make_server(
        rpc_routing_key='mars_rpc_queue',
        # event_routing_keys=[evt_q],
    )

    server.register_rpc(obj.get_device_status, "get_device_status")
    server.register_rpc(obj.start_scan, "start_scan")
    server.register_rpc(obj.end_scan, "end_scan")

    set_logger_level(server, 'debug')
    print ("controller started")
    run_server(server)

if __name__ == '__main__':
    main()
