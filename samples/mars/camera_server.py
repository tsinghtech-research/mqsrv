#!/usr/bin/env python
import eventlet
eventlet.monkey_patch()

from eventlet import sleep
from mqsrv.logger import set_logger_level
from mqsrv.client import make_client
from mqsrv.server import make_server, run_server

class CameraService:
    def __init__(self, evt_q, interval=0.1):
        self.evt_client = self.evt_client.get_pubber(evt_q)
        self.is_running = False
        self.has_send_stop_evt = False
        self.stop_evt = event.Event()
        self.interval = interval

    def start(self):
        self.is_running = True
        self.has_send_stop_evt = False
        self.stop_evt = event.Event()
        return True

    def stop(self, timeout=10):
        if not self.is_running:
            return True

        self.is_running = False
        start_t = time.time()
        self.stop_evt.wait(timeout=timeout)
        return True

    def run(self):
        while True:
            if not self.is_running and not self.has_send_stop_evt:
                self.stop_evt.send()
                self.has_send_stop_evt = True

            if not self.is_running:
                sleep(self.interval)
                continue



@click.command()
def main():
    evt_q = 'mars_evtq_camera'

    ctrl_client = make_client()
    set_logger_level(ctrl_client, 'debug')
    ctrl_caller = ctrl_client.get_caller('mars_rpcq_camera')
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
