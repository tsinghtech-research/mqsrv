#!/usr/bin/env python
import eventlet
eventlet.monkey_patch()

from eventlet import sleep
import json
import socket
from time import sleep
from common import parse_addr

cmds = [
    {
        'DeviceCode': 'PDA_001',
        'Command': 'get_device_status',
    },

    {
        'DeviceCode': 'PDA_001',
        'Command': 'shutdown',
    },
    {
        'DeviceCode': 'PDA_001',
        'Command': 'start_scan',
    },
    {
        'DeviceCode': 'PDA_001',
        'Command': 'end_scan',
    },
]

def recv_thread(sock):
    fd = sock.makefile('r')
    while True:
        s = fd.readline()
        if not s:
            break

        print (s)

def main(addr='127.0.0.1:8081'):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(parse_addr(addr))

    eventlet.spawn_n(recv_thread, sock)
    fd = sock.makefile('w')
    for cmd in cmds:
        if cmd['Command'] == 'end_scan':
            sleep(10)

        msg = json.dumps(cmd)
        fd.write(msg+'\n')
        fd.flush()


if __name__ == "__main__":
    import typer
    typer.run(main)
