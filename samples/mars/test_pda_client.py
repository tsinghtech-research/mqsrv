#!/usr/bin/env python
import eventlet
eventlet.monkey_patch()

from eventlet import sleep
from eventlet.green import threading
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

recv_reply = False

def recv_thread(sock):
    global recv_reply
    while True:
        s = sock.recv(512)
        if not s:
            break
        if s[0] == '\x02':
            print (s)
        else:
            recv_reply = True

def main(addr='127.0.0.1:8081'):
    global recv_reply
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(parse_addr(addr))

    eventlet.spawn_n(recv_thread, sock)
    for i in range(2):
        print (f"round {i}")
        for cmd in cmds:
            if cmd['Command'] == 'end_scan':
                sleep(5)

            recv_reply = False
            msg = json.dumps(cmd)
            sock.send((msg+'\n').encode())
            print (cmd)

            while not recv_reply:
                sleep(0.01)

    sock.close()

if __name__ == "__main__":
    import typer
    typer.run(main)
