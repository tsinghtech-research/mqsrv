#!/usr/bin/env python
import six
import traceback
import sys
import os
import os.path as osp
cur_d = osp.dirname(__file__)
sys.path.insert(0, cur_d+'/../')

import eventlet
from mqsrv.logger import set_logger_level
from mqsrv.client import make_client

eventlet.monkey_patch()

def main(broker_url):
    client = make_client()
    set_logger_level(client, 'debug')

    caller = client.get_caller('server_rpc_queue')
    pubber = client.get_pubber('server_event_queue')

    print('Requesting fib(30)')
    exc, result = caller.fib_fn(n=30)
    print ("result1", result)
    exc, result = caller.fibclass_fib(n=30)
    print ("result2", result)
    if exc:
        print ("="*10)
        print ("Exception from Server, traceback on server:")
        traceback.print_exception(*exc)

        print ("="*10)
        print ("Reraising")
        # raise exc[1]

    pubber('new', {'hello': 1})

    client.release()

if __name__ == '__main__':
    main('pyamqp://')
