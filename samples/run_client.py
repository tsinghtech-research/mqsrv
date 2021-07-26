#!/usr/bin/env python
import sys
import os.path as osp
cur_d = osp.dirname(__file__)
sys.path.insert(0, cur_d+'/../')

from greenthread.monkey import monkey_patch; monkey_patch()

from greenthread.green import *
from loguru import logger
import traceback
import sys

from mqsrv.client import make_client

def main(broker_url):
    client = make_client()

    caller = client.get_caller('server_rpc_queue')
    pubber = client.get_pubber('server_event_queue')

    for i in range(10):
        print ("sending echo")
        exc, result = caller.echo("hello")

    t = 3
    print ('-'*10)
    print (f"slepping {t}s")
    green_sleep(t)
    print (f'wake up')

    for i in range(10):
        print ("sending echo")
        exc, result = caller.echo("hello")

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


    print ("processing slow")
    _, ret = caller.fibclass_process_slow()
    print ("processing slow", ret)

    client.release()

if __name__ == '__main__':
    main('pyamqp://')
