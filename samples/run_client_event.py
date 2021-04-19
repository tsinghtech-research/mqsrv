#!/usr/bin/env python
import eventlet
eventlet.monkey_patch()

import six
import traceback
import sys
import os
import os.path as osp
import time
cur_d = osp.dirname(__file__)
sys.path.insert(0, cur_d+'/../')

from mqsrv.logger import set_logger_level
from mqsrv.client import make_client

def main(broker_url):
    client = make_client()
    set_logger_level(client, 'debug')

    caller = client.get_caller('server_rpc_key')
    pubber = client.get_pubber('server_event_key')
    msg = time.time()
    print (msg)
    pubber('new', {'hello': msg})
    client.release()

if __name__ == '__main__':
    main('pyamqp://')
