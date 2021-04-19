#!/usr/bin/env python
import eventlet
eventlet.monkey_patch()

import six
import traceback
import sys
import os
import os.path as osp
cur_d = osp.dirname(__file__)
sys.path.insert(0, cur_d+'/../')

from mqsrv.logger import set_logger_level
from mqsrv.client import make_client

def main(broker_url):
    client = make_client()
    set_logger_level(client, 'debug')

    caller = client.get_caller('server_rpc_queue')
    pubber = client.get_pubber('server_event_queue')
    pubber('new', {'hello': 1})
    client.release()

if __name__ == '__main__':
    main('pyamqp://')
