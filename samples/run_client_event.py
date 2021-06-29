#!/usr/bin/env python
import sys
import os.path as osp
cur_d = osp.dirname(__file__)
sys.path.insert(0, cur_d+'/../')

from mqsrv.monkey import monkey_patch; monkey_patch()

import time
from mqsrv.green import green_sleep
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
