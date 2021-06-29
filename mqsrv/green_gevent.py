import gevent
from gevent import event
from gevent import pool
from gevent import queue

green_sleep = gevent.sleep

green_spawn = gevent.spawn

def green_thread_join(thr):
    return thr.join()

GreenQueue = queue.Queue

GreenPool = pool.Pool

def green_pool_join(pool, timeout=None):
    pool.join(timeout)

class GreenEvent:
    def __init__(self):
        self.ret = event.AsyncResult()

    def set(self, data):
        self.ret.set(data)

    def get(self, timeout=None):
        if timeout is None:
            block = True
        else:
            block = False
        return self.ret.get(block=block, timeout=timeout)
