import gevent
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
        self.ret = gevent.AsyncResult()

    def set(self, data):
        self.ret.set(data)

    def get(self, timeout=None):
        return self.ret.get(timeout)
