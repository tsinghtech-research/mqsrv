import eventlet
from eventlet.queue import LightQueue

green_sleep = eventlet.sleep

green_spawn = eventlet.spawn

def green_thread_join(thr):
    return thr.wait()

GreenQueue = LightQueue

GreenPool = eventlet.GreenPool

def green_pool_join(pool, timeout=None):
    pool.waitall()

class GreenEvent:
    def __init__(self):
        self.evt = eventlet.Event()

    def set(self, data):
        self.evt.send(data)

    def get(self, timeout=None):
        return self.evt.wait(timeout)
