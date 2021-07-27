from greenthread.green import *

import uuid

class ServiceBase:
    def __init__(self, name, event_handler=None, events={}, rpc_prefix='', debug=False, validate=False):
        self.name = name

        self.event_handler = event_handler
        self.events = events

        self.debug = debug
        self.validate = validate

        self.rpc_prefix = rpc_prefix

    def publish(self, evt_type, evt_data={}):
        if self.debug:
            assert evt_type in self.events

        if self.event_handler:
            self.event_handler(evt_type, evt_data)

class RunnerBase(ServiceBase):
    def __init__(self, name, *args, interval=0.01, **kws):
        super().__init__(name, *args, **kws)

        self.should_stop = False
        self.interval = interval
        self.runlet = None

    def run(self):
        while not self.should_stop:
            if self.is_idle():
                green_sleep(self.interval)
                continue

            self.process()

    def process(self):
        raise NotImplementedError

    def setup(self):
        self.runlet = green_spawn(self.run)

    def teardown(self):
        self.should_stop = True
        self.runlet.wait()


class TaskQueue:
    def __init__(self, qsize):
        self.task_q = GreenQueue(qsize)
        self.req_events = {}

        for meth in ['empty', 'get']:
            setattr(self, meth, getattr(self.task_q, meth))

    def put(self, d, req_id=None):
        assert d
        self.task_q.put((req_id, d))
        if req_id:
            self.req_events[req_id] = GreenEvent()

    def wait(self, req_id, timeout=None):
        assert req_id in self.req_events
        ret_val = self.req_events[req_id].get(timeout)
        self.req_events.pop(req_id)
        return ret_val

    def call(self, d):
        req_id = str(uuid.uuid1())
        self.put(d, req_id=req_id)
        return self.wait(req_id)

    def send(self, req_id, data={}):
        if req_id:
            self.req_events[req_id].set(data)
