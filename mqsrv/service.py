import uuid
import eventlet
from eventlet import event
from eventlet.queue import LightQueue

from .logger import get_logger, set_logger_level

class ServiceBase:
    def __init__(self, name, event_handler=None, events={}, rpc_prefix='', logger=None, logger_kws={}, debug=False, validate=False):
        self.name = name

        if not logger:
            logger = get_logger(self.name, **logger_kws)
        self.logger = logger

        self.event_handler = event_handler
        self.events = events

        self.debug = debug
        self.validate = validate

        self.rpc_prefix = rpc_prefix

    set_logger_level = set_logger_level

    def publish(self, evt_type, evt_data={}):
        if self.debug:
            assert evt_type in self.events

        if self.event_handler:
            self.event_handler(evt_type, evt_data)

class RunnerBase(ServiceBase):
    def __init__(self, name, *args, interval=0.1, **kws):
        super().__init__(name, *args, **kws)

        self.should_stop = False
        self.interval = interval
        self.runlet = None

    def run(self):
        while not self.should_stop:
            if self.is_idle():
                eventlet.sleep(self.interval)
                continue

            self.process()

    def process(self):
        raise NotImplementedError

    def setup(self):
        self.runlet = eventlet.spawn(self.run)

    def teardown(self):
        self.should_stop = True
        self.runlet.wait()


class TaskQueue:
    def __init__(self, qsize):
        self.task_q = LightQueue(qsize)
        self.req_events = {}

        for meth in ['empty', 'get']:
            setattr(self, meth, getattr(self.task_q, meth))

    def put(self, d, req_id=None):
        assert d
        self.task_q.put((req_id, d))
        if req_id:
            self.req_events[req_id] = event.Event()

    def wait(self, req_id):
        assert req_id in self.req_events
        ret_val = self.req_events[req_id].wait()
        self.req_events.pop(req_id)
        return ret_val

    def call(self, d):
        req_id = str(uuid.uuid1())
        self.put(d, req_id=req_id)
        return self.wait(req_id)

    def send(self, req_id, data={}):
        if req_id:
            self.req_events[req_id].send(data)
