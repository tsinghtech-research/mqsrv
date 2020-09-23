import socket
import eventlet
from kombu import Connection, Producer, Consumer, Queue, uuid, Exchange
from .rpc_utils import rpc_encode_req, rpc_decode_rep
from .base import get_rpc_exchange, get_event_exchange, get_logger, get_connection

class Publisher:
    def __init__(self, client, routing_key):
        self.client = client
        self.routing_key = routing_key

    def publish(self, *args, **kws):
        self.client.publish(self.routing_key, *args, **kws)

    def __call__(self, *args, **kws):
        self.publish(*args, **kws)

class _Method:
    def __init__(self, client, routing_key, method):
        self.client = client
        self.routing_key = routing_key
        self.method = method

    def __call__(self, *args, **kws):
        return self.client.call(self.routing_key, self.method, *args, **kws)

    def call_async(self, *args, **kws):
        return self.client.call_async(self.routing_key, self.method, *args, **kws)

class Caller:
    def __init__(self, client, routing_key):
        self.client = client
        self.routing_key = routing_key

    def __getattr__(self, meth):
        return _Method(self.client, self.routing_key, meth)

class MessageQueueClient:
    def __init__(
            self,
            connection,
            rpc_exchange,
            callback_queue,
            event_exchange,
            logger=None,
            interval=0.1):

        if not logger:
            self.logger = get_logger('mqclient')

        self.send_conn = connection
        self.recv_conn = connection.clone()

        self.callback_queue = callback_queue

        if rpc_exchange is None:
            rpc_exchange = get_rpc_exchange()

        self.rpc_exchange = rpc_exchange
        self.event_exchange = event_exchange

        self.interval = interval
        self.should_stop = False

        self.req_events = {}

        self.runner = eventlet.spawn(self.run)

    def on_response(self, message):
        req_id = message.properties['correlation_id']
        self.logger.debug(f"response [{req_id}]")

        if req_id in self.req_events:
            error, result = rpc_decode_rep(message.payload)
            self.req_events[req_id].send((error, result))
            del self.req_events[req_id]

    def run(self):
        with Consumer(self.recv_conn,
                      on_message=self.on_response,
                      queues=[self.callback_queue],
                      no_ack=True):
            while not self.should_stop:
                try:
                    self.recv_conn.drain_events(timeout=self.interval)
                except socket.timeout:
                    continue

    def call_async(self, routing_key, meth, **kws):
        req_id = uuid()
        self.req_events[req_id] = eventlet.Event()
        self.logger.debug(f"request: [{routing_key}, {self.callback_queue.name}, {req_id}] {meth}")

        with Producer(self.send_conn) as producer:
            producer.publish(
                rpc_encode_req(req_id, meth, kws),
                exchange=self.rpc_exchange,
                routing_key=routing_key,
                reply_to=self.callback_queue.name,
                correlation_id=req_id,
            )
        return self.req_events[req_id]

    def call(self, *args, **kws):
        evt = self.call_async(*args, **kws)
        return evt.wait()

    def publish(self, routing_key, evt_type, evt_data):
        with Producer(self.send_conn) as producer:
            producer.publish(
                [evt_type, evt_data],
                exchange=self.event_exchange,
                routing_key=routing_key,
            )

    def release(self):
        self.should_stop = True
        self.runner.wait()
        self.recv_conn.release()
        self.send_conn.release()

    close = release

    def get_pubber(self, routing_key):
        return Publisher(self, routing_key)

    def get_caller(self, routing_key):
        return Caller(self, routing_key)

def make_client(conn=None, rpc_exchange=None, callback_queue=None, event_exchange=None, **kws):
    conn = get_connection(conn)
    if not isinstance(rpc_exchange, Exchange):
        rpc_exchange = get_rpc_exchange()

    if callback_queue is None:
        callback_queue = Queue('rpc-'+uuid(), exclusive=True, auto_delete=True)

    if not isinstance(event_exchange, Exchange):
        event_exchange = get_event_exchange()

    return MessageQueueClient(conn, rpc_exchange, callback_queue, event_exchange, **kws)
