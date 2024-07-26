from loguru import logger
import socket
from kombu import Connection, Producer, Consumer, Queue, uuid, Exchange

from greenthread.green import *
from .rpc_utils import rpc_encode_req, rpc_decode_rep
from .base import get_rpc_exchange, get_event_exchange, get_connection, declare_entity

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
    
class ConnectionPool:
    def __init__(self, conn, maxsize=10):
        self._resources = GreenQueue(maxsize)
        for _ in range(maxsize):
            self._resources.put((get_connection(conn), Queue('cbq-'+uuid(), exclusive=True, auto_delete=True)))
            
    def get(self):
        return self._resources.get()
    
    def release(self, resource):
        self._resources.put(resource)
        
    def close(self):
        while not self._resources.empty():
            conn, cbq = self._resources.get()
            conn.release()

class MessageQueueClient:
    def __init__(
            self,
            connection,
            rpc_exchange,
            event_exchange,
            serializer=None,
            conn_pool_maxsize=1):

        self.serializer = serializer
        conn_pool_maxsize = conn_pool_maxsize
        
        if rpc_exchange is None:
            rpc_exchange = get_rpc_exchange()
        self.rpc_exchange = rpc_exchange

        if event_exchange is None:
            event_exchange = get_event_exchange()
        self.event_exchange = event_exchange
        
        self.should_stop = False
        self.req_events = {}
        
        self.conn_pool = ConnectionPool(connection, conn_pool_maxsize)
        self.lock = Semaphore()

    def on_response(self, message):
        req_id = message.properties['correlation_id']
        callback_queue = message.delivery_info['routing_key']
        logger.debug(f"receiving response [{callback_queue}, {req_id}]")

        if req_id in self.req_events:
            error, result = rpc_decode_rep(message.payload)
            self.req_events[req_id].set((req_id, error, result))

    def call_async(self, routing_key, meth, *args, **kws):
        conn, callback_queue = self.conn_pool.get()
        req_id = 'corr-'+uuid()
        logger.debug(f"sending request: [{routing_key}, {callback_queue.name}, {req_id}] {meth}")

        self.req_events[req_id] = GreenEvent()

        with self.lock:
            with Producer(conn) as producer:
                producer.publish(
                    rpc_encode_req(req_id, meth, args, kws),
                    exchange=self.rpc_exchange,
                    routing_key=routing_key,
                    declare=[callback_queue],
                    reply_to=callback_queue.name,
                    correlation_id=req_id,
                    serializer=self.serializer,
                )
        
        green_spawn(self._drain_events, conn, callback_queue, req_id)
                
        return self.req_events[req_id]
    
    def _drain_events(self, conn, callback_queue, req_id):
        with self.lock:
            with Consumer(conn,
                        accept=['pickle', 'json'],
                        on_message=self.on_response,
                        queues=[callback_queue],
                        no_ack=True):
                while not self.req_events[req_id].ready() and not self.should_stop:
                    try:
                        conn.drain_events(timeout=0.1)
                    except socket.timeout:
                        continue
                    
                self.conn_pool.release((conn, callback_queue))

    def call(self, *args, timeout=None, **kws):
        evt = self.call_async(*args, **kws)
        try:
            req_id, *ret = evt.get(timeout)
            del self.req_events[req_id]
            return ret
        except BaseException as e:
            evt.set(None)
            raise e

    def publish(self, routing_key, evt_type, evt_data):
        conn, callback_queue = self.conn_pool.get()
        with Producer(conn) as producer:
            producer.publish(
                [evt_type, evt_data],
                exchange=self.event_exchange,
                routing_key=routing_key,
                serializer=self.serializer,
            )
        self.conn_pool.release((conn, callback_queue))

    def release(self):
        self.should_stop = True
        self.conn_pool.close()

    close = release
    teardown = release

    def get_pubber(self, routing_key):
        return Publisher(self, routing_key)

    def get_caller(self, routing_key):
        return Caller(self, routing_key)

def make_client(conn=None, rpc_exchange=None, event_exchange=None, **kws):
    # conn = get_connection(conn)
    if not isinstance(rpc_exchange, Exchange):
        rpc_exchange = get_rpc_exchange()

    if not isinstance(event_exchange, Exchange):
        event_exchange = get_event_exchange()

    return MessageQueueClient(conn, rpc_exchange, event_exchange, **kws)
