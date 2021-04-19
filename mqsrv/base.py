from kombu import Connection, Exchange
from .contract import RPC_EXCHANGE, EVT_EXCHANGE

def get_rpc_exchange(name=None):
    if not name:
        name = RPC_EXCHANGE
    return Exchange(name, type='topic', durable=True)

def get_event_exchange(name=None):
    if not name:
        name = EVT_EXCHANGE
    return Exchange(name, type='topic', durable=True, delivery_mode=Exchange.PERSISTENT_DELIVERY_MODE)

def get_connection(conn=None):
    if isinstance(conn, Connection):
        return conn

    if conn is None:
        conn = Connection()
    elif isinstance(conn, str):
        conn = Connection(conn)
    else:
        raise

    return conn

def declare_entity(obj, conn):
    obj(conn).declare()
