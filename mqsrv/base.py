import logging
import structlog
from kombu import Connection, Exchange
from .contract import RPC_EXCHANGE, EVT_EXCHANGE

def set_logger_level(o, level):
    level = getattr(logging, level.upper())
    o.logger.setLevel(level)

def get_logger(name, level='info'):
    level = getattr(logging, level.upper())
    logging.basicConfig(format="%(message)s")
    logging.getLogger(name).setLevel(level)

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M.%S"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.dev.ConsoleRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    return structlog.getLogger(name)

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
