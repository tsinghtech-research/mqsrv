def is_patched_eventlet():
    try:
        import eventlet
    except ImportError:
        return False
    return eventlet.patcher.is_monkey_patched('socket')

def is_patched_gevent():
    try:
        import gevent.monkey
    except ImportError:
        return False

    import socket
    return 'gevent' in str(socket.socket)

if is_patched_eventlet():
    print ("eventlet backend is used")
    from .green_eventlet import *
elif is_patched_gevent():
    print ("gevent backend is used")
    from .green_gevent import *
else:
    print ("no green backend used!")
