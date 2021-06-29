#!/usr/bin/env python

def patch_gevent():
    import gevent.monkey
    gevent.monkey.patch_all()

def patch_eventlet():
    import eventlet
    return eventlet.monkey_patch()

def is_patched_eventlet():
    import eventlet
    return eventlet.patcher.is_monkey_patched('socket')

def is_patched_gevent():
    import socket
    return 'gevent' in str(socket.socket)

def test_eventlet():
    print ("test eventlet")
    patch_eventlet()

    assert is_patched_eventlet()
    assert not is_patched_gevent()

def test_gevent():
    print ("test gevent")
    patch_gevent()

    assert not is_patched_eventlet()
    assert is_patched_gevent()

if __name__ == "__main__":
    import sys
    sel = sys.argv[1]
    if sel == 'eventlet':

        test_eventlet()
    else:
        assert sel == 'gevent'
        test_gevent()
