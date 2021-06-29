def monkey_patch():
    import os
    backend = os.environ.get('GREEN_BACKEND', 'eventlet')
    if backend == 'eventlet':
        import eventlet
        eventlet.monkey_patch()
    else:
        import gevent.monkey
        gevent.monkey.patch_all()
