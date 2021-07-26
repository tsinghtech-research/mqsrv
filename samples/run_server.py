#!/usr/bin/env python
import sys
import os
import os.path as osp
os.environ['GREEN_BACKEND'] = 'gevent'

cur_dir = osp.dirname(__file__)
sys.path.insert(0, cur_dir+'/../')

import toml
import signal
from greenthread.monkey import monkey_patch; monkey_patch()

from greenthread.green import *
from loguru import logger
import time
from kombu import Connection, Exchange
from mqsrv.base import get_rpc_exchange
from mqsrv.server import MessageQueueServer, run_server, make_server
from daemonize import Daemonize
import click

def echo(a):
    return a

def fib_fn(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib_fn(n - 1) + fib_fn(n - 2)

def handle_event(evt_type, evt_data):
    print ("handle event", evt_type, evt_data)

class FibClass:
    rpc_prefix = 'fibclass'

    def setup(self):
        print ("fib setuped")

    def teardown(self):
        print ("fib teared down")

    def fib(self, n):
        return fib_fn(n)

    def worker(self):
        time.sleep(1)
        return True

    def process_slow(self):
        return tpool_execute(self.worker)

def run(config):
    print ("config", config)
    fib_obj = FibClass()
    addr = "amqp://guest:guest@0.0.0.0:5672/"
    rpc_queue = 'server_rpc_queue'
    evt_queue = 'server_event_queue'
    server = make_server(
        conn = addr,
        rpc_routing_key=rpc_queue,
        event_routing_keys=[evt_queue],
    )

    server.register_rpc(echo)
    server.register_rpc(fib_fn)

    server.register_context(fib_obj)
    server.register_rpc(fib_obj.fib)
    server.register_rpc(fib_obj.process_slow)

    server.register_event_handler('new', handle_event)

    run_server(server)

@click.group()
@click.option('--pidfile', default='/tmp/mqsrv/run_server.pid')
@click.pass_context
def cli(ctx, pidfile):
    ctx.ensure_object(dict)
    ctx.obj['pidfile'] = pidfile

@cli.command()
@click.option('--config', default=osp.join(cur_dir, 'config.toml'))
@click.option('--logfile', default='/tmp/mqsrv/run_server.log')
@click.option('--fg', is_flag=True)
@click.pass_context
def start(ctx, config, logfile, fg):
    config = osp.abspath(config)
    pidfile = ctx.obj['pidfile']
    app = "run_server"
    os.makedirs(osp.dirname(pidfile), exist_ok=True)
    kws = {}
    if fg:
        kws['foreground'] = True

    if logfile and not fg:
        fp = open(logfile, 'w')
        keep_fds = [fp.fileno()]
        logger.remove()
        logger.add(fp, level='DEBUG')
        kws['keep_fds'] = keep_fds

    def action():
        cfg = toml.load(config)
        run(cfg)

    daemon = Daemonize(app=app, pid=pidfile, action=action, **kws)
    logger.info(f"start {app} at {pidfile}")
    daemon.start()

@cli.command()
@click.pass_context
def stop(ctx):
    pidfile = ctx.obj['pidfile']
    pid = int(open(pidfile).read())
    logger.info(f"stop {app} at {pidfile} {pid}")
    os.kill(pid, signal.SIGTERM)


if __name__ == "__main__":
    cli()
