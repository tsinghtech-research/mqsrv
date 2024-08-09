from mqsrv.server import make_server, run_server

def hello(name):
    return f"Hello, {name}!"

server = make_server(conn='amqp://guest:guest@localhost:5672//', 
                     rpc_routing_key='rpc_queue')
server.register_rpc(hello)

run_server(server)