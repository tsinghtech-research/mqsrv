from mqsrv.server import make_server, run_server

def handle_user_registered(evt_type, evt_data):
    print(f"New user registered: {evt_data['username']} (ID: {evt_data['user_id']})")

server = make_server(conn='amqp://guest:guest@localhost:5672//', 
                     event_routing_keys=['event_queue'])
server.register_event_handler('user_registered', handle_user_registered)

run_server(server)