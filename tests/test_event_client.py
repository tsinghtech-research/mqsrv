from mqsrv.client import make_client

client = make_client(conn='amqp://guest:guest@localhost:5672//')
publisher = client.get_pubber('event_queue')

publisher('user_registered', {'user_id': 123, 'username': 'john_doe'})