from mqsrv.client import make_client

client = make_client(conn='amqp://guest:guest@localhost:5672//')
caller = client.get_caller('rpc_queue')

error, result = caller.hello('World')
print(result)  # Output: Hello, World!