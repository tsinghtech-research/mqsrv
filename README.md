# mqsrv

mqsrv is a message queue-based Remote Procedure Call (RPC) and event publish/subscribe system. It provides a flexible framework for building distributed systems and microservice architectures.

## Features

- Supports both synchronous and asynchronous RPC calls
- Event publishing and subscription mechanism
- Connection pool management
- Exception handling
- Flexible serialization options
- Concurrent processing based on greenthread

## Prerequisites

Before installing and using mqsrv, ensure you have a message queue system set up and running. mqsrv is designed to work with various message queue implementations, with RabbitMQ being a popular choice. You can download and install RabbitMQ from their official website:

[RabbitMQ Official Download Page](https://www.rabbitmq.com/download.html)

Follow the installation instructions specific to your operating system. Once installed, make sure the RabbitMQ server is running before proceeding with the mqsrv installation and usage.

## Installation

```bash
pip install mqsrv
```

## Quick Start
### RPC Example
#### Server Side

```python
from mqsrv.server import make_server, run_server

def hello(name):
    return f"Hello, {name}!"

server = make_server(conn='amqp://guest:guest@localhost:5672//', 
                     rpc_routing_key='rpc_queue')
server.register_rpc(hello)

run_server(server)
```

#### Client Side
```python
from mqsrv.client import make_client

client = make_client(conn='amqp://guest:guest@localhost:5672//')
caller = client.get_caller('rpc_queue')

error, result = caller.hello('World')
print(result)  # Output: Hello, World!
```

### Event Example
#### Subscriber
```python

from mqsrv.server import make_server, run_server

def handle_user_registered(evt_type, evt_data):
    print(f"New user registered: {evt_data['username']} (ID: {evt_data['user_id']})")

server = make_server(conn='amqp://guest:guest@localhost:5672//', 
                     event_routing_keys=['event_queue'])
server.register_event_handler('user_registered', handle_user_registered)

run_server(server)
```

#### Publisher
```python
from mqsrv.client import make_client

client = make_client(conn='amqp://guest:guest@localhost:5672//')
publisher = client.get_pubber('event_queue')

publisher('user_registered', {'user_id': 123, 'username': 'john_doe'})
```

## License
This project is licensed under the MIT License.