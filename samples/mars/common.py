def parse_addr(addr):
    ip, port = addr.split(':')
    port = int(port)
    return ip, port
