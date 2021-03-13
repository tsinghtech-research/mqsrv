def parse_addr(addr):
    ip, port = addr.split(':')
    port = int(port)
    return ip, port

def sock_send(sock, msg):
    return sock.send(msg.encode())
