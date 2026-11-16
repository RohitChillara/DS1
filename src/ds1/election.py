import socket
import time

sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
sock.setsockopt(socket.SOL_SOCKET,socket.SO_BROADCAST,1)
sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
sock.bind(('0.0.0.0',5850))

while True:
    sock.sendto("Hi".encode('utf-8'),('255.255.255.255',5000))
    time.sleep(5)