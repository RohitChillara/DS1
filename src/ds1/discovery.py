from __future__ import annotations
import socket
import asyncio
from ds1 import messages as m,state as s
from ds1.config import UDP_PORT
from pydantic import ValidationError
from ds1 import monitor

def create_socket(port,log):
    sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    sock.setsockopt(socket.SOL_SOCKET,socket.SO_BROADCAST,1)
    sock.bind(('0.0.0.0',UDP_PORT))
    log.info("Socket created on port %d",port)
    return sock

class DiscoveryProtocol(asyncio.DatagramProtocol):
    def __init__(self,log):
        self.queue = asyncio.Queue(maxsize=100)
        self.transport = None
        self.log = log
    def connection_made(self, transport):
        self.transport = transport
    def datagram_received(self, data, addr):
        try:
            message = data.decode()
            if self.queue.full():
                self.log.warning(f"{message} from {addr} dropped due to insufficient queue")
                return
            else:
                self.queue.put_nowait((message,addr))
        except UnicodeDecodeError:
            self.log.warning(f"Data from {addr} dropped due to decode error")
    def error_received(self, exc):
        self.log.warning("UDP error %s",exc)
    def connection_lost(self, exc):
        pass

async def announce(transport, log):
    while True:
        hello = m.Message(type="HELLO", sender=s.MyDetails.node_id, leader=s.Members.leader)
        transport.sendto(m.encode(hello), ("255.255.255.255", UDP_PORT))
        log.debug("HELLO broadcast")
        await asyncio.sleep(2)

async def handler(protocol, log):
    while True:
        data, addr = await protocol.queue.get()
        try:
            msg = m.decode(data)
        except ValidationError:
            log.warning(f"Malformed packet from {addr}")
            continue

        if msg.sender == s.MyDetails.node_id or msg.type not in ("HELLO", "HELLO_ACK"):
            continue

        is_new = msg.sender not in s.Members.members
        is_alive = msg.sender in s.Heartbeats.last_beat

        if is_new:
            s.Members.members[msg.sender] = addr[0]
            asyncio.create_task(monitor.heartbeats(msg.sender, log))
            log.info(f"Discovered node {msg.sender} at {addr[0]}")

        if msg.type == "HELLO" and not is_alive:
            ack = m.Message(type="HELLO_ACK", sender=s.MyDetails.node_id, leader=s.Members.leader)
            protocol.transport.sendto(m.encode(ack), addr)
            log.info(f"HELLO_ACK sent to {msg.sender}")


async def discovery(log):
    loop = asyncio.get_running_loop()
    sock = create_socket(UDP_PORT,log)
    transport, protocol = await loop.create_datagram_endpoint(lambda:DiscoveryProtocol(log),sock=sock)
    await asyncio.gather(announce(transport,log), handler(protocol,log))