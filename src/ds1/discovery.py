from __future__ import annotations
from typing import Optional
import socket
import asyncio
import logging
import argparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)-7s [node %(node_id)s] %(message)s",
    datefmt="%H:%M:%S",
)
base_logger = logging.getLogger(__name__)

member_list : dict[str,tuple[str,int]] = {}

def create_socket(port,log):
    sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    sock.setsockopt(socket.SOL_SOCKET,socket.SO_BROADCAST,1)
    sock.bind(('0.0.0.0',port))
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
        if exc is not None:
            self.log.warning("UDP connection lost error: %s",exc)
        else:
            self.log.info("UDP endpoint closed")


async def announce(transport,node_id,log,port):
    while True:
        transport.sendto(f"HELLO {node_id}".encode(),('255.255.255.255',port))
        log.info(f"HELLO broadcasted by node:{node_id}")
        await asyncio.sleep(2)


async def handler(protocol,node_id,port,log):
    while True:
        message,addr = await protocol.queue.get()
        message = message.split(" ")
        if len(message) != 2:
            log.warning(f"Malformed data packet received from {addr}")
        else:
            try:
                if message[0] == "HELLO" and int(message[1])!=node_id:
                    log.info(f"HELLO recieved from {message[1]}")
                    protocol.transport.sendto(f"HELLO_ACK {node_id}".encode(),addr)
                    if message[1] not in member_list:
                        member_list[message[1]] = addr
                        log.info(f"HELLO received from {message[1]} and added to the active member list")
                elif message[0] == "HELLO_ACK" and int(message[1])!=node_id: 
                     log.info(f"ACK received from node: {message[1]}")
                     print(member_list)
            except ValueError:
                log.warning(f"Missing node_id received from {addr}")
            

async def main(node_id,log,port):
    loop = asyncio.get_running_loop()
    sock = create_socket(port,log)
    transport, protocol = await loop.create_datagram_endpoint(lambda:DiscoveryProtocol(log),sock=sock)
    await asyncio.gather(announce(transport,node_id,log,port), handler(protocol,node_id,port,log))


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("node_id", type=int)
    parser.add_argument("--port", type=int, default=5858)   
    args = parser.parse_args()
    node_id = args.node_id
    port = args.port
    log = logging.LoggerAdapter(base_logger,{"node_id":node_id})
    asyncio.run(main(node_id,log,port))
