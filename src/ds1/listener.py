import socket
import asyncio
import argparse

async def connection(id:str):

    sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    sock.bind(('0.0.0.0',5000))

    while True:
        msg, address = await sock.recvfrom(1024)
        print(f"Receiver: R{id}, msg: {msg.decode('utf-8')}, sender: {address}")



if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("node_id",type=str)

    id = parser.parse_args()

    asyncio.run(connection(id.node_id))
