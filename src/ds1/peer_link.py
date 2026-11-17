from __future__ import annotations
from typing import Literal
import asyncio
import logging
import argparse 
from functools import partial
from pydantic import BaseModel, ValidationError
from dataclasses import dataclass,field
import time
import socket

@dataclass
class NodeState:
    node_id: int
    leader : int | None
    term: int = 0
    last_seen:dict[int,float] = field(default_factory=dict)

class Message(BaseModel):
    type: Literal["HEARTBEAT","ELECTION","COORDINATOR","OK"]
    sender: int
    term: int

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)-7s [node %(node_id)s] %(message)s",
    datefmt="%H:%M:%S",
)
base_logger = logging.getLogger(__name__)

async def handler(log,reader,writer):
    addr = writer.get_extra_info("peername")
    while True:
        line = await reader.readline()
        if not line:
            writer.close()
            await writer.wait_closed()
            log.warning(f"{addr}-connection closed")
            return
        try:
            m = Message.model_validate_json(line)
            if m.type == "HEARTBEAT":
                log.info(f"Heartbeat received from {m.sender}")
                state.last_seen[m.sender] = time.monotonic()
                continue
        except ValidationError as e:
            log.error(f"Malformed data sent by {addr}, Error: {e}")
            continue


async def heartbeats(addr,log):
    host , port = addr
    while True:
        writer = None
        try:
            reader , writer = await asyncio.open_connection(host,port)
            while True:
                msg = Message(type="HEARTBEAT",sender=state.node_id,term=state.term)
                m = msg.model_dump_json() + '\n'
                writer.write(m.encode())
                log.info(f"Hearbeat from {state.node_id} is sent")
                await writer.drain()
                await asyncio.sleep(1)
        except (ConnectionRefusedError, ConnectionResetError, BrokenPipeError) as cre:
            log.error(f"Connection to {addr} lost, retrying in 2s")
            await asyncio.sleep(2) 
        except OSError as e:
            log.error(f"Cannot reach {addr}: {e.__class__.__name__}, retrying in 2s")
            await asyncio.sleep(2)  

        finally:
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except (ConnectionResetError, BrokenPipeError):
                    pass

async def watch_peers(log):
    while True:
        dead = []
        for nodes in state.last_seen:
            if time.monotonic() - state.last_seen[nodes] <= 3:
                continue
            else:
                dead.append(nodes)
                log.warning(f"{nodes} stopped responding for {time.monotonic()-state.last_seen[nodes]} and is presumed dead.")
        if dead:
            for dead_nodes in dead:
                state.last_seen.pop(dead_nodes)
        await asyncio.sleep(0.5)

async def main(log):
    server = await asyncio.start_server(partial(handler,log),host='192.168.10.2',port=8000)

    tasks = [heartbeats(('192.168.10.3',8000),log)]

    try:
        await asyncio.gather(*tasks,watch_peers(log))
    finally:
        server.close()
        server.close_clients()
        await server.wait_closed()


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("node_id",type=int)
    args = parser.parse_args()
    log = logging.LoggerAdapter(base_logger,{"node_id":args.node_id})
    state = NodeState(node_id=args.node_id, leader=None)
    asyncio.run(main(log))



    
    