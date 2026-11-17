from __future__ import annotations
import asyncio
from functools import partial
import time
from ds1 import messages as m, state as s
from ds1.config import TCP_PORT

STALL_LIMIT = 2.0
DEAD_AFTER = 3.0
CHECK_EVERY = 0.5

_outbound: dict[int, asyncio.StreamWriter] = {}

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
            msg = m.decode(line)
            if msg.type == "HEARTBEAT":
                log.debug(f"Heartbeat received from {msg.sender}")
                s.Heartbeats.last_beat[msg.sender] = time.monotonic()
                continue
        except Exception as e:
            log.error(f"Malformed data sent by {addr}, Error: {e}")
            continue

async def heartbeats(peer_id: int, log):
    while True:
        writer = None
        try:
            host = s.Members.members[peer_id]
            reader, writer = await asyncio.open_connection(host, TCP_PORT)
            _outbound[peer_id] = writer
            log.info(f"Outbound link to {peer_id} established")
            while not writer.is_closing():
                msg = m.Message(type="HEARTBEAT", sender=s.MyDetails.node_id,
                                term=s.Members.term, leader=s.Members.leader)
                writer.write(m.encode(msg))
                await writer.drain()
                log.debug(f"Heartbeat sent to {peer_id}")
                await asyncio.sleep(1)
            log.info(f"Outbound link to {peer_id} closed by watcher")
        except OSError as e:
            log.warning(f"Cannot reach {peer_id}: {e.__class__.__name__}")
        finally:
            if _outbound.get(peer_id) is writer:
                _outbound.pop(peer_id)
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError:
                    pass
        await asyncio.sleep(2)

async def watch_peers(log):
    last_tick = time.monotonic()
    while True:
        now = time.monotonic()
        if now - last_tick > STALL_LIMIT:
            log.warning(f"Local stall of {now - last_tick:.1f}s, resetting peer timers")
            for p in s.Heartbeats.last_beat:
                s.Heartbeats.last_beat[p] = now
        last_tick = now
        dead = [p for p, t in s.Heartbeats.last_beat.items() if now - t > DEAD_AFTER]
        for p in dead:
            log.warning(f"{p} silent for {now - s.Heartbeats.last_beat[p]:.1f}s, presumed dead")
            s.Heartbeats.last_beat.pop(p)
            writer = _outbound.pop(p, None)
            if writer is not None:
                writer.close()
        await asyncio.sleep(CHECK_EVERY)

async def peer_link(log):
    server = await asyncio.start_server(partial(handler,log),host='0.0.0.0',port=TCP_PORT)
    try:
        await watch_peers(log)
    finally:
        server.close()
        await server.wait_closed()