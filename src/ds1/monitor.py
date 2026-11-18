from __future__ import annotations
import asyncio
import time
from ds1 import messages as m, state as s
from ds1 import peer_link as p, election as el

BEAT_EVERY = 1.0      # seconds between heartbeats
DEAD_AFTER = 3.0      # seconds without a heartbeat before a peer is presumed dead
STALL_LIMIT = 2.0     # gap between watcher runs that means *we* were frozen
CHECK_EVERY = 0.5



async def heartbeats(peer_id: int, log):
    """One task per peer, started by discovery. Sender handles (re)connecting."""
    while True:
        msg = m.Message(type="HEARTBEAT", sender=s.MyDetails.node_id,
                        term=s.Members.term, leader=s.Members.leader)
        await p.sender(peer_id, msg, log)
        await asyncio.sleep(BEAT_EVERY)


async def watch_peers(log):
    last_tick = time.monotonic()
    while True:
        now = time.monotonic()

        # 1. we were paused or blocked: don't blame peers for our own gap
        if now - last_tick > STALL_LIMIT:
            log.warning(f"Local stall of {now - last_tick:.1f}s, resetting peer timers")
            for peer in s.Heartbeats.last_beat:
                s.Heartbeats.last_beat[peer] = now
        last_tick = now   # always update, stall or not

        # 2. find peers silent for too long
        dead = [peer for peer, t in s.Heartbeats.last_beat.items() if now - t > DEAD_AFTER]

        # 3. forget them, close our link, trigger election if it was the leader
        for peer in dead:
            log.warning(f"{peer} silent for {now - s.Heartbeats.last_beat[peer]:.1f}s, presumed dead")
            s.Heartbeats.last_beat.pop(peer)
            writer = p._outbound.pop(peer, None)
            if writer is not None:
                writer.close()
            if peer == s.Members.leader:
                s.Members.leader = None
                el.trigger_election(log)

        await asyncio.sleep(CHECK_EVERY)



async def monitor(log):
    asyncio.create_task(watch_peers(log))