from __future__ import annotations

import asyncio
import time

from ds1 import comms as c
from ds1 import messages as m
from ds1 import state as s
from ds1 import store as st

OK_TIMEOUT = 2.0            # wait 1: OK (or COORDINATOR) from a higher node
COORDINATOR_TIMEOUT = 5.0   # wait 2: after an OK, the winner must announce (> OK_TIMEOUT)

ok_flag = asyncio.Event()           # set by on_ok          -> a higher node is alive
coordinator_flag = asyncio.Event()  # set by on_coordinator -> someone won, follow them

election_running = False            # guard: one election at a time on this node


async def _wait_for_ok_or_coordinator(timeout: float):
    """Wait until OK or COORDINATOR arrives, or the timeout passes."""
    waiters = [asyncio.create_task(ok_flag.wait()),
               asyncio.create_task(coordinator_flag.wait())]
    try:
        await asyncio.wait(waiters, timeout=timeout,
                           return_when=asyncio.FIRST_COMPLETED)
    finally:
        for w in waiters:
            w.cancel()


def _adopt_epoch(msg: m.Message, log):
    """Replication bookkeeping only: never decides who leads."""
    if msg.epoch > st.get_epoch():
        st.set_epoch(msg.epoch)                      # saved to disk before it's used
        log.info(f"Epoch {msg.epoch} adopted from node {msg.sender}")


def _claim(log):
    """Become leader. No await in here, so nothing can interrupt it halfway."""
    if s.Members.leader != s.MyDetails.node_id:
        st.set_epoch(st.get_epoch() + 1)
    s.Members.leader = s.MyDetails.node_id
    announce = m.Message(type="COORDINATOR", sender=s.MyDetails.node_id,
                         leader=s.MyDetails.node_id, epoch=st.get_epoch())
    for n in c.peers():                              # NEW: COORDINATOR carries the new epoch
        c.send(n, announce, log)
    log.info(f"Leader claimed by {s.MyDetails.node_id}, epoch {st.get_epoch()}")


# ---------- the election ----------

def trigger_election(log):
    """The only way to start an election: from the watcher, on_election, or a retry."""
    if election_running or not s.Members.is_synced:
        log.debug("Election skipped: not yet synced, or an election is already running")
        return
    c.spawn(start_election(log), log)


async def start_election(log):
    global election_running
    if election_running:
        return
    election_running = True
    retry = False
    try:
        ok_flag.clear()
        coordinator_flag.clear()

        higher = [n for n in c.peers() if n > s.MyDetails.node_id]
        if higher:
            c.broadcast("ELECTION", higher, log)
            log.info(f"Election raised by node {s.MyDetails.node_id}, asked {higher}")

            await _wait_for_ok_or_coordinator(OK_TIMEOUT)
            if coordinator_flag.is_set():
                log.info(f"Leader {s.Members.leader} announced during election, not claiming")
                return

            if ok_flag.is_set():
                try:
                    await asyncio.wait_for(coordinator_flag.wait(), timeout=COORDINATOR_TIMEOUT)
                    log.info(f"Following leader {s.Members.leader}")
                except asyncio.TimeoutError:
                    log.warning("Got OK but no COORDINATOR followed, restarting election")
                    retry = True
                return

        # nobody higher answered, nobody announced: I am the highest alive
        _claim(log)
    finally:
        election_running = False
        if retry:
            trigger_election(log)


# ---------- handlers: called by peer_link.handler ----------

def on_election(msg: m.Message, log):
    if msg.sender < s.MyDetails.node_id:
        c.send(msg.sender, c.make_msg("OK"), log)     # back off, I'm higher
        trigger_election(log)


def on_ok():
    ok_flag.set()


def on_coordinator(msg: m.Message, log):
    _adopt_epoch(msg, log)                 # NEW: whatever the decision below, remember the epoch
    if not s.Members.is_synced:
        s.Members.leader = msg.sender      # learn it; startup syncs, then elects
        return
    if msg.sender > s.MyDetails.node_id:
        old = s.Members.leader
        s.Members.leader = msg.sender
        coordinator_flag.set()
        s.Heartbeats.last_beat[msg.sender] = time.monotonic()
        if old != msg.sender:              # repeated announcements of the same leader aren't news
            log.info(f"Leader changed from {old} to {msg.sender}")
    else:
        trigger_election(log)
        log.info(f"Coordinator request refused for {msg.sender}")