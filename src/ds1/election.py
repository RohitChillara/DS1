from __future__ import annotations

import asyncio
from typing import Literal

from ds1 import messages as m
from ds1 import peer_link as p
from ds1 import state as s
import time

OK_TIMEOUT = 2.0            # wait 1: OK (or COORDINATOR) from a higher node
COORDINATOR_TIMEOUT = 5.0   # wait 2: after an OK, the winner must announce (> OK_TIMEOUT)

ok_flag = asyncio.Event()           # set by on_ok          -> a higher node is alive
coordinator_flag = asyncio.Event()  # set by on_coordinator -> someone won, follow them

election_running = False            # guard: one election at a time on this node
_tasks: set[asyncio.Task] = set()   # strong refs, so fire-and-forget tasks are not garbage-collected


# ---------- helpers ----------

def _spawn(coro, log) -> asyncio.Task:
    """Start a background task, keep a reference, and log it if it crashes."""
    task = asyncio.create_task(coro)
    _tasks.add(task)

    def _done(t: asyncio.Task):
        _tasks.discard(t)
        if not t.cancelled() and t.exception() is not None:
            log.error(f"Background task failed: {t.exception()!r}")

    task.add_done_callback(_done)
    return task


def _msg(kind: Literal["ELECTION", "COORDINATOR", "OK"]) -> m.Message:
    """Every message carries the current term and leader. The term is raised in _claim, not here."""
    return m.Message(type=kind, sender=s.MyDetails.node_id,
                     term=s.Members.term, leader=s.Members.leader)


def _send(peer_id: int, msg: m.Message, log):
    _spawn(p.sender(peer_id, msg, log), log)   # fire and forget, never block


def _peers() -> list[int]:
    """Alive peers if we have heard any heartbeats, otherwise everyone discovery knows. Never ourselves."""
    me = s.MyDetails.node_id
    source = s.Heartbeats.last_beat if s.Heartbeats.last_beat else s.Members.members
    return sorted(n for n in source if n != me)


def _broadcast(kind: Literal["ELECTION", "COORDINATOR"], targets: list[int], log):
    msg = _msg(kind)
    for n in targets:
        _send(n, msg, log)


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


def _claim(log):
    """Become leader. No await in here, so nothing can interrupt it halfway."""
    s.Members.leader = s.MyDetails.node_id
    _broadcast("COORDINATOR", _peers(), log)
    log.info(f"Leader claimed by {s.MyDetails.node_id}, term {s.Members.term}")


# ---------- the election ----------

def trigger_election(log):
    """The only way to start an election: from the watcher, on_election, or a retry."""
    if not election_running:        
            _spawn(start_election(log), log)


async def start_election(log):
    global election_running
    if election_running:
        return
    election_running = True
    retry = False
    try:
        ok_flag.clear()
        coordinator_flag.clear()

        higher = [n for n in _peers() if n > s.MyDetails.node_id]
        if higher:
            _broadcast("ELECTION", higher, log)
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
        _send(msg.sender, _msg("OK"), log)        # back off, I'm higher
        trigger_election(log)


def on_ok():
    ok_flag.set()


def on_coordinator(msg: m.Message, log):
    if msg.sender > s.MyDetails.node_id:                
        old = s.Members.leader
        s.Members.leader = msg.sender
        coordinator_flag.set()
        s.Heartbeats.last_beat[msg.sender] = time.monotonic()
        log.info(f"Leader changed from {old} to {msg.sender}")
    else:
        trigger_election(log)
        log.info(f"Coordinator request refused for {msg.sender}")