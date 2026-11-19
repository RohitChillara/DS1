# comms.py: shared helpers for sending messages, used by election and replication.
# Imports messages, state and peer_link; nothing imports comms back, so there is no cycle.
from __future__ import annotations

import asyncio
from typing import Literal

from ds1 import messages as m, state as s, peer_link as p

_tasks: set[asyncio.Task] = set()   # strong refs, so fire-and-forget tasks are not garbage-collected


def spawn(coro, log) -> asyncio.Task:
    """Start a background task, keep a reference, and log it if it crashes."""
    task = asyncio.create_task(coro)
    _tasks.add(task)

    def _done(t: asyncio.Task):
        _tasks.discard(t)
        if not t.cancelled() and t.exception() is not None:
            log.error(f"Background task failed: {t.exception()!r}")

    task.add_done_callback(_done)
    return task


def make_msg(kind: Literal["ELECTION", "COORDINATOR", "OK"]) -> m.Message:
    """An election message from this node, carrying its current view of the leader."""
    return m.Message(type=kind, sender=s.MyDetails.node_id, leader=s.Members.leader)


def send(peer_id: int, msg: m.Message, log):
    spawn(p.sender(peer_id, msg, log), log)    # fire and forget, never block


def peers() -> list[int]:
    """Alive peers if we have heard any heartbeats, otherwise everyone discovery knows. Never ourselves."""
    me = s.MyDetails.node_id
    source = s.Heartbeats.last_beat if s.Heartbeats.last_beat else s.Members.members
    return sorted(n for n in source if n != me)


def broadcast(kind: Literal["ELECTION", "COORDINATOR"], targets: list[int], log):
    msg = make_msg(kind)
    for n in targets:
        send(n, msg, log)