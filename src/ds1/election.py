from __future__ import annotations
from ds1 import state as s, messages as m
from typing import Literal
import asyncio
from ds1 import peer_link as p

ok_flag = asyncio.Event()           # set by on_ok          -> candidate got an OK
objection_flag = asyncio.Event()    # set by on_ok          -> winner's claim was refused
coordinator_flag = asyncio.Event()  # set by on_coordinator -> loser learns the winner announced

election_running = False            # guard: one election at a time on this node


def _msg(kind: Literal["ELECTION", "COORDINATOR", "OK"]) -> m.Message:
    if kind == "COORDINATOR":       # announce the new term, I'm the leader
        return m.Message(type=kind, sender=s.MyDetails.node_id,
                         term=s.Members.term + 1, leader=s.MyDetails.node_id)
    return m.Message(type=kind, sender=s.MyDetails.node_id,
                     term=s.Members.term, leader=s.Members.leader)


def _send(peer_id: int, msg: m.Message, log):
    asyncio.create_task(p.sender(peer_id, msg, log))     # fire and forget, never block


async def start_election(log):
    global election_running
    if election_running:
        return
    election_running = True
    try:
        ok_flag.clear()
        higher = [n for n in s.Members.members if n > s.MyDetails.node_id]
        for n in higher:
            _send(n, _msg("ELECTION"), log)
        log.info(f"Election raised by node {s.MyDetails.node_id}, asked {higher}")
        await flag_event(log)

    except asyncio.TimeoutError:
        try:
            objection_flag.clear()
            for n in list(s.Members.members):
                _send(n, _msg("COORDINATOR"), log)
            log.info(f"Coordinator raised by node {s.MyDetails.node_id}")
            await asyncio.wait_for(objection_flag.wait(), timeout=1)
            election_running = False
            asyncio.create_task(start_election(log))       # refused: run again

        except asyncio.TimeoutError:
            s.Members.leader = s.MyDetails.node_id
            s.Members.term += 1
            log.info(f"Leader claimed by {s.Members.leader}, term {s.Members.term}")
    finally:
        election_running = False


async def flag_event(log):
    await asyncio.wait_for(ok_flag.wait(), timeout=3)      # TimeoutError -> caller claims
    coordinator_flag.clear()
    try:
        await asyncio.wait_for(coordinator_flag.wait(), timeout=1)
        return
    except asyncio.TimeoutError:
        global election_running
        election_running = False
        asyncio.create_task(start_election(log))


# ---------- handlers: called by peer_link.handler ----------

def on_election(msg: m.Message, log):
    if msg.sender < s.MyDetails.node_id:
        _send(msg.sender, _msg("OK"), log)                 # back off, I'm higher
        asyncio.create_task(start_election(log))


def on_ok(msg: m.Message, log):
    ok_flag.set()
    objection_flag.set()


def on_coordinator(msg: m.Message, log):
    if msg.term < s.Members.term:
        return                                             # stale: ignore, no reply
    if (msg.term == s.Members.term and s.Members.leader is not None
            and s.Members.leader >= msg.sender):
        return                                             # same term, already following a higher leader
    if msg.sender > s.MyDetails.node_id:
        s.Members.leader = msg.sender                      # accept silently
        s.Members.term = msg.term
        coordinator_flag.set()
        log.info(f"Accepted leader {msg.sender}, term {msg.term}")
    else:
        _send(msg.sender, _msg("OK"), log)                 # refuse: I'm higher