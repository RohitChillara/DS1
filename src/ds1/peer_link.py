from __future__ import annotations
import asyncio
import time
from functools import partial
from ds1 import messages as m, state as s
from ds1.config import TCP_PORT

CONNECT_TIMEOUT = 1.0

_outbound: dict[int, asyncio.StreamWriter] = {}
_locks: dict[int, asyncio.Lock] = {}


async def handler(log, reader, writer):
    from ds1 import election                    # imported here to avoid the import cycle
    addr = writer.get_extra_info("peername")
    try:
        while True:
            line = await reader.readline()
            if not line:
                break
            try:
                msg = m.decode(line)
            except ValueError as e:              # pydantic ValidationError is a ValueError
                log.error(f"Malformed data from {addr}: {e}")
                continue

            if (msg.leader is not None and msg.leader < s.MyDetails.node_id
            and msg.type == "HEARTBEAT" and not election.election_running):
                election.trigger_election(log)
            if msg.type == "HEARTBEAT":
                s.Heartbeats.last_beat[msg.sender] = time.monotonic()
            elif msg.type == "ELECTION":
                election.on_election(msg, log)
            elif msg.type == "OK":
                election.on_ok()
            elif msg.type == "COORDINATOR":
                election.on_coordinator(msg, log)
            else:
                log.warning(f"Unexpected {msg.type} from {msg.sender}")
    except OSError as e:
        log.warning(f"Inbound link {addr} error: {e.__class__.__name__}")
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass                                 # already broken: nothing left to close


async def sender(peer_id: int, message: m.Message, log) -> bool:
    """Send one message to peer_id, connecting first if needed. Returns True if written."""
    is_known = peer_id in s.Members.members
    is_alive = peer_id in s.Heartbeats.last_beat
    if not is_known:
        log.debug(f"{message.type} to unknown node {peer_id} dropped")
        return False

    lock = _locks.setdefault(peer_id, asyncio.Lock())
    async with lock:                             # one connect at a time per peer
        writer = _outbound.get(peer_id)
        if writer is None or writer.is_closing():
            try:
                host = s.Members.members[peer_id]
                _, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, TCP_PORT), timeout=CONNECT_TIMEOUT)
                _outbound[peer_id] = writer
                log.info(f"Link to {peer_id} opened ({'alive' if is_alive else 'not yet alive'})")
            except (OSError, asyncio.TimeoutError) as e:
                log.debug(f"{message.type} to {peer_id} dropped: {e.__class__.__name__}")
                return False
        try:
            writer.write(m.encode(message))
            await writer.drain()
            return True
        except OSError as e:
            log.debug(f"{message.type} to {peer_id} failed: {e.__class__.__name__}")
            _outbound.pop(peer_id, None)
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass                             # already broken: nothing left to close
            return False


async def peer_link(log):
    server = await asyncio.start_server(partial(handler, log), host="0.0.0.0", port=TCP_PORT)
    log.info("Peer server listening on TCP %d", TCP_PORT)
    # FIX: no serve_forever / async with. On Ctrl+C they wait for open connections and hang.
    try:
        await asyncio.Event().wait()             # the server already accepts; just stay alive
    finally:
        server.close()
        server.close_clients()                   # Python >= 3.13
        await server.wait_closed()