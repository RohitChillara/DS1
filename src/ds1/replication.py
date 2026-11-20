from __future__ import annotations
from ds1 import comms as c, messages as m, state as s, store as st
import asyncio
import random

# ---------- replication timeouts (seconds) ----------
SYNC_TIMEOUT = 2.0          # backup: SYNC -> waiting for STATE
STATE_TIMEOUT = 2.0         # leader: STATE -> waiting for STATE_ACK or REQUEST
REQUEST_TIMEOUT = 5.0       # backup: REQUEST -> waiting for DATA (carries up to 3 MB)
DATA_TIMEOUT = 5.0          # leader: DATA -> waiting for DATA_ACK (transfer + disk write)
MAX_RETRIES = 3             # attempts per message before giving up

# ---------- replication schedule ----------
SYNC_INTERVAL = 30.0        # backups: periodic sync
SYNC_JITTER = 5.0           # + random 0..SYNC_JITTER seconds, so backups don't sync at once


sync_event = asyncio.Event()     # set by on_state:         the leader's position arrived
req_event = asyncio.Event()      # set by collect_data:     a DATA arrived, so the REQUEST was answered
sync_finish = asyncio.Event()    # set by check_status:     caught up with the leader
ack_event = asyncio.Event()      # set by check_ack_status: every STATE recipient answered
data_events: dict[int, asyncio.Event] = {}      # one per backup: set by on_data_ack
store_lock = asyncio.Lock()      # one disk write at a time, even when DATA handling is spawned


def _msg(kind: m.MsgType, obj: dict | None = None):
    if kind == "SYNC":
        msg = m.Message(type="SYNC", sender=s.MyDetails.node_id)
    elif kind == "STATE":
        epoch, seq = st.last_position()
        msg = m.Message(type="STATE", sender=s.MyDetails.node_id, leader=s.Members.leader,
                        epoch=epoch, seq=seq)
    elif kind == "DATA":
        msg = m.Message(type="DATA", sender=s.MyDetails.node_id, epoch=obj["epoch"],
                        seq=obj["seq"], image_id=obj["image_id"], file_name=obj["file_name"],
                        size=obj["size"], data=obj.get("data"))
    elif kind in ("DATA_ACK", "STATE_ACK", "REQUEST"):
        msg = m.Message(type=kind, sender=s.MyDetails.node_id, epoch=obj["epoch"],
                        seq=obj["seq"])
    else:
        raise ValueError(f"Unknown message kind {kind}")
    return msg


def _target() -> tuple[int, int]:
    return (s.CurrentMessage.leader_epoch, s.CurrentMessage.leader_seq)


# ---------- backup side: sync ----------

async def sync(log, attempt=1):
    if attempt > MAX_RETRIES:
        log.warning("Max sync attempts breached")
        return
    if s.Members.leader is None or s.Members.leader == s.MyDetails.node_id:
        return
    sync_event.clear()
    sync_finish.clear()
    c.send(s.Members.leader, _msg("SYNC"), log)
    try:
        await asyncio.wait_for(sync_event.wait(), timeout=SYNC_TIMEOUT)
        position = st.last_position()
        if position >= _target():
            log.info(f"Sync: already up to date at {position}")
            return
        log.info(f"Sync: at {position}, leader at {_target()}, requesting")
        if not await raise_request(position, log):
            return
        await asyncio.wait_for(sync_finish.wait(), timeout=SYNC_INTERVAL // 2)
        log.info(f"Sync complete at {st.last_position()}")
    except asyncio.TimeoutError:
        log.warning(f"Sync attempt {attempt} timed out, re-attempting")
        await sync(log, attempt + 1)


async def raise_request(position, log) -> bool:
    """Send REQUEST with our last position; the leader then pushes the missing photos.
    Returns True as soon as the first DATA arrives, False if the leader never answers."""
    msg = _msg(kind="REQUEST", obj={"epoch": position[0], "seq": position[1]})
    for attempt in range(MAX_RETRIES):
        req_event.clear()
        c.send(s.Members.leader, msg, log)
        try:
            await asyncio.wait_for(req_event.wait(), timeout=REQUEST_TIMEOUT)
            log.debug("Request for data successful")
            return True
        except asyncio.TimeoutError:
            log.warning(f"REQUEST attempt {attempt + 1} got no DATA")
    log.warning("No response from leader on request, sync failed")
    return False


# ---------- backup side: handlers (called from peer_link.handler) ----------

def on_state(msg: m.Message, log):
    """Remember the leader's position, then wake up sync."""
    s.CurrentMessage.leader_epoch = msg.epoch
    s.CurrentMessage.leader_seq = msg.seq
    sync_event.set()


async def collect_data(msg: m.Message, log):
    """Mark the REQUEST as answered, store one DATA, ACK it, and check whether we are caught up.
    peer_link.handler spawns this as its own task; store_lock keeps the disk writes one at a time,
    so a retried DATA or a second push can't overwrite another photo's metadata entry."""
    req_event.set()                       # arrival answers the REQUEST; the disk write doesn't count
    try:
        async with store_lock:
            await asyncio.to_thread(st.save_photo, msg, log)
    except Exception as e:
        log.error(f"Storing {(msg.epoch, msg.seq)} failed: {e!r}")
        return                            # no DATA_ACK: the leader retries this photo
    epoch, seq = st.last_position()
    ack = _msg(kind="DATA_ACK", obj={"epoch": epoch, "seq": seq})
    c.send(msg.sender, ack, log)
    check_status(log)


def check_status(log):
    if st.last_position() >= _target():
        sync_finish.set()
        log.info("Sync successful")


# ---------- leader side ----------

async def state(log, recipients: set[int] | None = None, attempts: int = 1):
    """Announce the leader's position to the backups and wait until each one answered."""
    if s.Members.leader != s.MyDetails.node_id:
        return
    if attempts > MAX_RETRIES:
        log.warning(f"STATE: no reply from {sorted(recipients)} after {MAX_RETRIES} attempts")
        return
    if recipients is None:
        recipients = set(c.peers())
    if not recipients:
        return

    msg = _msg(kind="STATE")
    s.Bookkeeper.recipients = set(recipients)
    s.Bookkeeper.acks = set()
    ack_event.clear()
    c.broadcast("STATE", recipients, log, message=msg)

    try:
        await asyncio.wait_for(ack_event.wait(), timeout=STATE_TIMEOUT)
    except asyncio.TimeoutError:
        left = s.Bookkeeper.recipients - s.Bookkeeper.acks
        log.warning(f"STATE attempt {attempts}: no reply from {sorted(left)}, retrying")
        await state(log, recipients=left, attempts=attempts + 1)


def check_ack_status(msg: m.Message, log):
    """Called for STATE_ACK and REQUEST: either one means this backup got the STATE."""
    s.Bookkeeper.acks.add(msg.sender)                     
    if s.Bookkeeper.recipients <= s.Bookkeeper.acks:
        ack_event.set()


def on_sync(msg: m.Message, log):
    if s.Members.leader != s.MyDetails.node_id:
        return                                   # only the leader answers SYNC
    c.send(msg.sender, _msg("STATE"), log)       # the backup retries if this gets lost


async def on_request(msg: m.Message, log):
    """Push every photo after the backup's position, one DATA at a time. Start with c.spawn."""
    if s.Members.leader != s.MyDetails.node_id:
        return
    peer = msg.sender
    event = data_events.setdefault(peer, asyncio.Event())
    to_send = sorted(k for k in s.Storage.image_metadata if k > (msg.epoch, msg.seq))
    log.info(f"Pushing {len(to_send)} photo(s) to {peer}")

    for key in to_send:
        meta = s.Storage.image_metadata[key]
        data = await asyncio.to_thread(st.get_photo, meta["image_id"])
        out = _msg("DATA", {**meta, "data": data})
        for attempt in range(MAX_RETRIES):
            event.clear()
            c.send(peer, out, log)
            try:
                await asyncio.wait_for(event.wait(), timeout=DATA_TIMEOUT)
                break                                    # this photo is ACKed: next one
            except asyncio.TimeoutError:
                log.warning(f"No DATA_ACK from {peer} for {key}, attempt {attempt + 1}")
        else:
            log.warning(f"Push to {peer} stopped at {key}; it will catch up on its next sync")
            return                                       # never skip a photo: leave no gaps


def on_data_ack(msg: m.Message, log):
    event = data_events.get(msg.sender)
    if event is not None:
        event.set()                                      # only releases the push to this backup