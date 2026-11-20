import argparse, asyncio, logging, time, json, random
from ds1 import state as s, discovery, peer_link, election, monitor, replication as r, store as st
from ds1.config import JSON_DIR, BIN_DIR, EPOCH_DIR

METADATA_FILE = JSON_DIR / "metadata.json"
EPOCH_FILE = EPOCH_DIR / "epoch.json"
STARTUP_WAIT = 5.0          # discovery + heartbeats fill in the leader before the startup sync

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)-7s [node %(node_id)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ds1")   # no LoggerAdapter needed


async def elect(log):
    await asyncio.sleep(STARTUP_WAIT)            # discovery + heartbeats fill in the leader
    try:
        leader = s.Members.leader
        if leader is not None and leader != s.MyDetails.node_id:
            await r.sync(log)                    # catch up from the current leader
    finally:
        s.Members.is_synced = True               # always, even if the sync failed
    election.trigger_election(log)


async def periodic_sync(log):
    """Backups only: every SYNC_INTERVAL (+ jitter), catch up with the leader."""
    while not s.Members.is_synced:               # the startup sync runs first, in elect()
        await asyncio.sleep(1)
    while True:
        await asyncio.sleep(r.SYNC_INTERVAL + random.uniform(0, r.SYNC_JITTER))
        leader = s.Members.leader
        if leader is None or leader == s.MyDetails.node_id:
            continue                             # no leader (election running) or I am the leader
        if election.election_running:
            continue
        log.debug("Periodic sync")
        await r.sync(log)                        # awaited: never two syncs from this task at once


def prepare_files() -> None:
    """Create the folders, and the starting files only if they don't exist yet.
    Writing them unconditionally would erase this node's photos and epoch on every restart."""
    for d in (JSON_DIR, BIN_DIR, EPOCH_DIR):
        d.mkdir(parents=True, exist_ok=True)     
    for f in (METADATA_FILE, EPOCH_FILE):
        try:
            with open(f, "x") as h:              # "x": create only if missing
                h.write("{}")
        except FileExistsError:
            pass


def load() -> None:
    """Call once at startup, after prepare_files() and after the node id is set."""
    for leftover in BIN_DIR.glob("*.tmp"):        # incomplete writes from a crash never counted
        leftover.unlink()

    with open(METADATA_FILE) as f:
        data = json.load(f)
    s.Storage.image_metadata = {st._str_to_key(k): meta for k, meta in data.items()}

    # images with no metadata entry were never committed: delete them
    known = {meta["image_id"] for meta in s.Storage.image_metadata.values()}
    for path in BIN_DIR.iterdir():
        if path.name not in known:
            path.unlink()

    with open(EPOCH_FILE) as f:
        s.Storage.epoch = json.load(f)
    from_log = max((epoch for epoch, _ in s.Storage.image_metadata), default=0)
    if from_log > st.get_epoch():
        st.set_epoch(from_log)


async def main(log):
    await asyncio.gather(peer_link.peer_link(log), discovery.discovery(log),
                         monitor.watch_peers(log), elect(log), periodic_sync(log))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("node_id", type=int)
    a = p.parse_args()
    s.MyDetails.node_id = a.node_id              # set first: the epoch file is keyed by node id
    s.MyDetails.start_time = time.monotonic()
    s.Members.leader = None
    s.Members.members = {}
    s.Heartbeats.last_beat = {}
    log = logging.LoggerAdapter(logger, {"node_id": a.node_id})

    prepare_files()
    load()
    log.info(f"Loaded {len(s.Storage.image_metadata)} photo(s), last position {st.last_position()}, "
             f"epoch {st.get_epoch()}")

    try:
        asyncio.run(main(log))
    except KeyboardInterrupt:
        pass