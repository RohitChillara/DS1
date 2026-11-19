import argparse, asyncio, logging, time,json
from ds1 import state as s, discovery, peer_link, election, monitor,replication as r, store as st
from pathlib import Path
from ds1.config import JSON_DIR, BIN_DIR, EPOCH_DIR

METADATA_FILE = JSON_DIR / "metadata.json"
EPOCH_FILE = EPOCH_DIR / "epoch.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)-7s [node %(node_id)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ds1")   # no LoggerAdapter needed

async def elect(log):
    await asyncio.sleep(5)                       # discovery + heartbeats fill in the leader
    try:
        leader = s.Members.leader
        if leader is not None and leader != s.MyDetails.node_id:
            await r.sync(log)                    # catch up from the current leader
    finally:
        s.Members.is_synced = True               # always, even if the sync failed
    election.trigger_election(log)


def load() -> None:
    """Call once at startup, after node.py has created the folders and files."""
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
    await asyncio.gather(peer_link.peer_link(log), discovery.discovery(log),monitor.watch_peers(log),elect(log),load())


if __name__ == "__main__":
    JSON_DIR.mkdir(exist_ok=True)
    BIN_DIR.mkdir(exist_ok=True)
    EPOCH_DIR.mkdir(exist_ok=True)
    p = argparse.ArgumentParser()
    p.add_argument("node_id", type=int)
    a = p.parse_args()
    s.MyDetails.node_id = a.node_id
    s.MyDetails.start_time = time.monotonic()
    s.Members.leader = None
    s.Members.members = {}
    s.Heartbeats.last_beat = {}
    log = logging.LoggerAdapter(logger, {"node_id": a.node_id})
    try: 
        asyncio.run(main(log))

    except KeyboardInterrupt: 
        pass