import argparse, asyncio, logging
from ds1 import state as s, discovery, peer_link
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)-7s [node %(node_id)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ds1")   # no LoggerAdapter needed

async def main(log):
    await asyncio.gather(peer_link.peer_link(log), discovery.discovery(log))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("node_id", type=int)
    a = p.parse_args()
    s.MyDetails.node_id = a.node_id
    s.Members.leader = None
    s.Members.members = {}
    s.Members.term = 0
    s.Heartbeats.last_beat = {}
    log = logging.LoggerAdapter(logger, {"node_id": a.node_id})
    try: 
        asyncio.run(main(log))

    except KeyboardInterrupt: 
        pass