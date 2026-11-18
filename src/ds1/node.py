import argparse, asyncio, logging, time
from ds1 import state as s, discovery, peer_link, election, monitor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)-7s [node %(node_id)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ds1")   # no LoggerAdapter needed

async def elect(log):
    await asyncio.sleep(5)
    election.trigger_election(log)
    

async def main(log):
    await asyncio.gather(peer_link.peer_link(log), discovery.discovery(log),monitor.watch_peers(log),elect(log))


if __name__ == "__main__":
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