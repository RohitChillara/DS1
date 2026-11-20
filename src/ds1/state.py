class Members:
    leader: int | None = None
    members: dict[int, str] = {}
    is_synced: bool = False


class Storage:
    image_metadata: dict[tuple[int, int], dict] = {}
    epoch: dict[str, int] = {}


class CurrentMessage:
    leader_epoch: int = 0          # the leader's position from its last STATE
    leader_seq: int = 0


class Bookkeeper:
    recipients: set[int] = set()
    acks: set[int] = set()


class MyDetails:
    node_id: int = 0
    start_time: float = 0.0        # node.py sets it; declared here so it's visible


class Heartbeats:
    last_beat: dict[int, float] = {}