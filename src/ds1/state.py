from dataclasses import dataclass,field
import json

@dataclass
class Members:
    leader : int | None
    members : dict[int,str] = field(default_factory=dict)
    is_synced : bool = False


@dataclass
class Storage:
    image_metadata: dict[tuple[int, int], dict] = field(default_factory=dict)
    epoch: dict[str, int] = field(default_factory=dict)

@dataclass(frozen=True)
class MyDetails:
    node_id: int

@dataclass
class Heartbeats:
    last_beat : dict[int,float] = field(default_factory=dict)