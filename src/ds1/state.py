from dataclasses import dataclass,field

@dataclass
class Members:
    leader : int | None
    members : dict[int,str] = field(default_factory=dict)

@dataclass(frozen=True)
class MyDetails:
    node_id: int

@dataclass
class Heartbeats:
    last_beat : dict[int,float] = field(default_factory=dict)