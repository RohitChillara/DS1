# messages.py: the message format only. No networking, no state.
from typing import Literal
from pydantic import BaseModel

MsgType = Literal["HELLO", "HELLO_ACK", "JOIN", "WELCOME", "MEMBER_JOIN", "MEMBER_LEAVE",
                  "HEARTBEAT", "ELECTION", "OK", "COORDINATOR", "STATE", "ACK"]


class Message(BaseModel):
    type: MsgType
    sender: int
    term: int = 0
    leader: int | None = None
    seq: int | None = None
    epoch: int = 0                      # default, so messages without an epoch still validate
    members: dict[int, tuple[str, int]] | None = None


def encode(msg: Message) -> bytes:
    """For TCP: one JSON object per line."""
    return (msg.model_dump_json() + "\n").encode()


def decode(data: bytes) -> Message:
    """Raises pydantic.ValidationError on malformed input."""
    return Message.model_validate_json(data)