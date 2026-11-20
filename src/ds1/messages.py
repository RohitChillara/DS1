# messages.py: the message format only. No networking, no state.
from typing import Literal
from pydantic import BaseModel, ConfigDict

MsgType = Literal["HELLO", "HELLO_ACK", "HEARTBEAT",
                  "ELECTION", "OK", "COORDINATOR",
                  "SYNC", "STATE", "STATE_ACK",
                  "REQUEST", "DATA", "DATA_ACK"]


class Message(BaseModel):
    model_config = ConfigDict(ser_json_bytes="base64", val_json_bytes="base64")
    type: MsgType
    sender: int
    leader: int | None = None

    # replication position: STATE, STATE_ACK, REQUEST, DATA, DATA_ACK
    epoch: int = 0
    seq: int = 0

    # DATA only: the photo's metadata; the raw bytes follow the header line
    image_id: str | None = None
    file_name: str | None = None
    size: int = 0
    upload_id: str | None = None        # client-made id: a retry after a failover is recognised
    data: bytes | None = None

def encode(msg: Message) -> bytes:
    """For TCP: one JSON object per line."""
    return (msg.model_dump_json() + "\n").encode()


def decode(data: bytes) -> Message:
    """Raises pydantic.ValidationError on malformed input."""
    return Message.model_validate_json(data)