import json, os, uuid
from pathlib import Path
from ds1.config import JSON_DIR, BIN_DIR, EPOCH_DIR
from ds1 import state as s

# Folders and the starting files ({} each) are created by node.py before load() runs.
METADATA_FILE = JSON_DIR / "metadata.json"
EPOCH_FILE = EPOCH_DIR / "epoch.json"


# ---------- disk helpers ----------

def _atomic_write_json(path: Path, obj) -> None:
    tmp = path.with_name(path.name + ".tmp")      # metadata.json -> metadata.json.tmp
    with open(tmp, "w") as f:
        json.dump(obj, f)
        f.flush()                                 # Python's buffer -> the operating system
        os.fsync(f.fileno())                      # the operating system -> the physical disk
    os.replace(tmp, path)                         # swap it in: all or nothing


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as f:                    # "wb": a leftover .tmp must not block a retry
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


# ---------- key conversion: tuples in memory, strings on disk ----------

def _key_to_str(key: tuple[int, int]) -> str:
    return f"{key[0]}-{key[1]}"                   # (2, 15) -> "2-15"


def _str_to_key(text: str) -> tuple[int, int]:
    epoch, seq = text.split("-")
    return int(epoch), int(seq)                   # "2-15" -> (2, 15)


def _save_index(index: dict) -> None:
    _atomic_write_json(METADATA_FILE, {_key_to_str(k): meta for k, meta in index.items()})


def _my_key() -> str:
    return str(s.MyDetails.node_id)               # JSON keys are strings

# ---------- epoch ----------

def get_epoch() -> int:
    return s.Storage.epoch.get(_my_key(), 0)


def set_epoch(n: int) -> None:
    """The only way the epoch changes. Saved before memory is updated."""
    new = dict(s.Storage.epoch)
    new[_my_key()] = n
    _atomic_write_json(EPOCH_FILE, new)
    s.Storage.epoch = new


# ---------- photos ----------

def last_position() -> tuple[int, int]:
    """Highest (epoch, seq) stored, derived from the index so it can never drift."""
    return max(s.Storage.image_metadata, default=(0, 0))


def save_photo(photo, photo_bin: bytes) -> None:
    key = (photo.epoch, photo.seq)

    # order: image first, then metadata (the commit marker), then epoch, then memory
    _atomic_write_bytes(BIN_DIR / photo.image_id, photo_bin)

    new_index = dict(s.Storage.image_metadata)
    new_index[key] = photo.model_dump()
    _save_index(new_index)

    if photo.epoch > get_epoch():
        set_epoch(photo.epoch)

    s.Storage.image_metadata = new_index          # memory last: only after the disk has it


def remove(key: tuple[int, int]) -> None:
    """Drop an entry (used by sync for unconfirmed leftovers): metadata first, then the image."""
    meta = s.Storage.image_metadata.get(key)
    if meta is None:
        return
    new_index = dict(s.Storage.image_metadata)
    del new_index[key]
    _save_index(new_index)                        # once this is saved, the photo no longer exists
    s.Storage.image_metadata = new_index
    (BIN_DIR / meta["image_id"]).unlink(missing_ok=True)


def get_photo(image_id: str) -> bytes:
    """Return the image bytes. Raises FileNotFoundError if the photo doesn't exist."""
    try:
        uuid.UUID(image_id)                       # rejects anything that isn't a UUID, e.g. "../config.py"
    except ValueError:
        raise FileNotFoundError(f"Photo {image_id} doesn't exist") from None
    path = BIN_DIR / image_id
    if not path.is_file():
        raise FileNotFoundError(f"Photo {image_id} doesn't exist")
    return path.read_bytes()