import json
from pathlib import Path

JSON_DIR = Path("photos_json")
JSON_DIR.mkdir()
BIN_DIR = Path("photos_bin")
BIN_DIR.mkdir()
last_position = 1
next_postion = last_position + 1


async def save_photo(photo):
    images_metadata: dict[str,dict] = {}
    with open(JSON_DIR/"metadata.json","r") as f:
        images_metadata = f.read()
    images_metadata[last_position] = photo
    with open(JSON_DIR/"metadata.json","w")