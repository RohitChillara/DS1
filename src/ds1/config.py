from pathlib import Path
UDP_PORT = 9000
TCP_PORT = 8000
JSON_DIR= JSON_DIR = Path("photos_json")
BIN_DIR = Path("photos_bin")
EPOCH_DIR = Path("epoch")
# ---------- photos ----------
MAX_PHOTO_SIZE = 3 * 1024 * 1024    # 3 MB; reject larger DATA headers before reading the bytes