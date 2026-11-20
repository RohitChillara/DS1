# web.py: the HTTP side of a node. Every node serves the page, the cluster status and the
# photos from its own replica. Only the leader accepts uploads; the page asks /api/status
# who leads and sends uploads there.
from __future__ import annotations

import asyncio
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response

from ds1 import replication as r, state as s, store as st
from ds1.config import HTTP_PORT, MAX_PHOTO_SIZE

STATIC = Path(__file__).parent / "static"
_log = None                                          # the node's logger, set in serve()

app = FastAPI(title="DS1 photo cluster")
# The page is served by any node but uploads go to the leader, which is a different origin.
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def _node_url(node_id: int) -> str | None:
    if node_id == s.MyDetails.node_id:
        return None                                  # the page uses its own origin for this node
    host = s.Members.members.get(node_id)
    return f"http://{host}:{HTTP_PORT}" if host else None


@app.get("/")
async def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/status")
async def status():
    me = s.MyDetails.node_id
    leader = s.Members.leader
    alive = sorted(s.Heartbeats.last_beat)
    return {
        "node": me,
        "leader": leader,
        "is_leader": leader == me,
        "leader_url": _node_url(leader) if leader is not None else None,
        "epoch": st.get_epoch(),
        "position": list(st.last_position()),
        "photos": len(s.Storage.image_metadata),
        "nodes": [{"id": me, "alive": True, "self": True}]
                 + [{"id": n, "alive": n in alive, "self": False} for n in sorted(s.Members.members)],
        "max_photo_size": MAX_PHOTO_SIZE,
    }


@app.get("/api/photos")
async def photos():
    return [{"image_id": p["image_id"], "file_name": p["file_name"], "size": p["size"],
             "epoch": p["epoch"], "seq": p["seq"]}
            for p in reversed(st.list_photos())]     # newest first


@app.get("/api/photos/{image_id}")
async def photo(image_id: str):
    try:
        data = await asyncio.to_thread(st.get_photo, image_id)
    except FileNotFoundError:
        raise HTTPException(404, "Photo not found")
    return Response(data, media_type=_media_type(image_id),
                    headers={"Cache-Control": "public, max-age=31536000, immutable"})


@app.post("/api/photos")
async def upload(request: Request):
    """Body: the raw image bytes. Headers: X-File-Name, X-Upload-Id (made by the client once
    and reused on every retry, so a retry after a failover is never stored twice)."""
    upload_id = request.headers.get("x-upload-id")
    file_name = request.headers.get("x-file-name", "photo")
    if not upload_id:
        raise HTTPException(400, "X-Upload-Id header is required")
    if s.Members.leader != s.MyDetails.node_id:
        # 421 Misdirected Request: the page re-reads /api/status and retries at the leader
        raise HTTPException(421, {"error": "not leader", "leader": s.Members.leader})
    data = await request.body()
    if not data:
        raise HTTPException(400, "Empty upload")
    if len(data) > MAX_PHOTO_SIZE:
        raise HTTPException(413, f"Photo is larger than {MAX_PHOTO_SIZE // (1024 * 1024)} MB")
    try:
        return await r.upload(_safe_name(file_name), data, upload_id, _log)
    except r.NotLeader:
        raise HTTPException(421, {"error": "not leader", "leader": s.Members.leader})


def _safe_name(name: str) -> str:
    from urllib.parse import unquote
    name = unquote(name).replace("\\", "/").split("/")[-1].strip()
    return name[:200] or "photo"


def _media_type(image_id: str) -> str:
    meta = next((p for p in s.Storage.image_metadata.values() if p["image_id"] == image_id), None)
    name = (meta or {}).get("file_name", "").lower()
    for ext, kind in ((".png", "image/png"), (".gif", "image/gif"), (".webp", "image/webp"),
                      (".heic", "image/heic"), (".jpg", "image/jpeg"), (".jpeg", "image/jpeg")):
        if name.endswith(ext):
            return kind
    return "application/octet-stream"


async def serve(log):
    """Run the web server inside the node's event loop, next to the peer tasks."""
    global _log
    _log = log
    config = uvicorn.Config(app, host="0.0.0.0", port=HTTP_PORT, log_level="warning",
                            lifespan="off")
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None    # Ctrl+C is handled by node.py, not uvicorn
    log.info(f"Web UI on http://0.0.0.0:{HTTP_PORT}")
    await server.serve()