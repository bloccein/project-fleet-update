from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request, UploadFile, File, Query
from fastapi.responses import FileResponse
from packaging.version import Version, InvalidVersion

from db import init_db, q, exec_
from udp_device import start_udp_listener
from smp_ops import upload_image_udp, UdpTarget
from hash_compute import compute_hash

import logging

log = logging.getLogger()

DATA_DIR = Path("data")
FW_DIR = DATA_DIR / "firmware"
FW_DIR.mkdir(parents=True, exist_ok=True)

UDP_HOST = "0.0.0.0"
UDP_PORT = 9999

# If you need smpclient UDP transport params (timeouts/mtu), set here:
DEFAULT_SMP_UDP_PARAMS: dict[str, Any] = {}


UDP_TRANSPORT = None
WORKERS: dict[str, asyncio.Task] = {}
EVENTS: dict[str, asyncio.Event] = {}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ev(device_id: str) -> asyncio.Event:
    ev = EVENTS.get(device_id)
    if ev is None:
        ev = asyncio.Event()
        EVENTS[device_id] = ev
    return ev


def _ensure_worker(device_id: str) -> None:
    t = WORKERS.get(device_id)
    if t is None or t.done():
        WORKERS[device_id] = asyncio.create_task(_device_worker(device_id))


def _latest_firmware() -> Optional[dict[str, Any]]:
    rows = q("SELECT version, sha256, filename, size_bytes, created_at FROM firmware")
    if not rows:
        return None

    parsed: list[tuple[Version, dict[str, Any]]] = []
    for r in rows:
        d = dict(r)
        try:
            v = Version(d["version"])
        except InvalidVersion:
            # if someone uploaded junk version, ignore it for auto-update
            continue
        parsed.append((v, d))

    if not parsed:
        return None

    parsed.sort(key=lambda x: x[0], reverse=True)
    return parsed[0][1]


def _version_lt(a: str, b: str) -> bool:
    return Version(a) < Version(b)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global UDP_TRANSPORT
    init_db()
    try:
        UDP_TRANSPORT = await start_udp_listener(
            UDP_HOST, UDP_PORT, handle_device_ping
        )
        yield
    finally:
        if UDP_TRANSPORT is not None:
            UDP_TRANSPORT.close()
            UDP_TRANSPORT = None

app = FastAPI(
    title="SMP Repo Server (HTTP admin, UDP device pings)",
    version="1.0.0",
    lifespan=lifespan,
)

# -------------------------
# Admin HTTP: firmware repo
# -------------------------
@app.post("/firmware")
async def upload_firmware(
    request: Request,
    version: str = Query(..., description="Semantic version like 1.2.3"),
    file: Optional[UploadFile] = File(default=None),
) -> dict[str, Any]:
    # Validate version
    try:
        Version(version)
    except InvalidVersion:
        raise HTTPException(400, f"Invalid version: {version}")

    # Accept multipart or raw body
    if file is not None:
        content = await file.read()
        orig_name = file.filename or "firmware.bin"
    else:
        content = await request.body()
        orig_name = request.headers.get("X-Filename", "firmware.bin")

    if not content:
        raise HTTPException(400, "Empty upload")

    digest = sha256(content).hexdigest()

    # Deduplicate by hash
    exists = q("SELECT sha256, version, size_bytes, created_at FROM firmware WHERE sha256=?", (digest,))
    if exists:
        d = dict(exists[0])
        d["dedup"] = True
        return d

    fw_name = f"{digest[:12]}_{Path(orig_name).name}"
    (FW_DIR / fw_name).write_bytes(content)

    exec_(
        "INSERT INTO firmware(sha256, version, filename, size_bytes, created_at) VALUES(?,?,?,?,?)",
        (digest, version, fw_name, len(content), now_iso()),
    )
    return {"sha256": digest, "version": version, "size_bytes": len(content)}


@app.get("/firmware")
def list_firmware() -> list[dict[str, Any]]:
    rows = q("SELECT version, sha256, size_bytes, created_at FROM firmware ORDER BY created_at DESC")
    return [dict(r) for r in rows]


@app.get("/firmware/{sha256}/download")
def download_firmware(sha256_hex: str):
    rows = q("SELECT filename FROM firmware WHERE sha256=?", (sha256_hex,))
    if not rows:
        raise HTTPException(404, "not found")
    path = FW_DIR / rows[0]["filename"]
    if not path.exists():
        raise HTTPException(500, "file missing on disk")
    return FileResponse(path)


@app.get("/devices")
def list_devices() -> list[dict[str, Any]]:
    rows = q("SELECT device_id, last_ip, last_seen_at, last_version, last_status, last_message FROM device ORDER BY device_id")
    return [dict(r) for r in rows]


# -------------------------
# Device UDP: ping -> DFU
# -------------------------
# In-memory “last ping info” needed by the worker (per device)
PING_STATE: dict[str, dict[str, Any]] = {}


def handle_device_ping(msg: dict, addr: tuple[str, int]) -> None:
    """
    Device sends:
      {"kind":"ping","device_id":"node01","version":"1.0.0","smp_port":1337}
    Server compares against latest available version and, if newer, uploads it using smpclient over UDP.
    """
    device_id = str(msg.get("device_id", "")).strip()
    version = str(msg.get("version", "")).strip()
    smp_port = msg.get("smp_port", 1337)

    if not device_id or not version:
        return

    try:
        Version(version)
    except InvalidVersion:
        # record but don't attempt update
        exec_(
            """
            INSERT INTO device(device_id, last_ip, last_seen_at, last_version, last_status, last_message)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(device_id) DO UPDATE SET
              last_ip=excluded.last_ip,
              last_seen_at=excluded.last_seen_at,
              last_version=excluded.last_version,
              last_status=excluded.last_status,
              last_message=excluded.last_message
            """,
            (device_id, addr[0], now_iso(), version, "failed", "invalid version in ping"),
        )
        return

    # Persist device info
    exec_(
        """
        INSERT INTO device(device_id, last_ip, last_seen_at, last_version, last_status, last_message)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(device_id) DO UPDATE SET
          last_ip=excluded.last_ip,
          last_seen_at=excluded.last_seen_at,
          last_version=excluded.last_version
        """,
        (device_id, addr[0], now_iso(), version, None, None),
    )

    # Cache ping state for worker
    try:
        smp_port = int(smp_port)
    except Exception:
        smp_port = 1337

    PING_STATE[device_id] = {"ip": addr[0], "smp_port": smp_port, "version": version}

    # Trigger worker
    _ensure_worker(device_id)
    _ev(device_id).set()


async def _device_worker(device_id: str) -> None:
    """
    On each ping:
    - pick latest firmware by version
    - if device version < latest version => upload latest image via SMP/UDP to device
    """
    while True:
        await _ev(device_id).wait()
        _ev(device_id).clear()

        state = PING_STATE.get(device_id)
        if not state:
            continue

        latest = _latest_firmware()
        if latest is None:
            exec_("UPDATE device SET last_status=?, last_message=? WHERE device_id=?",
                  ("idle", "no firmware available on server", device_id))
            continue

        dev_ver = state["version"]
        srv_ver = latest["version"]

        # If already up-to-date, do nothing
        if not _version_lt(dev_ver, srv_ver):
            exec_("UPDATE device SET last_status=?, last_message=? WHERE device_id=?",
                  ("ok", f"up-to-date ({dev_ver})", device_id))
            continue

        # Load image bytes
        fw_path = FW_DIR / latest["filename"]
        if not fw_path.exists():
            exec_("UPDATE device SET last_status=?, last_message=? WHERE device_id=?",
                  ("failed", "server firmware file missing on disk", device_id))
            continue

        image = fw_path.read_bytes()
        ip = state["ip"]
        port = state["smp_port"]

        async def progress_cb(uploaded: int, total: int, msg: str):
            exec_(
                "UPDATE device SET last_status=?, last_message=? WHERE device_id=?",
                ("running", f"{msg} {uploaded}/{total}", device_id),
            )

        try:
            exec_("UPDATE device SET last_status=?, last_message=? WHERE device_id=?",
                  ("running", f"updating {dev_ver} -> {srv_ver} ({latest['sha256'][:8]}...)", device_id))

            await upload_image_udp(
                target=UdpTarget(host=ip, port=port, params=DEFAULT_SMP_UDP_PARAMS),
                image=image,
                slot=1,
                confirm=False,  # safest; let app confirm after healthy boot
                progress_cb=progress_cb,
            )

            exec_("UPDATE device SET last_status=?, last_message=? WHERE device_id=?",
                  ("ok", f"uploaded {srv_ver} to {ip}:{port}", device_id))

        except Exception as e:
            exec_("UPDATE device SET last_status=?, last_message=? WHERE device_id=?",
                  ("failed", f"{type(e).__name__}: {e}", device_id))
