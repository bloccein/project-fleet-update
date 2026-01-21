from __future__ import annotations

import asyncio
import logging
import os
from functools import lru_cache
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Request, UploadFile, File, Query
from packaging.version import Version, InvalidVersion

from .db import init_db, q, exec_, upload_firmware_to_minio, download_firmware_from_minio
from .udp_device import start_udp_listener
from .smp_ops import upload_image_udp, UdpTarget
from .hash_compute import compute_hash

def _ensure_logging() -> None:
    root = logging.getLogger()
    if root.handlers:
        return
    level_name = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


@lru_cache(maxsize=1)
def _log() -> logging.Logger:
    """
    Prefer the hosting server logger if present (uvicorn/gunicorn), otherwise fall back.
    """
    for name in ("uvicorn.error", "gunicorn.error"):
        candidate = logging.getLogger(name)
        if candidate.hasHandlers():
            return candidate
    return logging.getLogger(__name__)


UDP_HOST = os.getenv("UDP_HOST", "0.0.0.0")
UDP_PORT = int(os.getenv("UDP_PORT", "9999"))
# Useful for verifying from another machine: send any UDP datagram and get a reply.
UDP_PONG = os.getenv("UDP_PONG", "1").strip().lower() not in {"0", "false", "no", "off"}
UDP_PONG_ANY = os.getenv("UDP_PONG_ANY", "0").strip().lower() in {"1", "true", "yes", "on"}
UDP_LOG_PINGS = os.getenv("UDP_LOG_PINGS", "0").strip().lower() in {"1", "true", "yes", "on"}

# If you need smpclient UDP transport params (timeouts/mtu), set here:
DEFAULT_SMP_UDP_PARAMS: dict[str, Any] = {}

app = FastAPI(title="SMP Repo Server (HTTP admin, UDP device pings)", version="1.0.0")

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
    """Return the most recently inserted firmware (by created_at timestamp)."""
    rows = q(
        "SELECT version, sha256, filename, size_bytes, created_at FROM firmware ORDER BY created_at DESC LIMIT 1",
        (),
    )
    if not rows:
        return None
    return dict(rows[0])


def _version_lt(a: str, b: str) -> bool:
    return Version(a) < Version(b)


@app.on_event("startup")
async def _startup():
    global UDP_TRANSPORT
    _ensure_logging()
    init_db()
    try:
        UDP_TRANSPORT = await start_udp_listener(
            UDP_HOST,
            UDP_PORT,
            handle_device_ping,
            respond_pong=UDP_PONG,
            respond_any=UDP_PONG_ANY,
        )
        # Use WARNING so it shows up even when the environment defaults to WARNING.
        _log().warning(
            "UDP listener started on %s:%s (pong=%s, pong_any=%s)",
            UDP_HOST,
            UDP_PORT,
            UDP_PONG,
            UDP_PONG_ANY,
        )
    except Exception:
        _log().exception("Failed to start UDP listener on %s:%s", UDP_HOST, UDP_PORT)
        raise


@app.on_event("shutdown")
async def _shutdown():
    global UDP_TRANSPORT
    if UDP_TRANSPORT is not None:
        UDP_TRANSPORT.close()
        UDP_TRANSPORT = None


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

    digest = compute_hash(content)

    # Deduplicate by hash
    exists = q("SELECT sha256, version, size_bytes, created_at FROM firmware WHERE sha256=%s", (digest,))
    if exists:
        d = dict(exists[0])
        d["dedup"] = True
        return d

    # Upload to MinIO
    object_name = upload_firmware_to_minio(digest, content)

    exec_(
        "INSERT INTO firmware(sha256, version, filename, size_bytes, created_at) VALUES(%s,%s,%s,%s,%s)",
        (digest, version, object_name, len(content), now_iso()),
    )
    return {"sha256": digest, "version": version, "size_bytes": len(content)}


@app.get("/firmware")
def list_firmware() -> list[dict[str, Any]]:
    rows = q("SELECT version, sha256, size_bytes, created_at FROM firmware ORDER BY created_at DESC", ())
    return [dict(r) for r in rows]


@app.get("/firmware/{sha256}/download")
def download_firmware(sha256_hex: str):
    from fastapi.responses import Response
    from minio.error import S3Error

    rows = q("SELECT sha256 FROM firmware WHERE sha256=%s", (sha256_hex,))
    if not rows:
        raise HTTPException(404, "not found")

    try:
        content = download_firmware_from_minio(sha256_hex)
        return Response(content=content, media_type="application/octet-stream")
    except S3Error:
        raise HTTPException(500, "file missing in storage")


@app.get("/devices")
def list_devices() -> list[dict[str, Any]]:
    rows = q("SELECT device_id, last_ip, last_seen_at, last_version, last_status, last_message FROM device ORDER BY device_id", ())
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

    if UDP_LOG_PINGS:
        _log().info("UDP ping device_id=%s version=%s smp_port=%s from=%s:%s", device_id, version, smp_port, addr[0], addr[1])

    try:
        Version(version)
    except InvalidVersion:
        # record but don't attempt update
        exec_(
            """
            INSERT INTO device(device_id, last_ip, last_seen_at, last_version, last_status, last_message)
            VALUES(%s,%s,%s,%s,%s,%s)
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
        VALUES(%s,%s,%s,%s,%s,%s)
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
    - pick latest inserted firmware (by created_at)
    - download from MinIO and upload via SMP/UDP to device
    """
    while True:
        await _ev(device_id).wait()
        _ev(device_id).clear()

        state = PING_STATE.get(device_id)
        if not state:
            continue

        latest = _latest_firmware()
        if latest is None:
            exec_("UPDATE device SET last_status=%s, last_message=%s WHERE device_id=%s",
                  ("idle", "no firmware available on server", device_id))
            continue

        dev_ver = state["version"]
        srv_ver = latest["version"]

        # Load image bytes from MinIO
        try:
            image = download_firmware_from_minio(latest["sha256"])
        except Exception:
            exec_("UPDATE device SET last_status=%s, last_message=%s WHERE device_id=%s",
                  ("failed", "server firmware file missing in storage", device_id))
            continue

        ip = state["ip"]
        port = state["smp_port"]

        async def progress_cb(uploaded: int, total: int, msg: str):
            exec_(
                "UPDATE device SET last_status=%s, last_message=%s WHERE device_id=%s",
                ("running", f"{msg} {uploaded}/{total}", device_id),
            )

        try:
            exec_("UPDATE device SET last_status=%s, last_message=%s WHERE device_id=%s",
                  ("running", f"updating {dev_ver} -> {srv_ver} ({latest['sha256'][:8]}...)", device_id))

            await upload_image_udp(
                target=UdpTarget(host=ip, port=port, params=DEFAULT_SMP_UDP_PARAMS),
                image=image,
                slot=1,
                confirm=False,  # safest; let app confirm after healthy boot
                progress_cb=progress_cb,
            )

            exec_("UPDATE device SET last_status=%s, last_message=%s WHERE device_id=%s",
                  ("ok", f"uploaded {srv_ver} to {ip}:{port}", device_id))

        except Exception as e:
            exec_("UPDATE device SET last_status=%s, last_message=%s WHERE device_id=%s",
                  ("failed", f"{type(e).__name__}: {e}", device_id))
