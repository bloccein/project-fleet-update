#!/usr/bin/env python3
import asyncio
import os
import struct
import time
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from typing import Dict, Optional, Any

try:
    from minio import Minio
    from minio.error import S3Error
except ImportError:  # pragma: no cover
    Minio = None  # type: ignore[assignment]
    S3Error = Exception  # type: ignore[assignment]

from smpclient import SMPClient
from smpclient.generics import error
from smpclient.requests.image_management import ImageStatesWrite
from smpclient.requests.os_management import ResetWrite
from smpclient.transport.udp import SMPUDPTransport

# -----------------------------
# Config
# -----------------------------
PING_LISTEN_HOST = "0.0.0.0"
PING_LISTEN_PORT = 9997

MCUMGR_PORT = 1337          # Zephyr mcumgr UDP port (often 1337)
UPLOAD_SLOT = 0

DEBOUNCE_SECONDS = 600      # don't re-upload more often than this per device IP
UPLOAD_TIMEOUT = 5.0        # UDP transport timeout

# MinIO Config (compatible with smp_repo_server defaults)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123456")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() in ("true", "1", "yes")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "firmware")
# MinIO key prefix to scan for firmware images. Default matches smp_repo_server/db.py.
FIRMWARE_MINIO_PREFIX = os.getenv("FIRMWARE_MINIO_PREFIX", "images/").lstrip("/")

DEFAULT_SMP_UDP_PARAMS: dict[str, Any] = {}

def _minio_client() -> Minio:
    if Minio is None:
        raise RuntimeError("MinIO support requires the 'minio' package (pip install minio)")
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def _latest_minio_object_name() -> str:
    client = _minio_client()
    latest_name: Optional[str] = None
    latest_mtime: Optional[datetime] = None

    for obj in client.list_objects(MINIO_BUCKET, prefix=FIRMWARE_MINIO_PREFIX, recursive=True):
        mtime = getattr(obj, "last_modified", None)
        if mtime is None:
            continue
        if latest_mtime is None or mtime > latest_mtime:
            latest_mtime = mtime
            latest_name = obj.object_name

    if not latest_name:
        raise RuntimeError(f"No firmware objects found in MinIO bucket={MINIO_BUCKET} prefix={FIRMWARE_MINIO_PREFIX!r}")

    return latest_name


@dataclass
class FirmwareCache:
    object_name: str
    etag: Optional[str]
    content: bytes
    source: str


class _SMPUDPTransportWithPort(SMPUDPTransport):
    def __init__(self, *, port: int, mtu: int) -> None:
        super().__init__(mtu=mtu)
        self._port = port

    async def connect(self, address: str, timeout_s: float, port: int = 1337) -> None:  # noqa: ARG002
        await super().connect(address, timeout_s, port=self._port)


def _compute_mcuboot_hash_hex(blob: bytes) -> str:
    """
    MCUboot "image hash" for mcumgr image state (header+body+protected TLVs).
    Returns a hex string.
    """
    if len(blob) < 32:
        raise ValueError("firmware too short for MCUboot header")

    magic, _load, hdr_sz, prot_sz, img_sz = struct.unpack_from("<IIHHI", blob, 0)
    if magic != 0x96F3B83D:
        raise ValueError(f"bad MCUboot magic: {hex(magic)}")

    hashed_len = hdr_sz + img_sz + prot_sz
    if len(blob) < hashed_len:
        raise ValueError("firmware too short for MCUboot hashed length")

    return sha256(blob[:hashed_len]).hexdigest()


# -----------------------------
# SMP upload logic
# -----------------------------
async def smp_upload_firmware(device_ip: str, image: bytes, *, source: str) -> None:
    print(f"[+] Starting upload to {device_ip}:{MCUMGR_PORT} ({len(image)} bytes) from {source}")

    mtu = DEFAULT_SMP_UDP_PARAMS.get("mtu", 1500)
    try:
        mtu = int(mtu)
    except Exception as e:
        raise ValueError(f"DEFAULT_SMP_UDP_PARAMS['mtu'] must be an int (got {mtu!r})") from e

    async with SMPClient(_SMPUDPTransportWithPort(port=MCUMGR_PORT, mtu=mtu), device_ip) as client:
        try:
            total = len(image)
            uploaded = 0

            async for off in client.upload(image, slot=UPLOAD_SLOT, upgrade=False, use_sha=True):
                uploaded = min(int(off), total)
                if total:
                    pct = (uploaded * 100) // total
                    print(f"\r    Upload: {uploaded}/{total} bytes ({pct}%)", end="")

            if uploaded != total:
                raise RuntimeError(f"Upload incomplete: {uploaded}/{total}")

            print("\n[+] Upload complete")

            # Set image state to "test" (pending) so MCUboot swaps on reboot.
            hash_hex = _compute_mcuboot_hash_hex(image)
            wr = await client.request(ImageStatesWrite(hash=bytes.fromhex(hash_hex), confirm=False))
            if error(wr):
                raise RuntimeError(f"ImageStatesWrite error: {wr}")
            print("[+] Image state set (test)")

            rr = await client.request(ResetWrite())
            if error(rr):
                raise RuntimeError(f"ResetWrite error: {rr}")
            print("[+] Device reset requested")

        except Exception as e:
            print(f"[!] SMP error while uploading to {device_ip}: {e}")
            raise


# -----------------------------
# UDP Ping server
# -----------------------------
class PingProtocol(asyncio.DatagramProtocol):
    def __init__(self, on_ping):
        super().__init__()
        self.on_ping = on_ping

    def datagram_received(self, data: bytes, addr):
        ip, port = addr[0], addr[1]
        msg = data.decode(errors="ignore").strip().lower()
        print(f"[.] UDP from {ip}:{port} -> {msg!r}")
        self.on_ping(ip, msg)


async def main() -> None:
    firmware_lock = asyncio.Lock()
    firmware_cache: Optional[FirmwareCache] = None

    async def _load_firmware_minio(object_name: str, *, etag_hint: Optional[str] = None) -> FirmwareCache:
        def _get_only() -> bytes:
            client = _minio_client()
            resp = client.get_object(MINIO_BUCKET, object_name)
            try:
                return resp.read()
            finally:
                resp.close()
                resp.release_conn()

        etag = etag_hint
        if etag is None:
            def _etag_only() -> Optional[str]:
                client = _minio_client()
                stat = client.stat_object(MINIO_BUCKET, object_name)
                return getattr(stat, "etag", None)

            etag = await asyncio.to_thread(_etag_only)

        content = await asyncio.to_thread(_get_only)
        src = f"minio:{MINIO_BUCKET}/{object_name}"
        return FirmwareCache(object_name=object_name, etag=etag, content=content, source=src)

    async def get_firmware() -> FirmwareCache:
        nonlocal firmware_cache
        async with firmware_lock:
            # MinIO: pick latest object by last_modified and cache by (object_name, ETag).
            try:
                object_name = await asyncio.to_thread(_latest_minio_object_name)
            except S3Error as e:
                raise RuntimeError(f"MinIO list failed for bucket={MINIO_BUCKET} prefix={FIRMWARE_MINIO_PREFIX!r}: {e}") from e

            try:
                def _etag_only() -> Optional[str]:
                    client = _minio_client()
                    stat = client.stat_object(MINIO_BUCKET, object_name)
                    return getattr(stat, "etag", None)

                current_etag = await asyncio.to_thread(_etag_only)
            except S3Error as e:
                raise RuntimeError(f"MinIO stat failed for {MINIO_BUCKET}/{object_name}: {e}") from e

            if (
                firmware_cache is not None
                and firmware_cache.object_name == object_name
                and firmware_cache.etag
                and current_etag == firmware_cache.etag
            ):
                return firmware_cache

            try:
                firmware_cache = await _load_firmware_minio(object_name, etag_hint=current_etag)
            except S3Error as e:
                raise RuntimeError(f"MinIO download failed for {MINIO_BUCKET}/{object_name}: {e}") from e
            return firmware_cache

    # Validate firmware source early (fail fast on bad config)
    try:
        initial_fw = await get_firmware()
    except Exception as e:
        raise SystemExit(str(e)) from e

    loop = asyncio.get_running_loop()

    last_upload_at: Dict[str, float] = {}
    in_progress: Dict[str, asyncio.Task] = {}

    def handle_ping(device_ip: str, msg: str) -> None:
        # trigger only on exact "ping"
        print(f"[device]: {msg}")

        now = time.time()

        # debounce per device IP
        last = last_upload_at.get(device_ip, 0.0)
        if now - last < DEBOUNCE_SECONDS:
            print(f"[~] Ignoring ping from {device_ip} (debounce {DEBOUNCE_SECONDS}s)")
            return

        # avoid concurrent upload per device
        if device_ip in in_progress and not in_progress[device_ip].done():
            print(f"[~] Upload already in progress for {device_ip}")
            return

        last_upload_at[device_ip] = now

        async def runner():
            try:
                fw = await get_firmware()
                await smp_upload_firmware(device_ip, fw.content, source=fw.source)
                print(f"[+] Done handling ping from {device_ip}")
            except Exception as e:
                print(f"[!] Upload task failed for {device_ip}: {e}")

        in_progress[device_ip] = asyncio.create_task(runner())
        print(f"[+] Triggered upload for {device_ip}")

    # Start UDP server
    transport, _ = await loop.create_datagram_endpoint(
        lambda: PingProtocol(handle_ping),
        local_addr=(PING_LISTEN_HOST, PING_LISTEN_PORT),
    )

    print(f"[+] Listening for 'ping' on UDP {PING_LISTEN_HOST}:{PING_LISTEN_PORT}")
    print(f"[+] Firmware: {initial_fw.source} ({len(initial_fw.content)} bytes)")
    print(f"[+] mcumgr UDP port: {MCUMGR_PORT}")
    print(f"[+] Send a ping with:  echo -n ping | nc -u -w1 <server_ip> {PING_LISTEN_PORT}")

    try:
        # run forever
        await asyncio.Future()
    finally:
        transport.close()


if __name__ == "__main__":
    asyncio.run(main())
