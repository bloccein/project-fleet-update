from __future__ import annotations

from pydantic import BaseModel, Field


class FirmwareEntry(BaseModel):
    version: str
    sha256: str
    size_bytes: int
    created_at: str


class DeviceEntry(BaseModel):
    device_id: str
    last_ip: str | None = None
    last_seen_at: str | None = None
    last_version: str | None = None
    last_status: str | None = None
    last_message: str | None = None


class DevicePing(BaseModel):
    kind: str = "ping"
    device_id: str
    version: str
    # device's SMP-over-UDP port (Zephyr mcumgr UDP); if omitted defaults to 1337
    smp_port: int = 1337
