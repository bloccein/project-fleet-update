from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Awaitable, Callable, Any

from smpclient import SMPClient
from smpclient.transport.udp import SMPUDPTransport
from smpclient.requests.image_management import ImageStatesRead, ImageStatesWrite
from smpclient.requests.os_management import ResetWrite
from smpclient import error


@dataclass(frozen=True)
class UdpTarget:
    host: str
    port: int
    params: dict[str, Any]


class SMPFailure(RuntimeError):
    pass


async def upload_image_udp(
    *,
    target: UdpTarget,
    image: bytes,
    slot: int = 1,
    confirm: bool = False,  # safer: test image
    progress_cb: Callable[[int, int, str], Awaitable[None]],
) -> None:
    total = len(image)
    img_hash = sha256(image).digest()

    transport = SMPUDPTransport(**target.params)
    address = f"{target.host}:{target.port}"

    await progress_cb(0, total, f"connecting {address}")

    async with SMPClient(transport, address) as client:
        r = await client.request(ImageStatesRead())
        if error(r):
            raise SMPFailure(f"ImageStatesRead error: {r}")

        await progress_cb(0, total, f"uploading slot {slot}")
        uploaded = 0
        async for off in client.upload(image, slot=slot, upgrade=False, use_sha=True):
            uploaded = min(int(off), total)
            await progress_cb(uploaded, total, "uploading")

        if uploaded != total:
            raise SMPFailure(f"Upload incomplete: {uploaded}/{total}")

        await progress_cb(total, total, "setting image state")
        wr = await client.request(ImageStatesWrite(hash=img_hash if not confirm else None, confirm=confirm))
        if error(wr):
            raise SMPFailure(f"ImageStatesWrite error: {wr}")

        await progress_cb(total, total, "rebooting")
        rr = await client.request(ResetWrite())
        if error(rr):
            raise SMPFailure(f"ResetWrite error: {rr}")

    await progress_cb(total, total, "done")
