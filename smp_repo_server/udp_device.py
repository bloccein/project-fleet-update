from __future__ import annotations

import asyncio
import json
from typing import Callable


class DevicePingProtocol(asyncio.DatagramProtocol):
    def __init__(self, on_ping: Callable[[dict, tuple[str, int]], None]):
        self.on_ping = on_ping

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        try:
            msg = json.loads(data.decode("utf-8"))
        except Exception:
            return
        if msg.get("kind") != "ping":
            return
        self.on_ping(msg, addr)


async def start_udp_listener(host: str, port: int, on_ping):
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: DevicePingProtocol(on_ping),
        local_addr=(host, port),
    )
    return transport
