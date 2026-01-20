from __future__ import annotations

import asyncio
import json
from typing import Callable, Optional


class DevicePingProtocol(asyncio.DatagramProtocol):
    def __init__(
        self,
        on_ping: Callable[[dict, tuple[str, int]], None],
        *,
        respond_pong: bool = False,
        respond_any: bool = False,
    ):
        self.on_ping = on_ping
        self.respond_pong = respond_pong
        self.respond_any = respond_any
        self.transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        # Called by asyncio; save for sendto() responses.
        self.transport = transport  # type: ignore[assignment]

    def _maybe_reply(self, addr: tuple[str, int]) -> None:
        if not self.respond_pong or self.transport is None:
            return
        try:
            payload = json.dumps({"kind": "pong"}).encode("utf-8")
        except Exception:
            return
        self.transport.sendto(payload, addr)

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        if self.respond_any:
            self._maybe_reply(addr)
        try:
            msg = json.loads(data.decode("utf-8"))
        except Exception:
            return
        if msg.get("kind") != "ping":
            return
        self.on_ping(msg, addr)
        self._maybe_reply(addr)


async def start_udp_listener(
    host: str,
    port: int,
    on_ping,
    *,
    respond_pong: bool = False,
    respond_any: bool = False,
):
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: DevicePingProtocol(on_ping, respond_pong=respond_pong, respond_any=respond_any),
        local_addr=(host, port),
    )
    return transport
