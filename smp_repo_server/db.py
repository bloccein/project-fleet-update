from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterable

DB_PATH = Path("data/server.db")


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _connect() as conn:
        conn.executescript(
            """
            PRAGMA journal_mode=WAL;

            -- Firmware: store only (file path), version, sha256, size, created_at
            CREATE TABLE IF NOT EXISTS firmware (
              sha256 TEXT PRIMARY KEY,
              version TEXT NOT NULL,
              filename TEXT NOT NULL,
              size_bytes INTEGER NOT NULL,
              created_at TEXT NOT NULL
            );

            -- Devices: learned from pings
            CREATE TABLE IF NOT EXISTS device (
              device_id TEXT PRIMARY KEY,
              last_ip TEXT,
              last_seen_at TEXT,
              last_version TEXT,
              last_status TEXT,
              last_message TEXT
            );
            """
        )


def q(sql: str, args: Iterable[Any] = ()) -> list[sqlite3.Row]:
    with _connect() as conn:
        cur = conn.execute(sql, tuple(args))
        return cur.fetchall()


def exec_(sql: str, args: Iterable[Any] = ()) -> None:
    with _connect() as conn:
        conn.execute(sql, tuple(args))
        conn.commit()
