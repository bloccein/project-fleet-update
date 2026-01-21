from __future__ import annotations

import os
from typing import Any
from io import BytesIO

import psycopg
from psycopg.rows import dict_row
from minio import Minio
from minio.error import S3Error

# ---------------------------
# PostgreSQL Config
# ---------------------------
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER", "fw")
PG_PASSWORD = os.getenv("PG_PASSWORD", "fwpass")
PG_DB = os.getenv("PG_DB", "firmware")

# ---------------------------
# MinIO Config
# ---------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123456")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() in ("true", "1", "yes")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "firmware")


def _get_conninfo() -> str:
    return f"host={PG_HOST} port={PG_PORT} user={PG_USER} password={PG_PASSWORD} dbname={PG_DB}"


def _connect() -> psycopg.Connection:
    return psycopg.connect(_get_conninfo(), row_factory=dict_row)


def _get_minio() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def init_db() -> None:
    """Initialize PostgreSQL tables and MinIO bucket."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS firmware (
                    sha256 TEXT PRIMARY KEY,
                    version TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                );

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
        conn.commit()

    # Ensure MinIO bucket exists
    client = _get_minio()
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)


def q(sql: str, args: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    """Execute a SELECT query and return results as list of dicts."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            return cur.fetchall()


def exec_(sql: str, args: tuple[Any, ...] = ()) -> None:
    """Execute a non-SELECT query (INSERT, UPDATE, DELETE)."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, args)
        conn.commit()


# ---------------------------
# MinIO Storage Functions
# ---------------------------
def upload_firmware_to_minio(sha256: str, content: bytes) -> str:
    """
    Upload firmware content to MinIO.
    Returns the object name.
    """
    client = _get_minio()
    object_name = f"images/{sha256}.bin"

    client.put_object(
        MINIO_BUCKET,
        object_name,
        BytesIO(content),
        length=len(content),
        content_type="application/octet-stream",
    )

    return object_name


def download_firmware_from_minio(sha256: str) -> bytes:
    """
    Download firmware content from MinIO.
    Raises S3Error if not found.
    """
    client = _get_minio()
    object_name = f"images/{sha256}.bin"

    response = client.get_object(MINIO_BUCKET, object_name)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def firmware_exists_in_minio(sha256: str) -> bool:
    """Check if firmware exists in MinIO."""
    client = _get_minio()
    object_name = f"images/{sha256}.bin"

    try:
        client.stat_object(MINIO_BUCKET, object_name)
        return True
    except S3Error:
        return False
