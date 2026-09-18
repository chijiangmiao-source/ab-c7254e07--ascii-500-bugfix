"""SQLite persistence for upload sessions, per-chunk digests and the bitmap.

A chunk row exists *only* for confirmed chunks, so the set of chunk indexes
is the reception bitmap. WAL + a process-wide write lock make concurrent
chunk uploads (FastAPI sync routes run in a threadpool) safe and crash-safe:
chunk payloads are fsynced before their row is committed, and publication
marks the session complete inside the same transaction that verifies every
required chunk row is still present.
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from . import config

_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
PRAGMA synchronous=FULL;

CREATE TABLE IF NOT EXISTS sessions (
    upload_id      TEXT PRIMARY KEY,
    filename       TEXT NOT NULL,
    file_size      INTEGER NOT NULL CHECK (file_size >= 0),
    chunk_size     INTEGER NOT NULL CHECK (chunk_size > 0),
    total_chunks   INTEGER NOT NULL CHECK (total_chunks >= 0),
    file_sha256    TEXT NOT NULL,
    expires_at     TEXT NOT NULL,  -- ISO-8601 UTC, e.g. 2026-09-15T12:00:00+00:00
    status         TEXT NOT NULL DEFAULT 'open'
                     CHECK (status IN ('open', 'complete', 'expired')),
    assembled_path TEXT,
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    completed_at   TEXT
);

CREATE TABLE IF NOT EXISTS chunks (
    upload_id   TEXT NOT NULL REFERENCES sessions(upload_id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    sha256      TEXT NOT NULL,
    size        INTEGER NOT NULL CHECK (size >= 0),
    received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    PRIMARY KEY (upload_id, chunk_index)
);
"""

_write_lock = threading.Lock()


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(config.DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db() -> None:
    Path(config.DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = _connect()
    try:
        conn.executescript(_SCHEMA)
        conn.commit()
    finally:
        conn.close()


@contextmanager
def get_conn() -> Iterator[sqlite3.Connection]:
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def write_tx() -> Iterator[sqlite3.Connection]:
    """Serialize writers and commit/rollback as a unit."""
    conn = _connect()
    try:
        with _write_lock:
            try:
                yield conn
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
    finally:
        conn.close()


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None
