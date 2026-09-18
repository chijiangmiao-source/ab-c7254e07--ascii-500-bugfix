"""Filesystem paths and runtime configuration, all overridable via environment."""
from __future__ import annotations

import os
from pathlib import Path


def _data_dir() -> Path:
    return Path(os.environ.get("DATA_DIR", "/data")).resolve()


DATA_DIR: Path = _data_dir()
# SQLite registry (sessions, per-chunk digests, reception bitmap derived from chunks rows)
DB_PATH: Path = Path(os.environ.get("DB_PATH", str(DATA_DIR / "registry.db")))
# Confirmed chunk payloads, one file per (upload_id, chunk_index)
CHUNKS_DIR: Path = DATA_DIR / "chunks"
# Atomic, published, assembled files
FILES_DIR: Path = DATA_DIR / "files"
# Staging area for fsync + rename publishing
TEMP_DIR: Path = DATA_DIR / "tmp"
