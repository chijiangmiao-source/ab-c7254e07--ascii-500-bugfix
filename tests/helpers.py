"""Helpers shared by the test suite."""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone


def make_upload(client, content: bytes, chunk_size: int, *, expires_in: int = 3600,
                filename: str = "scan.bin", file_sha256: str | None = None,
                file_size: int | None = None):
    body = {
        "filename": filename,
        "file_size": len(content) if file_size is None else file_size,
        "chunk_size": chunk_size,
        "file_sha256": file_sha256 or hashlib.sha256(content).hexdigest(),
        "expires_at": (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat(),
    }
    r = client.post("/api/v1/uploads", json=body)
    return r


def split(content: bytes, chunk_size: int) -> list[bytes]:
    return [content[i:i + chunk_size] for i in range(0, len(content), chunk_size)]


def put_chunk(client, upload_id, index, data, digest=None):
    return client.put(
        f"/api/v1/uploads/{upload_id}/chunks/{index}",
        content=data,
        headers={"X-Chunk-SHA256": digest or hashlib.sha256(data).hexdigest()},
    )


def status(client, upload_id):
    return client.get(f"/api/v1/uploads/{upload_id}")
