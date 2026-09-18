"""Finalization: atomic publication on success, integrity error otherwise."""
from __future__ import annotations

import hashlib
from pathlib import Path

from tests.helpers import make_upload, put_chunk, split, status


def _content(n: int, seed: bytes) -> bytes:
    out = bytearray()
    while len(out) < n:
        out.extend(hashlib.sha256(seed + len(out).to_bytes(4, "big")).digest())
    return bytes(out[:n])


def test_happy_path_publishes_atomically(client, data_env):
    content = _content(450, b"happy")
    s = make_upload(client, content, chunk_size=100).json()
    uid = s["upload_id"]
    parts = split(content, 100)
    for i, p in enumerate(parts):
        r = put_chunk(client, uid, i, p)
        assert r.status_code == 200
    last = put_chunk(client, uid, len(parts) - 1, parts[-1]).json()
    assert last["complete"] is True
    assert last["idempotent"] is True  # retransmit after completion

    st = status(client, uid).json()
    assert st["status"] == "complete"
    assert st["missing_chunks"] == []
    published = Path(st["assembled_path"])
    assert published.is_file()
    assert published.read_bytes() == content
    # No staging leftovers
    assert list((data_env / "tmp").iterdir()) == []


def test_finalize_with_missing_chunks_409(client):
    content = _content(400, b"miss")
    s = make_upload(client, content, chunk_size=100).json()
    uid = s["upload_id"]
    parts = split(content, 100)
    put_chunk(client, uid, 0, parts[0])
    r = client.post(f"/api/v1/uploads/{uid}/finalize")
    assert r.status_code == 409
    err = r.json()["error"]
    assert err["code"] == "incomplete_upload"
    assert err["details"]["missing_chunks"] == [1, 2, 3]
    # session kept open
    assert status(client, uid).json()["status"] == "open"


def test_whole_file_digest_mismatch_integrity_error(client, data_env):
    content = _content(250, b"bad")
    s = make_upload(client, content, chunk_size=100, file_sha256="ab" * 32).json()
    uid = s["upload_id"]
    parts = split(content, 100)
    assert len(parts) == 3
    put_chunk(client, uid, 0, parts[0])
    put_chunk(client, uid, 1, parts[1])
    r = put_chunk(client, uid, 2, parts[2])  # final chunk triggers assembly
    assert r.status_code == 422
    err = r.json()["error"]
    assert err["code"] == "integrity_error"
    assert err["details"]["declared_sha256"] == "ab" * 32
    assert err["details"]["actual_sha256"] == hashlib.sha256(content).hexdigest()

    # Progress retained, session still open, nothing published.
    st = status(client, uid).json()
    assert st["status"] == "open"
    assert st["chunks_received"] == 3
    assert st["missing_chunks"] == []
    assert st["assembled_path"] is None
    assert not (data_env / "files" / uid).exists() or not any(
        (data_env / "files" / uid).iterdir()
    )

    # Idempotent retransmit after the failure still succeeds (no rewrite).
    rr = put_chunk(client, uid, 2, parts[2])
    assert rr.status_code == 200 and rr.json()["idempotent"] is True

    # Explicit finalize reports the same integrity error deterministically.
    fr = client.post(f"/api/v1/uploads/{uid}/finalize")
    assert fr.status_code == 422
    assert fr.json()["error"]["code"] == "integrity_error"


def test_finalize_complete_session_is_idempotent(client):
    content = _content(200, b"done")
    s = make_upload(client, content, chunk_size=100).json()
    uid = s["upload_id"]
    for i, p in enumerate(split(content, 100)):
        put_chunk(client, uid, i, p)
    r1 = client.post(f"/api/v1/uploads/{uid}/finalize")
    r2 = client.post(f"/api/v1/uploads/{uid}/finalize")
    assert r1.status_code == 200 and r2.status_code == 200
    assert r1.json()["status"] == "complete"
    assert r1.json()["assembled_path"] == r2.json()["assembled_path"]
