"""Resume boundaries across an API restart: confirmed chunks are never lost."""
from __future__ import annotations

import hashlib
from pathlib import Path

from tests.helpers import make_upload, put_chunk, split, status


def _content(n: int, seed: bytes) -> bytes:
    out = bytearray()
    while len(out) < n:
        out.extend(hashlib.sha256(seed + len(out).to_bytes(4, "big")).digest())
    return bytes(out[:n])


def test_resume_after_restart_boundary(client, restart_client, data_env):
    content = _content(1024, b"resume")
    r = make_upload(client, content, chunk_size=200)
    s = r.json()
    uid = s["upload_id"]
    parts = split(content, 200)
    total = len(parts)  # 6, tail 24 bytes

    # Confirm some chunks interleaved with gaps.
    for i in (0, 2, 5):
        assert put_chunk(client, uid, i, parts[i]).status_code == 200

    # Simulate restart: a new client/app reopens the same DATA_DIR.
    st = status(restart_client, uid).json()
    assert st["missing_chunks"] == [1, 3, 4]
    assert st["chunks_received"] == 3
    assert st["status"] == "open"

    # Confirmed chunk must not be treated as missing after restart.
    rr = put_chunk(restart_client, uid, 0, parts[0])
    assert rr.status_code == 200
    assert rr.json()["idempotent"] is True

    # Finish from the restarted process, including the short tail.
    for i in (1, 3, 4):
        rr = put_chunk(restart_client, uid, i, parts[i])
        assert rr.status_code == 200, (i, rr.text)
        # Only the final missing chunk (index 4) triggers completion.
        assert rr.json()["complete"] is (i == 4)

    st = restart_client.get(f"/api/v1/uploads/{uid}").json()
    assert st["status"] == "complete"
    assert st["missing_chunks"] == []

    published = Path(st["assembled_path"])
    assert published.is_file()
    assert published.read_bytes() == content
    assert hashlib.sha256(published.read_bytes()).hexdigest() == s["file_sha256"]


def test_restart_does_not_resurrect_rejected_chunks(client, restart_client):
    content = _content(400, b"reject")
    s = make_upload(client, content, chunk_size=100).json()
    uid = s["upload_id"]
    parts = split(content, 100)

    assert put_chunk(client, uid, 0, parts[0]).status_code == 200
    # Rejected: digest mismatch, wrong length, conflict.
    assert put_chunk(client, uid, 1, parts[1], digest="00" * 32).status_code == 422
    assert put_chunk(client, uid, 2, b"x" * 99).status_code == 422
    assert put_chunk(client, uid, 3, b"x" * 100).status_code == 200
    conflict = bytes([parts[3][0] ^ 0x01]) + parts[3][1:]
    assert put_chunk(client, uid, 3, conflict).status_code == 409

    st = status(restart_client, uid).json()
    assert st["missing_chunks"] == [1, 2]
    assert st["chunks_received"] == 2


def test_resume_each_chunk_is_exact_size_boundary(client, restart_client):
    # File size an exact multiple of chunk size: no short chunk at all.
    content = _content(500, b"exact")
    s = make_upload(client, content, chunk_size=100).json()
    uid = s["upload_id"]
    parts = split(content, 100)
    assert len(parts) == 5 and all(len(p) == 100 for p in parts)

    assert put_chunk(client, uid, 4, parts[4]).status_code == 200

    st = status(restart_client, uid).json()
    assert st["missing_chunks"] == [0, 1, 2, 3]

    # A byte-too-long chunk in the last position fails the length rule
    # before any conflict check, so nothing is recorded/changed.
    r = put_chunk(restart_client, uid, 4, parts[4] + b"x")
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_upload"
    # Original confirmed chunk is intact.
    assert put_chunk(restart_client, uid, 4, parts[4]).json()["idempotent"] is True


def test_resume_with_two_restarts_completes(client, restart_client, data_env):
    content = _content(333, b"twice")
    s = make_upload(client, content, chunk_size=100).json()
    uid = s["upload_id"]
    parts = split(content, 100)

    assert put_chunk(client, uid, 0, parts[0]).status_code == 200

    # First restart, add one more.
    assert put_chunk(restart_client, uid, 1, parts[1]).status_code == 200

    # Second restart (another fresh app over the same directory).
    from fastapi.testclient import TestClient
    from app.main import create_app
    with TestClient(create_app()) as third:
        st = third.get(f"/api/v1/uploads/{uid}").json()
        assert st["missing_chunks"] == [2, 3]
        for i in (2, 3):
            assert put_chunk(third, uid, i, parts[i]).status_code == 200
        st = third.get(f"/api/v1/uploads/{uid}").json()
        assert st["status"] == "complete"
        assert Path(st["assembled_path"]).read_bytes() == content


def test_chunk_payload_files_survive_restart(client, restart_client, data_env):
    content = _content(250, b"files")
    s = make_upload(client, content, chunk_size=100).json()
    uid = s["upload_id"]
    parts = split(content, 100)
    put_chunk(client, uid, 0, parts[0])
    chunk_file = data_env / "chunks" / uid / "0"
    assert chunk_file.is_file()
    assert chunk_file.read_bytes() == parts[0]
