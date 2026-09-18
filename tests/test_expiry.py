"""Expiry rules: expired sessions reject chunks without touching progress."""
from __future__ import annotations

import time

from tests.helpers import make_upload, put_chunk, split, status


def test_expired_session_rejects_chunk_410(client):
    content = b"q" * 250
    s = make_upload(client, content, chunk_size=100, expires_in=1).json()
    uid = s["upload_id"]
    parts = split(content, 100)
    assert put_chunk(client, uid, 0, parts[0]).status_code == 200

    time.sleep(1.2)
    r = put_chunk(client, uid, 1, parts[1])
    assert r.status_code == 410
    body = r.json()
    assert body["error"]["code"] == "session_expired"
    # details show what remains, no progress modified
    assert body["error"]["details"]["missing_chunks"] == [1, 2]

    st = status(client, uid).json()
    assert st["expired"] is True
    assert st["status"] == "expired"
    assert st["chunks_received"] == 1
    assert st["missing_chunks"] == [1, 2]


def test_expired_session_repeated_chunks_still_rejected(client):
    s = make_upload(client, b"a" * 150, chunk_size=100, expires_in=1).json()
    uid = s["upload_id"]
    put_chunk(client, uid, 0, b"a" * 100)
    time.sleep(1.2)
    # Even an idempotent retransmit is refused once expired: the session is closed.
    r = put_chunk(client, uid, 0, b"a" * 100)
    assert r.status_code == 410
    assert status(client, uid).json()["chunks_received"] == 1


def test_expired_session_cannot_finalize(client):
    s = make_upload(client, b"a" * 150, chunk_size=100, expires_in=1).json()
    uid = s["upload_id"]
    put_chunk(client, uid, 0, b"a" * 100)
    time.sleep(1.2)
    r = client.post(f"/api/v1/uploads/{uid}/finalize")
    assert r.status_code == 410
    assert r.json()["error"]["code"] == "session_expired"


def test_future_session_not_expired(client):
    s = make_upload(client, b"a" * 150, chunk_size=100, expires_in=3600).json()
    st = status(client, s["upload_id"]).json()
    assert st["expired"] is False
    assert st["status"] == "open"
