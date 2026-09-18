"""Chunk reception: length rules, digests, idempotency, conflicts, bitmap."""
from __future__ import annotations

import hashlib

from tests.helpers import make_upload, put_chunk, split, status


def _session(client, content, chunk_size=100, **kw):
    r = make_upload(client, content, chunk_size=chunk_size, **kw)
    assert r.status_code == 201, r.text
    return r.json()


def test_full_and_last_chunk_lengths_accepted(client):
    content = b"z" * 250
    s = _session(client, content, chunk_size=100)
    chunks = split(content, 100)
    assert len(chunks) == 3 and len(chunks[-1]) == 50
    for i, part in enumerate(chunks):
        r = put_chunk(client, s["upload_id"], i, part)
        assert r.status_code == 200, (i, r.text)
    assert status(client, s["upload_id"]).json()["missing_chunks"] == []


def test_non_last_chunk_wrong_length_rejected_and_not_recorded(client):
    content = b"z" * 250
    s = _session(client, content, chunk_size=100)
    uid = s["upload_id"]
    r = put_chunk(client, uid, 0, b"z" * 99)  # one byte short
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_upload"
    assert r.json()["error"]["details"]["expected_length"] == 100
    assert status(client, uid).json()["missing_chunks"] == [0, 1, 2]


def test_last_chunk_wrong_length_rejected(client):
    content = b"z" * 250
    s = _session(client, content, chunk_size=100)
    uid = s["upload_id"]
    # last chunk must be 50 bytes, send 51
    r = put_chunk(client, uid, 2, b"z" * 51)
    assert r.status_code == 422
    assert r.json()["error"]["details"]["expected_length"] == 50


def test_chunk_digest_mismatch_not_recorded(client):
    s = _session(client, b"a" * 150, chunk_size=100)
    uid = s["upload_id"]
    r = put_chunk(client, uid, 0, b"a" * 100, digest="00" * 32)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "checksum_mismatch"
    st = status(client, uid).json()
    assert st["chunks_received"] == 0
    assert st["missing_chunks"] == [0, 1]


def test_same_index_same_content_is_idempotent(client):
    content = b"".join(bytes([i % 256]) * 100 for i in range(3))
    s = _session(client, content, chunk_size=100)
    uid = s["upload_id"]
    parts = split(content, 100)
    r1 = put_chunk(client, uid, 1, parts[1])
    assert r1.status_code == 200 and r1.json()["idempotent"] is False
    r2 = put_chunk(client, uid, 1, parts[1])
    assert r2.status_code == 200
    body = r2.json()
    assert body["idempotent"] is True
    assert body["chunks_received"] == 1
    assert status(client, uid).json()["chunks_received"] == 1


def test_same_index_different_content_conflict_409(client):
    content = b"a" * 200
    s = _session(client, content, chunk_size=100)
    uid = s["upload_id"]
    r1 = put_chunk(client, uid, 0, b"a" * 100)
    assert r1.status_code == 200
    different = b"b" * 100
    r2 = put_chunk(client, uid, 0, different)
    assert r2.status_code == 409
    err = r2.json()["error"]
    assert err["code"] == "chunk_conflict"
    assert err["details"]["existing_sha256"] == hashlib.sha256(b"a" * 100).hexdigest()
    # stored chunk untouched: retransmitting the original is still idempotent
    r3 = put_chunk(client, uid, 0, b"a" * 100)
    assert r3.status_code == 200 and r3.json()["idempotent"] is True


def test_out_of_range_indexes(client):
    content = b"a" * 250
    s = _session(client, content, chunk_size=100)
    uid = s["upload_id"]
    r = put_chunk(client, uid, 3, b"x")
    assert r.status_code == 416
    assert r.json()["error"]["code"] == "chunk_out_of_range"
    # progress untouched
    assert status(client, uid).json()["missing_chunks"] == [0, 1, 2]


def test_unknown_session_chunk_404_structured(client):
    r = put_chunk(client, "nope", 0, b"x")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "session_not_found"


def test_missing_digest_header_structured(client):
    s = _session(client, b"a" * 150, chunk_size=100)
    r = client.put(f"/api/v1/uploads/{s['upload_id']}/chunks/0", content=b"a" * 100)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_upload"


def test_malformed_digest_header_structured(client):
    s = _session(client, b"a" * 150, chunk_size=100)
    r = client.put(
        f"/api/v1/uploads/{s['upload_id']}/chunks/0",
        content=b"a" * 100,
        headers={"X-Chunk-SHA256": "not-hex"},
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_upload"


def test_missing_list_always_ascending(client):
    content = os_bytes(400)[:400]
    assert len(content) == 400
    s = _session(client, content, chunk_size=100)
    uid = s["upload_id"]
    parts = split(content, 100)
    for i in (3, 0, 2):  # upload out of order; 1 stays missing
        assert put_chunk(client, uid, i, parts[i]).status_code == 200
    st = status(client, uid).json()
    assert st["missing_chunks"] == [1]
    assert st["chunks_received"] == 3


def os_bytes(n: int) -> bytes:
    block = hashlib.sha256(b"seed").digest()
    return (block * (n // 32 + 1))[:n]
