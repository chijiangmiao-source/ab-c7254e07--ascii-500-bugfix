"""Session creation and validation rules."""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone

from tests.helpers import make_upload


def test_create_computes_ceil_total_chunks(client):
    # 5 chunks: four full ones + a 100-byte tail
    content = bytes(range(256)) * 4  # 1024 bytes
    r = make_upload(client, content, chunk_size=256)
    assert r.status_code == 201
    body = r.json()
    assert body["total_chunks"] == 4
    assert body["status"] == "open"
    assert body["chunks_received"] == 0
    assert body["missing_chunks"] == [0, 1, 2, 3]


def test_create_ceil_division_with_remainder(client):
    r = make_upload(client, b"a" * 100, chunk_size=30)
    assert r.status_code == 201
    assert r.json()["total_chunks"] == 4  # ceil(100/30)


def test_create_rejects_bad_sha256_format(client):
    r = client.post(
        "/api/v1/uploads",
        json={
            "filename": "x.bin",
            "file_size": 10,
            "chunk_size": 4,
            "file_sha256": "ABC",
            "expires_at": "2030-01-01T00:00:00+00:00",
        },
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "validation_error"


def test_create_rejects_past_expiry(client):
    r = make_upload(client, b"abc", chunk_size=1, expires_in=-10)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_upload"


def test_create_rejects_naive_expiry(client):
    r = client.post(
        "/api/v1/uploads",
        json={
            "filename": "x.bin",
            "file_size": 0,
            "chunk_size": 1,
            "file_sha256": hashlib.sha256(b"").hexdigest(),
            "expires_at": "2030-01-01T00:00:00",
        },
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "validation_error"


def test_create_rejects_path_traversal_filename(client):
    r = make_upload(client, b"abc", chunk_size=1, filename="../evil.bin")
    assert r.status_code == 422


def test_zero_byte_file_publishes_immediately_when_digest_matches(client):
    r = make_upload(client, b"", chunk_size=1, filename="empty.bin")
    assert r.status_code == 201
    body = r.json()
    assert body["total_chunks"] == 0
    assert body["status"] == "complete"
    assert body["missing_chunks"] == []
    st = status_resp = client.get(f"/api/v1/uploads/{body['upload_id']}").json()
    assert st["status"] == "complete"
    assert st["assembled_path"].endswith("empty.bin")


def test_zero_byte_file_wrong_digest_rejected(client):
    r = make_upload(client, b"", chunk_size=1, file_sha256="00" * 32)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_upload"


def test_chunk_size_must_be_positive(client):
    r = client.post(
        "/api/v1/uploads",
        json={
            "filename": "x.bin",
            "file_size": 10,
            "chunk_size": 0,
            "file_sha256": "00" * 32,
            "expires_at": "2030-01-01T00:00:00+00:00",
        },
    )
    assert r.status_code == 422
