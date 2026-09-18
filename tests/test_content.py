"""Byte-range previews: GET /api/v1/uploads/{upload_id}/content.

Acceptance coverage:
* single ranges (206 + Content-Range + whole-file ETag), incl. cross-chunk;
* open-ended and suffix forms; overlapping ranges normalized/merged;
* multipart/byteranges with a stable boundary and per-part headers;
* open sessions gated on confirmed row + payload present + digest match,
  otherwise 409 range_unavailable with ascending chunk indexes and no scan
  byte in the error body;
* malformed / unsatisfiable ranges -> 416 with Content-Range: bytes */size;
* complete session without Range streams the published file as 200;
* the SAME interval returns identical bytes and ETag before and after
  publication; expiry / upload-concurrency behavior of other endpoints is
  untouched.
"""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timedelta, timezone

from tests.helpers import make_upload, put_chunk, split, status  # noqa: F401


def _content(n: int, seed: bytes) -> bytes:
    out = bytearray()
    while len(out) < n:
        out.extend(hashlib.sha256(seed + len(out).to_bytes(4, "big")).digest())
    return bytes(out[:n])


def _open_session(client, content, chunk_size, **kw):
    r = make_upload(client, content, chunk_size=chunk_size, **kw)
    assert r.status_code == 201, r.text
    return r.json()


def _confirm(client, uid, parts, indexes):
    for i in indexes:
        r = put_chunk(client, uid, i, parts[i])
        assert r.status_code == 200, (i, r.text)


def _get(client, uid, rng):
    headers = {"Range": rng} if rng is not None else {}
    return client.get(f"/api/v1/uploads/{uid}/content", headers=headers)


def _parse_multipart(body: bytes, content_type: str) -> list[tuple[str, bytes]]:
    m = re.search(r"boundary=([^;]+)$", content_type)
    assert m, content_type
    boundary = m.group(1).encode()
    parts = body.split(b"--" + boundary)
    result = []
    for part in parts:
        part = part.strip(b"\r\n")
        if not part or part == b"--":
            continue
        head, _, payload = part.partition(b"\r\n\r\n")
        cr = re.search(rb"Content-Range: bytes (\d+-\d+/\d+)", head)
        assert cr, head
        result.append((cr.group(1).decode(), payload))
    return result


# ---------------------------------------------------------------- single range


def test_single_range_within_chunk_206(client):
    content = _content(250, b"a")
    s = _open_session(client, content, 100)
    parts = split(content, 100)
    _confirm(client, s["upload_id"], parts, [0])
    r = _get(client, s["upload_id"], "bytes=10-19")
    assert r.status_code == 206
    assert r.content == content[10:20]
    assert r.headers["content-range"] == "bytes 10-19/250"
    assert r.headers["content-length"] == "10"
    assert r.headers["etag"] == f'"{s["file_sha256"]}"'
    assert r.headers["accept-ranges"] == "bytes"


def test_single_cross_chunk_range_streams_without_temp_package(client, data_env):
    content = _content(450, b"cross")  # chunks 0..3 x100 + chunk4 x50
    s = _open_session(client, content, 100)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, [0, 1, 2])

    r = _get(client, uid, "bytes=90-220")  # crosses 0/1 and 1/2
    assert r.status_code == 206, r.text
    assert r.content == content[90:221]
    assert r.headers["content-range"] == "bytes 90-220/450"
    # Cross-chunk reads must not materialize an assembled/temp whole file.
    assert list((data_env / "tmp").iterdir()) == []


def test_open_ended_range_on_open_session_all_rows_confirmed(client):
    # An open-ended range reaches EOF, so for an *open* session it can only be
    # served when every chunk is confirmed -- here the declared whole-file
    # digest is wrong, so publication fails and the session stays open with
    # all chunk rows present.
    content = _content(250, b"open")
    s = _open_session(client, content, 100, file_sha256="cd" * 32)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, [0, 1])
    last = put_chunk(client, uid, 2, parts[2])
    assert last.status_code == 422  # integrity failure keeps the session open
    assert status(client, uid).json()["status"] == "open"
    r = _get(client, uid, "bytes=150-")
    assert r.status_code == 206
    assert r.content == content[150:]
    assert r.headers["content-range"] == "bytes 150-249/250"


def test_suffix_range(client):
    content = _content(250, b"suffix")
    s = _open_session(client, content, 100)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, [1, 2])
    r = _get(client, uid, "bytes=-70")  # bytes 180..249, chunks 1 (tail) + 2
    assert r.status_code == 206
    assert r.content == content[-70:]
    assert r.headers["content-range"] == "bytes 180-249/250"


def test_suffix_longer_than_file_covers_start(client):
    content = _content(250, b"big-suffix")
    s = _open_session(client, content, 100)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, range(3))
    r = _get(client, uid, "bytes=-100000")
    assert r.status_code == 206
    assert r.content == content
    assert r.headers["content-range"] == "bytes 0-249/250"


def test_overlapping_ranges_merged_into_single_part(client):
    content = _content(250, b"merge")
    s = _open_session(client, content, 100)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, [0])
    r = _get(client, uid, "bytes=0-50,40-90,20-30")
    assert r.status_code == 206
    assert r.headers["content-type"] == "application/octet-stream"
    assert r.headers["content-range"] == "bytes 0-90/250"
    assert r.content == content[0:91]


def test_end_beyond_file_is_clamped(client):
    content = _content(250, b"clamp")
    s = _open_session(client, content, 100)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, [2])
    r = _get(client, uid, "bytes=240-9999")
    assert r.status_code == 206
    assert r.content == content[240:]
    assert r.headers["content-range"] == "bytes 240-249/250"


# ----------------------------------------------------------------- availability


def test_range_touching_missing_chunk_is_409_and_leaks_no_bytes(client):
    content = _content(350, b"miss")
    s = _open_session(client, content, 100)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, [0, 2])
    r = _get(client, uid, "bytes=90-120")  # crosses into missing chunk 1
    assert r.status_code == 409
    err = r.json()["error"]
    assert err["code"] == "range_unavailable"
    assert err["details"]["missing_chunks"] == [1]
    assert err["details"]["corrupt_chunks"] == []
    assert err["details"]["unavailable_chunks"] == [1]
    # Error body is JSON metadata only: none of the confirmed scan bytes leak.
    assert parts[0] not in r.content and parts[2] not in r.content
    assert r.headers["content-type"] == "application/json"


def test_multipart_range_with_gap_is_409_with_ascending_indexes(client):
    content = _content(550, b"gap")
    s = _open_session(client, content, 100)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, [0, 2, 4])  # 1, 3, 5 missing
    r = _get(client, uid, "bytes=50-160,250-360,450-549")
    assert r.status_code == 409
    details = r.json()["error"]["details"]
    assert details["missing_chunks"] == [1, 3, 5]
    assert details["requested_ranges"] == [[50, 160], [250, 360], [450, 549]]


def test_chunk_present_in_db_but_missing_on_disk_is_unavailable(client, data_env):
    content = _content(250, b"disk-miss")
    s = _open_session(client, content, 100)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, [0, 1])
    # Simulate lost payload despite the confirming row.
    from app import storage

    storage.chunk_path(uid, 1).unlink()
    r = _get(client, uid, "bytes=150-160")
    assert r.status_code == 409
    assert r.json()["error"]["details"]["missing_chunks"] == [1]
    # A range wholly inside the intact chunk still succeeds.
    ok = _get(client, uid, "bytes=0-9")
    assert ok.status_code == 206 and ok.content == content[0:10]


def test_corrupt_chunk_payload_is_409_with_corrupt_indexes(client, data_env):
    content = _content(350, b"rot")
    s = _open_session(client, content, 100)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, [0, 1, 2])
    from app import storage

    p1 = storage.chunk_path(uid, 1)
    tampered = bytearray(p1.read_bytes())
    tampered[0] ^= 0xFF
    p1.write_bytes(bytes(tampered))

    r = _get(client, uid, "bytes=90-120")
    assert r.status_code == 409
    err = r.json()["error"]
    assert err["code"] == "range_unavailable"
    assert err["details"]["corrupt_chunks"] == [1]
    assert err["details"]["missing_chunks"] == []
    assert parts[0] not in r.content

    # A suffix starting inside the corrupt chunk fails too; range inside a
    # healthy neighbor still succeeds.
    assert _get(client, uid, "bytes=-60").status_code == 409
    good = _get(client, uid, "bytes=0-9")
    assert good.status_code == 206 and good.content == content[0:10]


def test_409_does_not_modify_session_state(client):
    content = _content(350, b"state")
    s = _open_session(client, content, 100)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, [0])
    assert _get(client, uid, "bytes=90-120").status_code == 409
    st = status(client, uid).json()
    assert st["status"] == "open"
    assert st["chunks_received"] == 1
    assert st["missing_chunks"] == [1, 2, 3]


# -------------------------------------------------------------------- multipart


def test_multipart_ranges_have_stable_boundary_and_part_headers(client):
    content = _content(250, b"mp")
    s = _open_session(client, content, 100)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, range(3))
    rng = "bytes=0-9,50-59,240-249"
    r1 = _get(client, uid, rng)
    r2 = _get(client, uid, rng)
    assert r1.status_code == r2.status_code == 206
    ct = r1.headers["content-type"]
    assert ct.startswith("multipart/byteranges; boundary=")
    # Deterministic ("stable") boundary for identical requests.
    assert ct == r2.headers["content-type"]
    assert r1.content == r2.content
    assert r1.headers["etag"] == r2.headers["etag"]

    parsed = _parse_multipart(r1.content, ct)
    assert [cr for cr, _ in parsed] == ["0-9/250", "50-59/250", "240-249/250"]
    assert [p for _, p in parsed] == [content[0:10], content[50:60], content[240:250]]


def test_multipart_cross_chunk_parts(client):
    content = _content(450, b"mpcross")
    s = _open_session(client, content, 100)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, range(4))
    r = _get(client, uid, "bytes=90-120,290-320")
    assert r.status_code == 206
    parsed = _parse_multipart(r.content, r.headers["content-type"])
    assert [p for _, p in parsed] == [content[90:121], content[290:321]]


# --------------------------------------------------------------- 416 semantics


def test_start_beyond_eof_is_416_with_content_range(client):
    content = _content(250, b"416a")
    s = _open_session(client, content, 100)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, range(3))
    r = _get(client, uid, "bytes=250-")
    assert r.status_code == 416
    assert r.headers["content-range"] == "bytes */250"
    assert r.json()["error"]["code"] == "range_not_satisfiable"
    assert r.json()["error"]["details"]["size"] == 250


def test_malformed_ranges_are_416(client):
    content = _content(250, b"416b")
    s = _open_session(client, content, 100)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, range(3))
    for bad in ("bytes=100-50", "bytes=-0", "bytes=abc", "bytes=", "items=0-10", "bytes=10-20,"):
        r = _get(client, uid, bad)
        assert r.status_code == 416, (bad, r.status_code, r.text)
        assert r.headers["content-range"] == "bytes */250", bad
        assert r.json()["error"]["code"] == "range_not_satisfiable", bad


def test_416_takes_priority_over_missing_chunks(client):
    content = _content(250, b"prio")
    s = _open_session(client, content, 100)  # nothing confirmed
    uid = s["upload_id"]
    r = _get(client, uid, "bytes=1000-2000")
    assert r.status_code == 416
    assert r.headers["content-range"] == "bytes */250"


def test_partially_satisfiable_multipart_drops_unservable_spec(client):
    content = _content(250, b"partial")
    s = _open_session(client, content, 100)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, range(3))
    # Two satisfiable specs plus one past EOF: the bad spec is dropped and
    # the surviving two are still returned as a multipart response.
    r = _get(client, uid, "bytes=0-9,50-59,1000-2000")
    assert r.status_code == 206
    parsed = _parse_multipart(r.content, r.headers["content-type"])
    assert [cr for cr, _ in parsed] == ["0-9/250", "50-59/250"]
    assert [p for _, p in parsed] == [content[0:10], content[50:60]]

    # A single surviving spec collapses to a normal single-part 206.
    single = _get(client, uid, "bytes=0-9,1000-2000")
    assert single.status_code == 206
    assert single.headers["content-type"] == "application/octet-stream"
    assert single.headers["content-range"] == "bytes 0-9/250"
    assert single.content == content[0:10]


def test_zero_length_file_any_range_is_416(client):
    s = _open_session(client, b"", 1, filename="empty2.bin")
    for rng in ("bytes=0-", "bytes=-1"):
        r = _get(client, s["upload_id"], rng)
        assert r.status_code == 416
        assert r.headers["content-range"] == "bytes */0"


# ------------------------------------------------------------- complete session


def test_complete_session_without_range_streams_file_200(client):
    content = _content(250, b"full")
    s = _open_session(client, content, 100)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, range(3))
    assert status(client, uid).json()["status"] == "complete"
    r = _get(client, uid, None)
    assert r.status_code == 200
    assert r.content == content
    assert r.headers["content-length"] == "250"
    assert r.headers["etag"] == f'"{s["file_sha256"]}"'
    assert "content-range" not in r.headers


def test_complete_session_still_supports_ranges(client):
    content = _content(250, b"full-range")
    s = _open_session(client, content, 100)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, range(3))
    r = _get(client, uid, "bytes=90-120")
    assert r.status_code == 206
    assert r.content == content[90:121]
    assert r.headers["content-range"] == "bytes 90-120/250"


def test_open_session_without_range_is_gated_like_full_range(client):
    content = _content(250, b"norange-open")
    s = _open_session(client, content, 100)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, [0])
    r = _get(client, uid, None)
    assert r.status_code == 409
    assert r.json()["error"]["details"]["missing_chunks"] == [1, 2]
    assert r.headers["content-type"] == "application/json"
    assert parts[0] not in r.content


def test_open_session_with_all_rows_no_range_streams_200(client):
    # Wrong declared digest keeps the session open even with every chunk row.
    content = _content(250, b"norange-all")
    s = _open_session(client, content, 100, file_sha256="ef" * 32)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, [0, 1])
    assert put_chunk(client, uid, 2, parts[2]).status_code == 422
    r = _get(client, uid, None)
    assert r.status_code == 200
    assert r.content == content
    assert r.headers["etag"] == f'"{s["file_sha256"]}"'


def test_zero_byte_file_content_is_empty_200(client):
    s = _open_session(client, b"", 1, filename="empty.bin")
    assert s["status"] == "complete"
    r = _get(client, s["upload_id"], None)
    assert r.status_code == 200
    assert r.content == b""
    assert r.headers["content-length"] == "0"


# -------------------------------------------------- before/after publication parity


def test_same_interval_identical_bytes_and_etag_before_and_after_complete(client):
    content = _content(450, b"parity")
    s = _open_session(client, content, 100)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, [0, 1, 2])

    before_single = _get(client, uid, "bytes=90-120")
    before_multi = _get(client, uid, "bytes=90-120,195-205")

    _confirm(client, uid, parts, [3, 4])
    assert status(client, uid).json()["status"] == "complete"

    after_single = _get(client, uid, "bytes=90-120")
    after_multi = _get(client, uid, "bytes=90-120,195-205")
    assert after_single.status_code == 206
    assert after_single.content == before_single.content == content[90:121]
    assert after_single.headers["etag"] == before_single.headers["etag"]
    assert after_single.headers["content-range"] == before_single.headers["content-range"]
    # Multipart boundary is derived from (session, intervals), not from status.
    assert after_multi.content == before_multi.content
    assert after_multi.headers["content-type"] == before_multi.headers["content-type"]
    assert after_multi.headers["etag"] == before_multi.headers["etag"]

    # Published bytes are the same file the inspector already previewed.
    whole = _get(client, uid, None)
    assert whole.status_code == 200 and whole.content == content
    assert whole.headers["etag"] == before_single.headers["etag"]


def test_preview_survives_restart_then_completes_with_identical_bytes(
    client, restart_client
):
    content = _content(350, b"restart-parity")
    s = _open_session(client, content, 100)
    uid = s["upload_id"]
    parts = split(content, 100)
    _confirm(client, uid, parts, [0, 1])
    rng = "bytes=90-120"
    before = _get(client, uid, rng)

    seen = _get(restart_client, uid, rng)
    assert seen.status_code == 206
    assert seen.content == before.content
    assert seen.headers["etag"] == before.headers["etag"]

    _confirm(restart_client, uid, parts, [2, 3])
    after = _get(restart_client, uid, rng)
    assert after.content == before.content == content[90:121]
    assert after.headers["etag"] == before.headers["etag"]
    assert status(restart_client, uid).json()["status"] == "complete"


# ----------------------------------------------------- unknown session / misc


def test_unknown_session_content_404(client):
    r = client.get("/api/v1/uploads/does-not-exist/content")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "session_not_found"


def test_expired_open_session_still_serves_confirmed_ranges_but_not_gaps(client):
    s = _open_session(client, _content(250, b"exp"), 100, expires_in=1)
    uid = s["upload_id"]
    parts = split(_content(250, b"exp"), 100)
    _confirm(client, uid, parts, [0])
    import time

    time.sleep(1.2)
    assert status(client, uid).json()["status"] == "expired"
    # Reads of confirmed bytes remain available (GET never mutates progress).
    ok = _get(client, uid, "bytes=0-9")
    assert ok.status_code == 206 and ok.content == parts[0][0:10]
    # Gated exactly like an open session where chunks are unconfirmed.
    blocked = _get(client, uid, "bytes=90-120")
    assert blocked.status_code == 409
    assert blocked.json()["error"]["details"]["missing_chunks"] == [1]
    # Existing expiry behavior for writes is unchanged.
    wr = put_chunk(client, uid, 1, parts[1])
    assert wr.status_code == 410


def test_content_request_does_not_block_or_change_concurrent_upload(data_env):
    import threading

    from fastapi.testclient import TestClient

    from app.main import create_app

    content = _content(400, b"conc")
    with TestClient(create_app()) as writer, TestClient(create_app()) as reader:
        s = writer.post(
            "/api/v1/uploads",
            json={
                "filename": "scan.bin",
                "file_size": 400,
                "chunk_size": 100,
                "file_sha256": hashlib.sha256(content).hexdigest(),
                "expires_at": (
                    datetime.now(timezone.utc) + timedelta(hours=1)
                ).isoformat(),
            },
        ).json()
        uid = s["upload_id"]
        parts = split(content, 100)
        assert put_chunk(writer, uid, 0, parts[0]).status_code == 200

        seen: list[int] = []

        def preview():
            seen.append(
                reader.get(
                    f"/api/v1/uploads/{uid}/content",
                    headers={"Range": "bytes=0-9"},
                ).status_code
            )

        t = threading.Thread(target=preview)
        t.start()
        r = put_chunk(writer, uid, 1, parts[1])
        t.join()
        assert r.status_code == 200
        assert seen == [206]
        st = writer.get(f"/api/v1/uploads/{uid}").json()
        assert st["chunks_received"] == 2
        assert st["missing_chunks"] == [2, 3]
