#!/usr/bin/env python3
"""One-shot end-to-end acceptance check for the resumable upload service.

Runs entirely over HTTP (default target ``$API_BASE_URL``) and additionally
starts a *fresh* API process inside this container pointed at the same
``/data`` volume, proving that a restarted instance resumes from confirmed
chunk positions. Exits non-zero on the first failed assertion.
"""
from __future__ import annotations

import hashlib
import os
import socket
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

API_BASE = os.environ.get("API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
CHUNK_SIZE = 256 * 1024

_passed = 0
_failed: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        global _passed
        _passed += 1
        print(f"  PASS  {name}")
    else:
        _failed.append(f"{name}: {detail}")
        print(f"  FAIL  {name} -- {detail}")


def wait_ready(base: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        try:
            r = httpx.get(f"{base}/api/v1/health", timeout=2)
            if r.status_code == 204:
                return
        except httpx.HTTPError as exc:  # container still starting
            last = str(exc)
        time.sleep(0.5)
    raise RuntimeError(f"API at {base} not ready: {last}")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_restarted_instance() -> tuple[subprocess.Popen, str]:
    """Fresh OS process sharing /data, standing in for an API restart."""
    port = free_port()
    env = dict(os.environ, DATA_DIR=str(DATA_DIR))
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn", "app.main:app",
            "--host", "127.0.0.1", "--port", str(port),
        ],
        cwd=os.environ.get("APP_CWD", os.getcwd()),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    base = f"http://127.0.0.1:{port}"
    try:
        wait_ready(base)
    except Exception:
        proc.terminate()
        out = proc.stdout.read().decode() if proc.stdout else ""
        raise RuntimeError(f"restarted instance failed to start:\n{out}")
    return proc, base


def payload(size: int, seed: int) -> bytes:
    # Deterministic, incompressible scan-file-shaped bytes (the service
    # itself never returns fixed/canned payloads).
    import random

    rng = random.Random(seed)
    return rng.randbytes(size)


def create(client: httpx.Client, content: bytes, expires_seconds: int = 3600,
           filename: str = "axle_scan.bin", declare_digest: str | None = None) -> dict:
    expiry = datetime.now(timezone.utc) + timedelta(seconds=expires_seconds)
    r = client.post(
        "/api/v1/uploads",
        json={
            "filename": filename,
            "file_size": len(content),
            "chunk_size": CHUNK_SIZE,
            "file_sha256": declare_digest or hashlib.sha256(content).hexdigest(),
            "expires_at": expiry.isoformat(),
        },
    )
    check(f"create[{filename}] -> 201", r.status_code == 201, r.text)
    return r.json()


def put(client: httpx.Client, upload_id: str, index: int, data: bytes,
        digest: str | None = None) -> httpx.Response:
    return client.put(
        f"/api/v1/uploads/{upload_id}/chunks/{index}",
        content=data,
        headers={"X-Chunk-SHA256": digest or hashlib.sha256(data).hexdigest()},
    )


def split(data: bytes, chunk_size: int) -> list[bytes]:
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]


def main() -> int:
    print(f"== Verifying resumable upload API at {API_BASE} ==")
    wait_ready(API_BASE)
    client = httpx.Client(base_url=API_BASE, timeout=10)

    # ---- 1. happy path with interruption, ordering, restart, resume --------
    print("[1] create -> partial upload -> restart -> resume -> atomic publish")
    content = payload(CHUNK_SIZE * 4 + 12345, seed=20260915)
    chunks = [content[i * CHUNK_SIZE:(i + 1) * CHUNK_SIZE]
              for i in range(-(-len(content) // CHUNK_SIZE))]
    sess = create(client, content)
    uid = sess["upload_id"]
    total = len(chunks)
    check("total_chunks == ceil(size/chunk_size)",
          sess["total_chunks"] == total, f"{sess['total_chunks']} != {total}")

    # Upload out of order and stop mid-stream (simulated broken connection).
    r = put(client, uid, 0, chunks[0]); check("chunk 0 accepted", r.status_code == 200, r.text)
    r = put(client, uid, 2, chunks[2]); check("chunk 2 accepted out of order", r.status_code == 200, r.text)

    status = client.get(f"/api/v1/uploads/{uid}").json()
    check("missing chunks ascending [1,3,4]",
          status["missing_chunks"] == [1, 3, 4], str(status["missing_chunks"]))
    check("chunks_received == 2", status["chunks_received"] == 2, str(status))

    # Idempotent retransmit of identical content.
    r = put(client, uid, 0, chunks[0])
    check("same chunk retransmit -> 200 idempotent",
          r.status_code == 200 and r.json().get("idempotent") is True, r.text)

    # Different content for a confirmed index -> 409, stored bytes untouched.
    # XOR with 0x01 preserves the exact byte length (non-last chunks must
    # equal chunk_size) while guaranteeing different content.
    evil = bytes([chunks[2][0] ^ 0x01]) + chunks[2][1:]
    r = put(client, uid, 2, evil)
    check("different content for same index -> 409", r.status_code == 409, r.text)
    check("409 body structured", r.json()["error"]["code"] == "chunk_conflict", r.text)

    # Wrong claimed digest -> 422, not recorded.
    r = put(client, uid, 1, chunks[1], digest="00" * 32)
    check("bad chunk digest -> 422", r.status_code == 422, r.text)
    check("error code checksum_mismatch",
          r.json()["error"]["code"] == "checksum_mismatch", r.text)

    # Non-last chunk with wrong (short) length -> 422.
    r = put(client, uid, 3, chunks[3][:-1])
    check("short non-last chunk -> 422", r.status_code == 422, r.text)

    # Out-of-range index -> 416.
    r = put(client, uid, total, b"x")
    check("index == total_chunks -> 416", r.status_code == 416, r.text)

    status = client.get(f"/api/v1/uploads/{uid}").json()
    check("bitmap unpolluted after rejections",
          status["missing_chunks"] == [1, 3, 4] and status["chunks_received"] == 2,
          str(status["missing_chunks"]))

    # --- restart: a brand-new process reads the same persisted /data --------
    proc, restarted_base = start_restarted_instance()
    try:
        r2 = httpx.get(f"{restarted_base}/api/v1/uploads/{uid}", timeout=10)
        check("restarted instance sees session + bitmap",
              r2.status_code == 200 and r2.json()["missing_chunks"] == [1, 3, 4],
              r2.text)

        rclient = httpx.Client(base_url=restarted_base, timeout=10)
        # Retransmit chunk 0 through the new process: idempotent, proving the
        # confirmed position was not mistaken for missing.
        r = put(rclient, uid, 0, chunks[0])
        check("retransmit after restart -> 200 idempotent",
              r.status_code == 200 and r.json()["idempotent"] is True, r.text)

        for idx in (1, 3, 4):
            r = put(rclient, uid, idx, chunks[idx])
            check(f"resume chunk {idx} through restarted process",
                  r.status_code == 200, r.text)

        status = rclient.get(f"/api/v1/uploads/{uid}").json()
        check("all chunks received", status["chunks_received"] == total, str(status))
        check("missing list empty", status["missing_chunks"] == [], str(status))
        check("status complete", status["status"] == "complete", str(status))
    finally:
        proc.terminate()
        proc.wait(timeout=10)

    # Published file exists on disk (shared volume), byte-identical.
    status = client.get(f"/api/v1/uploads/{uid}").json()
    published = Path(status["assembled_path"])
    check("assembled file present under files/",
          published.is_file() and str(published).startswith(str(DATA_DIR / "files")),
          str(published))
    disk = published.read_bytes()
    check("published bytes identical to source", disk == content,
          f"len disk={len(disk)} src={len(content)}")
    check("published SHA-256 matches declaration",
          hashlib.sha256(disk).hexdigest() == hashlib.sha256(content).hexdigest(), "")

    # ---- 2. expiry: past-due sessions reject chunks, progress untouched -----
    print("[2] expiry rejects new chunks without polluting progress")
    small = payload(CHUNK_SIZE * 2 + 7, seed=7)
    schunks = split(small, CHUNK_SIZE)
    sess2 = create(client, small, expires_seconds=2, filename="expires.bin")
    uid2 = sess2["upload_id"]
    r = put(client, uid2, 0, schunks[0]); check("expiring session chunk 0 -> 200", r.status_code == 200, r.text)
    time.sleep(2.5)
    r = put(client, uid2, 1, schunks[1])
    check("chunk after expiry -> 410", r.status_code == 410, r.text)
    check("error code session_expired", r.json()["error"]["code"] == "session_expired", r.text)
    status = client.get(f"/api/v1/uploads/{uid2}").json()
    check("expired session missing list preserved",
          status["expired"] is True and status["missing_chunks"] == [1, 2], str(status))

    # ---- 3. whole-file digest mismatch: explicit integrity failure ---------
    print("[3] wrong whole-file SHA-256 -> integrity error, progress retained")
    bad = payload(CHUNK_SIZE + 3, seed=99)  # exactly two chunks, 3-byte tail
    bchunks = split(bad, CHUNK_SIZE)
    assert len(bchunks) == 2 and len(bchunks[1]) == 3
    sess3 = create(client, bad, filename="corrupt.bin", declare_digest="11" * 32)
    uid3 = sess3["upload_id"]
    r = put(client, uid3, 0, bchunks[0]); check("corrupt session chunk 0 -> 200", r.status_code == 200, r.text)
    r = put(client, uid3, 1, bchunks[1])
    check("final chunk triggers 422 integrity_error",
          r.status_code == 422 and r.json()["error"]["code"] == "integrity_error", r.text)
    # Idempotent replay still succeeds and never overwrites progress.
    r = put(client, uid3, 1, bchunks[1])
    check("replay after integrity failure -> 200 idempotent",
          r.status_code == 200 and r.json()["idempotent"] is True, r.text)
    r = client.post(f"/api/v1/uploads/{uid3}/finalize")
    check("finalize re-reports integrity error",
          r.status_code == 422 and r.json()["error"]["code"] == "integrity_error", r.text)
    status = client.get(f"/api/v1/uploads/{uid3}").json()
    check("failed session not published",
          status["status"] == "open" and status["assembled_path"] is None, str(status))

    # ---- 4. zero-byte file publishes immediately ---------------------------
    print("[4] zero-byte file")
    sess4 = create(client, b"", filename="empty.bin")
    check("empty file session complete", sess4["status"] == "complete", str(sess4))
    empty_path = Path(sess4["assembled_path"])
    check("empty file published as 0 bytes", empty_path.is_file() and empty_path.stat().st_size == 0,
          str(empty_path))

    # ---- 5. generic structured errors --------------------------------------
    print("[5] error envelope and input validation")
    r = client.get("/api/v1/uploads/does-not-exist")
    check("unknown session -> 404 structured",
          r.status_code == 404 and r.json()["error"]["code"] == "session_not_found", r.text)
    r = client.post("/api/v1/uploads", json={"filename": "x", "file_size": -1,
                                             "chunk_size": 0,
                                             "file_sha256": "zz",
                                             "expires_at": "not-a-date"})
    check("invalid create body -> 422 validation_error",
          r.status_code == 422 and r.json()["error"]["code"] == "validation_error", r.text)
    # Raw request with NO X-Chunk-SHA256 header at all (helper always adds one).
    r = client.put(f"/api/v1/uploads/{uid}/chunks/0", content=chunks[0])
    check("chunk without digest header -> 422 invalid_upload",
          r.status_code == 422 and r.json()["error"]["code"] == "invalid_upload", r.text)

    # ---- 6. byte-range preview before and after publication ---------------
    print("[6] range previews: gating, multipart, 416, parity after complete")
    preview = payload(CHUNK_SIZE * 3 + 543, seed=424242)
    pchunks = split(preview, CHUNK_SIZE)  # 4 chunks, last 543 bytes
    psess = create(client, preview, filename="preview.bin")
    puid = psess["upload_id"]
    etag = f'"{hashlib.sha256(preview).hexdigest()}"'

    r = put(client, puid, 0, pchunks[0])
    check("preview session chunk 0 -> 200", r.status_code == 200, r.text)
    r = put(client, puid, 2, pchunks[2])
    check("preview session chunk 2 -> 200", r.status_code == 200, r.text)

    def get_range(rng):
        return client.get(f"/api/v1/uploads/{puid}/content",
                          headers={"Range": rng} if rng else {})

    # Single range wholly inside a confirmed chunk.
    r = get_range("bytes=10-73")
    check("single range -> 206 + Content-Range + ETag",
          r.status_code == 206
          and r.content == preview[10:74]
          and r.headers["content-range"] == f"bytes 10-73/{len(preview)}"
          and r.headers["etag"] == etag, r.text[:200])

    # Range crossing into unconfirmed chunk 1 -> 409, ascending indexes, no leak.
    cross = f"bytes={CHUNK_SIZE - 50}-{CHUNK_SIZE + 50}"
    before_cross = get_range(cross)
    r = before_cross
    leak_ok = pchunks[0] not in r.content and pchunks[2] not in r.content
    check("cross-chunk gap -> 409 range_unavailable, no scan bytes",
          r.status_code == 409
          and r.json()["error"]["code"] == "range_unavailable"
          and r.json()["error"]["details"]["missing_chunks"] == [1]
          and leak_ok, r.text[:200])

    # Multipart request where one window needs missing chunk 1.
    r = get_range(f"bytes=0-99,{CHUNK_SIZE}-{CHUNK_SIZE + 99}")
    check("multipart with a gap -> 409",
          r.status_code == 409 and r.json()["error"]["details"]["missing_chunks"] == [1],
          r.text[:200])

    # Malformed / unsatisfiable ranges -> 416 with Content-Range: bytes */size.
    r = get_range("bytes=999999999-")
    check("start past EOF -> 416 bytes */size",
          r.status_code == 416
          and r.headers["content-range"] == f"bytes */{len(preview)}"
          and r.json()["error"]["code"] == "range_not_satisfiable", r.text[:200])
    r = get_range("bytes=nonsense")
    check("malformed range -> 416 bytes */size",
          r.status_code == 416
          and r.headers["content-range"] == f"bytes */{len(preview)}", r.text[:200])

    # Overlapping specs are normalized/merged into one part.
    r = get_range("bytes=0-99,50-149")
    check("overlapping ranges merged -> single 206",
          r.status_code == 206
          and r.headers["content-range"] == f"bytes 0-149/{len(preview)}"
          and r.content == preview[0:150], r.text[:200])

    # Finish the upload; the previously blocked range must now return the same
    # bytes and ETag from the same URL.
    r = put(client, puid, 1, pchunks[1]); check("chunk 1 -> 200", r.status_code == 200, r.text)
    r = put(client, puid, 3, pchunks[3]); check("chunk 3 -> 200 complete", r.status_code == 200, r.text)
    check("preview session complete",
          client.get(f"/api/v1/uploads/{puid}").json()["status"] == "complete", "")

    after_cross = get_range(cross)
    lo, hi = CHUNK_SIZE - 50, CHUNK_SIZE + 50
    check("same range after publish: identical bytes + ETag",
          after_cross.status_code == 206
          and after_cross.content == preview[lo:hi + 1]
          and after_cross.headers["etag"] == etag,
          f"status={after_cross.status_code}")

    # Multipart cross-chunk windows: stable boundary across identical requests.
    mr = f"bytes=10-73,{CHUNK_SIZE - 10}-{CHUNK_SIZE + 10},{2 * CHUNK_SIZE}-{2 * CHUNK_SIZE + 4}"
    m1 = get_range(mr)
    m2 = get_range(mr)
    parts_ok = (preview[10:74] in m1.content
                and preview[CHUNK_SIZE - 10:CHUNK_SIZE + 11] in m1.content
                and preview[2 * CHUNK_SIZE:2 * CHUNK_SIZE + 5] in m1.content)
    check("multipart/byteranges cross-chunk with stable boundary",
          m1.status_code == 206
          and m1.headers["content-type"].startswith("multipart/byteranges; boundary=")
          and m1.headers["content-type"] == m2.headers["content-type"]
          and m1.content == m2.content and parts_ok, m1.text[:200])

    # Suffix and open-ended forms.
    r = get_range("bytes=-543")
    check("suffix range -> last 543 bytes",
          r.status_code == 206 and r.content == preview[-543:], r.text[:120])
    r = get_range(f"bytes={len(preview) - 10}-")
    check("open-ended range -> final 10 bytes",
          r.status_code == 206 and r.content == preview[-10:]
          and r.headers["content-range"] == f"bytes {len(preview) - 10}-{len(preview) - 1}/{len(preview)}",
          r.text[:120])

    # No Range on a complete session -> 200 streamed published file.
    r = get_range(None)
    check("complete session GET without Range -> 200 full file",
          r.status_code == 200 and r.content == preview
          and r.headers["etag"] == etag and "content-range" not in r.headers, "")

    # Corrupt payload on disk -> 409 with corrupt index, bytes withheld.
    corrupt = payload(CHUNK_SIZE + 10, seed=77)
    cchunks = split(corrupt, CHUNK_SIZE)
    csess = create(client, corrupt, filename="corruptprev.bin",
                   declare_digest="ee" * 32)  # stays open after all rows exist
    cuid = csess["upload_id"]
    put(client, cuid, 0, cchunks[0])
    rr = put(client, cuid, 1, cchunks[1])
    check("wrong-declared session stays open", rr.status_code == 422, rr.text)
    target = DATA_DIR / "chunks" / cuid / "1"
    tampered = bytearray(target.read_bytes())
    tampered[0] ^= 0x01
    target.write_bytes(bytes(tampered))
    r = client.get(f"/api/v1/uploads/{cuid}/content",
                   headers={"Range": f"bytes={CHUNK_SIZE - 5}-{CHUNK_SIZE + 4}"})
    check("on-disk corrupt chunk -> 409 corrupt_chunks, no bytes",
          r.status_code == 409
          and r.json()["error"]["code"] == "range_unavailable"
          and r.json()["error"]["details"]["corrupt_chunks"] == [1]
          and cchunks[1] not in r.content, r.text[:200])
    # Healthy neighbor chunk is still previewable.
    r = client.get(f"/api/v1/uploads/{cuid}/content", headers={"Range": "bytes=0-9"})
    check("healthy chunk still previewable",
          r.status_code == 206 and r.content == corrupt[0:10], r.text[:120])

    print(f"\n== {_passed} passed, {len(_failed)} failed ==")
    if _failed:
        for item in _failed:
            print(f"  FAILED: {item}")
        return 1
    print("ACCEPTANCE OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
