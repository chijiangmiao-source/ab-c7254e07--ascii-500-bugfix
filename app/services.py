"""Business logic: session lifecycle, chunk reception, finalization.

All state transitions are serialized by :func:`app.db.write_tx`; chunk bytes
are fsynced *before* their confirming row is committed, which is what lets a
restart resume exactly from confirmed positions.
"""
from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone

from . import db, storage
from .errors import (
    ChecksumMismatch,
    ChunkConflict,
    ChunkOutOfRange,
    IncompleteUpload,
    IntegrityError,
    InvalidUpload,
    SessionExpired,
    SessionNotFound,
)
from .models import CreateUploadRequest, UploadStatus

_EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _get_session(conn, upload_id: str) -> dict:
    row = conn.execute(
        "SELECT * FROM sessions WHERE upload_id = ?", (upload_id,)
    ).fetchone()
    if row is None:
        raise SessionNotFound(
            f"Upload session '{upload_id}' does not exist.",
            {"upload_id": upload_id},
        )
    return dict(row)


def _received_indexes(conn, upload_id: str) -> list[int]:
    rows = conn.execute(
        "SELECT chunk_index FROM chunks WHERE upload_id = ? ORDER BY chunk_index",
        (upload_id,),
    ).fetchall()
    return [r["chunk_index"] for r in rows]


def _mark_expired_if_due(session: dict) -> dict:
    """Persist the expired flag once expiry passes; never alters chunk rows."""
    if session["status"] == "open" and utcnow() >= datetime.fromisoformat(session["expires_at"]):
        with db.write_tx() as conn:
            conn.execute(
                "UPDATE sessions SET status = 'expired' "
                "WHERE upload_id = ? AND status = 'open'",
                (session["upload_id"],),
            )
        session["status"] = "expired"
    return session


def create_upload(payload: CreateUploadRequest) -> dict:
    if payload.expires_at <= utcnow():
        raise InvalidUpload(
            "expires_at must be in the future.",
            {"expires_at": payload.expires_at.isoformat()},
        )

    total_chunks = -(-payload.file_size // payload.chunk_size)  # ceil division

    # Validate the zero-byte case *before* anything is written so a digest
    # mismatch leaves neither a session row nor an orphan published file.
    if total_chunks == 0 and payload.file_sha256 != _EMPTY_SHA256:
        raise InvalidUpload(
            "file_size is 0 but file_sha256 is not the SHA-256 of an empty file.",
            {"declared": payload.file_sha256, "expected": _EMPTY_SHA256},
        )

    upload_id = uuid.uuid4().hex

    with db.write_tx() as conn:
        conn.execute(
            "INSERT INTO sessions "
            "(upload_id, filename, file_size, chunk_size, total_chunks, "
            " file_sha256, expires_at, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'open')",
            (
                upload_id,
                payload.filename,
                payload.file_size,
                payload.chunk_size,
                total_chunks,
                payload.file_sha256,
                payload.expires_at.isoformat(),
            ),
        )

        if total_chunks == 0:
            # Zero-byte file with the correct digest: publish immediately.
            path = storage.assemble_and_publish(
                upload_id, 0, payload.file_sha256, payload.filename
            )
            conn.execute(
                "UPDATE sessions SET status = 'complete', assembled_path = ?, "
                "completed_at = ? WHERE upload_id = ?",
                (str(path), utcnow().isoformat(), upload_id),
            )

    with db.get_conn() as conn:
        return _get_session(conn, upload_id)


def load_snapshot(upload_id: str) -> dict:
    """Load the single shared view of a session used by a content request.

    The persistence query, range planning, storage reads and response all use
    this snapshot, so a preview can never observe a different bitmap from the
    one it validated against. Expiry is applied (and persisted once) exactly as
    for the status endpoint.
    """
    with db.get_conn() as conn:
        session = _get_session(conn, upload_id)
        rows = conn.execute(
            "SELECT chunk_index, sha256, size FROM chunks "
            "WHERE upload_id = ? ORDER BY chunk_index",
            (upload_id,),
        ).fetchall()

    session = _mark_expired_if_due(session)
    return {"session": session, "chunks": {r["chunk_index"]: dict(r) for r in rows}}


def get_status(upload_id: str) -> UploadStatus:
    snapshot = load_snapshot(upload_id)
    session = snapshot["session"]
    received = sorted(snapshot["chunks"])
    total = session["total_chunks"]
    received_set = set(received)
    missing = [i for i in range(total) if i not in received_set]
    expired = session["status"] == "expired" or utcnow() >= datetime.fromisoformat(
        session["expires_at"]
    )

    return UploadStatus(
        upload_id=session["upload_id"],
        filename=session["filename"],
        file_size=session["file_size"],
        chunk_size=session["chunk_size"],
        total_chunks=total,
        file_sha256=session["file_sha256"],
        expires_at=datetime.fromisoformat(session["expires_at"]),
        expired=expired,
        status=session["status"],
        chunks_received=len(received),
        missing_chunks=missing,
        assembled_path=session["assembled_path"],
    )


def receive_chunk(upload_id: str, index: int, claimed_sha256: str, body: bytes) -> dict:
    with db.get_conn() as conn:
        session = _get_session(conn, upload_id)
        received = _received_indexes(conn, upload_id)

    session = _mark_expired_if_due(session)
    if session["status"] == "expired":
        missing = [i for i in range(session["total_chunks"]) if i not in set(received)]
        raise SessionExpired(
            "This upload session has expired; no chunks are accepted and no "
            "progress was modified.",
            {
                "upload_id": upload_id,
                "expires_at": session["expires_at"],
                "missing_chunks": missing,
            },
        )

    total = session["total_chunks"]
    if not 0 <= index < total:
        raise ChunkOutOfRange(
            f"chunk_index {index} is outside [0, {total - 1}].",
            {"chunk_index": index, "total_chunks": total},
        )

    # Length rule: every chunk except the last must equal chunk_size exactly.
    expected_length = (
        session["chunk_size"]
        if index < total - 1
        else session["file_size"] - (total - 1) * session["chunk_size"]
    )
    if len(body) != expected_length:
        raise InvalidUpload(
            "Chunk length does not match the size declared for this session; "
            "the chunk was not recorded.",
            {
                "chunk_index": index,
                "received_length": len(body),
                "expected_length": expected_length,
            },
        )

    actual_sha256 = hashlib.sha256(body).hexdigest()
    if actual_sha256 != claimed_sha256:
        raise ChecksumMismatch(
            "Uploaded chunk bytes do not match the supplied X-Chunk-SHA256; "
            "the chunk was not recorded.",
            {
                "upload_id": upload_id,
                "chunk_index": index,
                "claimed_sha256": claimed_sha256,
                "actual_sha256": actual_sha256},
        )

    idempotent = False
    newly_inserted = False
    with db.write_tx() as conn:
        existing = conn.execute(
            "SELECT sha256 FROM chunks WHERE upload_id = ? AND chunk_index = ?",
            (upload_id, index),
        ).fetchone()
        if existing is not None:
            if existing["sha256"] != actual_sha256:
                raise ChunkConflict(
                    f"Chunk {index} was already confirmed with different content; "
                    "the stored chunk was left untouched.",
                    {
                        "chunk_index": index,
                        "existing_sha256": existing["sha256"],
                        "received_sha256": actual_sha256,
                    },
                )
            idempotent = True
        else:
            # Durable payload first, confirming database row second.
            storage.save_chunk_atomic(upload_id, index, body)
            conn.execute(
                "INSERT INTO chunks (upload_id, chunk_index, sha256, size) "
                "VALUES (?, ?, ?, ?)",
                (upload_id, index, actual_sha256, len(body)),
            )
            newly_inserted = True

        count = conn.execute(
            "SELECT COUNT(*) AS n FROM chunks WHERE upload_id = ?", (upload_id,)
        ).fetchone()["n"]

    complete = session["status"] == "complete"
    # Only a *newly confirmed* chunk may trigger assembly: an idempotent
    # retransmit must stay a 200 success even when the declared whole-file
    # digest is wrong (the explicit integrity failure is reported via
    # finalize/status, and recorded progress is never rewritten).
    if newly_inserted and count == total and not complete:
        _assemble_and_complete(session)
        complete = True
    elif count == total and not complete:
        # A concurrent request may have completed assembly while this
        # idempotent retransmit was in flight: re-read the committed status.
        with db.get_conn() as conn:
            row = conn.execute(
                "SELECT status FROM sessions WHERE upload_id = ?", (upload_id,)
            ).fetchone()
        complete = row is not None and row["status"] == "complete"

    return {
        "upload_id": upload_id,
        "chunk_index": index,
        "received": True,
        "chunks_received": count,
        "total_chunks": total,
        "complete": complete,
        "idempotent": idempotent,
    }


def _assemble_and_complete(session: dict) -> str:
    """Assemble confirmed chunks, verify digest, publish atomically.

    On whole-file digest mismatch the session stays open with every chunk
    recorded (missing list empty) and an :class:`IntegrityError` is raised.
    """
    upload_id = session["upload_id"]
    with db.write_tx() as conn:
        # Re-check under the write lock: nothing may have been removed.
        rows = conn.execute(
            "SELECT chunk_index FROM chunks WHERE upload_id = ?", (upload_id,)
        ).fetchall()
        present = {r["chunk_index"] for r in rows}
        missing = [i for i in range(session["total_chunks"]) if i not in present]
        if missing:
            raise IncompleteUpload(
                "Cannot finalize: chunks are missing.",
                {"upload_id": upload_id, "missing_chunks": missing},
            )

        try:
            path = storage.assemble_and_publish(
                upload_id,
                session["total_chunks"],
                session["file_sha256"],
                session["filename"],
            )
        except ValueError as exc:
            raise IntegrityError(
                "Assembled file SHA-256 does not match the value declared at "
                "session creation; session kept open with recorded progress.",
                {
                    "upload_id": upload_id,
                    "declared_sha256": session["file_sha256"],
                    "actual_sha256": str(exc),
                },
            ) from exc

        conn.execute(
            "UPDATE sessions SET status = 'complete', assembled_path = ?, "
            "completed_at = ? WHERE upload_id = ? AND status != 'complete'",
            (str(path), utcnow().isoformat(), upload_id),
        )
        return str(path)


def finalize(upload_id: str) -> dict:
    with db.get_conn() as conn:
        session = _get_session(conn, upload_id)
        received = _received_indexes(conn, upload_id)

    session = _mark_expired_if_due(session)
    if session["status"] == "expired":
        raise SessionExpired(
            "This upload session has expired; it cannot be finalized.",
            {"upload_id": upload_id, "expires_at": session["expires_at"]},
        )
    if session["status"] == "complete":
        return {
            "upload_id": upload_id,
            "complete": True,
            "status": "complete",
            "assembled_path": session["assembled_path"],
            "missing_chunks": [],
        }

    total = session["total_chunks"]
    missing = [i for i in range(total) if i not in set(received)]
    if missing:
        raise IncompleteUpload(
            "Cannot finalize: chunks are missing.",
            {"upload_id": upload_id, "missing_chunks": missing},
        )

    path = _assemble_and_complete(session)
    return {
        "upload_id": upload_id,
        "complete": True,
        "status": "complete",
        "assembled_path": path,
        "missing_chunks": [],
    }
