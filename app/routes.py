"""HTTP routes for resumable uploads."""
from __future__ import annotations

from fastapi import APIRouter, Header, Request, Response

from . import content, services
from .errors import InvalidUpload
from .models import ChunkAck, CreateUploadRequest, UploadStatus, validate_sha256

router = APIRouter(prefix="/api/v1")


@router.post("/uploads", status_code=201)
def create_upload(payload: CreateUploadRequest) -> dict:
    """Register an upload session from file size, chunk size, digest, expiry."""
    session = services.create_upload(payload)
    return _session_summary(session)


@router.get("/uploads/{upload_id}", response_model=UploadStatus)
def upload_status(upload_id: str) -> UploadStatus:
    """Return session state with missing chunk indexes in ascending order."""
    return services.get_status(upload_id)


@router.get("/uploads/{upload_id}/content")
def upload_content(upload_id: str, request: Request) -> Response:
    """Stream scan bytes before and after publication from the same address.

    A range over an open session is served (206, single or multipart) only
    when every touched chunk is confirmed, present and digest-valid; otherwise
    409 ``range_unavailable`` lists the offending chunks without leaking bytes.
    A complete session without ``Range`` streams the published file as 200.
    """
    return content.serve_content(upload_id, request.headers.get("range"))


@router.put(
    "/uploads/{upload_id}/chunks/{chunk_index}",
    response_model=ChunkAck,
)
async def put_chunk(
    upload_id: str,
    chunk_index: int,
    request: Request,
    x_chunk_sha256: str | None = Header(default=None),
) -> dict:
    """Accept one raw application/octet-stream chunk.

    Retransmitting the same index with identical content is idempotent (200);
    different content for a confirmed index is rejected with 409.
    """
    if chunk_index < 0:
        # FastAPI already rejects negative path ints; this is defensive only.
        raise InvalidUpload(
            "chunk_index must start at 0.", {"chunk_index": chunk_index}
        )
    if not x_chunk_sha256:
        raise InvalidUpload(
            "Missing required X-Chunk-SHA256 header with the hex SHA-256 "
            "of this chunk's bytes.",
            {"header": "X-Chunk-SHA256"},
        )
    try:
        claimed = validate_sha256(x_chunk_sha256, "X-Chunk-SHA256")
    except ValueError as exc:
        raise InvalidUpload(str(exc), {"header": "X-Chunk-SHA256"}) from exc
    body = await request.body()
    return services.receive_chunk(upload_id, chunk_index, claimed, body)


@router.post("/uploads/{upload_id}/finalize")
def finalize_upload(upload_id: str) -> dict:
    """Explicitly check completeness; 409 with missing list or 422 on integrity failure."""
    return services.finalize(upload_id)


def _session_summary(session: dict) -> dict:
    total = session["total_chunks"]
    return {
        "upload_id": session["upload_id"],
        "filename": session["filename"],
        "file_size": session["file_size"],
        "chunk_size": session["chunk_size"],
        "total_chunks": total,
        "file_sha256": session["file_sha256"],
        "expires_at": session["expires_at"],
        "status": session["status"],
        "chunks_received": 0,
        "missing_chunks": list(range(total)),
        "assembled_path": session.get("assembled_path"),
    }


@router.get("/health")
def health() -> Response:
    return Response(status_code=204)
