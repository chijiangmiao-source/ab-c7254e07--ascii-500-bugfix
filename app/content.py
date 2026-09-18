"""Byte-level preview/download of an upload session's content.

``GET /api/v1/uploads/{upload_id}/content`` serves the *same* address while a
scan is still uploading and after it is published:

* **open session** — bytes are streamed straight from the confirmed chunk
  files. Every chunk touched by the requested span must (a) have a confirming
  row in the snapshot, (b) exist on disk and (c) match its recorded SHA-256.
  If any touched chunk is missing or corrupt the answer is ``409
  range_unavailable`` with ascending chunk indexes, and *no scan byte* is put
  on the wire.
* **complete session** — bytes are streamed from the atomically published
  file. Without a ``Range`` header the whole file is returned as ``200``.

Confirmed chunk files are write-once/immutable, so the snapshot taken by
:func:`app.services.load_snapshot` stays valid for the storage reads and the
response: a concurrent upload can only add new chunk files, never change the
ones being streamed. Cross-chunk spans open each touched chunk in turn and
never assemble a temporary whole-file copy.
"""
from __future__ import annotations

import hashlib
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import quote

from fastapi.responses import Response, StreamingResponse

from . import ranges as ranges_mod
from . import services, storage
from .errors import RangeNotSatisfiable, RangeUnavailable

_CONTENT_TYPE = "application/octet-stream"
_CRLF = b"\r\n"


def _etag(session: dict) -> str:
    # The whole-file digest declared at creation (and verified on publication)
    # identifies byte content before and after completion identically.
    return f'"{session["file_sha256"]}"'


def _ascii_filename(filename: str) -> str:
    """Lossy pure-ASCII fallback for the legacy ``filename=`` parameter.

    RFC 6266 carries the real name in ``filename*`` (percent-encoded UTF-8);
    ``filename=`` is the fallback for agents without ``filename*`` support and
    HTTP header values must be Latin-1, so every non-printable-ASCII character
    is replaced rather than passed through.
    """
    fallback = "".join(
        ch if 32 <= ord(ch) < 127 and ch not in '\\"' else "_"
        for ch in filename
    ).strip()
    return fallback or "upload.bin"


def _content_disposition(filename: str) -> str:
    return (
        f"attachment; filename=\"{_ascii_filename(filename)}\"; "
        f"filename*=UTF-8''{quote(filename, safe='')}"
    )


def _unsatisfiable(size: int, message: str, raw_range: str) -> RangeNotSatisfiable:
    return RangeNotSatisfiable(
        message,
        {"size": size, "range": raw_range},
        headers={"Content-Range": f"bytes */{size}"},
    )


def _plan(session: dict, chunks: dict, intervals: list[tuple[int, int]]) -> dict:
    """Map requested byte intervals onto chunk indexes and verify each one.

    Returns ``{"chunks": sorted required indexes}`` or raises
    :class:`RangeUnavailable` listing every missing/corrupt chunk.
    """
    chunk_size = session["chunk_size"]
    file_size = session["file_size"]
    total = session["total_chunks"]
    upload_id = session["upload_id"]

    required: set[int] = set()
    for start, end in intervals:
        required.update(range(start // chunk_size, end // chunk_size + 1))

    missing: list[int] = []
    corrupt: list[int] = []
    for index in sorted(required):
        expected_len = storage.chunk_length(index, chunk_size, file_size, total)
        row = chunks.get(index)
        if row is None:
            missing.append(index)
            continue
        problem = storage.inspect_chunk(
            upload_id, index, row["size"], row["sha256"]
        )
        if problem == "missing":
            missing.append(index)
        elif problem == "corrupt" or row["size"] != expected_len:
            corrupt.append(index)
    if missing or corrupt:
        unavailable = sorted(set(missing) | set(corrupt))
        raise RangeUnavailable(
            "The requested byte range touches chunks that are not confirmed "
            "or whose stored bytes fail their recorded SHA-256; no content is "
            "returned. Upload (or re-upload) the listed chunk indexes and "
            "retry the range.",
            {
                "upload_id": upload_id,
                "requested_ranges": [[s, e] for s, e in intervals],
                "missing_chunks": missing,
                "corrupt_chunks": corrupt,
                "unavailable_chunks": unavailable,
            },
        )
    return {"chunks": sorted(required)}


def _single_part_response(
    session: dict, start: int, end: int, *, source: str
) -> StreamingResponse:
    size = session["file_size"]
    upload_id = session["upload_id"]

    def iterator() -> Iterator[bytes]:
        if source == "published":
            yield from storage.iter_file_span(
                _published_path(session), start, end
            )
        else:
            yield from storage.iter_logical_span(
                upload_id,
                start,
                end,
                session["chunk_size"],
                size,
                session["total_chunks"],
            )

    headers = {
        "Content-Range": f"bytes {start}-{end}/{size}",
        "Content-Length": str(end - start + 1),
        "ETag": _etag(session),
        "Accept-Ranges": "bytes",
        "Content-Disposition": _content_disposition(session["filename"]),
    }
    return StreamingResponse(
        iterator(), status_code=206, media_type=_CONTENT_TYPE, headers=headers
    )


def _multipart_response(
    session: dict, intervals: list[tuple[int, int]], *, source: str
) -> StreamingResponse:
    size = session["file_size"]
    upload_id = session["upload_id"]
    # Deterministic boundary: identical requests produce an identical wire
    # boundary ("stable boundary"), never random per response.
    material = f"{upload_id}:{intervals}".encode()
    boundary = "----AxleRange" + hashlib.sha256(material).hexdigest()[:32]

    part_headers: list[bytes] = []
    total = 0
    for start, end in intervals:
        head = (
            f"--{boundary}\r\n"
            f"Content-Type: {_CONTENT_TYPE}\r\n"
            f"Content-Range: bytes {start}-{end}/{size}\r\n\r\n"
        ).encode("ascii")
        part_headers.append(head)
        total += len(head) + (end - start + 1) + len(_CRLF)
    closing = f"--{boundary}--\r\n".encode("ascii")
    total += len(closing)

    def iterator() -> Iterator[bytes]:
        for (start, end), head in zip(intervals, part_headers, strict=True):
            yield head
            if source == "published":
                yield from storage.iter_file_span(
                    _published_path(session), start, end
                )
            else:
                yield from storage.iter_logical_span(
                    upload_id,
                    start,
                    end,
                    session["chunk_size"],
                    size,
                    session["total_chunks"],
                )
            yield _CRLF
        yield closing

    headers = {
        "Content-Length": str(total),
        "ETag": _etag(session),
        "Accept-Ranges": "bytes",
        "Content-Disposition": _content_disposition(session["filename"]),
    }
    return StreamingResponse(
        iterator(),
        status_code=206,
        media_type=f"multipart/byteranges; boundary={boundary}",
        headers=headers,
    )


def _published_path(session: dict) -> Path:
    return Path(session["assembled_path"])


def _whole_file_response(session: dict, *, source: str) -> StreamingResponse:
    size = session["file_size"]
    upload_id = session["upload_id"]

    def iterator() -> Iterator[bytes]:
        if source == "published":
            yield from storage.iter_file_span(_published_path(session), 0, size - 1)
        elif size:
            yield from storage.iter_logical_span(
                upload_id,
                0,
                size - 1,
                session["chunk_size"],
                size,
                session["total_chunks"],
            )

    return StreamingResponse(
        iterator(),
        status_code=200,
        media_type=_CONTENT_TYPE,
        headers={
            "Content-Length": str(size),
            "ETag": _etag(session),
            "Accept-Ranges": "bytes",
            "Content-Disposition": _content_disposition(session["filename"]),
        },
    )


def serve_content(upload_id: str, range_header: str | None) -> Response:
    """Build the response for ``GET .../content`` (see module docstring)."""
    snapshot = services.load_snapshot(upload_id)
    session = snapshot["session"]
    chunks = snapshot["chunks"]
    size = session["file_size"]
    complete = session["status"] == "complete"

    if complete:
        published = _published_path(session)
        if not published.is_file():
            # Publication is atomic and nothing ever deletes the file; a
            # missing published file means external damage, not a preview gap.
            raise RuntimeError(f"published file missing for complete session {upload_id}")
        source = "published"
    else:
        source = "chunks"

    if range_header is None or not range_header.strip():
        if complete:
            return _whole_file_response(session, source=source)
        # Open/expired session: the "range" is the entire file, gated exactly
        # like any other span before a single scan byte is streamed.
        if size == 0:
            return _whole_file_response(session, source=source)
        intervals = [(0, size - 1)]
        _plan(session, chunks, intervals)
        return _whole_file_response(session, source=source)

    try:
        intervals = ranges_mod.parse_byte_ranges(range_header, size)
    except ranges_mod.RangeHeaderError as exc:
        raise _unsatisfiable(size, str(exc), range_header) from exc

    if not complete:
        _plan(session, chunks, intervals)

    if len(intervals) == 1:
        start, end = intervals[0]
        return _single_part_response(session, start, end, source=source)
    return _multipart_response(session, intervals, source=source)
