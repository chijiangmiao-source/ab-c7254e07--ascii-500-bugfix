"""On-disk storage for chunk payloads and atomic file publication.

Layout under DATA_DIR::

    chunks/<upload_id>/<index>   confirmed chunk bytes (fsynced)
    tmp/<upload_id>.<suffix>     assembly staging file (fsynced)
    files/<upload_id>/<filename> atomically published result

Renames within the same directory are atomic on POSIX; the session row is
marked complete only after the publish survives an fsync, so a restart can
never mistake a confirmed chunk for a missing one.

Confirmed chunk files are written once (temp-file + fsync + rename) and never
modified or deleted, so range previews can stream them directly: a cross-chunk
read opens the affected chunk files in turn and never materializes a whole
copy of the scan.
"""
from __future__ import annotations

import hashlib
import os
from collections.abc import Iterator
from pathlib import Path

from . import config

_READ_BLOCK = 1024 * 1024


def ensure_dirs() -> None:
    config.CHUNKS_DIR.mkdir(parents=True, exist_ok=True)
    config.TEMP_DIR.mkdir(parents=True, exist_ok=True)
    config.FILES_DIR.mkdir(parents=True, exist_ok=True)


def chunk_dir(upload_id: str) -> Path:
    return config.CHUNKS_DIR / upload_id


def chunk_path(upload_id: str, index: int) -> Path:
    return chunk_dir(upload_id) / str(index)


def save_chunk_atomic(upload_id: str, index: int, data: bytes) -> None:
    """Durably write a chunk payload via temp-file + fsync + atomic rename."""
    directory = chunk_dir(upload_id)
    directory.mkdir(parents=True, exist_ok=True)
    final = chunk_path(upload_id, index)
    tmp = directory / f".{index}.tmp"
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, final)
    # fsync the directory so the rename itself is durable.
    _fsync_dir(directory)


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def chunk_length(index: int, chunk_size: int, file_size: int, total_chunks: int) -> int:
    """Exact on-disk length of chunk ``index`` (the last one may be short)."""
    if index < total_chunks - 1:
        return chunk_size
    return file_size - (total_chunks - 1) * chunk_size


def inspect_chunk(
    upload_id: str, index: int, expected_size: int, expected_sha256: str
) -> str | None:
    """Verify a confirmed chunk payload against its database row.

    Returns ``None`` when the payload is present, has the expected length and
    the expected SHA-256; otherwise returns ``"missing"`` (no payload file) or
    ``"corrupt"`` (present but unreadable / wrong length / wrong digest).
    """
    path = chunk_path(upload_id, index)
    if not path.is_file():
        return "missing"
    digest = hashlib.sha256()
    try:
        seen = 0
        with open(path, "rb") as fh:
            while True:
                block = fh.read(_READ_BLOCK)
                if not block:
                    break
                seen += len(block)
                digest.update(block)
    except OSError:
        return "corrupt"
    if seen != expected_size or digest.hexdigest() != expected_sha256:
        return "corrupt"
    return None


def iter_file_span(path: Path, start: int, end: int) -> Iterator[bytes]:
    """Yield inclusive byte range ``[start, end]`` of a regular file."""
    remaining = end - start + 1
    with open(path, "rb") as fh:
        fh.seek(start)
        while remaining > 0:
            block = fh.read(min(_READ_BLOCK, remaining))
            if not block:
                raise OSError(f"short read from {path}")
            remaining -= len(block)
            yield block


def iter_logical_span(
    upload_id: str,
    start: int,
    end: int,
    chunk_size: int,
    file_size: int,
    total_chunks: int,
) -> Iterator[bytes]:
    """Stream a logical byte span straight from the confirmed chunk files.

    The span may cross chunk boundaries; each touched chunk is opened, read
    for its slice and closed again, so no temporary whole-scan copy is ever
    assembled for a preview request.
    """
    pos = start
    while pos <= end:
        index = pos // chunk_size
        chunk_start = index * chunk_size
        length = chunk_length(index, chunk_size, file_size, total_chunks)
        seg_end = min(end, chunk_start + length - 1)
        yield from iter_file_span(
            chunk_path(upload_id, index), pos - chunk_start, seg_end - chunk_start
        )
        pos = seg_end + 1


def assemble_and_publish(
    upload_id: str, total_chunks: int, expected_sha256: str, filename: str
) -> Path:
    """Assemble chunks in order, verify the whole-file SHA-256, publish atomically.

    Returns the published path. Raises ValueError on a digest mismatch; the
    staged file is removed in that case and no published file is touched.
    """
    out_dir = config.FILES_DIR / upload_id
    out_dir.mkdir(parents=True, exist_ok=True)
    final = out_dir / filename
    tmp = config.TEMP_DIR / f"{upload_id}.{os.getpid()}.assemble"

    digest = hashlib.sha256()
    try:
        with open(tmp, "wb") as out:
            for index in range(total_chunks):
                with open(chunk_path(upload_id, index), "rb") as part:
                    while True:
                        block = part.read(1024 * 1024)
                        if not block:
                            break
                        digest.update(block)
                        out.write(block)
            out.flush()
            os.fsync(out.fileno())

        actual = digest.hexdigest()
        if actual != expected_sha256:
            raise ValueError(actual)

        # Replace into place even if an earlier published copy exists (the
        # content is identical given the digest check).
        os.replace(tmp, final)
        _fsync_dir(out_dir)
        return final
    finally:
        if tmp.exists():
            tmp.unlink()
