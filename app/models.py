"""Pydantic request/response models."""
from __future__ import annotations

import re
from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def validate_sha256(value: str, field: str) -> str:
    value = value.strip().lower()
    if not _SHA256_RE.fullmatch(value):
        raise ValueError(f"{field} must be 64 lowercase hex characters (SHA-256)")
    return value


class CreateUploadRequest(BaseModel):
    filename: str = Field(min_length=1, max_length=255, description="Original file name")
    file_size: int = Field(ge=0, description="Total file size in bytes")
    chunk_size: int = Field(gt=0, description="Chunk size in bytes; every chunk but the last must equal this")
    file_sha256: str = Field(description="Hex SHA-256 of the whole file")
    expires_at: datetime = Field(description="ISO-8601 UTC instant after which chunks are rejected")

    @field_validator("filename")
    @classmethod
    def _safe_filename(cls, value: str) -> str:
        value = value.strip()
        if not value or value in {".", ".."} or "/" in value or "\\" in value or "\x00" in value:
            raise ValueError("filename must be a plain, non-empty, path-less name")
        return value

    @field_validator("file_sha256")
    @classmethod
    def _check_file_digest(cls, value: str) -> str:
        return validate_sha256(value, "file_sha256")

    @field_validator("expires_at")
    @classmethod
    def _aware_expiry(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("expires_at must include a timezone offset, e.g. 2026-09-16T00:00:00Z")
        return value.astimezone(timezone.utc)


class ChunkAck(BaseModel):
    upload_id: str
    chunk_index: int
    received: bool = True
    chunks_received: int
    total_chunks: int
    complete: bool
    idempotent: bool = False


class UploadStatus(BaseModel):
    upload_id: str
    filename: str
    file_size: int
    chunk_size: int
    total_chunks: int
    file_sha256: str
    expires_at: datetime
    expired: bool
    status: str
    chunks_received: int
    missing_chunks: list[int]
    assembled_path: str | None = None
