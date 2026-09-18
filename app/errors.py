"""Structured application errors and their FastAPI handlers.

Every failure response has the JSON shape::

    {"error": {"code": "<machine_readable>", "message": "<human readable>",
               "details": {...}}}
"""
from __future__ import annotations

from typing import Any

from fastapi import Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException


class APIError(Exception):
    """Base class for errors rendered as structured JSON payloads."""

    status_code: int = 400
    code: str = "bad_request"

    def __init__(
        self,
        message: str,
        details: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ):
        super().__init__(message)
        self.message = message
        self.details = details or {}
        self.headers = headers or {}

    def to_response(self) -> JSONResponse:
        body = {"error": {"code": self.code, "message": self.message, "details": self.details}}
        return JSONResponse(
            status_code=self.status_code, content=body, headers=self.headers
        )


class InvalidUpload(APIError):
    status_code = 422
    code = "invalid_upload"


class SessionNotFound(APIError):
    status_code = 404
    code = "session_not_found"


class ChunkOutOfRange(APIError):
    status_code = 416
    code = "chunk_out_of_range"


class ChecksumMismatch(APIError):
    status_code = 422
    code = "checksum_mismatch"


class ChunkConflict(APIError):
    status_code = 409
    code = "chunk_conflict"


class SessionExpired(APIError):
    status_code = 410
    code = "session_expired"


class IncompleteUpload(APIError):
    status_code = 409
    code = "incomplete_upload"


class IntegrityError(APIError):
    status_code = 422
    code = "integrity_error"


class RangeUnavailable(APIError):
    """Open session: requested span touches chunks that are not confirmed,
    whose payload is missing, or whose on-disk digest no longer matches.

    No scan bytes are sent on this response.
    """

    status_code = 409
    code = "range_unavailable"


class RangeNotSatisfiable(APIError):
    """Syntactically valid range that cannot be satisfied for the file size
    (e.g. suffix longer than the file, or start past the last byte).

    Carries ``Content-Range: bytes */<size>`` per RFC 9110.
    """

    status_code = 416
    code = "range_not_satisfiable"


def register_exception_handlers(app: Any) -> None:  # noqa: C901 - small dispatch table
    @app.exception_handler(APIError)
    async def _handle_api_error(_: Request, exc: APIError) -> JSONResponse:
        return exc.to_response()

    @app.exception_handler(RequestValidationError)
    async def _handle_validation(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content=jsonable_encoder(
                {
                    "error": {
                        "code": "validation_error",
                        "message": "Request parameters or body failed validation.",
                        "details": {"errors": exc.errors()},
                    }
                }
            ),
        )

    @app.exception_handler(StarletteHTTPException)
    async def _handle_http(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = {
            404: "not_found",
            405: "method_not_allowed",
            413: "payload_too_large",
            415: "unsupported_media_type",
        }.get(exc.status_code, "http_error")
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": code,
                    "message": str(exc.detail),
                    "details": {},
                }
            },
        )

    @app.exception_handler(Exception)
    async def _handle_unexpected(_: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": "internal_error",
                    "message": "An unexpected internal error occurred.",
                    "details": {"type": type(exc).__name__},
                }
            },
        )
