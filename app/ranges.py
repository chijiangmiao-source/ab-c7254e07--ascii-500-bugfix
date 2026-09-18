"""Parsing, normalization and merging of ``Range: bytes=...`` header values.

Supports the four forms callers rely on while pre-inspecting large scans::

    bytes=200-999      single interval (inclusive)
    bytes=200-         open-ended: 200 through the last byte
    bytes=-500         suffix: the final 500 bytes
    bytes=0-99,200-    multiple intervals, comma separated

Intervals are returned as inclusive ``(start, end)`` pairs, sorted ascending
and de-duplicated by merging overlaps. Ends beyond the file are clamped to the
last byte (RFC 9110 14.1.2); anything syntactically malformed or unsatisfiable
for the given size is reported as :class:`RangeHeaderError` so the route can
answer 416 with ``Content-Range: bytes */<size>``.
"""
from __future__ import annotations

import re

_INT = r"[0-9]+"
_SPEC_RE = re.compile(rf"^\s*({_INT})?\s*-\s*({_INT})?\s*$")


class RangeHeaderError(ValueError):
    """The Range header is malformed or cannot be satisfied for this size."""


def parse_byte_ranges(header: str, size: int) -> list[tuple[int, int]]:
    """Parse a ``Range`` header value into merged inclusive byte intervals."""
    raw = header.strip()
    if not raw.lower().startswith("bytes="):
        raise RangeHeaderError("Only byte ranges are supported (expected 'bytes=').")
    body = raw[len("bytes="):].strip()
    if not body:
        raise RangeHeaderError("Empty Range header.")

    intervals: list[tuple[int, int]] = []
    for spec in body.split(","):
        match = _SPEC_RE.match(spec)
        if match is None:
            raise RangeHeaderError(f"Malformed byte range spec: {spec.strip()!r}")
        start_s, end_s = match.group(1), match.group(2)

        if start_s is None:
            # Suffix form: bytes=-<suffix-length>. A zero suffix carries no
            # bytes and is simply dropped (RFC 9110 treats it as unsatisfiable
            # for that spec; it only yields 416 if nothing remains).
            if end_s is None or int(end_s) == 0 or size == 0:
                continue
            suffix = int(end_s)
            start = max(0, size - suffix)
            end = size - 1
        else:
            start = int(start_s)
            if end_s is None:
                # Open-ended: bytes=<start>-
                end = size - 1
            else:
                end = int(end_s)
            if start > end:
                raise RangeHeaderError(
                    f"Range start {start} is greater than end {end}."
                )
            if start >= size:
                # This spec cannot be satisfied; in a multi-range request the
                # remaining satisfiable specs are still served.
                continue
            end = min(end, size - 1)

        intervals.append((start, end))

    if not intervals:
        raise RangeHeaderError("No satisfiable byte range remains for this size.")
    return _merge_overlaps(intervals)


def _merge_overlaps(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Sort and union intervals that overlap (adjacent intervals stay distinct)."""
    ordered = sorted(intervals)
    merged: list[tuple[int, int]] = []
    for start, end in ordered:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged
