"""L1: the EDGAR daily form index (.idx) fixed-width parser.

The file is fixed-width, **not** CSV or whitespace-delimited (design doc
§4.2.3). Splitting on whitespace is wrong and not merely fragile: form types
like ``SEC STAFF ACTION`` contain spaces, and company names contain runs of
them, so a split-based parser emits plausible-looking garbage.

Column boundaries below were measured from a real 5,969-row file rather than
assumed — see docs/03-ingest-design.md §3.

Imports only ``edgar.errors`` and the contracts package. Does not handle: HTTP,
retries, or writing anything.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from typing import Final

from edgar_lakehouse_contracts.models import FilingIndexRecord

from ingest.edgar.errors import IndexFormatChanged

__all__ = ["EXPECTED_HEADER_TOKENS", "parse_form_index"]

# Slices, measured from the character positions that are blank in *every* data
# row of a real index (gutters at 15-16, 77-78, 86-90, 99-102). Note the header
# line claims Company Name starts at column 12; the data says 17. The header is
# decorative — the data wins.
_FORM_TYPE: Final[slice] = slice(0, 17)
_COMPANY_NAME: Final[slice] = slice(17, 79)
_CIK: Final[slice] = slice(79, 91)
_DATE_FILED: Final[slice] = slice(91, 103)
_FILE_NAME: Final[slice] = slice(103, None)

# EDGAR hard-wraps the column-name line across two physical lines, so validation
# normalizes whitespace across the whole header block and compares tokens.
EXPECTED_HEADER_TOKENS: Final[str] = "Form Type Company Name CIK Date Filed File Name"

_SEPARATOR: Final[re.Pattern[str]] = re.compile(r"^-{10,}\s*$")
_WHITESPACE: Final[re.Pattern[str]] = re.compile(r"\s+")
_DIGITS: Final[re.Pattern[str]] = re.compile(r"^\d+$")
_YYYYMMDD: Final[re.Pattern[str]] = re.compile(r"^\d{8}$")

# edgar/data/<cik>/<accession>.txt -- the only place the daily index carries an
# accession number. Anchored on the whole string so a layout change surfaces as a
# raised IndexFormatChanged rather than a silently empty key.
_ACCESSION_IN_PATH: Final[re.Pattern[str]] = re.compile(r"/(\d{10}-\d{2}-\d{6})\.txt$")


def parse_form_index(text: str) -> Iterator[FilingIndexRecord]:
    """Parse a daily form index into raw records.

    Validates the header before emitting anything and raises
    ``IndexFormatChanged`` on any mismatch — never partial or best-effort rows.
    Each row is additionally checked structurally (``cik`` all digits,
    ``date_filed`` eight digits), which catches a shifted column boundary even
    when the header itself is unchanged.

    Fields are returned exactly as published, stripped of the fixed-width
    padding only. No date parsing, no CIK zero-padding, no accession
    normalization: this is the raw record and typing happens in silver
    (AGENTS.md §5.6).

    Does not handle: fetching the file, or deciding what an empty index means.
    """
    lines = text.splitlines()
    body_start = _validate_header(lines)

    for line_number, line in enumerate(lines[body_start:], start=body_start + 1):
        if not line.strip():
            continue
        record = _parse_row(line, line_number)
        yield record


def _validate_header(lines: list[str]) -> int:
    """Return the index of the first data line, raising if the header is wrong.

    Does not handle: files with no separator line at all beyond raising.
    """
    separator_index = next(
        (i for i, line in enumerate(lines[:40]) if _SEPARATOR.match(line)),
        None,
    )
    if separator_index is None:
        raise IndexFormatChanged(
            "daily index has no '-----' separator line in its first 40 lines; "
            "the file is not an EDGAR form index or its layout has changed"
        )

    header_block = " ".join(lines[:separator_index])
    normalized = _WHITESPACE.sub(" ", header_block).strip()
    if EXPECTED_HEADER_TOKENS not in normalized:
        raise IndexFormatChanged(
            "daily index header does not match the expected fixed-width layout. "
            f"expected to find {EXPECTED_HEADER_TOKENS!r} in the header block, got: "
            f"{normalized[-200:]!r}"
        )
    return separator_index + 1


def _parse_row(line: str, line_number: int) -> FilingIndexRecord:
    """Slice one fixed-width row into a raw record.

    Does not handle: skipping bad rows. A row that fails the structural check
    means the column boundaries moved, which invalidates every other row too.
    """
    form_type = line[_FORM_TYPE].strip()
    company_name = line[_COMPANY_NAME].strip()
    cik = line[_CIK].strip()
    date_filed = line[_DATE_FILED].strip()
    file_name = line[_FILE_NAME].strip()

    if not _DIGITS.match(cik):
        raise IndexFormatChanged(
            f"line {line_number}: cik column [{_CIK.start}:{_CIK.stop}] is not numeric "
            f"({cik!r}) - the fixed-width column boundaries have shifted"
        )
    if not _YYYYMMDD.match(date_filed):
        raise IndexFormatChanged(
            f"line {line_number}: date_filed column [{_DATE_FILED.start}:{_DATE_FILED.stop}] "
            f"is not YYYYMMDD ({date_filed!r}) - the fixed-width column boundaries have shifted"
        )

    accession_match = _ACCESSION_IN_PATH.search(file_name)
    if accession_match is None:
        raise IndexFormatChanged(
            f"line {line_number}: file_name column [{_FILE_NAME.start}:] does not end in "
            f"/<accession>.txt ({file_name!r}) - either the boundaries have shifted or EDGAR "
            "changed the archive path layout. The accession is the filing's natural key, so "
            "emitting the row without one would push the failure into silver."
        )

    return FilingIndexRecord(
        company_name=company_name,
        form_type=form_type,
        cik=cik,
        date_filed=date_filed,
        file_name=file_name,
        accession_number=accession_match.group(1),
    )
