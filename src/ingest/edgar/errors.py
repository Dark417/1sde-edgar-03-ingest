"""L1: typed exceptions for the EDGAR source.

Typed rather than string-matched so callers can make the distinctions that
actually matter: "there is no data" is not "the fetch failed", and "the format
changed" is not "the network blipped".

Imports nothing internal.
"""

from __future__ import annotations

__all__ = [
    "EdgarError",
    "FetchFailed",
    "ForbiddenError",
    "IndexFormatChanged",
    "NoIndexForDate",
]


class EdgarError(Exception):
    """Base for every error raised by the EDGAR source layer."""


class NoIndexForDate(EdgarError):
    """No daily index exists for this date.

    Weekends and market holidays 404 legitimately (design doc §4.2.4). This is
    "zero filings", not a failure — the CLI treats it as success with zero
    records. Raised as a distinct type so the *caller* decides that, not the
    client.
    """


class IndexFormatChanged(EdgarError):
    """The daily index no longer matches the expected fixed-width layout.

    Raised instead of returning best-effort rows. The layout has changed before
    and will change again; silently emitting garbage rows from a shifted column
    boundary is the worst available outcome (AGENTS.md §5.8).
    """


class ForbiddenError(EdgarError):
    """EDGAR returned 403 — the User-Agent is missing or malformed.

    Never retried. Retrying a 403 does not fix the User-Agent and does move you
    closer to a ban (design doc §4.2.2).
    """


class FetchFailed(EdgarError):
    """A request failed and could not be recovered by retrying.

    The CLI maps this to exit code 1 (source fetch failure).
    """
