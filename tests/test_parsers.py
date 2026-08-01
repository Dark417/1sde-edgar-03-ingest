"""The fixed-width .idx parser (AGENTS.md §5.8, F-2 acceptance)."""

from __future__ import annotations

import pytest
from tests.conftest import FIXTURE_ROW_COUNT

from ingest.edgar.errors import IndexFormatChanged
from ingest.edgar.parsers import parse_form_index


def test_fixture_parses_to_expected_count(index_text: str) -> None:
    assert len(list(parse_form_index(index_text))) == FIXTURE_ROW_COUNT


def test_fields_are_verbatim(index_text: str) -> None:
    """No date parsing, no CIK padding, no accession normalization (§5.6)."""
    first = next(iter(parse_form_index(index_text)))
    assert first.form_type == "1-SA"
    assert first.company_name == "Hartley Opportunity Fund LLC"
    assert first.cik == "2056463"  # not zero-padded here; that is silver's job
    assert first.date_filed == "20260729"  # still a string
    assert first.file_name == "edgar/data/2056463/0001096906-26-001138.txt"


def test_form_types_containing_spaces_survive(index_text: str) -> None:
    """The case a whitespace-splitting parser gets silently wrong.

    'DEF 14A' and 'SCHEDULE 13G' contain spaces. A split()-based parser would
    put '14A' in company_name and shift every later field.
    """
    rows = list(parse_form_index(index_text))
    spaced = {r.form_type for r in rows if " " in r.form_type}
    assert "DEF 14A" in spaced
    assert "SCHEDULE 13G" in spaced
    # and the row after the space is still intact
    def_14a = next(r for r in rows if r.form_type == "DEF 14A")
    assert def_14a.date_filed == "20260729"
    assert def_14a.file_name.startswith("edgar/data/")


def test_every_row_is_structurally_sound(index_text: str) -> None:
    for row in parse_form_index(index_text):
        assert row.cik.isdigit()
        assert len(row.date_filed) == 8
        assert row.file_name.startswith("edgar/data/")
        assert row.company_name  # never blank


def test_malformed_header_raises_and_yields_nothing() -> None:
    """A changed layout must raise, never return best-effort rows (§5.8)."""
    bad = (
        "Description:           Daily Index\n"
        "Form Type   Ticker   Company Name   CIK\n"
        "-------------------------------------------------\n"
        "10-K             ACME CORP     0000012345   20260729    edgar/data/1/x.txt\n"
    )
    with pytest.raises(IndexFormatChanged, match="header does not match"):
        list(parse_form_index(bad))


def test_missing_separator_raises() -> None:
    with pytest.raises(IndexFormatChanged, match="separator"):
        list(parse_form_index("Form Type Company Name CIK Date Filed File Name\n"))


def test_shifted_columns_raise_even_with_a_valid_header(index_text: str) -> None:
    """The header can be right while the data has moved.

    Shifting the body by a full field width drags non-digits into the CIK
    column; without the per-row structural check this would emit 47 rows of
    plausible-looking garbage. A one-character shift is deliberately *not* used
    here: the padding gutters absorb it, which is why the check tests content
    rather than alignment.
    """
    lines = index_text.splitlines()
    separator = next(i for i, line in enumerate(lines) if line.startswith("---"))
    shifted = lines[: separator + 1] + [" " * 12 + line for line in lines[separator + 1 :]]
    with pytest.raises(IndexFormatChanged, match="column boundaries have shifted"):
        list(parse_form_index("\n".join(shifted)))


def test_blank_lines_are_skipped(index_text: str) -> None:
    padded = index_text + "\n\n   \n"
    assert len(list(parse_form_index(padded))) == FIXTURE_ROW_COUNT


def test_header_wrap_is_tolerated(index_text: str) -> None:
    """EDGAR hard-wraps the column-name line; that is the real file's shape."""
    assert "CIK\n" in index_text
    assert len(list(parse_form_index(index_text))) == FIXTURE_ROW_COUNT
