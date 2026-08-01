"""Guard rails that CI enforces (AGENTS.md §3, §8, global law 9).

These are grep-able and merciless on purpose. Without them, cross-repo drift
and forbidden dependencies are silent until production.
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pytest
from edgar_lakehouse_contracts.envelope import LandingEnvelope

import ingest
from ingest.streams.base import build_envelope

SRC = Path(__file__).resolve().parent.parent / "src"
PYPROJECT = Path(__file__).resolve().parent.parent / "pyproject.toml"

# AGENTS.md §3. The Files API is three lines of httpx; the SDK would pull a
# large dependency tree into a container that runs for ninety seconds.
FORBIDDEN_IMPORTS = ("pyspark", "pandas", "requests", "databricks")


def python_sources() -> list[Path]:
    return sorted(SRC.rglob("*.py"))


@pytest.mark.parametrize("module", FORBIDDEN_IMPORTS)
def test_forbidden_dependency_is_not_imported(module: str) -> None:
    pattern = re.compile(rf"^\s*(import|from)\s+{module}\b", re.MULTILINE)
    offenders = [p for p in python_sources() if pattern.search(p.read_text())]
    assert not offenders, f"forbidden import {module!r} in: {[str(p) for p in offenders]}"


@pytest.mark.parametrize("module", FORBIDDEN_IMPORTS)
def test_forbidden_dependency_is_not_declared(module: str) -> None:
    dependencies = PYPROJECT.read_text().split("[project.optional-dependencies]")[0]
    assert module not in dependencies, f"forbidden dependency {module!r} declared in pyproject"


def test_contracts_is_pinned_exactly() -> None:
    """`==`, not `>=`. A range across five repos means five versions in prod."""
    assert "edgar-lakehouse-contracts==" in PYPROJECT.read_text()


def test_contract_compat_every_envelope_field_exists_upstream() -> None:
    """The mitigation for the five-repo split (AGENTS.md §8).

    Assert every field this repo writes into an envelope exists in the pinned
    contracts version. Without this, drift is silent until production.
    """
    envelope = build_envelope(
        stream=__import__(
            "edgar_lakehouse_contracts.names", fromlist=["Stream"]
        ).Stream.FILING_INDEX,
        logical_date=date(2026, 7, 29),
        source_url="https://www.sec.gov/example",
        payload={"a": "b"},
    )
    written = set(envelope.model_dump(by_alias=True))
    declared = {field.alias or name for name, field in LandingEnvelope.model_fields.items()}
    assert written <= declared, f"fields not in the pinned contract: {written - declared}"

    # And the fields repo 4 depends on are actually present.
    for required in ("_batch_id", "_logical_date", "_schema_version", "_stream", "payload"):
        assert required in written


def test_no_schema_is_redefined_locally() -> None:
    """Repo 3 owns no schema; it imports them (AGENTS.md §1)."""
    offenders = [
        p
        for p in python_sources()
        if re.search(r"^\s*class\s+\w*(Envelope|Schema|Record)\b", p.read_text(), re.MULTILINE)
    ]
    assert not offenders, f"schema defined locally in: {[str(p) for p in offenders]}"


def test_no_hardcoded_bucket_or_host() -> None:
    """Global law 3: no hardcoded ARNs, hosts, or bucket names outside repos 1-2.

    The two sec.gov base URLs are the source itself, not configuration, and are
    excluded by name.
    """
    allowed = {"https://www.sec.gov", "https://data.sec.gov"}
    pattern = re.compile(r"https?://[\w.-]+")
    for path in python_sources():
        for match in pattern.findall(path.read_text()):
            assert match in allowed or "example" in match, f"hardcoded host {match} in {path}"


def test_version_is_exported() -> None:
    assert ingest.__version__ == "0.1.0"


def test_layer_rule_l0_imports_nothing_internal() -> None:
    """config.py and logging.py sit at L0 (AGENTS.md §4)."""
    for name in ("config.py", "logging.py"):
        source = (SRC / "ingest" / name).read_text()
        assert not re.search(r"^\s*from\s+ingest\.", source, re.MULTILINE), f"{name} imports L1+"


def test_cli_contains_no_parsing_or_http() -> None:
    """L4 is argument parsing and nothing else."""
    source = (SRC / "ingest" / "cli.py").read_text()
    assert "httpx" not in source
    assert "parse_form_index" not in source
