"""The CLI (F-5 acceptance) — exit codes are contract, --dry-run writes nothing."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import httpx
import pytest
import respx
from tests.conftest import USER_AGENT
from typer.testing import CliRunner

from ingest.cli import EXIT_CONFIG_ERROR, EXIT_FETCH_FAILED, EXIT_OK, EXIT_SINK_FAILED, app

runner = CliRunner()


@pytest.fixture
def local_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    monkeypatch.setenv("SEC_USER_AGENT", USER_AGENT)
    monkeypatch.setenv("INGEST_LOCAL_ONLY", "1")
    monkeypatch.setenv("LOCAL_LANDING_DIR", str(tmp_path / "landing"))
    return tmp_path


@pytest.fixture
def aws_env(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Full non-local config, with boto3 replaced by a mock."""
    monkeypatch.setenv("SEC_USER_AGENT", USER_AGENT)
    monkeypatch.setenv("RAW_BUCKET", "edgar-lake-raw-test")
    monkeypatch.setenv("LANDING_MODE", "volume")
    monkeypatch.setenv("DBX_HOST", "https://dbx.example.com")
    monkeypatch.setenv("DBX_TOKEN", "super-secret-pat-value")
    monkeypatch.setenv("LOCAL_ONLY", "false")
    client = MagicMock()
    monkeypatch.setattr("boto3.client", lambda *a, **k: client)
    return client


# ------------------------------------------------------------- argument errors


def test_unknown_stream_exits_2_and_lists_valid_values(local_env: Path) -> None:
    result = runner.invoke(app, ["run", "--stream", "bogus", "--logical-date", "2026-07-29"])
    assert result.exit_code == EXIT_CONFIG_ERROR
    for stream in ("filing_index", "company_submissions", "company_concept"):
        assert stream in result.output


def test_bad_date_exits_2(local_env: Path) -> None:
    result = runner.invoke(app, ["run", "--stream", "filing_index", "--logical-date", "29-07-2026"])
    assert result.exit_code == EXIT_CONFIG_ERROR
    assert "YYYY-MM-DD" in result.output


def test_missing_user_agent_exits_2(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGEST_LOCAL_ONLY", "1")
    result = runner.invoke(app, ["run", "--stream", "filing_index", "--logical-date", "2026-07-29"])
    assert result.exit_code == EXIT_CONFIG_ERROR
    assert "SEC_USER_AGENT" in result.output


# -------------------------------------------------------------- config-check


def test_config_check_exits_0_when_resolvable(local_env: Path) -> None:
    result = runner.invoke(app, ["config-check", "--local-only"])
    assert result.exit_code == EXIT_OK
    assert "sec_user_agent" in result.output


def test_config_check_exits_2_when_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """`docker run <img> config-check` with an empty env must say why."""
    result = runner.invoke(app, ["config-check"])
    assert result.exit_code == EXIT_CONFIG_ERROR
    assert "SEC_USER_AGENT" in result.output


def test_config_check_redacts_the_token(aws_env: MagicMock) -> None:
    result = runner.invoke(app, ["config-check"])
    assert result.exit_code == EXIT_OK
    assert "***redacted***" in result.output
    assert "super-secret-pat-value" not in result.output


# ------------------------------------------------------------------ dry run


@respx.mock
def test_dry_run_makes_zero_writes(aws_env: MagicMock, index_text: str) -> None:
    """F-5 acceptance: zero put_object and zero Files API calls."""
    respx.get(url__startswith="https://www.sec.gov").mock(
        return_value=httpx.Response(200, text=index_text)
    )
    dbx = respx.put(url__startswith="https://dbx.example.com").mock(
        return_value=httpx.Response(200)
    )

    result = runner.invoke(
        app, ["run", "--stream", "filing_index", "--logical-date", "2026-07-29", "--dry-run"]
    )

    assert result.exit_code == EXIT_OK
    assert aws_env.put_object.call_count == 0
    assert dbx.call_count == 0


@respx.mock
def test_dry_run_prints_both_target_paths(local_env: Path, index_text: str) -> None:
    respx.get(url__startswith="https://www.sec.gov").mock(
        return_value=httpx.Response(200, text=index_text)
    )
    result = runner.invoke(
        app, ["run", "--stream", "filing_index", "--logical-date", "2026-07-29", "--dry-run"]
    )
    assert result.exit_code == EXIT_OK
    assert "s3://" in result.output
    assert "/Volumes/edgar/landing/edgar/" in result.output
    # same filename, different prefix
    assert result.output.count("filing_index-20260729-") >= 3


@respx.mock
def test_dry_run_writes_no_local_file(local_env: Path, index_text: str) -> None:
    respx.get(url__startswith="https://www.sec.gov").mock(
        return_value=httpx.Response(200, text=index_text)
    )
    runner.invoke(
        app, ["run", "--stream", "filing_index", "--logical-date", "2026-07-29", "--dry-run"]
    )
    assert not (local_env / "landing").exists()


# ---------------------------------------------------------------- exit codes


@respx.mock
def test_successful_run_exits_0_and_writes(local_env: Path, index_text: str) -> None:
    respx.get(url__startswith="https://www.sec.gov").mock(
        return_value=httpx.Response(200, text=index_text)
    )
    result = runner.invoke(app, ["run", "--stream", "filing_index", "--logical-date", "2026-07-29"])
    assert result.exit_code == EXIT_OK
    written = list((local_env / "landing").rglob("*.json.gz"))
    assert len(written) == 1


@respx.mock
def test_source_fetch_failure_exits_1(local_env: Path) -> None:
    respx.get(url__startswith="https://www.sec.gov").mock(return_value=httpx.Response(500))
    result = runner.invoke(app, ["run", "--stream", "filing_index", "--logical-date", "2026-07-29"])
    assert result.exit_code == EXIT_FETCH_FAILED


@respx.mock
def test_forbidden_exits_1(local_env: Path) -> None:
    respx.get(url__startswith="https://www.sec.gov").mock(return_value=httpx.Response(403))
    result = runner.invoke(app, ["run", "--stream", "filing_index", "--logical-date", "2026-07-29"])
    assert result.exit_code == EXIT_FETCH_FAILED


@respx.mock
def test_changed_index_format_exits_1(local_env: Path) -> None:
    respx.get(url__startswith="https://www.sec.gov").mock(
        return_value=httpx.Response(200, text="Form Type Ticker CIK\n------------\n")
    )
    result = runner.invoke(app, ["run", "--stream", "filing_index", "--logical-date", "2026-07-29"])
    assert result.exit_code == EXIT_FETCH_FAILED


@respx.mock
def test_weekend_exits_0(local_env: Path) -> None:
    respx.get(url__startswith="https://www.sec.gov").mock(return_value=httpx.Response(404))
    result = runner.invoke(app, ["run", "--stream", "filing_index", "--logical-date", "2026-08-01"])
    assert result.exit_code == EXIT_OK


@respx.mock
def test_s3_failure_exits_3(aws_env: MagicMock, index_text: str) -> None:
    """The system of record failing is fatal."""
    respx.get(url__startswith="https://www.sec.gov").mock(
        return_value=httpx.Response(200, text=index_text)
    )
    aws_env.put_object.side_effect = RuntimeError("S3 is unavailable")
    result = runner.invoke(app, ["run", "--stream", "filing_index", "--logical-date", "2026-07-29"])
    assert result.exit_code == EXIT_SINK_FAILED


@respx.mock
def test_landing_push_failure_exits_0_with_s3_written(aws_env: MagicMock, index_text: str) -> None:
    """F-3 acceptance: Volume down -> exit 0, S3 object present, one ERROR log."""
    respx.get(url__startswith="https://www.sec.gov").mock(
        return_value=httpx.Response(200, text=index_text)
    )
    respx.put(url__startswith="https://dbx.example.com").mock(return_value=httpx.Response(503))

    result = runner.invoke(app, ["run", "--stream", "filing_index", "--logical-date", "2026-07-29"])

    assert result.exit_code == EXIT_OK
    assert aws_env.put_object.call_count == 1  # the system of record committed
    assert "LANDING_PUSH_FAILED" in result.output
    assert result.output.count("LANDING_PUSH_FAILED") == 1


# ------------------------------------------------------------------ logging


@respx.mock
def test_ingest_complete_carries_the_required_fields(local_env: Path, index_text: str) -> None:
    """§5.9: one summary line per run with the named fields."""
    import json

    respx.get(url__startswith="https://www.sec.gov").mock(
        return_value=httpx.Response(200, text=index_text)
    )
    result = runner.invoke(app, ["run", "--stream", "filing_index", "--logical-date", "2026-07-29"])
    line = next(line for line in result.output.splitlines() if '"event": "ingest_complete"' in line)
    payload = json.loads(line)
    for field in ("stream", "logical_date", "batch_id", "records", "bytes", "duration_s", "sinks"):
        assert field in payload


@respx.mock
def test_all_stdout_lines_are_json(local_env: Path, index_text: str) -> None:
    """§5.9: structured JSON logs to stdout — no stray plain-text lines."""
    import json

    respx.get(url__startswith="https://www.sec.gov").mock(
        return_value=httpx.Response(200, text=index_text)
    )
    result = runner.invoke(app, ["run", "--stream", "filing_index", "--logical-date", "2026-07-29"])
    for line in result.output.splitlines():
        if line.strip():
            json.loads(line)  # raises if any line is not JSON


# ------------------------------------------------------------------- local


@respx.mock
def test_local_dir_flag_implies_local_only(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, index_text: str
) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", USER_AGENT)
    respx.get(url__startswith="https://www.sec.gov").mock(
        return_value=httpx.Response(200, text=index_text)
    )
    result = runner.invoke(
        app,
        [
            "run",
            "--stream",
            "filing_index",
            "--logical-date",
            "2026-07-29",
            "--local-dir",
            str(tmp_path / "out"),
        ],
    )
    assert result.exit_code == EXIT_OK
    assert len(list((tmp_path / "out").rglob("*.json.gz"))) == 1


@respx.mock
def test_rerunning_leaves_one_object(local_env: Path, index_text: str) -> None:
    """§8.1: run it twice -> one object, not two."""
    respx.get(url__startswith="https://www.sec.gov").mock(
        return_value=httpx.Response(200, text=index_text)
    )
    for _ in range(2):
        runner.invoke(app, ["run", "--stream", "filing_index", "--logical-date", "2026-07-29"])
    assert len(list((local_env / "landing").rglob("*.json.gz"))) == 1


# ------------------------------------------------- local is the default path


@respx.mock
def test_runs_with_only_a_user_agent_set(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, index_text: str
) -> None:
    """No AWS, no SSM, no bucket — the path that works needs no configuration."""
    monkeypatch.setenv("SEC_USER_AGENT", USER_AGENT)
    monkeypatch.chdir(tmp_path)
    respx.get(url__startswith="https://www.sec.gov").mock(
        return_value=httpx.Response(200, text=index_text)
    )
    result = runner.invoke(app, ["run", "--stream", "filing_index", "--logical-date", "2026-07-29"])
    assert result.exit_code == EXIT_OK
    assert len(list((tmp_path / "local-landing").rglob("*.json.gz"))) == 1


def test_config_check_passes_with_only_a_user_agent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", USER_AGENT)
    result = runner.invoke(app, ["config-check"])
    assert result.exit_code == EXIT_OK
    assert '"local_only": true' in result.output


def test_local_mode_is_logged_not_silent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, index_text: str
) -> None:
    """A run that believed it wrote to S3 must be able to see that it did not."""
    monkeypatch.setenv("SEC_USER_AGENT", USER_AGENT)
    monkeypatch.setenv("LOCAL_LANDING_DIR", str(tmp_path / "landing"))
    with respx.mock:
        respx.get(url__startswith="https://www.sec.gov").mock(
            return_value=httpx.Response(200, text=index_text)
        )
        result = runner.invoke(
            app, ["run", "--stream", "filing_index", "--logical-date", "2026-07-29"]
        )
    assert '"landing_target"' in result.output
    assert '"mode": "local"' in result.output


def test_remote_flag_demands_aws_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """--remote with nothing configured is exit 2, never a silent local write."""
    monkeypatch.setenv("SEC_USER_AGENT", USER_AGENT)
    result = runner.invoke(
        app, ["run", "--stream", "filing_index", "--logical-date", "2026-07-29", "--remote"]
    )
    assert result.exit_code == EXIT_CONFIG_ERROR
    assert "RAW_BUCKET" in result.output


def test_local_dir_does_not_override_an_explicit_remote(monkeypatch: pytest.MonkeyPatch) -> None:
    """Silently ignoring --remote is the surprise the flag pair exists to prevent."""
    monkeypatch.setenv("SEC_USER_AGENT", USER_AGENT)
    result = runner.invoke(
        app,
        [
            "run",
            "--stream",
            "filing_index",
            "--logical-date",
            "2026-07-29",
            "--remote",
            "--local-dir",
            "/tmp/whatever",
        ],
    )
    assert result.exit_code == EXIT_CONFIG_ERROR
