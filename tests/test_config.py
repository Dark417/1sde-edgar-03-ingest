"""Config resolution and validation (F-1 acceptance)."""

from __future__ import annotations

import pytest
from tests.conftest import USER_AGENT

from ingest.config import ConfigError, Settings, load_settings, resolve_ssm


def test_missing_user_agent_names_the_env_var() -> None:
    """F-1 acceptance: the message must contain the env var name."""
    with pytest.raises(ConfigError) as excinfo:
        load_settings({"local_only": True})
    assert "SEC_USER_AGENT" in str(excinfo.value)


def test_user_agent_without_at_fails_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", "edgar-lakehouse-demo")
    with pytest.raises(ConfigError, match="contact email"):
        load_settings({"local_only": True})


def test_valid_local_only_config_resolves(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", USER_AGENT)
    settings = load_settings({"local_only": True})
    assert settings.local_only is True
    assert settings.raw_bucket is None  # not required in local mode


def test_raw_bucket_required_outside_local_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", USER_AGENT)
    monkeypatch.setenv("LANDING_MODE", "s3")
    with pytest.raises(ConfigError, match="RAW_BUCKET"):
        load_settings()


def test_volume_mode_requires_host_and_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", USER_AGENT)
    monkeypatch.setenv("RAW_BUCKET", "b")
    monkeypatch.setenv("LANDING_MODE", "volume")
    with pytest.raises(ConfigError) as excinfo:
        load_settings()
    assert "DBX_HOST" in str(excinfo.value)
    assert "DBX_TOKEN" in str(excinfo.value)


def test_volume_mode_accepts_full_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in (
        ("SEC_USER_AGENT", USER_AGENT),
        ("RAW_BUCKET", "b"),
        ("LANDING_MODE", "volume"),
        ("DBX_HOST", "https://dbx"),
        ("DBX_TOKEN", "secret"),
    ):
        monkeypatch.setenv(name, value)
    assert load_settings().landing_mode == "volume"


def test_max_rps_hard_cap_is_enforced(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", USER_AGENT)
    monkeypatch.setenv("MAX_RPS", "9")
    with pytest.raises(ConfigError, match="hard cap"):
        load_settings({"local_only": True})


def test_non_positive_max_rps_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", USER_AGENT)
    monkeypatch.setenv("MAX_RPS", "0")
    with pytest.raises(ConfigError, match="positive"):
        load_settings({"local_only": True})


def test_secrets_are_redacted(monkeypatch: pytest.MonkeyPatch) -> None:
    """config-check runs in a live task; printing a PAT there is a leak."""
    for name, value in (
        ("SEC_USER_AGENT", USER_AGENT),
        ("RAW_BUCKET", "b"),
        ("DBX_HOST", "https://dbx"),
        ("DBX_TOKEN", "super-secret-pat"),
    ):
        monkeypatch.setenv(name, value)
    redacted = load_settings().redacted()
    assert redacted["dbx_token"] == "***redacted***"
    assert "super-secret-pat" not in str(redacted)


def test_env_var_beats_ssm(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", USER_AGENT)
    monkeypatch.setenv("RAW_BUCKET", "from-env")
    monkeypatch.setenv("LANDING_MODE", "s3")
    monkeypatch.setattr("ingest.config.resolve_ssm", lambda name: "from-ssm")
    assert load_settings().raw_bucket == "from-env"


def test_ssm_is_used_when_env_is_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", USER_AGENT)
    monkeypatch.setenv("LANDING_MODE", "s3")
    monkeypatch.setattr(
        "ingest.config.resolve_ssm",
        lambda name: "from-ssm" if name.endswith("raw_bucket") else None,
    )
    assert load_settings().raw_bucket == "from-ssm"


def test_local_mode_never_contacts_ssm(monkeypatch: pytest.MonkeyPatch) -> None:
    """Local mode must work on a machine with no AWS configuration at all."""
    calls: list[str] = []
    monkeypatch.setenv("SEC_USER_AGENT", USER_AGENT)
    monkeypatch.setattr("ingest.config.resolve_ssm", lambda name: calls.append(name) or None)
    load_settings({"local_only": True})
    assert calls == []


def test_ingest_local_only_env_var_is_honoured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", USER_AGENT)
    monkeypatch.setenv("INGEST_LOCAL_ONLY", "1")
    assert load_settings().local_only is True


def test_resolve_ssm_returns_none_when_aws_is_unreachable() -> None:
    """Every boto3 failure is the same failure: 'SSM did not answer'."""
    assert resolve_ssm("/edgar-lakehouse/definitely/not/a/real/parameter") is None


def test_settings_are_frozen(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", USER_AGENT)
    settings = load_settings({"local_only": True})
    with pytest.raises(Exception, match=r"frozen|immutable"):
        settings.max_rps = 7.0  # type: ignore[misc]


def test_defaults_match_the_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", USER_AGENT)
    settings = load_settings({"local_only": True})
    assert settings.max_rps == 5.0
    assert settings.volume_path == "/Volumes/edgar/landing/edgar"
    assert settings.landing_mode == "volume"  # ADR-001's safe default


def test_settings_can_be_built_directly() -> None:
    assert Settings(sec_user_agent=USER_AGENT, local_only=True).max_rps == 5.0
