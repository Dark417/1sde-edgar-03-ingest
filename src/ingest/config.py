"""L0: settings and their resolution.

Resolution order is env var -> SSM (``/edgar-lakehouse/*``) -> fail with a
message naming the missing key. No bucket name, host, ARN, or path is ever
hardcoded here (AGENTS.global.md law 3).

Imports nothing internal. Does not handle: using any of these values — that is
the sinks' and client's job.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any, Literal

from pydantic import Field, SecretStr, ValidationInfo, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = ["ConfigError", "Settings", "load_settings", "resolve_ssm"]

# env var name -> SSM parameter name. Only these fall back to SSM; anything not
# listed is env-only by design.
SSM_FALLBACKS: dict[str, str] = {
    "RAW_BUCKET": "/edgar-lakehouse/s3/raw_bucket",
    "DBX_HOST": "/edgar-lakehouse/dbx/host",
    "VOLUME_PATH": "/edgar-lakehouse/dbx/volume_path",
    "LANDING_MODE": "/edgar-lakehouse/landing_mode",
}

DEFAULT_LOCAL_LANDING_DIR = "local-landing"


class ConfigError(Exception):
    """Configuration could not be resolved or is invalid.

    Always names the concrete missing key. The CLI maps this to exit code 2.
    """


def resolve_ssm(parameter_name: str) -> str | None:
    """Return an SSM parameter's value, or ``None`` if it cannot be read.

    Returns ``None`` rather than raising on *any* boto3 failure — a missing
    parameter, no credentials, and no network are all "SSM did not answer", and
    the caller turns that into one error message naming the env var the user can
    actually set. Importing boto3 lazily keeps ``--local-only`` usable on a
    machine with no AWS configuration at all.

    Does not handle: caching (see ``load_settings``) or decryption of
    SecureString parameters beyond ``WithDecryption``.
    """
    try:
        import boto3

        client = boto3.client("ssm")
        response = client.get_parameter(Name=parameter_name, WithDecryption=True)
        value = response["Parameter"]["Value"]
        return str(value)
    except Exception:
        return None


class Settings(BaseSettings):
    """Resolved runtime configuration.

    Field names map to upper-case env vars (``sec_user_agent`` <- ``SEC_USER_AGENT``).
    Values absent from the environment are looked up in SSM by ``load_settings``
    before this model is constructed.

    Does not handle: SSM lookup itself, or deciding which sinks to build.
    """

    model_config = SettingsConfigDict(env_file=None, extra="ignore", frozen=True)

    sec_user_agent: str
    landing_mode: Literal["s3", "volume"] = "volume"
    raw_bucket: str | None = None
    dbx_host: str | None = None
    dbx_token: SecretStr | None = None
    volume_path: str = "/Volumes/edgar/landing/edgar"
    max_rps: float = 5.0
    cik_universe_uri: str | None = None

    # Local mode. Not part of AGENTS.md F-1; see docs/03-ingest-design.md §8 for
    # why it exists and exactly what it relaxes.
    local_only: bool = False
    local_landing_dir: str = DEFAULT_LOCAL_LANDING_DIR

    max_rps_hard_cap: float = Field(default=8.0, frozen=True)

    @field_validator("sec_user_agent")
    @classmethod
    def _validate_user_agent(cls, value: str) -> str:
        """Require a contact email in the UA.

        The SEC 403s anonymous clients, and a 403 is never retried. Failing here
        with a clear message beats 403s at 06:00 UTC (design doc §4.2.2).
        """
        if "@" not in value:
            raise ValueError(
                "SEC_USER_AGENT must contain a contact email address (an '@'); "
                f"got {value!r}. Example: 'edgar-lakehouse-demo you@example.com'"
            )
        return value

    @field_validator("max_rps")
    @classmethod
    def _validate_max_rps(cls, value: float, info: ValidationInfo) -> float:
        """Enforce the 8 rps hard cap (design doc §4.2.1).

        SEC fair-access guidance is on the order of 10 req/s. Running above 8 is
        never worth it: the downside is a ban.
        """
        cap = float(info.data.get("max_rps_hard_cap", 8.0))
        if value <= 0:
            raise ValueError(f"MAX_RPS must be positive; got {value}")
        if value > cap:
            raise ValueError(f"MAX_RPS must not exceed the hard cap of {cap}; got {value}")
        return value

    @model_validator(mode="after")
    def _validate_sink_requirements(self) -> Settings:
        """Require only what the selected sinks actually need.

        In local-only mode neither S3 nor Databricks is contacted, so demanding
        a bucket name would be theatre. Outside it, ``raw_bucket`` is required
        because S3 is the system of record, and volume mode additionally
        requires the Databricks host and token.
        """
        if self.local_only:
            if not self.local_landing_dir:
                raise ValueError("LOCAL_LANDING_DIR must be set when LOCAL_ONLY is enabled")
            return self

        if not self.raw_bucket:
            raise ValueError(
                "RAW_BUCKET is required (S3 is the system of record). Set the env var, "
                "publish SSM /edgar-lakehouse/s3/raw_bucket, or pass --local-only."
            )
        if self.landing_mode == "volume":
            missing = [
                name
                for name, value in (("DBX_HOST", self.dbx_host), ("DBX_TOKEN", self.dbx_token))
                if not value
            ]
            if missing:
                raise ValueError(
                    f"LANDING_MODE=volume requires {' and '.join(missing)}; "
                    "set the env var(s) or use LANDING_MODE=s3."
                )
        return self

    def redacted(self) -> dict[str, Any]:
        """Return the settings as a dict with secrets replaced by a marker.

        Used by ``config-check``, which exists to be run in a new ECS task to
        find out why it will fail — printing a live PAT there would be a leak.
        """
        data = self.model_dump()
        if self.dbx_token is not None:
            data["dbx_token"] = "***redacted***"
        return data


def load_settings(overrides: dict[str, Any] | None = None, *, use_ssm: bool = True) -> Settings:
    """Resolve settings: env var -> SSM -> error naming the missing key.

    ``overrides`` (from CLI flags) win over both, so ``--local-only`` can relax
    requirements before SSM is ever consulted.

    Raises ``ConfigError`` — never a bare pydantic error — so the CLI has one
    thing to catch and map to exit code 2.

    Does not handle: caching. ``load_settings_cached`` does, for the CLI.
    """
    overrides = dict(overrides or {})
    local_only = bool(overrides.get("local_only")) or _env_truthy("INGEST_LOCAL_ONLY")
    if local_only:
        overrides["local_only"] = True

    resolved: dict[str, Any] = {}
    for env_name, ssm_name in SSM_FALLBACKS.items():
        key = env_name.lower()
        if key in overrides and overrides[key] is not None:
            continue
        if os.environ.get(env_name):
            continue
        # Local mode never contacts AWS: the whole point is that it works with no
        # AWS configuration present.
        if use_ssm and not local_only:
            value = resolve_ssm(ssm_name)
            if value is not None:
                resolved[key] = value

    resolved.update({k: v for k, v in overrides.items() if v is not None})

    try:
        return Settings(**resolved)
    except Exception as exc:
        raise ConfigError(_format_validation_error(exc)) from exc


@lru_cache(maxsize=1)
def load_settings_cached() -> Settings:
    """``load_settings`` with no overrides, memoized for the process lifetime."""
    return load_settings()


def _env_truthy(name: str) -> bool:
    """Return whether an env var is set to something meaning 'yes'."""
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _format_validation_error(exc: Exception) -> str:
    """Turn a pydantic ValidationError into a message naming the env vars.

    pydantic reports lower-case field names; operators set upper-case env vars.
    Translating here is the difference between an actionable error and a
    scavenger hunt.
    """
    from pydantic import ValidationError

    if not isinstance(exc, ValidationError):
        return str(exc)

    lines: list[str] = []
    for err in exc.errors():
        loc = err.get("loc") or ()
        field = str(loc[0]) if loc else ""
        message = err.get("msg", "")
        if err.get("type") == "missing" and field:
            lines.append(f"{field.upper()} is required but was not set (env var or SSM)")
        elif field:
            lines.append(f"{field.upper()}: {message}")
        else:
            lines.append(message)
    return "; ".join(lines)
