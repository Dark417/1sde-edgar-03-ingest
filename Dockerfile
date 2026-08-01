# syntax=docker/dockerfile:1

# ---------------------------------------------------------------- builder
FROM python:3.11-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    VIRTUAL_ENV=/opt/venv

WORKDIR /build

# The contracts wheel is fetched from the S3 wheels prefix by CI (pip cannot
# read s3:// directly) and passed in as build context. AGENTS.md §2.
COPY wheels/ /wheels/
COPY pyproject.toml README.md ./
COPY src/ ./src/

RUN uv venv "$VIRTUAL_ENV" \
 && uv pip install --python "$VIRTUAL_ENV/bin/python" \
      --find-links /wheels \
      --no-cache .

# ---------------------------------------------------------------- runtime
FROM python:3.11-slim AS runtime

# Non-root. The task writes only to the landing zone and a temp checkpoint;
# nothing in the image needs to be writable at runtime.
RUN groupadd --gid 1000 ingest \
 && useradd --uid 1000 --gid 1000 --create-home --shell /usr/sbin/nologin ingest

COPY --from=builder --chown=1000:1000 /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER 1000:1000
WORKDIR /home/ingest

# No HEALTHCHECK on purpose. This is a batch task that runs for ~90 seconds and
# exits, not a service — there is nothing to poll, and a HEALTHCHECK would make
# ECS treat a normal exit as a failure. Someone will eventually try to "fix"
# its absence; this comment is why they should not.

ENTRYPOINT ["python", "-m", "ingest.cli"]
CMD ["config-check"]
