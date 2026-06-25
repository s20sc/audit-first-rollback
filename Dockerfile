# Reproducibility container for the audit-first rollback artifact.
#
# Build:
#   docker build -t audit-first .
#
# Reproduce the twelve-cell chaos grid (1,200 trials, a few minutes):
#   docker run --rm audit-first
#
# Extract the latency distribution:
#   docker run --rm audit-first python scripts/run_latency.py
#
# Re-verify the sign-check assertions:
#   docker run --rm audit-first python scripts/sign_check.py
#
# Open an interactive shell:
#   docker run --rm -it audit-first /bin/bash

FROM python:3.11-slim-bookworm

LABEL org.opencontainers.image.title="audit-first-rollback"
LABEL org.opencontainers.image.description="Reference implementation and reproduction harness for audit-first rollback semantics"
LABEL org.opencontainers.image.licenses="Apache-2.0"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Stdlib-only at runtime; no external dependencies.
COPY pyproject.toml README.md LICENSE NOTICE reproduce.py ./
COPY src/      ./src/
COPY harness/  ./harness/
COPY scripts/  ./scripts/
COPY data/     ./data/
COPY tests/    ./tests/

RUN pip install -e .

# Default: re-run the 1,200-trial chaos grid, overwriting the reference
# data under data/chaos-grid/ and printing the PASS/FAIL verdict.
CMD ["python", "scripts/run_chaos_grid.py", "--trials", "50", "--seed", "112026"]
