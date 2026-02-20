#!/bin/bash

set -a
source .env
set +a

export PATH=".venv/bin:$PATH"

# ---------------------------------------------------------------------------
# Sentry CLI integration
# ---------------------------------------------------------------------------
# If SENTRY_DSN is set and sentry-cli is available, any non-zero exit code
# from the sync script is reported to Sentry as an error event via
# `sentry-cli send-event`.
# ---------------------------------------------------------------------------

PYTHON=".venv/bin/python"
SCRIPT="immich-sync.py"

if [[ -z "${SENTRY_DSN}" ]]; then
    echo "[run.sh] SENTRY_DSN is not set — running without Sentry reporting."
    "$PYTHON" "$SCRIPT" "$@"
    exit $?
fi

if ! command -v sentry-cli &>/dev/null; then
    echo "[run.sh] sentry-cli not found — running without Sentry reporting." >&2
    "$PYTHON" "$SCRIPT" "$@"
    exit $?
fi

# Run the sync and capture the exit code
"$PYTHON" "$SCRIPT" "$@"
EXIT_CODE=$?

if [[ $EXIT_CODE -ne 0 ]]; then
    # Report the failure to Sentry as an error event
    sentry-cli send-event \
        --level error \
        --message "lightroom-immich-sync exited with code ${EXIT_CODE}" \
        --tag "exit_code:${EXIT_CODE}"
fi

exit $EXIT_CODE
