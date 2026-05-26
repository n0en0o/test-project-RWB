#!/usr/bin/env sh
set -eu

URL="${URL:-http://localhost:8080/trends?limit=10}"
CONNECTIONS="${CONNECTIONS:-200}"
DURATION="${DURATION:-30s}"

if ! command -v hey >/dev/null 2>&1; then
  echo "hey is not installed. Install it with: go install github.com/rakyll/hey@latest" >&2
  exit 1
fi

hey -z "$DURATION" -c "$CONNECTIONS" "$URL"
