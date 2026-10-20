#!/usr/bin/env bash
set -euo pipefail

version=2.14.7
archive="nats-server-v${version}-linux-amd64.tar.gz"
release="https://github.com/nats-io/nats-server/releases/download/v${version}"
stage="${RUNNER_TEMP:?RUNNER_TEMP must be set}/nats-session-channel"
mkdir -p "$stage"
curl --proto '=https' --tlsv1.2 -fsSL -o "$stage/SHA256SUMS" "$release/SHA256SUMS"
curl --proto '=https' --tlsv1.2 -fsSL -o "$stage/$archive" "$release/$archive"
(
  cd "$stage"
  awk -v archive="$archive" '$2 == archive { print }' SHA256SUMS | sha256sum --check --strict -
  printf 'e5c20b1cb2c0566b54c544312e91e011f9e130c5c80f16a14f4cf28ef30b8be2  %s\n' "$archive" | sha256sum --check --strict -
  tar -xzf "$archive"
)
binary="$stage/nats-server-v${version}-linux-amd64/nats-server"
test -x "$binary"
echo "NATS_BIN=$binary" >> "${GITHUB_ENV:?GITHUB_ENV must be set}"
