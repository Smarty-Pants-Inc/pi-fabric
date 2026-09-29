#!/usr/bin/env bash
# Dev1-only real Forge proof; no build/install/configuration/authentication changes.
# Usage: bash proof/forge-ssh-bridge.sh /absolute/path/to/config.json
# Config fields and prerequisites are documented at the top of forge-ssh-bridge.mjs.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
[[ $# == 1 && -f $1 ]] || { echo "usage: $0 CONFIG.json" >&2; exit 2; }
# GNU timeout bounds the driver too; its finally handles normal timeout signals.
# Evidence and all scratch state belong under the configured .local directory.
exec timeout --signal=TERM --kill-after=30s 900s bun "$here/forge-ssh-bridge.mjs" "$1"
