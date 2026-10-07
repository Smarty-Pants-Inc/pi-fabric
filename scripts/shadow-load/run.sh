#!/usr/bin/env bash
# Full-load shadow test of a Fabric release candidate (smarty-dev#6477 stage 1).
#   scripts/shadow-load/run.sh <release-dir> [minutes] [orchestrate.mjs flags...]
# Exit 0 PASS, 1 FAIL, 2 harness error. The report directory is printed at the end.
set -euo pipefail
if [[ $# -lt 1 || "$1" == -h || "$1" == --help ]]; then
  sed -n '2,4p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
fi
release=$1; shift
minutes=20
if [[ $# -gt 0 && "$1" =~ ^[0-9]+([.][0-9]+)?$ ]]; then minutes=$1; shift; fi
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
node=${SHADOW_NODE:-node}
# One host, lowest CPU priority: the shadow mesh must not starve the fleet sharing this machine.
exec nice -n 19 "$node" --max-old-space-size=512 "$here/orchestrate.mjs" --release "$release" --minutes "$minutes" "$@"
