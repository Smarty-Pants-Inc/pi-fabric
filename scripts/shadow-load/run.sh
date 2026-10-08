#!/usr/bin/env bash
# Full-load shadow soak of a Fabric release candidate against a baseline (smarty-dev#6477 stage 1).
#   scripts/shadow-load/run.sh <candidate-release> <baseline-release> [minutes] [orchestrate.mjs flags...]
# Default 30 minutes. Exit 0 PASS, 1 FAIL, 2 harness error. The report directory is printed at the end.
set -euo pipefail
if [[ $# -lt 2 || "$1" == -h || "$1" == --help ]]; then
  sed -n '2,4p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
fi
candidate=$1; baseline=$2; shift 2
minutes=30
if [[ $# -gt 0 && "$1" =~ ^[0-9]+([.][0-9]+)?$ ]]; then minutes=$1; shift; fi
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
node=${SHADOW_NODE:-node}
# The CPU burner runs at nice 0 so the harness (nice 19) is CPU-starved like the fleet's lock holders.
burn_pct=80
burn_threads=$(nproc)
burn_duty=
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
  case "${args[i]}" in
    --burn-pct) burn_pct=${args[i+1]:-80} ;;
    --burn-threads) burn_threads=${args[i+1]:-$burn_threads} ;;
    --burn-duty) burn_duty=${args[i+1]:-} ;;
  esac
done
stats=$(mktemp "${TMPDIR:-/tmp}/shadow-burner-XXXXXX")
burner=
cleanup() {
  if [[ -n $burner ]]; then kill "$burner" 2>/dev/null || true; wait "$burner" 2>/dev/null || true; fi
  rm -f "$stats" "$stats.tmp"
}
trap cleanup EXIT
trap 'exit 130' INT TERM
if [[ $burn_pct != 0 ]]; then
  "$node" "$here/burner.mjs" --target-pct "$burn_pct" --threads "$burn_threads" ${burn_duty:+--duty "$burn_duty"} --stats "$stats" &
  burner=$!
fi
set +e
nice -n 19 "$node" --max-old-space-size=512 "$here/orchestrate.mjs" --release "$candidate" --baseline "$baseline" \
  --minutes "$minutes" --burner-stats "$stats" "$@"
code=$?
set -e
exit $code
