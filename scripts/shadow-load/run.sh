#!/usr/bin/env bash
# Full-load shadow soak of a Fabric release candidate against a baseline (smarty-dev#6477 stage 1).
#   scripts/shadow-load/run.sh <candidate-release> <baseline-release> [minutes] [--profile fleet|legacy] [orchestrate.mjs flags...]
# Default 30 minutes, profile fleet (README.md, calibration). Exit 0 PASS, 1 FAIL, 2 harness error. The report directory is printed at the end.
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

# Profiles. Flags given on the command line come after the profile's, so they override it.
#   fleet  (default): the values calibrated on epyc1 against Ryzen 1's fleet (README.md, "Calibration").
#   legacy: orchestrate.mjs's built-in defaults, i.e. the harness before calibration (a0c70c6d).
profile=fleet
rest=()
while [[ $# -gt 0 ]]; do
  if [[ "$1" == --profile ]]; then profile=${2:?--profile needs fleet or legacy}; shift 2; else rest+=("$1"); shift; fi
done
case "$profile" in
  fleet) profile_flags=(--mains-per-process 4 --heap-mb 640 --mem-budget-mb 32768 --state-mb 10
    --actor-save-s 10 --saves-in-flight 4 --churn-s 200 --burn-duty 0.5) ;;
  legacy) profile_flags=() ;;
  *) echo "unknown --profile $profile (fleet or legacy)" >&2; exit 2 ;;
esac
args=("${profile_flags[@]}" --profile "$profile" "${rest[@]}")

# The CPU burner runs at nice 0 so the harness (nice 19) is CPU-starved like the fleet's lock holders.
# --burn-duty D runs it at a fixed duty (D x threads CPUs); --burn-duty off returns to the controller.
burn_pct=80
burn_threads=$(nproc)
burn_duty=
for ((i = 0; i < ${#args[@]}; i++)); do
  case "${args[i]}" in
    --burn-pct) burn_pct=${args[i+1]:-80} ;;
    --burn-threads) burn_threads=${args[i+1]:-$burn_threads} ;;
    --burn-duty) burn_duty=${args[i+1]:-} ;;
  esac
done
[[ $burn_duty == off ]] && burn_duty=
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
  --minutes "$minutes" --burner-stats "$stats" "${args[@]}"
code=$?
set -e
exit $code
