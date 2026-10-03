#!/usr/bin/env bash
# smarty-dev#1246 policy: only test.yml/windows has the recorded Paul 2026-10-03
# PR-time windows-latest exception (pi-fabric#254 5964538892); exit: smarty-dev#3580,
# then Option A (Dev3 main-only). The parser enforces its exact allowlist.
set -euo pipefail

workflow_dir="${1:-.github/workflows}"
shopt -s nullglob
workflows=("$workflow_dir"/*.yml "$workflow_dir"/*.yaml)
if ((${#workflows[@]} == 0)); then
  echo "No workflows found in $workflow_dir" >&2
  exit 1
fi

# Resolve the script relative to this wrapper, not to the caller's working directory.
# The existing yaml dependency must be installed before invoking this guard.
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec node "$script_dir/no-hosted-runners.mjs" "${workflows[@]}"
