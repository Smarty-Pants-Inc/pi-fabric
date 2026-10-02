#!/usr/bin/env bash
# smarty-dev#1246 policy: this repository has no approved hosted-runner exceptions.
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
