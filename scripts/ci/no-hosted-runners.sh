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

# Match hosted image scalar/list values, not check names or release asset names.
# Author-written hosted-exception comments never authorize a hosted runner.
awk '
  {
    if ($0 ~ /^[[:space:]]*#[[:space:]]*hosted-exception[[:space:]]*:/) {
      printf "%s:%d: hosted-exception marker forbidden: no hosted exceptions are approved\n", FILENAME, FNR > "/dev/stderr"
      failed = 1
    }
    if ($0 !~ /^[[:space:]]*#/ &&
        (($0 ~ /(^[[:space:]]*-[[:space:]]*|:[[:space:]]*|\[[[:space:]]*|,[[:space:]]*)["\047]?(ubuntu|windows|macos)-[[:alnum:]_.-]+["\047]?([[:space:]]*($|,|\]|#))/) ||
         ($0 ~ /(^|[[:space:]-])(runs-on|os|runner):/ &&
          $0 ~ /(^|[^[:alnum:]_-])(ubuntu|windows|macos)-[[:alnum:]_.-]+([^[:alnum:]_-]|$)/))) {
      printf "%s:%d: unapproved hosted runner: %s\n", FILENAME, FNR, $0 > "/dev/stderr"
      failed = 1
    }
  }
  END { exit failed ? 1 : 0 }
' "${workflows[@]}"
