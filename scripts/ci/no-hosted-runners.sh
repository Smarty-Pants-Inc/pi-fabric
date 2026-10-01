#!/usr/bin/env bash
# Minimal smarty-dev#1246 guard; reconcile with dev-lead l1246's canonical guard.
set -euo pipefail

workflow_dir="${1:-.github/workflows}"
shopt -s nullglob
workflows=("$workflow_dir"/*.yml "$workflow_dir"/*.yaml)
if ((${#workflows[@]} == 0)); then
  echo "No workflows found in $workflow_dir" >&2
  exit 1
fi

# Match hosted image scalar/list values, not check names or release asset names.
# An exception applies only to the immediately following physical line.
awk '
  FNR == 1 { previous = "" }
  {
    if ($0 !~ /^[[:space:]]*#/ &&
        (($0 ~ /(^[[:space:]]*-[[:space:]]*|:[[:space:]]*|\[[[:space:]]*|,[[:space:]]*)["\047]?(ubuntu|windows|macos)-[[:alnum:]_.-]+["\047]?([[:space:]]*($|,|\]|#))/) ||
         ($0 ~ /(^|[[:space:]-])(runs-on|os|runner):/ &&
          $0 ~ /(^|[^[:alnum:]_-])(ubuntu|windows|macos)-[[:alnum:]_.-]+([^[:alnum:]_-]|$)/)) &&
        previous !~ /^[[:space:]]*# hosted-exception:/) {
      printf "%s:%d: unapproved hosted runner: %s\n", FILENAME, FNR, $0 > "/dev/stderr"
      failed = 1
    }
    previous = $0
  }
  END { exit failed ? 1 : 0 }
' "${workflows[@]}"
