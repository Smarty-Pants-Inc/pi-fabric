/**
 * smarty-dev#8257: task-local `systemd-run` PATH shim for capped task scopes.
 *
 * A capped task runs in its own memory-capped cgroup leaf. `systemd-run --user --scope`
 * would normally create a new transient scope outside that leaf and leave the cap. The
 * shim turns exactly that mode into an in-place `exec` of the target command (same pid,
 * same leaf, argv preserved byte for byte). A requested `MemoryMax=` can only lower the
 * whole task leaf's `memory.max`; it never raises the root cap and swap stays 0.
 * Every other systemd-run mode is passed unchanged to the absolute real binary.
 *
 * The result is a self-contained POSIX shell script: `exec` keeps the pid on any runtime
 * (Bun has no `process.execve`) and nothing in it evaluates caller-supplied text.
 * It is a guard for supported tools, not a same-UID sandbox.
 */

/** Single-quote a value for POSIX sh. Callers reject newlines and NUL first. */
const shellQuote = (value: string): string => `'${value.replaceAll("'", `'\\''`)}'`;

const assertSafe = (label: string, value: string, pattern: RegExp): void => {
  if (!pattern.test(value) || value.includes("\0") || /[\r\n]/.test(value)) {
    throw new Error(`taskScopeWrapperSource: invalid ${label}: ${JSON.stringify(value)}`);
  }
};

export const taskScopeWrapperSource = (
  realSystemdRun: string,
  cgroupRelative: string,
  memoryMaxBytes: number,
): string => {
  assertSafe("realSystemdRun (absolute path)", realSystemdRun, /^\/[^\0]*$/);
  if (realSystemdRun.split("/").some((part) => part === "..")) {
    throw new Error(`taskScopeWrapperSource: realSystemdRun must be normalized: ${realSystemdRun}`);
  }
  // A unified (v2) cgroup path from the verified receipt: no traversal, no shell-active text.
  assertSafe("cgroupRelative", cgroupRelative, /^(\/[A-Za-z0-9_.:@\\-]+)+$/);
  if (cgroupRelative.split("/").some((part) => part === "." || part === "..")) {
    throw new Error(`taskScopeWrapperSource: cgroupRelative must not traverse: ${cgroupRelative}`);
  }
  if (!Number.isSafeInteger(memoryMaxBytes) || memoryMaxBytes < 1) {
    throw new Error(`taskScopeWrapperSource: memoryMaxBytes must be a positive safe integer, got ${String(memoryMaxBytes)}`);
  }
  return `#!/bin/sh
# Fabric task systemd-run shim (smarty-dev#8257). Generated; do not edit.
# 'systemd-run --user --scope [-p MemoryMax=|MemorySwapMax=] [-u UNIT] [--description TEXT] [-q] [-G] -- CMD...'
# execs CMD in place inside this task's capped cgroup leaf. Other modes run the real binary.
real=${shellQuote(realSystemdRun)}
leaf=${shellQuote(cgroupRelative)}
root_max=${memoryMaxBytes}

fail() {
  printf 'fabric task systemd-run: %s\\n' "$1" >&2
  exit 1
}

# Mode scan: find --user and --scope among the options before the command.
user=0 scope=0 skip=0
for arg do
  if [ "$skip" = 1 ]; then skip=0; continue; fi
  case $arg in
    --) break ;;
    --user) user=1 ;;
    --scope) scope=1 ;;
    -u|-p|-H|-M|-E|--unit|--property|--description|--slice|--host|--machine|--uid|--gid|--nice|\\
    --working-directory|--root-directory|--setenv|--service-type|--expand-environment|--json|--job-mode|\\
    --background|--path-property|--socket-property|--timer-property|--on-active|--on-boot|--on-startup|\\
    --on-unit-active|--on-unit-inactive|--on-calendar) skip=1 ;;
    # getopt permits e.g. -qp VALUE or -qu UNIT. Consume that value during the
    # mode scan so a later --scope cannot be missed; strict parsing refuses
    # unsupported clusters instead of falling through to an escaping scope.
    -q*p|-G*p|-q*u|-G*u) skip=1 ;;
    -*) ;;
    *) break ;;
  esac
done
[ "$user$scope" = 11 ] || exec "$real" "$@"

# Only a process inside this task's leaf is covered; anything else keeps real semantics.
self=
while IFS= read -r line; do
  case $line in 0::*) self=\${line#0::} ;; esac
done 2>/dev/null < /proc/self/cgroup
case $self in
  "$leaf"|"$leaf"/*) ;;
  *) exec "$real" "$@" ;;
esac

# quantity VALUE: sets bytes (empty for infinity), clamped to root_max; returns 1 if invalid.
quantity() {
  bytes=
  case $1 in
    infinity) return 0 ;;
    ''|*[!0-9KMGT]*) return 1 ;;
  esac
  n=\${1%[KMGT]}
  suffix=\${1#"$n"}
  case $n in ''|*[!0-9]*) return 1 ;; esac
  while :; do case $n in 0?*) n=\${n#0} ;; *) break ;; esac; done
  case $suffix in
    '') m=1 ;;
    K) m=1024 ;;
    M) m=1048576 ;;
    G) m=1073741824 ;;
    T) m=1099511627776 ;;
    *) return 1 ;;
  esac
  if [ \${#n} -gt 18 ] || [ "$n" -gt $((root_max / m)) ]; then bytes=$root_max; else bytes=$((n * m)); fi
}

want=
property() {
  case $1 in
    MemoryMax=*)
      quantity "\${1#MemoryMax=}" || fail "invalid property $1"
      if [ -n "$bytes" ] && { [ -z "$want" ] || [ "$bytes" -lt "$want" ]; }; then want=$bytes; fi ;;
    MemorySwapMax=*)
      # Validated only: the task leaf already has memory.swap.max=0, which cannot be tightened.
      quantity "\${1#MemorySwapMax=}" || fail "invalid property $1" ;;
    *) fail "unsupported property in a capped task: $1 (supported: MemoryMax, MemorySwapMax)" ;;
  esac
}

unit() {
  case $1 in
    ''|*[!A-Za-z0-9:_.@\\\\-]*) fail "invalid unit name: $1" ;;
  esac
}

while [ $# -gt 0 ]; do
  arg=$1
  case $arg in
    --) shift; break ;;
    --user|--scope|-q|--quiet|-G|--collect|--no-ask-password) shift ;;
    -p|--property) [ $# -ge 2 ] || fail "$arg needs a value"; property "$2"; shift 2 ;;
    --property=*) property "\${arg#--property=}"; shift ;;
    -u|--unit) [ $# -ge 2 ] || fail "$arg needs a value"; unit "$2"; shift 2 ;;
    --unit=*) unit "\${arg#--unit=}"; shift ;;
    --description) [ $# -ge 2 ] || fail "$arg needs a value"; shift 2 ;;
    --description=*) shift ;;
    --*) fail "unsupported option in a capped task: $arg (a new scope would leave the task memory cap)" ;;
    -p?*) property "\${arg#-p}"; shift ;;
    -u?*) unit "\${arg#-u}"; shift ;;
    -*) fail "unsupported option in a capped task: $arg (a new scope would leave the task memory cap)" ;;
    *) break ;;
  esac
done
[ $# -gt 0 ] || fail "missing command"

if [ -n "$want" ]; then
  dir=/sys/fs/cgroup$leaf
  IFS= read -r swap < "$dir/memory.swap.max" || fail "cannot read $dir/memory.swap.max"
  [ "$swap" = 0 ] || fail "task leaf swap is not 0 ($swap)"
  IFS= read -r current < "$dir/memory.max" || fail "cannot read $dir/memory.max"
  case $current in ''|*[!0-9]*) fail "task leaf memory.max is not a byte cap ($current)" ;; esac
  limit=$want
  [ "$limit" -le "$root_max" ] || limit=$root_max
  if [ "$limit" -lt "$current" ]; then
    IFS= read -r used < "$dir/memory.current" || fail "cannot read $dir/memory.current"
    [ "$limit" -gt "$used" ] || fail "MemoryMax=$limit is not above the whole task's current usage ($used bytes)"
    printf '%s\\n' "$limit" > "$dir/memory.max" || fail "cannot lower $dir/memory.max to $limit"
    IFS= read -r applied < "$dir/memory.max" || fail "cannot verify $dir/memory.max"
    case $applied in ''|*[!0-9]*) fail "memory.max not applied ($applied)" ;; esac
    [ "$applied" -le "$limit" ] || fail "memory.max not applied ($applied > $limit)"
  fi
fi

exec "$@"
`;
};
