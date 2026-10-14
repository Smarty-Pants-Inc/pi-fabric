/**
 * smarty-dev#8257: task-local `systemd-run` PATH shim for capped task scopes.
 *
 * A capped task runs in its own memory-capped cgroup leaf. `systemd-run --user --scope`
 * would normally create a new transient scope outside that leaf and leave the cap. The
 * shim turns exactly that mode into an in-place `exec` of the target command (same pid,
 * same leaf, argv preserved byte for byte). A requested `MemoryMax=` can only lower the
 * whole task leaf's `memory.max`; it never raises the root cap and swap stays 0.
 * Inside the task leaf, any other `--user` mode (service, timer, path, socket, shell or an
 * unknown option) is refused: a user-manager unit would escape the task memory cap and
 * agents.stop. Non-user modes, `--version`/`--help`, and callers outside the leaf are passed
 * unchanged to the absolute real binary.
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
  residentShared = false,
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
# execs CMD in place inside this task's capped cgroup leaf. Other --user modes are refused
# inside the leaf; non-user modes, --version/--help and callers outside the leaf run the real binary.
real=${shellQuote(realSystemdRun)}
leaf=${shellQuote(cgroupRelative)}
root_max=${memoryMaxBytes}
resident_shared=${residentShared ? 1 : 0}

fail() {
  printf 'fabric task systemd-run: %s\\n' "$1" >&2
  exit 1
}

# systemd-run 259 long options (getopt_long table, incl. hidden --system/--capsule/--tty);
# a trailing = marks a required argument.
long_options='help version user system capsule= scope unit= description= slice= slice-inherit
remain-after-exit expand-environment= send-sighup host= machine= service-type= wait uid= gid= nice=
setenv= property= tty pty pty-late pipe quiet verbose on-active= on-boot= on-startup= on-unit-active=
on-unit-inactive= on-calendar= on-timezone-change on-clock-change timer-property= path-property=
socket-property= no-block no-ask-password collect working-directory= same-dir shell job-mode=
ignore-failure background= json= root-directory= same-root-dir no-pager'

# canon ARG: resolve a long option the way getopt_long does. An exact name wins, otherwise a unique
# prefix (--us = --user). Sets opt to the canonical --name plus the original =value suffix
# byte for byte, and takes=1 if it requires an argument. Returns 1 for an ambiguous or unknown
# name, or a value given to a no-argument option. Only compares; never evaluates the text.
canon() {
  name=\${1#--}
  name=\${name%%=*}
  opt= takes=0
  case $name in ''|*[!a-z-]*) return 1 ;; esac
  for known in $long_options; do
    case \${known%=} in
      "$name") opt=$known; break ;;
      "$name"*) if [ -z "$opt" ]; then opt=$known; else opt=' '; fi ;;
    esac
  done
  case $opt in ''|' ') return 1 ;; esac
  case $opt in *=) takes=1; opt=\${opt%=} ;; esac
  set -- "\${1#--"$name"}"
  [ "$takes" = 1 ] || [ -z "$1" ] || return 1
  opt=--$opt$1
}

# Mode scan: find --user, --scope and --version/--help among the options before the command
# (systemd-run stops option parsing at the first non-option, like this scan). Long options are
# resolved by canon; an ambiguous or unknown option (which may take a value) makes the scan
# unprovable: unknown=1.
user=0 scope=0 info=0 skip=0 unknown=0
for arg do
  if [ "$skip" = 1 ]; then skip=0; continue; fi
  case $arg in
    --) break ;;
    --*)
      canon "$arg" || { unknown=1; break; }
      case $opt in
        --user) user=1 ;;
        --scope) scope=1 ;;
        --version|--help) info=1 ;;
        *=*) ;;
        *) skip=$takes ;;
      esac ;;
    -?*)
      # getopt short cluster (e.g. -qp VALUE, -qpVALUE, -qu UNIT): walk it so a value is
      # consumed exactly when getopt would consume it and a later --scope/--user is never hidden.
      rest=\${arg#-}
      while [ -n "$rest" ]; do
        c=\${rest%"\${rest#?}"}
        rest=\${rest#?}
        case $c in
          h) info=1 ;;
          C|H|M|u|p|E) [ -n "$rest" ] || skip=1; break ;;
          r|d|R|t|T|P|q|v|G|S) ;;
          *) unknown=1; break 2 ;;
        esac
      done ;;
    *) break ;;
  esac
done
if [ "$unknown" = 0 ]; then
  [ "$user" = 1 ] || exec "$real" "$@"
  # --version/--help: systemd-run prints and exits before creating any unit.
  [ "$info" = 0 ] || exec "$real" "$@"
fi

# Only a process inside this task's leaf is covered; anything else keeps real semantics.
self=
while IFS= read -r line; do
  case $line in 0::*) self=\${line#0::} ;; esac
done 2>/dev/null < /proc/self/cgroup
case $self in
  "$leaf"|"$leaf"/*) ;;
  *) exec "$real" "$@" ;;
esac

[ "$unknown" = 0 ] || fail "refused unknown or ambiguous option before the command in a capped task: rule smarty-dev#8257: user-manager units must not escape task memory cap/agents.stop"

# Inside the leaf only the in-place --user --scope mode is supported; any other user-manager
# unit (service, timer, path, socket, shell, unknown option) is refused, never dispatched.
[ "$scope" = 1 ] || fail "refused --user without --scope in a capped task: rule smarty-dev#8257: user-manager units must not escape task memory cap/agents.stop"

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
    --?*) canon "$arg" || fail "unknown or ambiguous option in a capped task: $arg"; arg=$opt ;;
  esac
  case $arg in
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
    if [ "$resident_shared" = 1 ]; then
      printf 'fabric-resident-shared-scope: code=RESIDENT_SHARED_CAP_TIGHTENING requested=%s current=%s\\n' "$limit" "$current" >&2
      exit 125
    fi
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
