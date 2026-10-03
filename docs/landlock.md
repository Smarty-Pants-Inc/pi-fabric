# Landlock filesystem confinement for Fabric bash

This opt-in Linux feature confines **local `pi.bash` calls through Fabric**
(including cooperative captured bash middleware). The text guard remains an
independent early warning. Native tools outside Fabric, `user_bash`, direct
`extensions.*`, PowerShell, and trusted Node/Bun/CPython executor escape hatches
are **not** covered. This is a filesystem safety boundary, not a hostile-code or
whole-session sandbox. Do not advertise those other execution paths as confined.

## Modes and the honest trial

Kernel 6.8 / Landlock ABI 4 has **no audit-only/warn mode**. There is no cheap,
rootless mechanism that can let arbitrary commands run unchanged while reporting
all the mutations Landlock would deny. Running a command twice is not a safe
substitute; text inspection and inotify are not denial evidence. Consequently
`warn` is rejected at configuration load, rather than accepted with fake data.
The authorized fallback is implemented:

```json
{ "executor": { "landlock": { "mode": "enforce" } } }
```

- `mode: "off"` is the default until rollout; bash behavior is unchanged.
- `mode: "enforce"` requires Linux with active Landlock ABI >=4. Unsupported
  kernels, missing helper, malformed policy, setup failures and incompatible
  opaque/managed overrides fail closed, never retry through an unconfined shell.
- macOS and Windows leave the wrapper **disabled**, even if `enforce` is selected.
- Put `executor.landlock.disabled: true` in the **global agent `fabric.json`** to
  kill confinement fleet-wide. Project values for `disabled` are ignored, so a
  lane cannot defeat that kill switch. The host file is re-read on every
  enforced local bash call, so already-running lanes stop confining on their
  next call without a reload (already-spawned confined children stay confined).
  The settings UI exposes both keys; change the save scope to global for the switch.

Run the 24-hour **enforce trial on one approved lane**, not a fictitious warn
soak. Neither this change nor its tests enable a trial or change installed/global
configuration. Review the policy first; fleet enforcement is a separate rollout.

### Per-command escape

Use the exact leading assignment in a `pi.bash` command:

```sh
PI_FABRIC_LANDLOCK_ESCAPE=1 your-command arguments
```

Fabric removes this reserved prefix, records an `escape` event **before spawning**,
and runs that one command unconfined through the same cooperative filters.
Every use emits a visible escape notice and is appended to
`<session cwd>/.pi/landlock-audit.jsonl`. If mandatory logging fails, no command
runs. An assignment inside the shell body, quoted/encoded spelling, `env ...`, or
an ambient inherited flag cannot lift a restriction already imposed by the
kernel. The escape does not bypass the text guard, approvals or timeouts.

The journal records UTC time, role, execution cwd, shell job directory and a
SHA-256 command digest (not potentially secret-bearing command text). Enforced
starts also record the actual write grants. These are **start/escape records, not
kernel denial/audit events**. Command stderr/status records actual failures;
it is not a complete syscall audit. Logs are lane-local and not tamper-evident:
commands/escapes can edit their own lane, and the same Unix user owns the host.
Audit opens pin `.pi` and reject symlinks, hard links and non-regular files.

## Paul's one-glance write policy

[`config/landlock-roles.json`](../config/landlock-roles.json) is the **only policy
file**. `default` entries apply to every role; `roles` appends reviewed additions
for the role in host `SMARTY_ROLE` (the `@commit` suffix is stripped). All three
roles currently inherit the same minimal list. Unknown roles receive only that
list, never a wider fallback. Each entry has a one-line reason.

- `$CWD` is the **session lane**, not a per-command `cwd` or shell `cd`.
- `$TMPDIR` is host-supplied only when it is an absolute, real directory owned by
  the current uid with no group/other permissions. Shared `/tmp`, `/var/tmp`,
  `/` and the home directory are never granted as TMPDIR. Otherwise a private
  `0700` temporary directory is created at first enforced use and exported to
  the child. Fabric-owned fallback temp is removed only after session close
  **and** confirmed exit of every operation that used it; an abort with an
  unknown exit retains it.
- **Grant identity.** Session-stable grants (`$CWD`, `$TMPDIR`, `$GIT_COMMON_DIR`,
  `$AGENT_RUN_DIR`, caches, devices) are resolved and pinned (`realpath`,
  device, inode) by the host at first enforced use, before any confined command
  runs. Each later call re-validates them; a replaced, renamed or symlinked grant
  is refused ("changed identity"), never re-credited. The helper receives
  `dev:ino:/path` and checks the opened `O_PATH|O_NOFOLLOW` descriptor's
  identity before adding the rule, closing the validate/open race.
- `$RUN_DIR` is this command's exact shell-job directory, never its shared parent.
- `$AGENT_RUN_DIR` is the worker's own run directory, supplied by Fabric as
  `PI_FABRIC_AGENT_RUN_DIR`, **not** the fleet/nested run root.
- `$GIT_COMMON_DIR` comes from the lane's `.git` / worktree `commondir` metadata.
  This intentionally grants shared repository administration/objects; it is not
  isolation between worktrees of the same repository.
- `~/.cache`, `~/.npm`, `~/.bun`, `/dev/null`, `/dev/tty` and `/dev/pts` are explicit
  shared exceptions. In particular `~/.bun` includes installed Bun tools, not
  only cache data. Review this breadth before rollout.

Absent optional paths are omitted, **not** created and not widened to their
parent. A grant absent (or dangling) at the first enforced call stays omitted for
the session: a later-created path is never admitted. Hosts must provision needed
cache directories before an enforced run. Temp custody starts before middleware
preparation; after close, a late launch is refused.
Paths containing newline, carriage return or NUL are rejected (the native
adapter's trusted launch format is newline-delimited). `/` is never a grant.
Policy changes are host/package policy, not an agent authorization mechanism;
keep installed package/config trusted if adversarial tampering is in scope.

## Native implementation and portability

`native/fabric-landlock.c` is a small MIT syscall adapter, built **statically** by
`scripts/build-landlock.mjs` during `bun run build` on Linux. Linux builds need
`cc` (or `CC`) and Linux syscall headers; there is no root, linked libc, dynamic
loader, libc static-library requirement or runtime compiler step. The freestanding
entry/syscall adapters support Linux x86_64 and aarch64; only x86_64 is tested here.
Other Linux architectures need a reviewed adapter before they can build. The package ships `dist/native/fabric-landlock`, the MIT C source and
its reviewed JSON policy. Build on each target Linux architecture; copying an
x86 binary to ARM or a non-Linux-built package to Linux does not enable it.
macOS/Windows builds skip C compilation and keep bash unchanged.

`landrun` was considered as a no-root static Go CLI, but there is no locally
installed/pinned binary or locally verifiable license/ABI-v4 contract in this
offline lane. We did not fetch GitHub, dependencies or third-party binaries.
The `ponytail:` comment selects the auditable dependency-free C adapter instead.

The helper replaces the OS-spawned shell executable. It applies `no_new_privs`
and Landlock **before `exec` of the real shell**, so `BASH_ENV`, command prefixes,
redirections, Python/Perl, subprocesses and delayed/background children inherit
confinement. Cooperative `spawnHook` and output filters remain in order; reserved
policy environment keys are written by Fabric after middleware env filtering.
Existing Pi local operations retain cancellation, process groups and timeouts.

Handled filesystem rights: write-file, truncate, remove-file/directory, every
make/create right, and refer (rename/hard-link). Read/list/execute rights remain
unrestricted. Cross-boundary mutation returns `EACCES`; a hard link that would
gain access may return `EXDEV` per Landlock semantics. Inherited non-stdio file
descriptors are closed before shell exec.

## Limits / next slice

- **Signals are not confined on ABI 4.** AppArmor/PID namespace or ABI 6 signal
  scope is later work. No network, IPC broker or process-read/ptrace isolation.
- Kernel 6.8 does not mediate chmod/chown/timestamps and all other metadata-only
  operations with these Landlock rights. This slice protects file data and
  creation/removal/rename, not every possible filesystem effect.
- Existing hard-link aliases and explicitly shared cache/git directories weaken
  ownership separation. Open inherited stdout/stderr pipes remain writable.
  External services can mutate files on a command's behalf and are not confined.
- Cooperation is trusted: middleware must delegate to Fabric's supplied local
  backend; arbitrary extension code and the same-uid Pi host are not sandboxed.
- Off/escape are intentionally unconfined, and escaped commands' descendants are
  also unconfined. Fleet policy/role changes and rollout need independent review.

## Checks

Build the helper before running the kernel suite against `src`:

```sh
node scripts/build-landlock.mjs
nice -n 19 bunx vitest run tests/landlock-bash.test.ts tests/landlock-startup.test.ts tests/shell-middleware.test.ts
LANDLOCK_BENCH=1 nice -n 19 bunx vitest run tests/landlock-bash.test.ts
bun run typecheck
bun run build
bun run assert:lazy-graph
```

The kernel tests use actual `ActionRegistry` / `PiToolsProvider`, Pi's real
`ExtensionRunner`/in-memory session and a captured cooperative definition. They
assert `tool_call`/`tool_result`, no standalone override fallback, real `EACCES`
for rm/find/Python/Perl/tee, allowed lane/private temp writes, out-of-lane `cwd`
and symlink/BASH_ENV protection, run/git grants, escape journals, cancellation,
background inheritance and off behavior. Kernel tests skip only on non-Linux;
an unsupported Linux host fails with an explicit ABI check. Literal text guards
are tested independently: an earlier friendly refusal is not claimed as a
kernel denial. The optional benchmark interleaves actual calls in fresh modes,
with five warmups and 80 samples per mode; it asserts no flaky timing threshold.
