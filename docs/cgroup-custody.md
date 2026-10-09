# Linux execution cgroup custody

A dedicated systemd user scope owns execution membership on cgroup v2. The worker transfers the execution scope and identity to the parent before opening the target's admission pipe. An inherited service cgroup is never whole-tree signal authority.

Custody opens the scope directory once with `O_RDONLY | O_DIRECTORY`, compares `fstat` with its recorded device/inode, and performs procs/events/freeze/kill operations through `/proc/self/fd/<fd>/<file>`. Replacing the pathname cannot redirect signal authority. The fd closes after empty/removed scope confirmation; an active freezer signal holds it until thaw completes.

Admission requires writable `cgroup.freeze` **and** `cgroup.kill`. If either control is missing or unavailable, callers use the existing legacy process-group custody and downgrade warning, not a weakened cgroup receipt. TERM freezes the scope, waits for `frozen 1` through the shared events watcher (one-second bound), lists members, reads each member's `/proc/<pid>/stat` through the shared Linux identity helper, skips Z/X states and failed stat reads, signals only live frozen members and thaws in `finally`. Live frozen members cannot exit and have their PID reused between the stat read and signal. Zombies are not frozen and an outside parent can reap them, so their numeric PIDs are never signalled. Node has no `pidfd_send_signal`: the freezer is the atomic-signal answer. KILL uses pinned `cgroup.kill` only. Losing either control after admission fails closed; there is no unfrozen per-PID fallback or numeric KILL fallback.

Only scoped targets lose `DBUS_SESSION_BUS_ADDRESS` and `XDG_RUNTIME_DIR`, through the admission shell's `unset`. Direct-spawn fallback preserves the original environment/options. Removing only the first variable is insufficient: systemd discovers the user bus through the second. Nested Fabric launchers can discover the standard `/run/user/<uid>/bus` for their launcher alone; execution targets are scrubbed again. This is convenience hardening, not a same-UID security boundary.

Observe/exited/isAlive and stop do not enumerate `/proc` on the cgroup path. Drain waits use the shared `cgroup.events` watcher. Active-watch deadline wakes do not re-read membership. The named R-no-polling safety exception permits one membership read after at least 60 seconds per wait to recover a missed event. An unavailable watcher retains the bounded one-second legacy observation fallback.

## Accepted security residual (smarty-dev#7478)

Same-UID code can reconstruct user-bus discovery, ask its user manager to re-home a process, or write delegated cgroup membership. A process that leaves the retained scope can evade custody. Scope custody is not a sandbox; strong containment needs a separate UID and/or an enforced policy denying cgroup writes and user-manager access.

There is no stop-time escaper discovery. Matching SID, PGID, or a sampled PPID chain is not ownership of a sibling cgroup; killing an unrelated process is worse than missing an escaper. The live escape test documents this residual and asserts that stopping the original scope does **not** signal a same-session process outside it. Explicit custody of that fixture's separate scope is used only for test cleanup.
