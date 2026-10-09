# Linux execution cgroup custody

A dedicated systemd user scope owns execution membership on cgroup v2. The worker transfers the execution scope and its PID, birth ticks, session ID and process-group ID to the parent before opening the target's admission pipe. An inherited service cgroup is never whole-tree signal authority.

Custody opens the scope directory once with `O_RDONLY | O_DIRECTORY`, compares `fstat` with its recorded device/inode, and performs procs/events/freeze/kill operations through `/proc/self/fd/<fd>/<file>`. Replacing the pathname cannot redirect signal authority. The fd closes after empty/removed scope confirmation; an active freezer signal holds it until thaw completes.

TERM freezes the scope, waits for `frozen 1` through the shared events watcher (one-second bound), lists frozen members, sends the signal and thaws in `finally`. Frozen members cannot exit and have their PID reused between enumeration and signal. Node has no `pidfd_send_signal`: the freezer is the atomic-signal answer. KILL uses pinned `cgroup.kill`, or the same freezer path when that file is absent. A kernel without `cgroup.freeze` uses the legacy birth-and-membership-checked path and warns once that its PID-reuse window is non-atomic. Freeze timeout or unreadable state fails closed and attempts thaw; it does not fall back to unsafe signalling.

Targets do not inherit `DBUS_SESSION_BUS_ADDRESS` or `XDG_RUNTIME_DIR`. Removing only the first is insufficient: systemd discovers the user bus through the second. Source bus use is limited to custody launchers, not inference. Nested Fabric launchers can discover the standard `/run/user/<uid>/bus` for their launcher alone; execution targets are scrubbed again. This preserves nested custody, while plain `systemd-run --user --scope true` inside the target fails. A same-UID task can reconstruct that address, so this is not a security boundary.

At stop only, one `/proc` enumeration per retained receipt filters the user's app.slice by cgroup record, reads identity for those candidates (and the recorded leader anchor), and retains processes born at or after admission whose PPID chain, session or process group matches. Sibling scope/service/subtree escapers are frozen and only retained births are signalled; unrelated members are never whole-cgroup KILL targets. Observe/exited/isAlive hot paths do not scan `/proc`.

## Named security residual (#7248 follow-up)

Same-UID re-homing was already possible with the RC's descendant scan: the user manager becomes the parent, so the old PPID tree cannot follow it. Same UID can write delegated cgroup membership or call its user manager; changing file permissions cannot exclude code running as that UID. This change is cooperative custody plus cheap hardening, not hostile-code containment.

**Residual: reparent + change session/process group + change cgroup.** A task that removes all recorded anchors can evade stop-time detection. A hostile task can also race cooperative sampling or restore bus discovery. Strong containment needs a separate UID and/or an enforced Landlock policy that denies cgroup writes and user-manager access, tracked by #7248. Do not describe scope custody as a sandbox; accepting shipment with this residual requires the owning mission's named `security-gap` record.
