# Stop or remove a dead Main's durable actor

Run on the resident's machine, as its OS user, from any Fabric session or shell:

```sh
fabric-actors stop --resident /path/to/mesh/residency/<directory-prefix> --actor <id-or-name> --dry-run
fabric-actors stop --resident /path/to/mesh/residency/<directory-prefix> --actor <id-or-name>
fabric-actors remove --resident /path/to/mesh/residency/<directory-prefix> --actor <id-or-name>
```

A bare directory prefix searches `$PI_FABRIC_MESH_ROOT/residency`; alternatively
pass `--mesh-root`. Without either, the default is
`$PI_FABRIC_PROJECT_ROOT/.pi/fabric/mesh` (or the current directory).
Ambiguous resident prefixes and actor names are refused. Actor IDs and names
are exact and scoped to the selected host's root, not the caller's root.

The running resident host checks both its recorded Main PID/incarnation
(`main-generation.json` and the Main inbox owner) and a fresh root lease.
A live process **or** live lease blocks the command. Missing process evidence,
invalid identities, and unreadable safety data fail closed. `--force-live`
is the explicit override; it does not bypass target ownership checks.
`--dry-run` performs the same checks but changes no actor state.

Dead-root control without an override requires Linux `/proc` evidence. On
Windows, macOS, or other platforms it refuses with:
`operator dead-root control needs Linux /proc evidence; pass --force-live after confirming`.

Linux Main startup atomically publishes
`<meshRoot>/main-markers/<pid>-<linuxStartTime>.json` under the same root
publication fence as operator commits, before any actor/resident activation.
Before Pi supplies a native session context, the marker is explicitly unbound
(still starting); bootstrap binds it to the current root/session, including
same-process switches and resumes. Clean shutdown removes it. The census
ignores a live Main only when its exact PID/birth marker or existing positive
bindings place it elsewhere. A target-root/session marker vetoes control;
stale PID/birth markers do not bind a new process. An unmarked, unbound Pi Main
refuses with its PID and `Main without a Fabric marker (older release or still
starting); retry after it publishes, or confirm and pass --force-live`.

Stop cancels queued work and drains the actor's run, retaining its stopped
registry record. Remove first performs the same terminal stop/drain (so a
progressed worker cannot detach on caller abort), then uses the host's normal
removal path, including custody, participant cleanup and registry revocation. It may return a
`pending` state while the existing host finisher completes cleanup. Other actors
and the resident host are not stopped. No actor/registry files are edited by the
CLI, and it never starts or upgrades a resident host.

This requires a resident binary advertising `operatorActor`. Older binaries
are refused before a request is dispatched; no unsafe legacy command fallback
is used. Existing dead-root resident binaries need an explicit deployment of a
compatible host before this operator path is available.
