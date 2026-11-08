# Cooperative resident drain

**Adopt first:** graceful shutdown followed by the existing lease-fenced, single-owner
restart is the supported Main release-switch path. Kubernetes `preStop` and
systemd `ExecStop` are established shutdown examples, but external service-manager
integration is unnecessary here: the existing in-process endpoint supplies
bounded checkpoint and exit.

Without `--exit`, `fabric-resident-drain` negotiates a **reversible hold and
checkpoint** with a compatible loaded resident. A healthy operation holds
admission, lets accepted work finish naturally, checkpoints and reads back
custody, then resumes the same old resident. It returns nonzero JSON
`containment-unavailable`: complete attempt containment and the staged
`releaseChange` handover remain disabled. The separate Main stop-then-start path
is described below. This default CLI behavior does not stop actors or the host.

`fabric-resident-drain --exit` is a separate, explicit CLI action: an idle
compatible resident checkpoints, keeps admission held and closes gracefully.
Success attests only that exact old host's stopped exit, not complete attempt
containment or a serving target. Neither CLI mode launches a target, signals
processes, terminally stops an actor or removes a custody marker.

Legacy or uncertain roots retain the read-only preflight: they refuse without
request dispatch or changes to resident files. New source cannot retrofit the
endpoint or containment into an already-loaded legacy host.

```sh
fabric-resident-drain \
  --resident <resident-directory-or-unique-hex-prefix> \
  --mesh-root <absolute-mesh-root> \
  --expected-release <absolute-old-package-root> \
  --target-release <absolute-new-package-root>
```

All four arguments are required. The mesh root has no implicit fleet default.
Release arguments name retained package directories, not abbreviated Git SHAs or
mutable profile selectors. An expected-release mismatch or unverifiable owner
refuses before any transition. An older host without the immutable birth-bound
launcher identity also refuses; its extension path can identify a mismatch but
cannot prove launcher custody.

A cooperative host must advertise `fabric-resident-drain-1` and `releaseDrain`.
The CLI validates its immutable launch snapshot, target artifact and unchanged
topology, plus the existing live owning Main process, session and generation.
It never creates a Main attestation or starts a host to satisfy these checks.
A finite timeout resumes A without aborting accepted work; unreadable or
uncorrelated terminal evidence reports `needs-operator`, not successful service.

## Main graceful stop-then-start

When a Main starts on B and sees a live, compatible resident A for the same root,
it uses the existing in-process `releaseDrain(exit:true)` endpoint. This is a
stop-then-start restart, not the staged `releaseChange` handover. The Main retries
only correlated, read-only precommit busy refusals; A remains serving while
accepted requests and actor work finish naturally. Other refusals, missing or
uncorrelated evidence, and postcommit uncertainty are not retryable.

The Main proceeds only after it verifies the exact durable
`phase:"exited", outcome:"exited"` receipt with admission held and checkpoint
read-back, the host-written `reason:"stopped"` record for A's exact generation,
observation that A's original PID/birth has exited, and release of A's host
lease. It then calls ordinary
`ensureHost` on B (joining a normal concurrent B launch if one already exists)
and checks that the serving owner uses B's release. Queues and traces remain
retained; this path uses no actor terminal stop, external signal, new state or
protocol, or staged rollback.

This covers Main-initiated switches. This Main's ordinary file requests wait
behind reconciliation. An independent CLI/proxy writer racing the committed
drain can instead leave A held with `needs-operator`; it is diagnosed, not
retried or followed by a competing launch. The diagnosis names the recovery
command and requires rechecking A's exact PID/birth before an operator acts.
Queues and traces remain retained in that case.

A host without cooperative-drain support remains diagnosed and serving; this
path does not forcibly stop it. `fabric-resident-drain --exit` behavior is
unchanged: CLI success proves only A's stopped exit, not that B launched or
serves.

## Manual CLI exit and ordinary recovery

The following is the explicit CLI workflow for a manual-only Main integration;
current Main startup normally performs the automatic path above.

1. Retain both releases and arrange a maintenance window. Reload the **active
   owning Main onto B** through its normal runtime path while it adopts the
   still-running A resident. Confirm `config.json` selects B's
   `fabricExtensionPath` and `workerPath`, with the same root, session, mesh and
   actor directories. Keep this B Main active. Editing the file under an active
   A Main is insufficient: that Main can overwrite it and restart A.
2. Let running requests, actors and workers finish naturally. Idle queued actor
   input is allowed: it stays accepted and is checkpointed, not cancelled or
   consumed. Running work, pending host/request/worker custody or publication
   debt refuses `--exit`; it does not abort that work or silently wait out an
   unbounded request.
3. Run the command above with `--exit`, still using A as `--expected-release`
   and B as `--target-release`. The CLI checks both desired config and current
   owning Main select B before dispatch (`CONFIG_RELEASE_MISMATCH` or
   `ACTIVE_RELEASE_MISMATCH` otherwise). **`--target-release` does not update
   config or switch the active Main.**
4. Require exit code zero and `phase:"exited", outcome:"exited"`. This requires
   the correlated terminal receipt with admission still held and checkpoint
   read-back, the exact generation's host-written `reason:"stopped"` record,
   and observation that the original PID/birth is gone. A receipt written just
   before native exit, a missing owner or a launcher fallback record alone is
   insufficient. Uncertain evidence remains nonzero `needs-operator`.
5. Use the ordinary client `ensureHost`/wake path to obtain B. The active B
   Main's existing watchdog may already have launched it after A exited; this
   is normal recovery, not a launch performed by the drain CLI. Verify B's
   release identity and the same actor IDs, topics and accepted queue entries
   before declaring service restored. Do not launch A again in parallel.

After `--exit` holds admission, a checkpoint/close failure or deadline reports
`needs-operator` and keeps admission held; it does not resume A or authorize a
competing launch. This differs from the default reversible drain's timeout.

A separately launched successor does not invalidate proof that A stopped; it
also does not turn this CLI's success into a B-readiness attestation. This CLI
does not perform automatic A-to-B handover, rollback, or prove that every escaped
helper belonging to an old attempt exited. The staged `releaseChange` handover
and complete attempt-containment fallback remain disabled; the supported Main
stop-then-start path is separate and is described above.

The native manual-workflow regression covers both a refused in-flight inference
request and an allowed idle exit with nonempty project/session actor queues. It reloads the
real client onto B before exit, leaves its normal watchdog active, then uses the
ordinary launch path and checks the same IDs, topics and original queue entries
before those inputs execute exactly once. It checks no intervening A relaunch.

## Deployment shutdown patterns

Established deployment practice is to stop new ingress before graceful shutdown
and provide a finite budget for accepted work and checkpoints. An external hook is
not required for a Main restart: the existing in-process endpoint already supplies
bounded checkpoint and exit. Kubernetes `preStop` and systemd `ExecStop` are
established examples when an external service manager coordinates shutdown; these
settings are not substitutes for a resident admission/exit receipt:

- **Kubernetes:** make the workload unready using its readiness gate, then use a
  `preStop` hook for the bounded application drain/exit action. Set
  `terminationGracePeriodSeconds` to cover the hook **and** subsequent process
  termination, with margin: the hook runs inside that same grace period, not in
  an extra allowance. Configure any owning Main/restart path consistently with
  the intended release. A refused hook is not a successful checkpoint; preserve
  its evidence before the platform's eventual forced termination.
- **systemd:** stop ingress/readiness first and use an appropriate bounded
  `ExecStop` action. Set `TimeoutStopSec` for the complete stop/checkpoint budget;
  expiration can cause forced termination and does not prove custody was saved.
  A value such as 90 seconds is an example budget, not a safety guarantee.

**Pi 1.2 terminal-shutdown limitation:** affected runtime shutdown paths cap
extension hooks at approximately one second. Increasing the Kubernetes or
systemd budget alone does not extend that runtime cap. `--exit` joins natural
resident close before requesting runtime shutdown; it does not change Pi's
terminal hook budget or make ordinary TERM safe by itself. Validate the installed
runtime's actual shutdown path and actor count separately.

References: [Kubernetes Pod termination](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination)
and [systemd service timeouts](https://www.freedesktop.org/software/systemd/man/259/systemd.service.html#TimeoutStopSec=).

## Boundaries and default behavior

These legacy operations are not substitutes for cooperative PREPARE:

- `setActivationFilter` skips matching events; it is not a queue-preserving hold.
  Matching mesh events can be consumed, not retained behind the cursor.
  Direct calls are not covered by that filter.
- `pauseForRelease()` is an internal actor gate, not an operator request. The
  retained release transaction also needs separate request, control and lifecycle
  gates and checked publication barriers.
- `releaseChange` unconditionally defers before arming those gates. Its successful
  request exchange does not mean the release changed.
- `fabric-actors stop` is terminal cancellation. It refuses an actor with queued
  work, and with `--drop-queued` it moves that work to a stop archive.
  It must not be used as an installer drain.

Persistent host close retains committed actor queues better than terminal stop,
but an external quiet-state scan followed by shutdown can race a new admission.
A clean direct-child exit or empty process group cannot prove an escaped helper
has exited. These are among the reasons staged `releaseChange` handover and
complete attempt-containment fallback remain disabled; the supported Main
stop-then-start path instead uses the cooperative endpoint, exact A exit evidence
and ordinary `ensureHost`.

## Read-back and rollback

Legacy refusal reports `preflight`, `unchanged:true` and rollback `not-needed`.
A negotiated operation reports its resident-generated ID, held/checkpointed
state, actual resume and checked read-back. `containment-unavailable` is partial
progress, never release success. It has `unchanged:false` because PREPARE changed
admission briefly, and rollback `not-attempted` because no target started.

Public JSON contains only allowlisted evidence. Paths are relative to the explicit
mesh root. Authentication tokens use short SHA-256 fingerprints; private config,
Main nonce and complete checkpoint contents are not printed. Durable root-local
receipts retain private correlation data and must not be copied into public logs.
Native regressions check the same serving owner, unchanged accepted retry evidence
and exactly-once work completion after resume; concurrent input stays behind its
cursor while held.

Do not launch the old release as a rollback while any staged target may still
execute. A future staged rollback may be considered only after it proves target
startup admitted no work **and** the complete target attempt exited. After target
work admission, a read-back failure needs an operator; it cannot authorize a
competing old resident.

Complete attempt-owned containment and exit receipts remain required for staged
`releaseChange` handover and fallback; they are not prerequisites for the
separate Main stop-then-start path described above. The frozen containment
interface defaults to refusal. A real adapter must run in launcher custody with
its retained native handles; neither
the CLI nor the endpoint reopens a scope path from `owner.json`. The launcher alone
writes the immutable `attempt-launch-<attemptId>.json` beside `owner.json`, before
releasing the host. Its full receipt must match the launch environment and native
IPC receipt field for field. Readers use an owned, no-follow descriptor boundary;
a missing or different receipt never grants containment. A free lock, missing
owner or direct-child exit cannot authorize B or an A fallback. Leave
legacy residents serving and arrange any manual maintenance through their owner;
a quiet window alone is not a machine-verified drain.
