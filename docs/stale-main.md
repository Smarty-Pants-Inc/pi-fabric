# Release census

PR #190 shipped release observability. Process-transport spawns now resolve the profile's
active Fabric selection at launch time (smarty-dev#2665 option (a)). Durable recovery and
resident handover remain deferred to **smarty-dev#3285**. The census itself does not change
launch admission, agent routing, actor queue behavior, or resident retirement authority.

## Loaded generation versus active selection

Activation changes the profile's `settings.json` `packages` selector. It does not replace
code in an existing Main, worker, or resident host. A new process-transport worker uses
the active package's `dist/worker.js` and, when Fabric is enabled, `dist/index.js`, provided
its `dist/worker-protocol.json` version matches the parent manager's
`WORKER_PROTOCOL_VERSION`. An incompatible, unversioned, or incomplete active release
falls back to the parent with one warning naming both releases. A missing selector
falls back silently. Explicit source/custom worker paths remain caller-selected.
Spawn handles, results, and new workers' status records expose the selected canonical
root as `fabricRelease`. Existing resident hosts themselves retain their loaded
generation; selecting a compatible worker does not migrate resident ownership or
perform a resident handover. The report distinguishes those loaded paths from
the profile's current active selection; an active selector is not evidence that a running
process has changed generation.

The census and existing self-reload watch share `activeFabricRoot` and `loadedFabricRoot`
from `src/core/agent-dir.ts`, also exported by `src/lifecycle/self-reload.ts`. The existing
Main self-reload keeps its safe-settle checks for streaming, prompts, background work and
halts. A Main reload is not proof that a resident has changed generation. Neither the
census nor the selector helpers grant process-control or resident-retirement authority.

## Report

Run on each Linux host with the appropriate profile selected:

```sh
bin/fabric-releases
bin/fabric-releases --json > host-releases.json
bin/fabric-releases --snapshot host-one.json --snapshot host-two.json
```

`--settings FILE` selects a profile explicitly; `--host NAME` labels a host snapshot. The
command performs no network calls, process controls or writes. It reads only selected
non-secret process-environment keys, native argv, Fabric lineage/owner metadata, profile
settings and runtime release records, not prompts, history or authentication files.

JSON lists each Main and its workers and resident hosts, including actor tasks, by loaded
and active release. Recorded Mains use the actual `session:<sessionId>` lineage carried
by workers, so detached resident workers join their Main by ID rather than process
ancestry. Text groups workers by release on each Main's row and includes resident-host
and worker rows with lineage/run/actor IDs. Truly remote worker lineages have a null Main
PID on that host. Synthetic `/proc` fixtures are portable; live census requires Linux,
and other platforms can aggregate `--snapshot` files.

## Evidence and limits

New runtimes record a Main's loaded root when their lazy runtime initializes, binding it
to Linux process-start ticks. Registration and idle lifecycle hooks do not load the
runtime recorder or write a Main record. The report rejects PID-reused and exited records.

- `runtime-record`: a live PID and birth-time match identifies the Main's recorded loaded
  root and session lineage.
- `worker-inferred`: a legacy or unrecorded Main has exactly one observed worker release.
  This is an inference, not a runtime attestation of the Main's loaded code.
- `unknown`: no matching runtime record and no unique worker-release inference. Legacy
  Mains with no workers or mixed worker releases remain unknown.

The report never presents today's active selector as proof of a legacy Main's loaded
release. Run it again after a Main safely reloads and initializes its runtime for a
runtime-record observation. Resident hosts publish their immutable launch-time Fabric
extension path in `owner.json`; the census does not infer their loaded generation from
mutable desired configuration. Older owners may instead expose their resident entry path
in argv; without either source, the loaded release is unknown.

Other users' inaccessible or disappearing processes are counted in `skippedProcesses`;
this is a live, best-effort snapshot, not a fleet-wide atomic census. It is not a safety
policy, a launch guard, or a durable recovery mechanism. The separately reviewed
launch-enforcement and recovery contract is tracked in **smarty-dev#3285**.
