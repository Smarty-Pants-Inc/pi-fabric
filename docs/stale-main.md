# Stale Main admission and release report

A task worker uses its Main's loaded Fabric package. Activation changes the fleet profile's
`settings.json` `packages` selector. It does not replace code in an existing Main or select a
newer worker for that Main. The spawn guard and the self-reload watch share `activeFabricRoot`
and `loadedFabricRoot` from `src/core/agent-dir.ts`, also exported by `src/lifecycle/self-reload.ts`.

## Task admission

`agents.spawn`, `agents.run`, actor task activations and worker relaunches check the loaded
package against the profile selector before native launch. A stale Main's handle, status and
terminal result include this one-line `notice`:

```text
This Main runs <loaded>; the fleet runs <active>; it self-reloads at its next safe settle
```

Fabric publishes `ops.fabric.stale-main` once per Main identity and active release. Its data
includes the loaded and active roots, notice, refusal reason and critical releases. Publishing
is best effort when the mesh is disabled or unavailable. Replacing a manager or reloading in
the same process does not reset the deduplication. Atomic markers under the profile's
`fabric/stale-main-events/` directory also deduplicate its local resident hosts. Task-worker
runtimes do not publish their parent Main's ops notice. An unwritable profile keeps process-local
deduplication; a disabled mesh consumes the best-effort notice attempt.

A safety-critical release in `(loaded, active]` refuses the launch. Finish the Main's existing
work and let it settle, then use `/fabric-release-reload` or `/reload`. The automatic reload
keeps its existing streaming, prompt, background-work and halt checks. A refusal never stops
existing workers or substitutes a new worker release.

After a Main reload changes the resident config entry path, its obsolete resident host stops
taking new commands, drains live runs and exits even if durable subscriptions remain. The
reloaded Main starts the replacement host on its own release. Refused actor events are parked
in their durable queue (not failed or consumed), then retried by the replacement runtime;
repeated stale reloads do not exhaust their interrupted-run retry budget. Caller `ask` requests
still reject immediately so their caller hears the refusal.

## Safety metadata

The installer writes `<base>/<sha>.receipt.json`, next to `<base>/releases/`. Fabric reads the
optional `safetyCritical` boolean from those receipts and from a release's `install-receipt.json`
or `manifest.json`. It orders releases by `installedAt`, falling back to `activatedAt` for
legacy metadata. First-install time remains stable across reactivation.

Mutable safety marks live in `<base>/releases-safety.json`, alongside the `releases` directory.
This file is authoritative for policy entries; a `false` mark cannot erase a receipt's `true`
mark. If the adjacent file is absent, Fabric uses the package's shipped `releases-safety.json`.
The shipped policy marks B66 (the `/tmp` and kill-by-pattern guards) and B68 as safety critical.
Install receipts are immutable inputs and are never rewritten by Fabric.

```json
{
  "version": 1,
  "releases": {
    "<full-release-sha>": {
      "safetyCritical": true,
      "installedAt": "<ISO-8601-install-time-from-receipt>"
    }
  }
}
```

Keep receipts for pruned release trees. A policy row may supply a recorded install time when
its receipt is unavailable. Use the install record's time; filesystem mtimes, SHA sorting and
commit dates do not establish install order. Shipped, undated safety marks without a local
receipt, recorded time or surviving release directory do not establish an installation on this
host. If no critical release is evidenced as installed, missing chronology alone gives the stale
notice, not a refusal. When an installed critical release exists, missing or invalid chronology, a tied endpoint
time, an ambiguous critical boundary timestamp, an unreadable selector, or a rollback refuses an installed Main's spawn with a metadata
reason and the reload path. Equal loaded/active roots need no safety-gap check or notice.
Explicit development packages outside `releases/` receive the stale notice without a claimed
position in the fleet's release chronology.

## Report

Run on each Linux host with the fleet profile selected:

```sh
bin/fabric-releases
bin/fabric-releases --json > host-releases.json
bin/fabric-releases --snapshot host-one.json --snapshot host-two.json
```

`--settings FILE` selects a profile explicitly; `--host NAME` labels a host snapshot. The
command performs no network calls, process controls or writes. JSON lists each Main and its
workers and resident hosts, including actor tasks, by loaded and active release. Recorded
Mains use the actual `session:<sessionId>` lineage carried by workers, so detached resident
workers join their Main by ID rather than process ancestry. Text groups workers by release
on each Main's row and includes resident-host and worker rows with lineage/run/actor IDs.
Truly remote worker lineages have a null Main PID on that host. Synthetic `/proc` fixtures are
portable; live census requires Linux, and other platforms can aggregate `--snapshot` files.

New runtimes record a Main's loaded root when their lazy runtime initializes, binding it to
Linux process-start ticks. The report rejects PID-reused and exited records. Legacy Mains with
one observed worker release show `worker-inferred`; legacy Mains with no workers or mixed
worker releases show `unknown`. It never presents today's active selector as proof of a
legacy Main's loaded release. Run the report again after those Mains safely reload for a
runtime-record observation. Other users' inaccessible or disappearing processes are counted
in `skippedProcesses`; this is a live, best-effort snapshot, not a fleet-wide atomic census.
