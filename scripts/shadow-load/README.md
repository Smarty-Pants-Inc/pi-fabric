# Full-load shadow test (smarty-dev#6477 stage 1)

No Fabric release installs fleet-wide before it passes this test. On one host it creates a
throwaway mesh root and runs the candidate release against it at fleet load, then reports PASS or
FAIL.

```sh
scripts/shadow-load/run.sh <release-dir> [minutes] [flags]     # default 20 minutes
```

`<release-dir>` is a built release: an installed one, such as
`~/.local/share/smarty-dev/fabric/releases/<sha>`, or a checkout after `bun run build`. The exit
code is 0 for PASS, 1 for FAIL and 2 for a harness error. The report goes to `--out DIR` (default
`./shadow-load-<sha>-<time>/`): `report.md`, `report.json`, `progress.log`, every process log, the
observer, driver and canary results, and per-process stats. The throwaway root under `$TMPDIR` is
deleted at the end unless you pass `--keep`.

## The load

Everything runs the candidate's own code through its `dist/` entry points. Classes that no entry
re-exports, such as `ParticipantDirectory` and `ActorRegistryStore`, are taken from the exact chunk
that `dist/participants-cli.js` or `dist/residency/host.js` imports. Nothing is mocked.

| Part | Default | What runs |
|---|---|---|
| Mains | 80 (`--mains`), 4 per process (`--mains-per-process`) | one `MeshStore` and one `ParticipantDirectory` per Main, with the real 5 s heartbeat, host lease files and `confirmWritable` |
| Resident hosts | 15 (`--hosts`) x 10 durable actors (`--actors`) | `runResidentHostFromConfigPath` from `dist/residency/host.js`. Each host belongs to one of the Mains. |
| Registry saves | each actor every 30 s (`--actor-save-s`) | a `setTools` call through the candidate's `ResidentActorClient`, made as the owning Main. This writes the registry and runs the fenced presence publication. |
| Bridges | 3 pairs (`--bridges`) | `bin/mesh-bridge run` on the hub with `bin/mesh-bridge agent` on a second throwaway spoke mesh, over a local pipe. They are restarted once at half time (`--restart-at 0.5`). |
| Spoke | 5 Mains (`--spoke-mains`) | their root presence is mirrored onto the hub by the bridges |
| Events | 3/s on the hub (`--rate`), 0.5/s on the spoke (`--spoke-rate`) | `fleet.work.*`, which the bridges carry. Every third hub event is addressed to the spoke. |

Every process runs at `nice -n 19` with `--max-old-space-size=512`. Total RSS is sampled every
second. If it exceeds `--mem-budget-mb` (8192), the run aborts and FAILs. With the defaults it
stays near 5 to 6 GB. `--mains-per-process 1` gives each Main its own process; that needs about
10 GB.

## The checks

- **(a) Leases and directory.** An independent observer checks every expected participant (each
  Main and each resident actor) once a second. It reads the owner host's lease file, plus the record
  file for actors, and every lease must have renewed within `--max-lease-s` (15) at every sample.
  Every 3 s it also lists the directory through the candidate's `ParticipantDirectory`, as
  `fabric-participants` does. Every participant must be listed and not stale. The report counts
  misses after the bridge restart separately (the incident where local records vanished). Any
  unexpected process exit also fails (a).
- **(b) Mesh lock.** The lock busy % must stay at or below `--max-busy` (30). The
  `FABRIC_MESH_LOCK_TIMEOUT` count must stay at or below `--max-timeouts` (0). When the candidate
  has the L8 lock stats (`dist/mesh-lock-stats-cli.js`), these numbers come from
  `fabric-mesh-lock-stats --json` over the load window. An older candidate records no L8 stats, so
  the observer samples `<mesh>/.lock` every 2 ms to get the busy %. The timeouts are then counted by
  `instrument.mjs`, which is preloaded into every process. It counts lock timeouts thrown by the
  candidate's full-budget `MeshStore` entry points. Bounded tries, which fail by design, are counted
  separately, as L8 does. The report always includes both sources.
- **(c) Canary**, checks 3 to 5 of the release-candidate canary. A round runs 30 s after the load
  starts, then every `--canary-every-s` (120), plus a final round at full load. Each round has
  three checks:
  - Mesh publish and read back, with arrival on the spoke shown for information.
  - A durable actor round trip: `setTools`, then `actorStatus`, through the resident host.
  - A forced preparation failure that recovers. A real `ActorRegistryStore` write happens between
    the publication fence's preparation and its validation, as on a resident host. It repeats
    until the directory's preparation retries run out and the publication fails. The next 5 s
    heartbeat must then publish within 15 s. A two-write burst must recover within the same
    refresh.

## Files

`orchestrate.mjs` runs the whole test. The other scripts are its child processes: `mains.mjs`,
`host.mjs`, `driver.mjs`, `observer.mjs`, `canary.mjs` and `instrument.mjs`. `candidate.mjs`
loads code from the release.
