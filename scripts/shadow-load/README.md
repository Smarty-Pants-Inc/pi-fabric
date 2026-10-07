# Full-load shadow soak (smarty-dev#6477 stage 1)

No Fabric release installs fleet-wide before it passes this soak (release rule 3). On one host it
creates a throwaway hub mesh with 4 spoke meshes, shaped like the real fleet, runs the candidate
release next to a baseline release at fleet load and under CPU contention, changes the pin mid-run
and restarts the bridges, then reports PASS or FAIL.

```sh
scripts/shadow-load/run.sh <candidate-release> <baseline-release> [minutes] [flags]   # default 30 minutes
```

Each release is a built release: an installed one, such as
`~/.local/share/smarty-dev/fabric/releases/<sha>`, or a checkout after `bun run build`. Pass the
same directory twice to calibrate a release against itself. The exit code is 0 for PASS, 1 for FAIL
and 2 for a harness error. The report goes to `--out DIR` (default
`./shadow-load-<candidate>-vs-<baseline>-<time>/`): `report.md`, `report.json`, `progress.log`,
every process log, the observer, driver, seed and canary results, and per-process stats. The
throwaway root under `$TMPDIR` is deleted at the end unless you pass `--keep`.

## The load

Everything runs the releases' own code through their `dist/` entry points. Classes that no entry
re-exports, such as `ParticipantDirectory` and `ActorRegistryStore`, are taken from the exact chunk
that `dist/participants-cli.js` or `dist/residency/host.js` imports. Nothing is mocked.

| Part | Default | What runs |
|---|---|---|
| Hub Mains | 80 (`--mains`), 8 per process (`--mains-per-process`) | one `MeshStore` and one `ParticipantDirectory` per Main, with the real 5 s heartbeat, host lease files and `confirmWritable`. The first half (`--baseline-fraction 0.5`) runs the baseline release, the rest the candidate. |
| Pin change | at 40% of the run (`--pin-at`) | the pin moves to the candidate. autoReload is ON for the first `--auto-reload-fraction` (0.5) of the Mains, which is the baseline half: within `--reload-jitter-s` (10) each one self-reloads as Pi does: `quiesce("reload")`, `close()`, the same process loads the candidate's code, `resumeLineage()`, `start()`, and publishes `ops.fabric.reloaded`. The candidate half has autoReload off. |
| Resident hosts | 15 (`--hosts`) x 10 durable actors (`--actors`) | `runResidentHostFromConfigPath` from the owning Main's release. Owners are spread over both halves. |
| Spokes | 4 (`--spokes`) x 62 Mains (`--spoke-mains`) | candidate Mains on 4 spoke meshes; the bridges mirror them onto the hub, so the hub lists ~250 mirrored remote roots, as on Ryzen 1. |
| Bridges | 5 pairs: 4 hub-spoke + 1 spoke-spoke (`--spoke-links`, spoke2 to spoke3 like Ryzen 2 to Ryzen 3) | `bin/mesh-bridge run` with `bin/mesh-bridge agent` over a local pipe. All are restarted once at 60% of the run (`--restart-at`). |
| Fleet-sized state | `--state-mb 4.8`, `--host-leases 600`, `--seed-participants 20`, `--actor-records 300`, `--actor-roots 40` | `seed.mjs`: before any Main starts, state.json is padded to 4.8 MB with participant- and actor-shaped records through the release's own `writeBatch`. Once the Mains run, a keeper clones a live Main's host lease and participant file into seeded hosts (new ids, shifted times) up to 600 host-lease files and ~500 listed participants, writes the missing actor registries (150 actors on 25 roots, with the live hosts' 150 on 15 roots: 300 on 40), and renews every seeded lease and record each 5 s like its owner would. Only the fleet's summary counts are used; no real record is copied. |
| Events | 1/s on the hub (`--rate`), 0.5/s per spoke (`--spoke-rate`), forwarder ~60/min (`--forward-per-min`, bursts averaging `--forward-burst` 6) | `fleet.work.*` (bridged). The github-factory-like forwarder publishes `fleet.work.github.<kind>` bursts, every third addressed to a spoke Main. |
| Review wakes | every 5 s (`--wake-s`) | a Main publishes `fleet.work.review.requested` addressed to one of its durable actors, then polls `actorStatus` through the resident host. |
| Registry saves | each actor every 30 s (`--actor-save-s`) | a `setTools` call through the candidate's `ResidentActorClient`, made as the owning Main. |
| CPU contention | `--burn-pct 80`, `--burn-threads $(nproc)` | `run.sh` starts `burner.mjs` at nice 0 before the harness: one duty-cycled busy loop per CPU, controlled on `/proc/stat` so the whole host (other tenants and the harness included) stays near 80% busy. The report gives host busy % and CPU PSI over the load window. `--burn-pct 0` disables it. |

The harness and every child run at `nice -n 19` (the burner does not), each child with
`--max-old-space-size=240` (`--heap-mb`). Total PSS is sampled every second; above
`--mem-budget-mb` (8192) the run aborts and FAILs. With the defaults it peaks near 7.5 to 8 GB
(the 4.8 MB state is parsed by every Main's store). The burner is outside the budget (a few MB).

## The checks

Any failed check FAILs the run.

- **(a) Leases and directory.** An independent observer checks every expected participant (each hub
  Main and each resident actor) once a second: the owner host's lease file, plus the record file
  for actors, must have renewed within `--max-lease-s` (15) at every sample, through the pin
  change and the bridge restart. Every 3 s it lists the directory through the candidate's
  `ParticipantDirectory`, as `fabric-participants` does; every expected participant must be listed
  and not stale. Any unexpected process exit also fails (a).
- **(b) Mesh lock.** Lock busy % <= `--max-busy` (30) and `FABRIC_MESH_LOCK_TIMEOUT` <=
  `--max-timeouts` (0) in the load window.
- **(c) Canary**, checks 3 to 5 of the release-candidate canary, 30 s after the load starts, then
  every `--canary-every-s` (120), plus a final round at full load: mesh publish and read back; a
  durable actor round trip; a forced preparation failure that the heartbeat recovers.
- **(d) Participant count** (every listed, non-stale participant, mirrors included) stays within
  +-`--max-count-drift-pct` (2) % of its load-window median, after the first load minute.
- **(e) Lock wait p99** < `--max-wait-p99-s` (5) s.
- **(f) Pin change**: every autoReload Main self-reloaded onto the candidate without error.
- **(memory)** total PSS <= `--mem-budget-mb`.

Lock numbers come from the candidate's L8 lock stats (`fabric-mesh-lock-stats --json`) when it has
them. Otherwise `instrument.mjs`, preloaded into every process, reads them off the lock protocol's
own files: the owner record carries the time its acquisition started, so wait = acquisition time
minus that, hold = acquisition until the `.lock.released.<token>` rename, busy % = summed hold over
complete load minutes. It also counts `FABRIC_MESH_LOCK_TIMEOUT` rejections of full-budget
`MeshStore` entry points (bounded tries separately, as L8 does), and wraps the reload target's
`MeshStore` too. The `.lock` presence sampler is reported as a cross-check.

## Files

`run.sh` starts the burner and `orchestrate.mjs`, which runs the whole soak. The other scripts are
its child processes: `seed.mjs`, `mains.mjs`, `host.mjs`, `driver.mjs`, `observer.mjs`,
`canary.mjs`, `burner.mjs` and `instrument.mjs`. `candidate.mjs` loads code from a release.
