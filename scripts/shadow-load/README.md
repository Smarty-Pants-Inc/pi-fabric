# Full-load shadow soak (smarty-dev#6477 stage 1)

No Fabric release installs fleet-wide before it passes this soak (release rule 3). On one host it
creates a throwaway hub mesh with 4 spoke meshes, shaped like the real fleet, runs the candidate
release next to a baseline release at fleet load and under CPU contention, changes the pin mid-run
and restarts the bridges, then reports PASS or FAIL.

```sh
scripts/shadow-load/run.sh <candidate-release> <baseline-release> [minutes] [--profile fleet|legacy] [flags]   # default 30 minutes
```

`--profile fleet` (the default) applies the values calibrated against Ryzen 1's fleet (see
[Calibration](#calibration)); flags after it override single values. `--profile legacy` runs
orchestrate.mjs's built-in defaults, the harness before calibration. The table below lists the
built-in defaults; the fleet profile's overrides are in the calibration section.

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
| Registry saves | each actor every 30 s (`--actor-save-s`), at most `--saves-in-flight` (1) per host | a `setTools` call through the candidate's `ResidentActorClient`, made as the owning Main. |
| Turn churn | off (`--churn-s 0`) | with `--churn-s S`, each hub Main's root status flips idle/running on average every S s (jitter 0.5S to 1.5S) and its directory publishes the changed record (`scheduleRefresh`), as a Main that starts and ends turns does. |
| CPU contention | `--burn-pct 80`, `--burn-threads $(nproc)` | `run.sh` starts `burner.mjs` at nice 0 before the harness: one duty-cycled busy loop per CPU, controlled on `/proc/stat` so the whole host (other tenants and the harness included) stays near 80% busy. With `--burn-duty D` the controller is off and every thread burns a fixed D of each 50 ms slice (D x threads CPUs of nice-0 work, however busy the host already is; on a host the harness saturates by itself the controller backs off to 0). The report gives host busy % and CPU PSI over the load window. `--burn-pct 0` disables it. |

The harness and every child run at `nice -n 19` (the burner does not), each child with
`--max-old-space-size=240` (`--heap-mb`). Total PSS is sampled every second; above
`--mem-budget-mb` (8192) the run aborts and FAILs. With the built-in defaults it peaks near 7.5
to 8.5 GB (the 4.8 MB state is parsed by every Main's store) and 8 Mains per process with a
240 MB heap run out of heap at the pin change (the reload holds a second store per Main). The
fleet profile (4 Mains per process, 640 MB heap, 10 MB state) peaks at 15 to 17 GB under a
32 GB budget. The burner is outside the budget (a few MB).

A Main whose `start()` rejects (its initial publish hit a busy lock) keeps its directory: the
release arms the heartbeat timer before that publish, so the directory joins later on its own.
Before calibration the harness started a second directory for the same Main on each retry, which
left up to 6 heartbeating directories per Main after a pin change and turned the reload into a
lock storm that never settled.

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

## Calibration

Target (smarty-dev#6477 stage 1): release 04930dfd against itself over 15 min should show Ryzen 1's
fleet lock load, lock busy 55 to 65% and 20 to 30 `FABRIC_MESH_LOCK_TIMEOUT` per minute, with
host CPU PSI near 50 to 60%, inside a 32 GB memory cap on epyc1 (32 CPUs, shared with other
tenants). Calibrated 2026-10-08 on epyc1; lock numbers from the instrument (04930dfd predates L8).
Runs r1 to r12 are 3 to 5 min, c1 and c2 the 15-min profile runs. "late pin" runs move the pin
change and the bridge restart to 90/95% of the run, so the window measures steady state.

| run | knobs (changed from the row above) | PSS peak | host CPU | PSI | lock busy | wait p99 | timeouts/min | gates failed |
|---|---|---|---|---|---|---|---|---|
| r1 | legacy defaults (ppp 8, heap 240, state 4.8, burner controller 80%), cap 32 GB | 8.6 GB | 84% | 18% | 19.3% | <=10 s | 33.6 | a b c d e f; 3 Mains processes OOM at the pin |
| r2 | ppp 4, heap 640, state 9 | 17.6 GB | 90% | 27% | 28.8% (41% pre-pin) | <=7.5 s | 70.1 (0 pre-pin) | a b c d e f |
| r3 | state 10, actor-save-s 10 | 17.9 GB | 86% | 22% | 23.8% | <=7.5 s | 60.7 | a b c d e f |
| r4 | fix: keep a Main's directory when start() rejects | 15.6 GB | 84% | 20% | 24.8% (35% pre-pin) | <=5 s | 28.1 (0 pre-pin) | a b d e |
| r5 | burner fixed duty 0.25 | 14.9 GB | 92% | 33% | 22.4% | <=7.5 s | 30 | a b c d e |
| r6 | state 12, saves-in-flight 4, duty 0.4, late pin | 15.3 GB | 90% | 27% | 33.1% | <=5 s | 15.5 | a b c d e |
| r7 | duty 0.7, late pin | 15.0 GB | 97% | 38% | 31.2% | <=2 s | 14.3 | a b c d |
| r8 | state 10, duty 0.4, churn-s 15, late pin | 16.0 GB | 95% | 39% | 19.0% | <=5 s | 157 | a b c d e |
| r9 | ppp 2 (as r8) | 19.0 GB | 97% | 48% | 21.5% | <=10 s | 168 | a b c d e f |
| r10 | ppp 4, state 16, churn off, duty 0.5, late pin | 18.2 GB | 95% | 33% | 29.8% | <=3 s | 13.8 | a b c d |
| r11 | state 10, actors 20 per host, late pin | 14.1 GB | 96% | 34% | 30.5% | <=3 s | 19 | a b c d |
| r12 | actors 10, churn-s 40, pin 40% | 16.1 GB | 96% | 39% | 19.8% | <=7.5 s | 118.6 | a b c d e |
| c1 | fleet profile with churn-s 120, 15 min | 16.7 GB | 96% | 42% | 20.2% (peak 28.3) | <=5 s | 45.3 | a b c d e |
| c2 | churn-s 300, 15 min | 16.4 GB | 95% | 38% | 27.7% (peak 39.1) | <=5 s | 18.0 | a b c d e |
| c3 | fleet profile (churn-s 200), 15 min | 15.8 GB | 96% | 38% | 21.0% (peak 23.1) | <=10 s | 30.6 | a b c d e |

What the runs show:

- Memory: 4 Mains per process with a 640 MB heap removes the reload OOM; 2 per process costs
  +3 GB and changes no lock number (r9 vs r8), so per-process packing is not what limits the lock.
- Lock busy has a ceiling near 30 to 35% on epyc1 with this release. More state (9 to 16 MB),
  more actors, more registry saves in flight and a hotter burner barely lengthen the holds (host
  holds stay 14 to 20 ms, bridge holds 30 to 45 ms). More writers do not raise busy either: turn
  churn at 15 to 40 s per Main drops busy to about 20% while timeouts jump to 120 to 170/min. The
  lock's FIFO admission then idles the lock while CPU-starved queue heads (nice 19) come back,
  and most timeouts are queued waiters that never got an attempt ("after 0 attempts").
- So 55 to 65% busy and 20 to 30 timeouts/min cannot be reached together here. Turn churn sets
  the timeout rate: 300 s gives 18/min, 200 s gives 30.6/min (the profile), 120 s gives 45/min.
  Lock busy stays near 20 to 30%. The fleet's busy
  figure needs re-checking against the instrument's definition (hold from owner record to
  release; the `.lock` presence sampler agrees within a few points) before it is used as a gate.
- PSI: the fixed-duty burner (0.5 x 32 CPUs at nice 0) gives 34 to 48% PSI; the controller
  (`--burn-pct 80`) adds nothing because the harness and other tenants keep the host above 80%.

The fleet profile (`run.sh` default; `--profile legacy` restores the built-in defaults):
`--mains-per-process 4 --heap-mb 640 --mem-budget-mb 32768 --state-mb 10 --actor-save-s 10
--saves-in-flight 4 --churn-s 200 --burn-duty 0.5`.

## Files

`run.sh` starts the burner and `orchestrate.mjs`, which runs the whole soak. The other scripts are
its child processes: `seed.mjs`, `mains.mjs`, `host.mjs`, `driver.mjs`, `observer.mjs`,
`canary.mjs`, `burner.mjs` and `instrument.mjs`. `candidate.mjs` loads code from a release.
