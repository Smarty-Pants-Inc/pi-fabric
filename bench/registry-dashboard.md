# Registry/dashboard seeded performance probe (#7791 / PR #740)

Opt-in local Node CPU probe; not a Vitest/CI timing gate and not a substitute for
perf-lead's installed-Pi profile. No credentials or real fleet state are needed.
`isolateTestFleetEnvironment()` runs before source imports; all data is created
under `os.tmpdir()` and removed after managers and workers finish.

## Workload and measurements

The same deterministic shape as round 1: 35 registries, 280 stopped session actors,
10,100,012 registry bytes (largest 1,800,000 bytes), manager lists at 4 Hz for
60 seconds, five atomic writes at 10-second boundaries. CPU is process user +
system usage divided by measured wall time. `registryReadCores` instruments the
registry read method only; `hiddenUiSnapshotReads` counts Main snapshot-input
reads while the widget is hidden. Envelope parse counts include writer parses.
Startup, seeding and shutdown are outside the measured window. The workload
continues to call `manager.list()` even with no UI events, exercising actual
registry reads. It does not measure live inference, resident Pi host costs or
an elapsed animation, and overall CPU includes list/projection work beyond the
registry-read slice.

## Reproduce on an identical PR base

From the candidate checkout with existing Bun-managed dependencies, Node >=24,
and the desired base ref locally available (round 2 used `b8c3ec34`):

```sh
repo=$PWD
scratch=$(mktemp -d "${TMPDIR:-/tmp}/registry-dashboard-repro.XXXXXX")
mkdir "$scratch/base"
git archive b8c3ec34 | tar -x -C "$scratch/base"
cp bench/registry-dashboard.ts "$scratch/base/bench/registry-dashboard.ts"
ln -s "$repo/node_modules" "$scratch/base/node_modules"
ln -s "$repo/node_modules" "$scratch/node_modules"
bun build --target=node --packages=external \
  "$scratch/base/bench/registry-dashboard.ts" --outfile "$scratch/before.mjs"
bun build --target=node --packages=external \
  bench/registry-dashboard.ts --outfile "$scratch/after.mjs"
(cd "$scratch/base" && node "$scratch/before.mjs" 60000) > before.json
node "$scratch/after.mjs" 60000 > after.json
```

Run sequentially with other builds/tests idle on the same machine. The harness
imports sources relative to its `bench/` location; the identical committed
harness is copied into the base snapshot, with no product edits. Bundling to
Node preserves the Node filesystem/CPU behavior instead of comparing Bun and
Node runtimes. Existing `node_modules` are linked, not installed. Preserve JSON
outputs outside scratch and clean only your owned scratch directory afterwards.

The acceptance thresholds apply to perf-lead's installed profile on the same
Pis: registry re-read <0.05 core and dashboard poll <0.1 core while hidden. If
that installed re-profile exceeds either threshold, fabric-v2 pins back to the
previous release; local seeded improvement alone cannot authorize acceptance.
