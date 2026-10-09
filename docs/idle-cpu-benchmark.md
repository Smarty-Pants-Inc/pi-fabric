# Linux idle CPU replay

`scripts/benchmark-idle-cpu.mjs` runs the compiled Fabric worker and activated
native Pi RPC Main in private, offline fixtures. It uses `/proc` CPU ticks, I/O
counters and per-thread context switches; it does not infer CPU from wall time.
Run a fresh `CC=/usr/bin/cc bun run build` first. The probe pins compiled artifact
hashes across the run and joins/validates all owned processes before removing
its temporary fixture roots.

All duration options (`--seconds`, `--profile-seconds`, `--warmup-seconds`) accept
finite positive values up to **600 seconds**. Defaults remain 30, 30 and 5 seconds.
The pending worker's request lifetime scales with the larger measurement/profile
window plus warmup and a 120-second startup margin; it must not expire during a
five-minute or ten-minute sample. A reproducible five-minute, unprofiled replay is:

```sh
node scripts/benchmark-idle-cpu.mjs --seconds 300 --warmup-seconds 5 \
  --no-profile --label round3 --scratch-dir "$TMPDIR" \
  --output-dir "$TASK_OUT/benchmark-300"
```

`--output-dir` preserves metrics, generated offline helper fixtures and cleanup
receipts; `--scratch-dir` selects the private parent for transient fixture roots.
Without those options the legacy `.local/repro-...` output and `.local/idle-scratch`
(or the OS temp directory) conventions remain. `--no-profile` skips the separate
attribution pass. Omit it to retain a separate profiled pass, not mix attribution
cost into the unprofiled sample. `--self-test` checks the instrumentation.

The seed is fixed, not randomized: 2,000 foreign completions, 1,000 foreign delivery
keys, a 4 MiB FILE state snapshot, 500 participant/host records each, 50 actor/run
and 50 Main-run directories. Fresh paths and timestamps differ by run. Preserve
`result.json` (seed counts, command, driver and compiled hashes, host/runtime and
raw measurement rows) and `terminal.json` (process cleanup receipts). A prior
sample is comparable with `--compare /path/to/result.json` only on the same host
and with the same requested duration. CPU core units are one fully occupied core;
report rates and process population, without normalizing away unrelated host load.
