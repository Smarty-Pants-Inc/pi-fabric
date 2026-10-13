# Idle wake gate

smarty-dev#7978, R-no-polling: an idle agent must sleep until an event wakes it.

`scripts/benchmark-idle-cpu.mjs` starts one idle process under `scripts/idle-wake-preload.cjs` and
counts how often its timers fire:

- `--mode main`: a native Pi RPC Main with the compiled Fabric extension, activated once through
  `fabric_exec` by a keyless offline provider. No network, no inference after activation.
- `--mode resident`: an idle compiled `ResidentHost` (the minimal config of
  `tests/residency-idle-poll.test.ts`).
- `--mode script --script FILE`: any Node script; the gate's self-test uses it.

The preload uses `node:async_hooks`. Each `Timeout` and `Immediate` resource gets its creation
callsite once, at `init`: the first stack frame outside `node:` internals, as `file:line function`
(source-mapped to `src/`). Each `before` callback is one wake. The window starts after the warm-up
(default 30 s) and lasts `--seconds` (default 300).

`fs.watchFile` polls inside libuv (a `StatWatcher`), so no `Timeout` shows it. The preload wraps
`fs.watchFile`/`fs.unwatchFile` and counts `1000 / interval` wakes per second for each watched file,
under its `watchFile` callsite (type `StatWatcher`). Live `STATWATCHER` async resources are reported
as a cross-check.

`--extension PATH` (repeatable) loads more Pi extensions into the Main after Fabric, as `pi -e PATH`
does, so another repository can gate its own extension (for example smarty-inbox). pi-fabric CI runs
Fabric only.

The gate FAILS (exit 1, top 10 callsites printed) when either rule breaks, for timer (`Timeout` and
`StatWatcher`) wakes outside the allowlist:

1. more than `--budget` (1.0) wakes per second in total;
2. one callsite fires more than once a minute (more than 5 times in 300 s).

`Immediate` runs and the process's voluntary context switches per second (`/proc`) are reported
only. Exit 2 means the gate itself broke.

## Allowlist

`scripts/idle-wake-allowlist.json` lists `{callsite, reason, issue}` entries. `callsite` is
`file function` (the report's callsite without `:line`), so a line shift needs no edit. Each entry
needs review and an issue. It is empty: the participant lease heartbeat is not excluded
(chief-of-staff, smarty-dev#11172); it moves to a ~180 s TTL renewed at most once a minute.

## CI

The `idle-wakes` job in `.github/workflows/test.yml` builds once, runs the self-test, then both
modes in parallel, and writes the table to the job summary and an artifact. It is report-only
while `IDLE_WAKE_ENFORCE: '0'`; set it to `'1'` to make a breach fail the job.

```sh
bun run build
node scripts/benchmark-idle-cpu.mjs --mode main
node scripts/benchmark-idle-cpu.mjs --mode resident --seconds 60 --warmup-seconds 10
```
