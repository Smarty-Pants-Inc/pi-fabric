# Early source-path/design receipt

2026-09-30 08:44 UTC; saved inside the first ten minutes.

Source-head receipt: `7f9da3f60ffdc24baf0d546b1d0ea3eaa560f4b0` from `evidence/source-head.txt` (gitless packet; no Git used). Both completed feasibility receipts read in full: `evidence/prune-result.md` and `evidence/prune-native-result.md`. Their probes were not rerun.

## Proposed bounded design (not implemented)

Use an existing participant per-key lock helper, extended to an async lineage fence keyed by the full root/project identity. Root registration, native launch intent/binding, resident acquisition, and prune must all share that fence. Existing host lease JSON carries the root generation and exact Linux boot/start identity plus append-only managed launch intents; lease renewal must preserve this authority. Unknown platform, malformed/unreadable authority, unresolved intent, remote owner, PID reuse, or any live incarnation refuses deletion. Prune defaults to reporting and deletes only validated root-owned presence/lease/discovery/residency auxiliary records. Do not use lease expiry or directory absence as native death.

## Scope stop discovered before edits

The approved resident acquisition hunk is not the actual resident native launch boundary:

1. `src/residency/client.ts:189–193`, `ResidencyClient.ensureHost()`: `spawnDetached(this.#hostPath, ["--config", this.#configPath], this.options.config.cwd)` launches the resident launcher. This caller has the root/config authority, but passes no explicit root/host/project authority to the generic helper. Publishing that launch's intent and passing an exact intent token requires this caller hunk outside the exclusive acquisition-only residency scope.
2. `src/residency/launcher.ts:87–102`: `spawnPi(config.piBinary, ..., { detached: false, ... })` creates the actual resident Pi child via cross-spawn, bypassing ProcessTransport and spawnDetached. Pre-effect intent and immediate binding for that child require this excluded launcher hunk (plus its config/context read).
3. `src/residency/host.ts:334` calls `#acquireLock()` only once the native Pi child has loaded ResidentHost. Fencing `#acquireLock()` cannot retroactively publish either earlier launch intent.

Per the instruction to name an essential protected/out-of-scope hunk and stop, no implementation, partial always-refusing API, tests, or native probes were started. Root/ProcessTransport changes alone would leave this required launch source uncovered. Need parent authorization for the two exact native-launch hunks, or an explicit existing authority contract covering them. No scope expansion was inferred.
