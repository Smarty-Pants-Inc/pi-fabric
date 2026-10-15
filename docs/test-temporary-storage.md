# Test temporary storage

`bun run test -- <Vitest arguments>` uses `scripts/run-tests.mjs`. The launcher creates one private
run directory through `scripts/test-temp.ts`, then sets `TMPDIR`, `TMP` and `TEMP` before starting
Vitest. `vitest.config.cts` reuses this owned root before fleet state or workers are created. Fixture
directories and inherited child tools therefore stay beneath it. Direct config imports still create
their own root. There is no shared `/tmp` fallback for long caller paths.

The global setup is registered once on the root config, not on each project. Its teardown calls
`cleanupIsolatedTestTemp()` after passing or failing runs. The config and setup share process-owned
cleanup callbacks. Each callback checks the original root and parent directory inodes and uses the
current deletion-guard wrapper. Repeated cleanup is safe; a replacement directory is not removed.
The exit fallback keeps the same pins and can retry a teardown blocked by a child handle.

The ordinary launcher starts Vitest in its own process group. It forwards `SIGTERM` and `SIGINT`,
waits until the group has no active writer, then cleans its inode-pinned run root. It preserves the
child's exit status, or reports `128 + signal` (143 for `SIGTERM`, 130 for `SIGINT`). This cleanup
does not depend on Vitest teardown or the config process's exit callback.

`SIGKILL` and a whole-scope timeout cannot run cleanup code. At the start of the next ordinary
command, the helper scans direct `pi-fabric-vitest-*` children of the same caller `TMPDIR`. It
removes only private harness-marked directories whose recorded owner pid and native process-start
identity no longer identify a live owner. On Linux, boot ID and kernel start ticks distinguish PID reuse.
On other POSIX systems, whole-second `ps lstart` can preserve a stale root during same-second PID
reuse, but never deletes a live-owner root; a later run sweeps it once the reusing process exits.
Live-owner roots, markerless directories, symlinks and unsafe or replaced directories are left alone. No other
caller's temp parent is swept. If no subsequent run starts, the stale root remains for caller or
runner cleanup; the launcher does not claim immediate cleanup after an uncatchable signal.

On Windows CI, the existing job-root adoption stays unchanged. A fresh private run child beneath
`RUNNER_TEMP/pi-fabric-vitest-job` is shared through its inherited marker; the shared job directory
itself is never adopted. Teardown does not remove this runner-owned path while an MSYS process can
still pin its mount. The runner removes it at job end. Per-process fleet state keeps its own cleanup.

The repository's deletion guard performs real deletes inside the pinned physical run root and
refuses outside deletes locally. No archival recorded-delete preloader is shipped here. Any
external recorder must delegate deletes inside this root, record or refuse outside requests, and
must not archive deleted fixtures inside the tree. The `sol-brake-*` package-copy parents belong
to the separate `smarty-dev/bin/smarty-test-env` fix, not to this repository.

Scope cut: smarty-dev#12182 carries the Unix-socket path budget for long fleet caller directories.
This harness does not reject a long caller path or shorten socket names. A test that constructs an
oversized Unix socket path still gets the native socket error. That follow-up owns the short-path
solution; this change preserves the current deletion guard and caller ownership.

## Native proof

From the repository root, set `TMPDIR` to a private directory under `/srv/scratch/paul/`, then run:

```sh
systemd-run --user --scope -p MemoryMax=24G -p MemorySwapMax=0 -p RuntimeMaxSec=1200 -- \
  node scripts/probe-test-temp-cleanup.mjs
```

The probe runs the ordinary `bun run test --config <small-fixture-config>` command, not Vitest
or a helper directly. Normal and intentionally failing runs preserve statuses 0 and 1. Mid-run
`SIGTERM` and `SIGINT` produce statuses 143 and 130. Each root must be absent **before any caller
cleanup**. A killed command leaves its root; the next normal command must sweep it at startup.
Another concurrent ordinary run proves that a live-owner root survives both startup sweeping and
its neighbor's cleanup. A markerless lookalike must survive too.

The probe records command argv, exit statuses, live and remaining inode counts, and a native
`/proc` check for active workers. Only after these assertions and all native writers have stopped
does it remove the controller-owned case parents. It leaves `summary.json` and per-command
`vitest.log` files in its evidence directory for the caller to archive and remove.
