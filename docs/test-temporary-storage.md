# Test temporary storage

`vitest.config.ts` creates one run directory through `scripts/test-temp.ts`, then sets `TMPDIR`,
`TMP` and `TEMP` before fleet state or workers are created. Fixture directories and inherited child
tools therefore stay beneath the run root. There is no shared `/tmp` fallback for long caller paths.

The global setup is registered once on the root config, not on each project. Its teardown calls
`cleanupIsolatedTestTemp()` after passing or failing runs. The config and setup share process-owned
cleanup callbacks. Each callback checks the original root and parent directory inodes and uses the
current deletion-guard wrapper. Repeated cleanup is safe; a replacement directory is not removed.
The exit fallback keeps the same pins and can retry a teardown blocked by a child handle.

External termination can skip teardown and exit callbacks. The caller owns the enclosing `TMPDIR`
and must remove it after its process scope has stopped. In the native proof, the single cleanup
place is `scripts/probe-test-temp-cleanup.mjs`'s `finally` block: it removes only the case directory
that the controller created, after child close and a native check that no active worker remains.
The proof stops the whole owned process group, matching a systemd scope termination.

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
systemd-run --user --scope -p MemoryMax=24G -p MemorySwapMax=0 -p RuntimeMaxSec=900 -- \
  node scripts/probe-test-temp-cleanup.mjs
```

The probe runs the real Vitest config on a small fixture, once normally, once with an intentional
failure, and once stopped mid-test. It records the live and remaining inode counts, checks that
normal and failing roots are removed, and checks zero leftovers after caller cleanup in all cases.
It leaves its evidence directory for the caller to archive and remove.
