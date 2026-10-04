# Mixed-release legacy renewal cadence (smarty-dev#4383)

## Finding

The RC2 compatibility path treated every live pre-lease peer as a reason to renew both shared records at the legacy half-life. With an explicit `{ version: 1, hostLeases: "files" }` policy, old `6b15d905` renews the shared host record on the policy cadence (600 s), while RC2 renewed it at the 7.5 s half-life threshold (10 s on the normal 5 s heartbeat). That is the source of Light's Ryzen 1 observation: RC2 Mains can write about 3x more than old Mains.

The legacy session record is different: old session-only readers expire it after a fixed 15 s, so compatibility must renew it at the 7.5 s threshold.

## RC3 fix

In compatibility mode:

- Legacy HOST renewal uses the configured host-file policy: 600 s with `hostLeases: "files"`, otherwise the old host half-life.
- Legacy SESSION renewal uses the fixed 7.5 s threshold.
- If both are due in one heartbeat, both puts are appended to one `writeBatch` commit.
- If only one is due, only that record is put.
- Host lease-file writes remain on their existing every-heartbeat path.

The focused regressions cover explicit-policy host cadence, old-reader session liveness, long host leases, and paired commits.

## Light's numbers

Light reported reads falling from about 6.6 MB/s to 1.0--1.3 MB/s, while writes rose from about 91 KB/s to 277--278 KB/s on Ryzen 1 (around 100 old Mains and a 2.35 MB state). The source comparison identifies the explicit host-file policy plus RC2's compatibility override as the cadence difference.

## Isolated benchmark

Fixture: 10 old host records + 10 RC3 Mains, 2.35 MB state, 5 s virtual heartbeats for 60 s; bytes/s is logical serialized `state.json` bytes per Main per second. RC3 was run from this checkout; old cadence is the source-compatible baseline (old explicit-policy host renewal is 0.1/minute long-run, with no host renewal in the 60 s window).

| Policy | Old commits/Main/min | RC3 commits/Main/min | Old bytes/Main/s | RC3 bytes/Main/s | RC3 stale old sessions |
| --- | ---: | ---: | ---: | ---: | ---: |
| Default | 6 | 6 | 235,296 | 235,296 | 0 |
| Explicit host-file | 0 (0.1 long-run) | 6 | 0 | 235,318 | 0 |

The explicit-policy RC3 cost is intentional: preserving old session-reader liveness requires the 7.5 s session writes even while the host record remains at 600 s. Thus the literal RC3 <= old total-commit target is incompatible with the fixed-15 s old-reader contract under an explicit host-file policy; the host-record cadence target is met.
