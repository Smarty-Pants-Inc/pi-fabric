# Residency round 4 validation

This records the remaining PR #704 / smarty-dev#6782 validation. The runtime
source is unchanged from the event-owned deadline and custody-debt fixes; this
finish adds only this validation record.

## Named-CI selector accounting

The previous 10-run comparison did not use identical selectors. Main selected
77 cases, while the final branch selector selected 74. The latter excluded all
four `renews and dispatches actor events during periodic mesh holds (%j)` rows
in `tests/residency-presence-batch.test.ts`:

| hold | gap | phase | files | platform |
| ---: | ---: | ---: | --- | --- |
| 4900 | 100 | 650 | false | native |
| 4900 | 100 | 50 | true | native |
| 9000 | 1000 | 650 | false | native |
| 4900 | 100 | 50 | true | win32 |

The last row simulates Windows platform/timer/I/O behavior on Linux; it does
not establish native Windows coverage. None of these rows was deleted.

The old `idle resident host polling (smarty-dev#6729)` suite became
`event-driven resident host idle exit (smarty-dev#6729 / #6782)`. Its seven
cases became eight: four retention parametrizations remain, three cases were
rewritten/renamed, and a startup-backlog/watch case was added:

- `rebuilds the actor ownership view once per second of clock time, not on every
  50 ms request tick` → `arms no recurring request/idle-exit timer below 60
  seconds and does not sample actors between events`.
- `never exits on a reused actor observation: an idle exit is confirmed by a
  current check` → `checks current actor custody at the one-shot idle deadline
  and restarts the window on settlement`.
- `counts the idle window from a durable actor run that started and ended
  between two cached samples` → `counts the full idle window from an actor
  run's settlement, not its last periodic sample`.
- Added: `watches requests and drains a startup backlog larger than one
  32-request batch without a poll`.

Thus the earlier branch count was **77 - 4 + 1 = 74**. With the original common
selector the branch has **78** cases, all passing in the fresh validation.
The original selector admits both old and new idle suite names.

## Timer contract

`ResidentHost` has two recurring scheduling sites, both using
`REQUEST_RECONCILE_MS = 60_000`: request/delivery reconciliation and V2 request
maintenance. Request publication watches are armed before readiness and the
startup backlog scan. Actor, agent, delivery, request and publication
settlement drive coalesced microtask idle checks; one deadline owns the
30-second idle grace. Final release rechecks current custody and mesh cursors.

The `arms no recurring request/idle-exit timer below 60 seconds ...` regression
instruments host interval registration and proves no actor sampling between
events. Finite configuration settling, request/delivery failure retries,
checked-writer/collector debt continuations, handover drain, and at most one
dormancy safety timeout per eligibility edge are not an idle polling interval.
Existing component heartbeats, actor mesh reconciliation, legacy archive
retention and launcher watchdog supervision are separate unchanged systems;
this is not a claim that all Fabric timers were removed. The launcher's
sleep-completion branch awaits native child exit with one bounded deadline.

## Fresh checks on Linux

```sh
bun run typecheck
bun run build
bun x vitest run tests/residency*.test.ts
bun x vitest run tests/mesh-lock-fairness.test.ts \
  --testNamePattern='^(?!.*stress harness).*bounded FIFO mesh admission'
```

- Typecheck and build passed, including compiled artifact/lazy graph assertions.
- Residency/launcher/wake: 48 files, 47 passed and 1 skipped;
  **790 passed / 0 failed / 2 skipped** cases, 584.02 seconds.
- Linux fairness subset: **13 passed / 0 failed / 2 stress cases excluded**.
- Original common named selector: **78/78 passed**, including all four presence
  rows. Reduced previous selector: 74/74 within the same executed evidence.
- Linux native dormant FIFO/exit test passed with real `ResidentHost` and
  compiled launcher, explicitly fake Pi/model inference. After native host and
  launcher exit, their RSS and CPU residency are zero.

The two residency skips are the native Windows-only legacy archive case and
the opt-in `FABRIC_PRESENCE_REAL_CLI` Main-exit presence case. Native Windows
fairness is not verified here. The fairness test, lock queue and atomic
admission code are unchanged from the local main comparison source.

The retained task artifacts contain the exact original/reduced selectors,
per-case executed JSON, timer grep, original 10-run counts and logs, original
five native FIFO runs, fresh native idle measurement, and the final delta and
checksum verification. Prior counts must not be presented as a same-selector
10-run comparison or as native Windows clearance.
