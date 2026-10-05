# Mesh lock namespace-reader rollout (#5157, after #4383)

**Do not change the default to protocol 2 yet.** `mesh.lockProtocol` and the bare
`MeshStore` constructor retain default `1`; explicit `1` and `2` remain selectable.
The acquisition algorithm and receipt format are separate: #505 writes Linux process
start time and PID namespace identity in **both** protocols. Keeping `1` therefore
does not make #505 safe to roll out alongside the older readers below.

## Mixed-version audit

A complete legacy receipt has three lines (token, PID, acquisition time); protocol 2
adds a fourth line (process start time). #505 appends a fifth line (PID namespace).
All records end with a newline: `split("\n")` has 4, 5, or 6 elements respectively.
The reviewed source is each revision's `src/mesh/store.ts`, including acquisition,
release, and stale reclaim, not just the selector.

| Release | Four-line protocol 2 | Five-line namespace receipt | Consequence |
| --- | --- | --- | --- |
| RC2 `e7bc3f24` | Accepted; native incarnation checked | Rejected: only split lengths 4/5 allowed | Dead owner wedges until a namespace-aware reader or fenced repair recovers it |
| RC2.1 `3d1f6854` | Accepted; native incarnation checked | Rejected: only split lengths 4/5 allowed | Same wedge; its store source is identical to RC2 |
| `dd65af9a` | Accepted; Linux start ticks checked | Extra lines ignored | Can wrongly reclaim a live foreign owner when its PID is absent locally or a local PID has different start ticks |
| `6b15d905` | Accepted; native incarnation checked | Rejected: only split lengths 4/5 allowed | Dead owner wedges |
| `195e5ac9` | Accepted; native incarnation checked | Rejected: only split lengths 4/5 allowed | Dead owner wedges |
| `147890953` | Accepted; Linux start ticks checked | Extra lines ignored | Same foreign-namespace wrongful-reclaim risk as `dd65af9a` |
| #505 (Astra round-1 repair) | Accepted | Accepted; namespace read before any local PID probe | Same-namespace dead/reused PID recovers; unknown foreign owner never expires, requiring fully fenced repair or recovery from its own namespace |

Four-line receipts carry no namespace, so even readers that accept them cannot prove
a namespaced PID belongs to their local namespace. Do not infer cross-namespace
safety from successful same-namespace protocol-2 contention.

## Behavioral evidence

Scratch worktrees of RC2 and `6b15d905` were fully built, then their compiled public
mesh entrypoints contended with #505 explicitly selecting protocol 2 on isolated roots.
Each pair completed **200 successful acquisitions per process**, with a critical-section
sentinel and entry/exit journal proving **zero overlapping holders**. Safe
`FABRIC_MESH_LOCK_OWNERSHIP_LOST` acquisition refusals are retryable in the probe and
reported separately; the initial RC2 run encountered one such refusal, not a double holder.

After SIGKILL of each version while holding the lock, #505 recovered the old three-line
receipt, but the old reader timed out and left #505's five-line dead receipt unchanged.
Thus mutual exclusion in ordinary contention passed; bidirectional crash recovery **failed**.
An additional contention run killed the victim in its **101st hold**, after both builds
had completed 100, with a target of 200 each. With the old holder killed, head reclaimed
it and both reached 200 after restarting the victim; with head killed, the old survivor
wedged at 100/200 and left the dead receipt unchanged. Every midpoint run had zero
overlapping holders. Thus 200 completions cannot honestly be claimed for the failing
crash direction. All children were awaited before scratch cleanup.

Separate real acquire/reclaim probes of all six historical source revisions confirmed
that four-line dead receipts recover everywhere, five-line dead receipts wedge on the
four strict readers, and the two permissive readers reclaim a fabricated young foreign
receipt using an unrelated local incarnation. #505 protects that same foreign receipt.
The other four revisions were source-bundled for these parser probes, not fully built
or subjected to the 200-acquisition contention test.

## Required rollout order

1. Fence and stop **all** writers and stale cleaners sharing a mesh root, including old
   Pi sessions, resident hosts, standalone bridges, maintenance jobs, and watchdog repair.
   Do not let `dd65af9a` or `147890953` inspect namespace receipts from a live writer.
2. Upgrade/restart every reader/cleaner to namespace-aware reclaim. A rolling deployment
   instead needs a reader-first compatibility release which understands the fifth field
   but still emits the legacy receipt; #505 is not that release because it already emits
   the fifth field even with protocol 1. Acceptance of extra lines alone is insufficient:
   the reader must branch on namespace before probing local PIDs or start times.
3. With old processes fenced, recover dead incompatible receipts using an upgraded reader
   (or trusted fenced repair); never remove a live lock just to unblock an old reader.
4. Enable namespace-bearing writers, then select protocol 2 everywhere and restart stores
   (selection is captured at construction). Promote default 2 only after old readers and
   cleaners have left every shared root and the bidirectional crash probes pass.

Do not suppress the namespace field or weaken fail-closed receipt validation to make
mixed-version probes pass. Protocol 1 is not a downgrade to the old Linux receipt.

## Real namespace acceptance: pending repository owner Light

The round-1 repair deliberately protects unknown foreign owners indefinitely. A
watchdog timeout, missing heartbeat, or stopped holder is not a safe reclamation
signal. An owner-namespace reader can recover a proven-dead holder; if that namespace
has disappeared, recovery needs a fully fenced maintenance window. Do not restore
age expiry to make the operational probe complete.

Ryzen 2 cannot supply real namespace proof: the requested
`unshare --user --map-root-user --pid --fork --mount-proc` fails writing `uid_map`
with `Operation not permitted`, and neither `bwrap` nor rootless `podman` is installed.
The available unit/process regressions and the compiled probe's `--self-test` are
not substitutes for distinct kernel namespaces.

**Proposed owner: Light on Ryzen 1. This is not an owner-authored acceptance gate.**
Before namespace-writer rollout (preferably before merge), Light should build the exact
candidate and run, from the repository root with a fresh persistent evidence directory:

```sh
: "${TASK_OUT:?set a persistent evidence directory}"
nice -n 19 bun run build
nice -n 19 node scripts/probe-mesh-pid-namespace.mjs \
  --launcher unshare --out "$TASK_OUT/namespace-proof"
```

This needs working unprivileged user/PID/mount namespaces and a writable `uid_map`,
plus Linux procfs. If Ryzen 1's supported sandbox is bubblewrap, substitute
`--launcher bwrap` (the launcher uses `bwrap --unshare-pid --dev-bind / / --proc /proc`).
Do not weaken host policies to run it. The output directory must be empty to prevent
stale handshakes from counting as evidence. The script imports the compiled public
`pi-fabric/mesh` entry, writes only isolated temporary mesh roots, joins owned children,
and removes those roots after preserving evidence.

Required evidence: successful `probe-result.json` with `realPidNamespaceProof: true`,
distinct recorded kernel namespaces and native five-field receipts; both protocols;
actual SIGSTOP/CONT holders in both host/sandbox directions whose waiters time out
without changing the receipt; preserved resumed-holder and successor state; 40
contention iterations per writer with no overlapping sentinel, 80 unique increasing
event sequences per protocol; and killed-holder recovery by readers in each holder's
own namespace. A killed foreign orphan must remain protected from unrelated local
PID/age recovery. Keep the receipt/proc-status records, journals, events, state and logs.
The self-test sets `realPidNamespaceProof: false` even when it passes.

If proof must wait until installation, Light must author the PR gate naming the exact
candidate, evidence location, rollout owner and rollback trigger. Proposed trigger:
any overlap, lost acknowledged state, wrong-namespace reclaim, failed owner-namespace
death recovery or missing/mismatched namespace evidence blocks rollout; if already
installed, fence every writer/cleaner, stop respawn, revert to the last accepted reader
build and repair only under that fence. Changing the selector back to 1 alone is not
rollback. Until Light supplies proof or an explicit owner-authored gate, the real
namespace review blocker remains open.
