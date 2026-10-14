DESIGN (design-pair, challenge record: .local/challenge-r1.md, .local/challenge-r2.md)

# Retention apply: owner-attributed callback fencing — #10678

**Base:** pi-fabric main `4e7c7de38c13e1a8ce79112706c8a54c4adacd8e`, including pi-fabric#1072. Every source anchor below refers to this pin, not to a proposed implementation.

**Owner:** fabric-store. **Deadline:** none. **State:** design only, with open enablement blockers. No product change, test run, deletion proof, owner approval or acceptance cut is claimed. All proposed tests and E2Es are **unexecuted**.

**Why / fits:** safe retention serves #7916's disk-reclamation goal and the product owner's build-less, data-safety principles. With apply refused today, narrowing unsupported ownership configurations is smaller than adding cross-root owner records or locking every mesh registry.

**Pair:** Opus 5.5 xhigh designer (`1bf6370c590f484c91c23d8faaf5ef66`) and resolution (`5b18b9cad3284765b0ca66b54b26f02d`); ONE Luna xhigh collaborator (`d4bc9839ba8a400ebce846469bbdfdb3`). All used process transport and read-only tasks. The lead corrected the initial Sol-designer brief to this design-pair allocation. The coordinator runs Sol 6.1 xhigh. These are explicit family switches for the lead to record on the issue. Both designer runs and the collaborator completed with exit 0; results were consumed and their native processes were absent before integration. Round 2: the Luna xhigh collaborator `52a2be9c7a1341898d6a40c795f95b6b` challenged frozen head `4a543fbd1488fdd30891a3413dcf76a88930f587` over process transport. It finished with exit 0 after 505 s; its result was consumed and its native process is absent. The Opus 5.5 xhigh designer `12c033b3b03c4db9987a781bfbf742c4` answered read-only and completed with exit 0 after 424 s; its result was consumed and its native process was absent before integration. **Two challenge rounds ran; both verdicts are CHANGES REQUIRED, not agreement.** The round-2 dispositions in §11 are this design's final response and were not re-challenged; a third round is not permitted. No CODE_REVIEW, SECURITY_REVIEW or ACCEPTANCE_AUDIT has run. Nothing here is AGREED or an implementation PASS.

## 1. Decision and numbering

Choose **option (a): reject unowned or noncanonical configuration at admission**. Do not add per-run owner-registry records. Unknown or legacy custody is retained. A proposed one-shot classification marker below is not an owner record, does not list registries and cannot establish actor custody.

The exact-one/two-registry proof is conditional on the producer, lifecycle and historical-attribution obligations in §3. Until those obligations and the other open findings close, **CLI `--apply`, public `sweepMeshRetention(...,{dryRun:false})`, and daily sweep apply remain refused together**.

| Acceptance source | Section here | Original issue-body mapping |
|---|---|---|
| Revised 10-13 item 1: ownership | §7 R1 | Body 1 |
| Revised item 2: accepted noncanonical configs | §7 R2 | Body 1 |
| Revised item 3: all references and archives | §7 R3 | Ownership/reference obligations of body 1–2 |
| Revised item 4: policies and locks | §7 R4 | Body 2 |
| Revised item 5: fd-bound registry reads | §7 R5 | Body 3 |
| Revised item 6: swaps, holds, compaction | §7 R6 | Body 4 |
| Revised item 7: clocks and existing gates | §7 R7 | Body 5 |
| Revised item 8: safe recorded tests | §7 R8 | Body 6 |
| Revised item 9: measurement | §7 R9 | Body 7; **not** the earlier comment item 9 |
| Revised item 10: coordinated enablement | §7 R10 | Body 8 |
| Earlier comment item 9: direct in-process refusal | §7 API9 | Separate requirement |
| Descriptor-custody addition | §7 FD | Separate requirement |

The supplied original body has items 1–8, not a separate body item 10. The revised item 10 answers enablement; no requirement is invented to fill that numbering gap.

## Prior art

_Section 2; the design-pair format requires this exact unnumbered heading._

Live official documentation and source were fetched on 2026-10-14. These are behaviour references; no third-party code is proposed for copying.

| Mature project / mechanism | Verified source and licence | Fit and limit |
|---|---|---|
| **systemd RuntimeDirectory** | [systemd.exec](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html), `RuntimeDirectory=` / `RuntimeDirectoryPreserve=`: unit-bound directory names under designated standard roots, lifecycle cleanup and explicit preservation. Project licence LGPL-2.1-or-later, verified in [Debian's systemd copyright record](https://metadata.ftp-master.debian.org/changelogs/main/s/systemd/unstable_copyright). | Bind a supported directory layout to its owner instead of accepting arbitrary foreign directories. Lifetime alone does not prove absence of actor references or revoke open fds. |
| **systemd-tmpfiles** | [tmpfiles.d](https://www.freedesktop.org/software/systemd/man/latest/tmpfiles.d.html), aging section: the aging algorithm takes an exclusive BSD lock on each selected object; busy objects and their descendants are skipped. [systemd-tmpfiles](https://www.freedesktop.org/software/systemd/man/latest/systemd-tmpfiles.html) separately defines age-based `--clean` and lock-respecting `--remove` for `r/R/D`. Same project licence as above. | Nonblocking lock-and-skip is the useful pattern. Do not conflate `--clean` with `--remove`, or infer this design's writer participation from an advisory-lock analogy. Fabric writers currently use mkdir/token registry locks, not flock per run. |
| **Git gc** | [Git v2.51.0 builtin/gc.c](https://kernel.googlesource.com/pub/scm/git/git/+/refs/tags/v2.51.0/builtin/gc.c), `lock_repo_for_gc`: `gc.pid` records pid and hostname, with a 12-hour recent-GC check; [git-gc](https://git-scm.com/docs/git-gc) documents concurrent-writer risk and pruning grace. GPL-2.0, verified in [Git COPYING](https://kernel.googlesource.com/pub/scm/git/git/+/refs/tags/v2.51.0/COPYING). | Process identity helps single flight, while reachability protects objects. Pid/host/age does not attribute a run to exact registries, and Git's acknowledged corruption residual is not acceptable deletion authority here. |
| **Kubernetes garbage collection** (round-2 collaborator candidate) | [Garbage collection](https://kubernetes.io/docs/concepts/architecture/garbage-collection/): owner references, finalizers and cascading deletion. The [garbagecollector package](https://pkg.go.dev/k8s.io/kubernetes/pkg/controller/garbagecollector?tab=licenses) is Apache-2.0, according to the collaborator's live check. | Owner references plus finalizers form a reference veto, but over a graph owned by an API server. That is not an adequate simpler fit here: there is no such graph, and it covers neither local names, inode generations nor fd custody. Recorded, not adopted. |

Descriptor-custody mechanism: Linux [F_SETLEASE](https://man7.org/linux/man-pages/man2/F_SETLEASE.2const.html) and [v6.16 fs/locks.c](https://kernel.googlesource.com/pub/scm/linux/kernel/git/torvalds/linux/+/refs/tags/v6.16/fs/locks.c), `check_conflicting_open`, establish the instantaneous write-lease conflict test. The kernel file is GPL-2.0-only (SPDX); this design reuses the repository's MIT native helper and syscall semantics, not kernel code. The man page is a reference, not copied code. This is not a persistent lease or a timing guarantee.

Evidence copies are in the lane's `.local/prior-art/`. A bounded arXiv query `all:"file system leases"` returned zero results at 02:26:29Z; a Hugging Face model search `filesystem lease` returned `[]`. These narrow searches do **not** prove no solution exists. GitHub calls were forbidden. Cargo documentation was checked but omitted as a mechanism: the fetched page did not verify its locking behaviour/licence. Existing kernel/library mechanisms suffice for the proposed probes; no bespoke process census is selected.

**ponytail:** reject unsupported overrides, retain ambiguous old data, reuse existing registry locks and the native lease helper. Do not invent a global owner database or increase cache/startup budgets.

## 3. Ownership admission and proof obligations

### Roots

- **M:** canonical mesh root.
- **P:** `M/actors`, the project registry.
- **S:** a direct, grammar-valid session child of P. The producer uses `fabricSessionId` (`src/fabric-runtime-state.ts:815`; `src/fabric-runtime-state.ts:1290–1294`), whereas the resident config's `sessionId` can differ (`src/fabric-runtime-state.ts:1398–1411`). Therefore do not validate S against `config.sessionId` alone.
- **T:** managed `fabricDataRoot()/pi-fabric-runs-*` roots (`src/agents/manager.ts:981–983`; `src/storage/retention.ts:613–680`).
- **Rr:** canonical `residentRoot(M, rootId)/runs` (`src/residency/protocol.ts:321–324`; `src/residency/host.ts:545`).
- **Ar:** `<registryRoot>/<actorId>/runs` (`src/actors/log-store.ts:156–169`).

Proposed exact reference sets: Π(Rr) = {P,S from the validated owner config}; Π(Ar under R) = {R}; Π(T) = ∅ **only for runs with trusted generation-bound one-shot provenance (B-8)**, never for actor, unknown or merely marked custody. This design selects no such provenance, so temp-root retention has no authority to collect a non-empty T run.

### Admission table — intentional compatibility narrowing

| Path today | Chosen admission / authority |
|---|---|
| Main/embedding caller `options.runRoot` or `PI_FABRIC_RUN_ROOT`, currently accepted at `src/agents/manager.ts:981–983` | **Reject caller-supplied overrides at ordinary Main/embedding admission.** This deliberately rejects more than lexical foreign-root detection, which cannot prove that an arbitrary accepted custom path is retention-inert. Do not scan all registries to guess its owner. |
| First-party resident AgentManager | Permit only the run root derived internally from a fully validated resident config, held host custody and its canonical Rr. The internal factory/binding must not be forgeable merely by supplying a `{residencyRoot}` string as another public constructor option. Its participation proof is part of B-4. |
| First-party process child with `PI_FABRIC_RUN_ROOT=<parent-run>/nested` | Preserve only the launch-bound lineage path from the worker (`src/worker.ts:783`; `src/agents/manager.ts:986–989`; `src/agents/manager.ts:1788–1789`). A matching basename is not sufficient ownership proof. Validate the inherited parent run/root identity; otherwise refuse the override. Nested actor or unclassified custody vetoes collection of the enclosing tree. |
| Resident `actorRoot` / `sessionActorRoot` | Normalize the legacy shapes at `src/residency/host.ts:130–135`, then require canonical P and a no-follow, direct S child of P. Reject symlinks, unresolved identity, `.`/`..`, separators or an invalid session component. Proposed component grammar: `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`. Require `residencyRoot` and the config location to match canonical `residentRoot(M, rootId)`, not merely each other. Current validation at `src/residency/host.ts:174–211` does not enforce this. |
| Main `PI_FABRIC_SESSION_ID` | Validate the same component grammar **before any actor registry or ResidencyClient is constructed**. Preserve distinct but valid `fabricSessionId` and Pi `sessionId`. Reject invalid overrides visibly; merely omitting ResidencyClient would still leave a noncanonical actor manager alive. Test the real producer→host-validator path. |
| Existing runs/configurations written before these checks | They do not become owned merely because their spelling now looks canonical. Retain unproven historical custody; certify only a population whose producer participation is established. No automatic migration/delete authority is granted. |

These are supported configuration paths **today**, not malicious injection or assumed owner-only invariants. The broader Main override rejection above is a coordinator simplification of the designer's namespace-pattern rejection; it avoids an unproved claim that custom roots lie outside every scanner. Its cost must be included in the census/ruling.

### Census before rejection

S1 adds a read-only built entry `node dist/storage/retention-census.js <mesh> --report <file>`. It inventories noncanonical resident configs, mismatched run/host attribution, unknown/actor-tagged T runs and historical populations whose ownership is not proved. Registry inventory is observation only: it acquires **no global lock** and grants **no deletion authority**. Fabric-store runs it on each fleet mesh, records every hit and its disposition, and accounts separately for environment-only embedding configurations the inventory cannot discover. S3 cannot merge its breaking rejection before that ruling. A test fixture is not a fleet census.

**Producer cutover barrier (R2-C7).** A census taken at one moment cannot see environment-only overrides. It also cannot see a pre-S3 process that keeps launching under an override that used to be accepted. The chosen route is an explicit fleet cutover/quiescence barrier, not a generation stamp on each run: a stamp that the same UID can write would repeat R2-C1.

1. After S3 is installed, fabric-store records a cutover receipt for each host. It names the installed head, the cutover time C, and every Main, resident host, actor manager and known embedder that loads pi-fabric.
2. For each producer started before C, the receipt proves that it has exited or restarted on the S3 generation. The proof uses pid identity and process start time, never a name pattern.
3. Environment-only embedders count only when their owner attests that they restarted. A host with any unenumerated or unproved producer has no receipt.
4. Rerun the census after cutover and record a disposition for every new or remaining noncanonical hit.

The §10 flip requires a receipt for every fleet host plus that post-cutover ruling. Reinstalling an older build voids the host's receipt. Data from before C, or whose producer generation is unknown, stays RETAIN: the barrier gates enablement and grants no migration or deletion authority. **B-5 OPEN.**

### Conditional proof; no membership shortcut

For each candidate, read all four references: `lastRunId`, `inFlightRun.id`, `preparing.runId`, `removal.runId` (`src/storage/retention.ts:682–705`). Missing/malformed fields or unknown roots veto the action. Pending-removal markers also veto their actor's candidates (`src/storage/retention-cli.ts:578–579`).

For an actor candidate, a bound row in Π(N) and absence of X's id from that pair are **necessary, not sufficient**. A coincidentally matching `actorId` does not prove no external registry references X. The participation audit must establish:

- **PO-1:** every actor-reference writer receives an actor-attributed run produced through the accepted admission path. Examples needing trace: `src/actors/manager.ts:3900`; restore at `src/actors/manager.ts:6430–6442`.
- **PO-2:** row adoption/import/restore cannot move or duplicate an identity/reference into a registry outside Π(N). Project adoption prepares and commits an ownership rewrite in the same registry (`src/actors/manager.ts:7387–7427`); this example is not an exhaustive proof. Legacy duplicate actor ids/run ids must retain, even when one matching row is visible in the pair.
- **PO-3:** every registry publication joins the same registry lock (`src/actors/registry-store.ts:251–259`; `src/actors/registry-store.ts:295–309`; `src/actors/registry-store.ts:416–434`).
- **PO-4:** all supported root replacements/removals join an exact root-lifecycle fence; see B-1.

**One-shot classification is not provenance (R2-C1).** A `status.retention="one-shot"` field would live in `status.json`. The worker rewrites that file repeatedly inside the run tree, which the same UID can modify (`src/worker.ts:409`; `src/worker.ts:1634`; `src/worker.ts:1869`; the tree is created at `src/agents/manager.ts:1509–1538`). Any writer of that tree can forge or copy (replay) a marker. A marker can also go stale after the run becomes associated with an actor or an unsupported producer outside Π(N).

Therefore **every T run judged by temp-root retention is RETAIN: actor-tagged, unmarked, unknown/historical and marked alike**, including closed roots and `retainRuns:true`. A producer-path test proves that the marker is emitted, not that it is genuine. Independent ARCHIVE-COMPLETE source transfer remains a separate policy: its full copied-snapshot and reference-resolution proof must pass; a marker contributes no authority.

ONE-SHOT authority requires trusted, generation-bound provenance (B-8) that meets all of the following:

- (i) No process that can write this run tree, or any other run tree, can forge or replay it.
- (ii) It is bound to the exact run id, the run directory's dev/ino generation and the admitted producer generation from the §3 cutover.
- (iii) It is issued only on a proven non-actor launch path, never by actor start or restore.
- (iv) Any later actor-reference publication that names the run invalidates it, or the provenance provably rules such a publication out.

This classifies a run; it is not a per-run owner record. It lists no registries, and option (a) is unchanged. This design selects no mechanism. S1–S4 neither emit a marker as authority nor mint a ONE-SHOT handle. Empty roots may use the independent EMPTY policy. Main one-shot temp bytes therefore accumulate until B-8 closes, and B-7 measures that cost. This is not a promise to reclaim legacy temp bytes.

**Conclusion:** option (a) is selected, but the historical/cohort and root-lifecycle proofs remain enablement blockers. No at-most-two-registry deletion authority is asserted on current main.

## 4. Every reachable retention callback

Each destructive callback must receive an explicit policy handle; a missing handle means RETAIN, not an implicit `NO_REGISTRY_REFERENCES`. Zero-registry policies still need target/descriptor custody. Locks are acquired per action, never around the whole prune. Compaction inherits its caller's policy, including young-run wrappers.

| Path / current-main anchor | Proposed policy and lock count | Implementation owner |
|---|---|---|
| Closed T `pruneClosedRunRoot`, `src/storage/retention.ts:476–505` | RETAIN (0) until a ONE-SHOT capability backed by B-8 provenance can be minted (see the policy-handle capability below); a marker alone never suffices. Young events compaction must use the same verdict, not an independent permission. | S2 |
| Orphan T branch, `src/storage/retention.ts:656–677` | Same rule; marked, unreadable, legacy and actor-tagged runs retain (0). | S2 |
| Empty run root, `src/storage/retention.ts:468–475` | EMPTY: exact tree contains only the owner marker (0); descriptor probe includes that file. | S2 |
| AgentManager close, `src/agents/manager.ts:3727–3737` | Independent ARCHIVE-COMPLETE only (ONE-SHOT only after B-8). A marker, or an unknown or actor source without completion, retains (0). Replace raw `removeTree` at this retention path. | S3 |
| Archive source commit, `src/agents/manager.ts:3103–3110`, called after `src/actors/manager.ts:5264–5265` | ARCHIVE-COMPLETE bound to the copied source generation, not a `void` copy result (0). | S3 source barrier; S4 caller |
| Budget directory, `src/agents/manager.ts:3743–3744` | Explicit NON-RUN, through the custody primitive (0). | S3 |
| ActorLogStore prune, `src/actors/log-store.ts:156–169`, to `src/storage/retention.ts:992–1020` | OWNER-LOCKED(R), all four refs (1). Missing exact registry/root binding retains. | S2 function; S3 log-store; S4 manager wiring |
| Session backups, `src/storage/retention.ts:920–961`; rotation/prune caller `src/actors/manager.ts:1832–1865` | NON-RUN plus clock and file custody (0). Prove rotation/writer quiescence; do not infer it from archive age. | S2 function; S4 caller |
| Child completions, `src/actors/child-completions.ts:352–381` | NON-RUN plus owner's `keepIds` and descriptor custody (0). | S2 |
| Resident request collection, `src/residency/retention.ts:465–481` | NON-RUN plus watermark and descriptor custody (0). | S2 |
| Sweep-slot cleanup, `src/storage/retention.ts:528–545` | NON-RUN plus existing live-sweeper proof (0). | S2 |
| Live resident v2 and Main branches, `src/residency/retention.ts:254–264`; `src/residency/retention.ts:358–367` | PAIR-LOCKED(P,S) under validated config and lifecycle custody (2). Fingerprint comparison alone is not a final read. | S2 |
| Compaction, `src/storage/retention.ts:802–916` | Caller policy (0/1/2); pinned-parent publication and post-publication veto rules. | S2 |
| Dead resident, `src/residency/retention.ts:39–72`, from `src/storage/retention-cli.ts:512–528` | PAIR-LOCKED from validated owner config (2), inside the host single-flight fence. | S2 function; S4 CLI |
| CLI resident actor archives/backups, `src/storage/retention-cli.ts:531–560` | Exact OWNER-LOCKED per archive action (1); backups explicitly NON-RUN (0). Replace whole-prune async lock. | S4 |
| CLI mesh actor archives/backups, `src/storage/retention-cli.ts:570–596` | Same exact policy; row/sessionFile mismatch retains. Never lock all discovered registries. | S4 |
| `removeUnreferencedRun` / Main wrapper, `src/storage/retention.ts:1034–1055` | Require explicit policy through the wrapper. No unsupported-platform fallback for these retention actions. | S2 |
| Legacy archive, `src/residency/legacy-run-archive.ts:171–221`; current two-field fence `src/residency/legacy-run-archive.ts:258–259` | MOVE-ONLY, extend to all four fields and preserve actual destination. No independent permission to delete the archive. | S2 |
| Offline actor archive move, `src/actors/remove-offline.ts:470–495` | MOVE-ONLY; root-lifecycle participation is in B-1. Removing a row cannot authorize deleting its external run. | Existing path; B-1 inventory |

Boundary census must also list root-tree deletion at `src/actors/manager.ts:3233` and live cleanup at `src/actors/manager.ts:3027`; they are lifecycle actors, not an escape from B-1. Scratch at `src/storage/scratch.ts:5–65` is not a retention path and is not converted by a blanket “annotate every rm in src” requirement. Registry reaper/release at `src/actors/registry-store.ts:494–503` and `src/actors/registry-store.ts:518–525` are lock-protocol participants, not candidate deleters. Their races still matter to release custody.

Host flock (`src/storage/retention-cli.ts:436–474`) and sweep flock (`src/storage/retention.ts:548–570`) give single flight, **not** actor-registry custody. Current mesh destructive branches are unreachable because sweep receives literal dry-run (`src/storage/retention-cli.ts:190–193`). In-process paths already exist; staged changes may only tighten them, not claim their existing gaps are solved.

### Policy-handle capability (R2-C2)

A policy handle is a runtime capability, not data. Only S1's `src/storage/registry-custody.ts` mints handles. Its minting function, handle class and state table are module-private and never re-exported through any barrel. A handle is genuine only if it is found, by object identity, in a module-private `WeakMap`. TypeScript brands, `kind`/`roots` fields, symbols reachable from exports, `instanceof` and JSON shapes grant nothing. Deleters call the exported verifier `requireCustody(handle, target, policy)`, which can only read that table. The module's scoped acquisition entry validates the request and invokes the action with the privately minted handle; it does not expose a constructor or a mint accepting asserted custody. Zero-lock policies (EMPTY, NON-RUN, ARCHIVE-COMPLETE, MOVE-ONLY) use the same module's evidence and target-generation binding, without acquiring registry locks.

- **Mint:** for registry policies, only after the §5 sequence succeeds: root fds pinned with no-follow; nonblocking lock mkdir; token owner file created; identities reverified after acquisition. The handle binds:
  - its policy and lock count;
  - for each root (sorted, at most two): the pinned fd, root dev/ino, lock dev/ino, owner dev/ino, token and root-lifecycle generation;
  - the target: the parent fd's dev/ino, the name and the target dev/ino;
  - the policy's evidence: archive-manifest identity, NON-RUN watermark or `keepIds`, or B-8 provenance;
  - the sweep id and pid.

  Registry-locked policies need the B-1 lifecycle generation. While B-1 is open they cannot be minted, so those callbacks, including today's in-process ones, RETAIN under the proposed policy (cost under B-7). No ONE-SHOT handle exists before B-8.
- **Lifetime:** one handle covers one action on one target in one process. Verification before the first move marks the handle consumed in a single synchronous JS step. After that, only the same action's internal continuation may proceed: recheck → probe → final read → unlink or publication → rollback. Release, revocation, any uncertainty, an exception, the end of the sweep or process exit invalidates the handle. It cannot be serialized: `toJSON` throws, and a clone, `postMessage` or child-process copy has a new identity. The lease helper child receives fds, never authority.
- **Rejected, then RETAIN with a sticky stop and zero target mutation:**
  - forged handles, including a plain `{kind:"owner-locked",roots:[…]}`;
  - stale handles: released, revoked, or any bound root, lock, owner, token or target identity differs on reverification;
  - reused handles: already consumed, or presented for a second target;
  - a cross-target, cross-policy or lock-count mismatch;
  - a handle from another sweep or process, or one checked by a duplicate module instance that cannot see the table.
- **Allowed control:** a genuine handle from a real acquisition over an unchanged eligible target authorizes exactly that action and is then released. A real writer then reacquires the lock.
- **Tests:** primitive cases go in S1's existing T3 file and callback-level cases in S2's T2a file; no new files. The capability defends against fabricated data. In-process code that calls the real acquisition still has to take the real locks.

## 5. Custody mechanisms

### Registry lock and lifecycle

Open and pin the judged registry root **before acquisition** with a no-follow descriptor walk of every directory component and `O_DIRECTORY|O_NOFOLLOW`; validate root dev/ino, ownership and closed permissions. Through that fd, make one nonblocking mkdir of `actors.json.lock`. `EEXIST`, error or uncertain identity means skip. Never wait or reap in retention. Open the resulting lock no-follow, create the existing-format token owner file exclusively through its fd and bind {token, root dev/ino, lock dev/ino, owner dev/ino}. Sort exact P/S acquisitions; if the second is busy, release the first and skip.

Once the exact owner locks are held, reverify the judged root identities and required lifecycle binding before any target move. Keep that custody from this identity verification through target move, final fd read, publication/removal, rollback and release. Release checks all bound identities and exact token, unlinks only its owner file through the lock fd, then uses nonrecursive rmdir through the pinned root. Uncertainty stops further candidate actions. A release recorder emulates this by quarantining only its own artifacts, not deleting them. Check canonical release and real writer re-acquisition, not merely “release was called”.

**B-1:** a root fd pins the old inode, not its pathname. Sequence: lock/pin old S → replace S → writer locks new S and publishes X → retention reads old S and sees absence. A final path stat is another TOCTOU, not a fence. Candidate reuse: `deleteRootActorTree` at `src/topology/main-publication-fence.ts:142–151`, with `takeMainPublicationFence` at `src/topology/main-publication-fence.ts:68–104`. Its reference-counted acquisition at `src/topology/main-publication-fence.ts:109–126` must not accidentally give retention shared exclusive ownership with a remover in the same process.

This candidate is **not adopted as proved**. Need every remover/renamer, exact path→rootId for P/S/Ar/Rr, no waiting acquisition and measured Main-publication cost. Existing fence publication refusal is at `src/topology/participant-directory.ts:2976`. It cannot be papered over with a global registry lock. Supported writers must bind publication to the judged root generation, or participate in the lifecycle mechanism; writing a new path while retention locks an old inode must never succeed unnoticed. Remaining designer/collaborator disagreement is in §12.

### Registry bytes and cache

Replace the authority of path `lstat`/`readJson`/stamp caching at `src/storage/retention.ts:726–783`. Under exact custody:

1. Open `actors.json` no-follow/nonblocking through the held root fd.
2. Require a regular, owned, closed file, expected link count and judged root generation.
3. Read bytes from **that same fd**, with bigint fstats before/after and a name→inode check through the held root.
4. Validate every row/reference; bind parsed bytes and generation {root dev/ino, file dev/ino, size, mtimeNs, ctimeNs} to the read.
5. A missing/malformed file, changed root/name, differing fstats, uncertain resolution or same-size/coarse-time ambiguity yields wildcard veto, never false absence.

Smallest cache rule: retain at most the one/two decoded registries needed for **this callback**, and reread fresh bytes for every authoritative final read. No parsed-result reuse across actions based on an old timestamp stamp. Do not increase the 64-entry actor-store cache or its racy window (`src/actors/registry-store.ts:69–85`). Discovery reads may skip early but grant no deletion authority. Same-UID/unfenced byte writers and prior writable registry descriptors belong in the producer/descriptor audit; unchanged coarse stamps alone cannot certify a generation.

No optional engine import at registration/idle. Extract narrow helper verification rather than importing an entire mesh/runtime barrel into a synchronous deleter. S1 must run cold import/idle, first use, `assert:lazy-graph` and build without relaxing graph budgets. This is a planned check, not a measured “no startup change” assertion.

### Descriptor custody

Current helper `native/fabric-mesh-lease.c:240–270` opens lexical paths at `AT_FDCWD`, reports `free/held/error/absent`, and releases a write lease immediately. It neither checks expected dev/ino nor vouches for a judged-present file that is now absent.

Extend it with a bound mode taking an inherited directory fd and **one expected dev/ino per relative target**, with no `..`/absolute components and no-follow traversal of each directory step. Inherited graveyard-container identity must also match. For every regular inode in the judged tree, close the deleter's own file descriptions before probing; otherwise its own open reader can correctly veto. The helper opens relative to the inherited directory, fstats, then probes `F_SETLEASE F_WRLCK`. Mismatch, missing output, `absent`, missing helper, digest failure, `EINVAL/ENOLCK` or timeout means keep. Only every expected inode reporting `free` permits continuation.

**Trusted executable handoff:** preserve the existing same-fd hash-and-execute pattern at `src/mesh/reader-proof.ts:330–335` and `src/mesh/reader-proof.ts:370–409`, including the helper/module install-chain permission checks. Do not regress to hash-by-path then execute-by-path. Example stdio assignment: verified executable fd → child fd 3, graveyard fd → child fd 4; execute `/proc/self/fd/3` with bound mode `--at-fd 4`. A child's `/proc/self/fd` is not its parent's fd table. Regenerate pinned per-architecture expected hashes in `native/toolchain.json`; current reproducibility/pinning enforcement is at `scripts/build-landlock.mjs:104–127`.

Order: enter exact registry/lifecycle custody → move target into private graveyard → recheck → descriptor probe → final references/archive proof → recheck → in-deleter gate → unlink, then rollback/release as appropriate. Apply probe semantics to every relevant regular file, including owner/backups and registry-generation custody where prior writable handles could defeat its read proof; inability to establish that custody vetoes.

A different-UID fd opened before chmod is **not revoked by chmod**. If it remains open, the write-lease probe refuses before unlink; its later data stays. If it writes and closes after an archive copy, snapshot/hash comparison below still detects uncopied data. An uncontended tree must still pass and delete in hosted CI.

**B-3:** an arbitrary same-UID/root opener or name swap after a one-shot probe remains possible. Supported Fabric writers cannot be silently exempted: census and fence them or prove quiescence. The arbitrary-process threat needs an explicit scope/risk ruling; this assignment does not grant one. The helper is an instantaneous probe, not a held lease or hard bound. The exact post-final-recheck temporal regression remains an enablement gate if an early refusal test cannot reach that seam.

### Archive-completion proof

`ActorLogStore.retainRun` currently returns `void` and skips/logs copy failures (`src/actors/log-store.ts:98–152`); its caller then commits source cleanup (`src/actors/manager.ts:5264–5265`). Change it to complete/incomplete evidence, never infer completion from return.

Capture the entire permitted source entry set, directory identities and each file's {dev,ino,size,mtimeNs,ctimeNs,SHA-256} while copying through source/destination fds. Compare source pre/post fstats; verify archive bytes by reread and fsync. Missing required entries, unknown entries, nested failures or a destination swap mean incomplete. No best-effort nested copy may produce completion.

Persist the manifest through the held archive directory before source cleanup. The AgentManager source barrier independently verifies it even while an older caller ignores the new return value. Missing/incomplete manifests veto or throw **before pending-source custody is cleared**. At the final deletion check, after the source move/probe, compare the source entry set and re-hash its bytes against that captured manifest; bind the actual archive inode and its verified contents too. “Fresh judgment looks closed” is not archive proof.

Reported sequence: foreign writer copies-era fd → archive copy → writes and closes → new deletion judgment/probe sees no opener. The manifest mismatch retains the late data. Control: unchanged fully archived source can be removed. References resolve to the verified archive; archive pruning separately uses OWNER-LOCKED. The S4 caller consumes the typed result and passes exact prune custody; S3 alone must already prevent source deletion on failure.

### Compaction, clock, gates

Compaction at `src/storage/retention.ts:802–916` publishes the new tail through the **pinned parent fd** using exclusive temp creation, fsync and no-replace link. Its old full-history inode moves to the private graveyard first. After publication, any reference/gate/target veto leaves that old inode in its actual located graveyard, reports it and makes the sweep sticky. Never delete it or roll it back over the new tail. Before publication, rollback must not replace a new canonical entry. Preserve fd-relative mtime restoration (`src/storage/retention.ts:889–897`). Current recheck/rollback/removal anchors: `src/storage/verified-remove.ts:473–529`.

Backups retain newest-by-name **and** newest-by-observed-age, all future/unparseable evidence, and prune nothing if ordering/clock evidence is uncertain or backward. Use fresh current time for rotation names (`src/actors/manager.ts:1832–1865`), never `max(existing future stamp)+1`. Apply the same conservative newest/custody rule to actor archives (`src/storage/retention.ts:964–1020`); locked live refs always veto regardless of age. No accepted clock verdict is invented.

Preserve hold/epoch/deep state-schema checks at `src/storage/retention-cli.ts:41–97`, rechecked **inside** action after the caller's check. Mesh apply becomes Linux-only with both uid APIs and required fd/lease facilities. The present Windows-helper allowance (`src/storage/retention-cli.ts:55–59`) is not a Linux proof. Retention wrappers must not bypass ownership checks through the earlier fallback at `src/storage/verified-remove.ts:412–427`.

Proposed acknowledged `retention-cli <mesh> --hold`: write hold exclusively/no-follow, then acquire and release the same per-action retention gate, and only then acknowledge. A sweep tries that gate around the final check/unlink sequence; busy means sticky stop. This is not a registry-writer/global ownership lock. **B-2:** raw hold creation between that final gate read and unlink still races. Keep apply refused until a mechanism meets the requirement or the lead obtains an explicit acceptance change; a status-file check is not atomic exclusion.

Any swap, unlocated graveyard, reference uncertainty, hold/gate refusal or uncertain release stops later candidates. “Refused this candidate” is not enough. In veto cases every target remains at its original path or the correctly reported graveyard, with zero target deletes.

## 6. Test catalogue and Decision C

All tests below are proposed. Local execution of real-delete paths and fixture cleanup is forbidden (#4664). A recorded test's teardown must not secretly delete. Hosted real-delete files require an explicit CI flag and fail, rather than skip, if their prerequisites are absent. Owner for every test/E2E is **fabric-store**, delegated to the named slice implementer.

| ID / proposed file | Why this level (Decision C) | Required pass and allowed control | Named real E2E |
|---|---|---|---|
| T1 `tests/retention-custody-census.test.ts` | Static participation/callback inventory makes the finite proof obligations reviewable; source text alone is not proof. | All four reference producers, registry publications, admission exceptions and lifecycle removers traced. Missing producer or duplicate/legacy attribution blocks authority. Do not hide required red evidence with an expected-fail test presented as PASS. | H1a/H1b/H1c and E2E-POST |
| T2a `tests/retention-fence-recorded.test.ts` | Record mutations at deterministic race seams; no local unlink. | Core rows in §4, including young wrappers, EMPTY, backups and compaction: veto has zero candidate inode/path deletes; allow control records only planned mutations. Unknown untagged T run with external `lastRunId` retains. R2-C1: fresh, forged, replayed (copied from another run) and later-rewritten markers, and a marked run that an actor later references, all RETAIN with zero deletes. A producer-only test is not integrity proof, and no allowed control deletes a marked run before B-8. R2-C2: a missing handle or a forged plain object at every callback retains. | H1a; E2E-POST |
| T2b `tests/retention-owner-fence-recorded.test.ts` | Owner close/archive paths would delete; record instead. | Complete vs incomplete archive, late write-and-close, nested failure; retain failed source/pending custody, allow unchanged complete source. | H1b; E2E-POST |
| T2c `tests/retention-mesh-fence-recorded.test.ts` | Full CLI planner/action races need controlled hooks with recorded target deletes. | Exact one/two locks, every four-field reference, late target/root/backup swap, raw and acknowledged hold, sticky later candidates. C2 sequence: replace S after lock, writer publishes X into successor; zero X deletes. Allowed unchanged/no-reference successor-free control deletes. | H1c; E2E-POST |
| T3 `tests/registry-custody-release.test.ts` | Release uses destructive namespace operations; emulate in owned quarantine. | Match exact token/root/lock/owner dev/ino, quarantine only recorder-owned artifacts, canonical names released, writer re-acquires. Swapped successor untouched; zero candidate touches. R2-C2 primitive cases: forged, stale, reused, cross-target, cross-policy, cloned/serialized and other-process handles are rejected before any recorded mutation. Allowed control: a genuine handle performs exactly one action, then releases. | H1c and H3 |
| T4 `tests/registry-custody-read.test.ts` | Read helper needs deterministic fd/open/read/stat swaps. | Parsed bytes/generation from same fd; swap/same-size/coarse-time uncertainty wildcard-vetoes. Fresh-read allowed control sees stable valid bytes; ≤2 decoded entries, existing cache budget unchanged. | H1c registry-swap case |
| T5 `tests/retention-descriptor-custody-recorded.test.ts` | Real lease helper, recorded deletes; foreign-fd setup is hosted where needed. | Preopen → chmod closed → caller final-check/write race → refusal before unlink, late bytes preserved. Expected-inode mismatch/absent/digest failure veto. No-opener control proceeds. State explicitly when refusal precedes the internal final recheck; do not claim an unreachable seam executed. | H2 via actual sweep entry; E2E-POST |
| T6 `tests/retention-backups-clock.test.ts` | Rotation/prune can delete; record. | Backup and actor-archive future, backward, unparseable and tied/coarse clock fixtures retain newest/uncertain set; fresh rotation name is not poisoned. Ordered, old, unheld control prunes. | H1b backups; H1c archives |
| T7 `tests/run-root-validation.test.ts` | Real constructor/validator proof before spawning; native cleanup remains recorded. | Reject ordinary explicit run-root overrides and noncanonical roots; real Main producer→host with valid distinct fabricSessionId succeeds. Internal resident/nested exceptions pass only with proved binding; fake string binding rejects. Actor launches remain actor-attributed; a forged/replayed marker supplies no authority, including after a later actor-reference publication. | H1b and E2E-POST resident startup |
| T8 `tests/retention-archive-complete.test.ts` | Source commit deletes; record it and the exact copy snapshot. | Foreign write-and-close after capture vetoes even with a free probe; copied target swap/incomplete/nested failure veto. Complete unchanged source plus exact archive allows. | H1b |
| T9 `tests/retention-compaction-publish.test.ts` | Publication/unlink/rollback require recorded fs seams. | Pinned-parent publish; post-publish late reference/hold/swap keeps old inode at actual graveyard and new tail untouched. Pre-publish keep safely restores only to empty name; clean control compacts exact bytes. | H1a/H1b/H1c appropriate callers |
| T10 `tests/retention-census-cli.test.ts` | Real read-only built census on fixture; fleet inventory still required. | Correct uncertainty/hit counts, no mutation. No-hit fixture does not substitute for fleet ruling. | Fleet census run, owner fabric-store, every hit disposed before S3 |
| T11 `tests/retention-apply-platform.test.ts` | Built CLI Windows refusal is itself a real entrypoint check. | Valid mesh + installed helper still refuses apply on Windows with zero changes before/after enablement; dry-run works. | Hosted Windows built CLI |
| API9 `tests/retention-apply-boundary.test.ts` | Required direct exported API, not a proxy CLI/helper test. | Valid state/hold/epoch/ownership fixture with `dryRun:false` refuses before any deletion callback; zero changes. Keep getter-mutation control by reading dryRun once. Only final coordinated flip changes expectation. | H1c; direct API is also independently required |

**Race matrix:** after judgment, root open/acquire, move, first recheck, probe, final reference read, publication, second recheck and immediately before first unlink. Inject each four-field late reference, root/target/backup/registry successor, hold and lock/release uncertainty. Test the reported sequence **and** the allowed unchanged counterpart. Empty-root/sweep-slot/request/completion deleters need their own NON-RUN/EMPTY controls; a run-only test is not exhaustive coverage.

Hosted files (not all helper tests are E2E):

- **H1a:** `tests/hosted/retention-deleters-real-delete.test.ts` drives built `node dist/storage/sweep-main.js '<request-json>'` (`src/storage/sweep-main.ts:7–15`) plus live resident public maintenance via a real resident. Covers T2a/T9 and non-run callers that the temp sweep cannot reach.
- **H1b:** `tests/hosted/retention-owners-real-delete.test.ts` launches the real Pi CLI/RPC and loads `dist/index.js` as its extension, then drives public actor start/archive/close and backup rotation. Reuse the launch pattern in `scripts/prove-retention-cli.mjs:9–21`; **`node dist/index.js` is not the Pi CLI**. Run canonical resident v2 and Main cases, completions and request maintenance on their actual owners.
- **H1c:** `tests/hosted/retention-mesh-apply-real-delete.test.ts` drives built `node dist/storage/retention-cli.js <mesh> --apply --report <file>` on the candidate flip head. Covers planner/action, four-field refs, lock release, compaction, clock and late holds/swaps. **R2-C6:** the same S4-owned file must also prove two more entrypoints on that head. Each needs the §8 safety leg: late references, holds, and root, target and registry swaps cause zero target deletes and a sticky stop, and retained bytes stay exact. Each also needs the allowed leg: exact integer before-minus-after bytes.
  - **Direct exported API:** a fresh Node process imports built `dist/storage/retention-cli.js` and calls `sweepMeshRetention(mesh,{dryRun:false})`; loading `src/` through a test transform does not count. Include a `dryRun` getter that changes after its first read, to prove the entry reads it once. API9's pre-flip refusal is a separate test and not this proof.
  - **Daily owner path:** a real Pi CLI/RPC Main on Linux (launch pattern `scripts/prove-retention-cli.mjs:9–21`) with a persistent, mesh-enabled ActorManager whose actor root is under `<mesh>/actors`, on a fresh mesh with no prior slot claim. Its constructor slot (`src/actors/manager.ts:958–959`) reaches daily maintenance (`src/actors/manager.ts:5547–5549`) and `#startMeshRetentionSweep` (`src/actors/manager.ts:5279–5303`). There, `startLockedMeshRetentionSweep` (`src/storage/retention.ts:570–583`) takes the sweep flock and spawns the detached built `retention-cli.js` child, with the lock as fd 3 and post-flip `meshRetentionSweepArgs` (`src/actors/manager.ts:313–314`).
    - Assert the lock claim and sweeper record, and that the running child's own argv contains `--apply`.
    - Assert the lock stays held until the child exits and the report appears at `<mesh>/.mesh-retention-report.json`.
    - Assert hold, epoch and state gates and sticky stops are honoured, and a second Main in the same interval starts no child.
    - Observe both child completion/lock release and the report with a bounded deadline, never a fixed sleep.
    - Add no public test trigger. If the slot cannot be reached deterministically, S4 reports a blocker.
    - An argv-only unit test, H1a's `sweep-main.js` or a direct CLI call is not this proof.
- **H2:** `tests/hosted/retention-descriptor-custody.test.ts` opens a writable fd as a second UID before chmod and holds it while the actual sweep attempts action. Refusal at the probe is a valid preservation result; late write bytes remain. Do not report “internal final recheck reached” when it was not. Also test write-and-close after archive capture and the no-opener actual-delete control. The exact final-recheck/unlink requested sequence must be tested on a reachable real action or remain open under B-3; a helper-only green is not acceptance.
- **H3:** `tests/hosted/retention-registry-contention.test.ts` drives CLI apply while real registry writers save concurrently. Produces measurements, not a fake fixed-sleep bound.

### Hosted admission, not silent skips

S1 edits `.github/workflows/test.yml` explicitly. The current affected selection at `.github/workflows/test.yml:141–146` does not guarantee a conditional hosted test runs with its required flag; it does **not** intrinsically exclude all hosted files.

Add a Linux real-delete step after build, with `FABRIC_HOSTED_REAL_DELETE=1`, a disposable second UID, private fixture root and `bunx vitest run tests/hosted/ --reporter=json --outputFile=<runner-receipt>`. `scripts/hosted-real-delete-receipt.mjs` must reject a missing report/test file, zero tests, any skipped/pending/failed required case or wrong head. Its expected inventory is the exact H1a/H1b/H1c/H2/H3 files present at that slice head, not every unrelated future hosted test. Upload the receipt. Add an explicit Windows T11 step. No flag means no real-delete execution locally; missing prerequisite **with** the hosted flag means failure, never success-by-skip.

Before the enablement PR merges, hosted real-delete tests run on its exact candidate head; no real-delete path is executed on this lane. Tests requiring the flip cannot make an earlier refusal-only slice claim enablement proof.

## 7. Acceptance ledger — mechanism, anchor, failure, proof

The mechanisms in §§3–5 and tests in §6 are definitions, not implemented protections. An OPEN entry forbids enablement even if all other tests pass.

| Item | Chosen mechanism / current-main anchors | Failure closed or still OPEN | Required test and allowed control |
|---|---|---|---|
| **R1 ownership** | Option (a) admission, producer/cohort proof; `src/agents/manager.ts:981–993`; `src/worker.ts:783`; `src/storage/retention.ts:613–680` | B writes A's Rr while references live only in B's registry; closed actor/legacy T false absence. **B-4 OPEN.** | T7 real B→A override refused; T2a untagged/actor-tagged legacy retained; legitimate bound resident/nested control; H1b/E2E-POST. |
| **R2 accepted noncanonical configs** | Canonical P/S and residency binding, producer validation before construction; `src/residency/host.ts:130–135`; `src/residency/host.ts:174–211`; `src/fabric-runtime-state.ts:815`; `src/fabric-runtime-state.ts:1290–1294`; `src/fabric-runtime-state.ts:1398–1411` | Accepted noncanonical roots fall outside discovery; producer/sessionId mismatch incorrectly rejected. **B-5 ruling required.** | T7 all legacy shapes and valid distinct session ids; fleet T10 census before rejection; actual Main→resident control. |
| **R3 all references/archive custody** | Four fields, removal veto, MOVE-ONLY and snapshot-bound ARCHIVE-COMPLETE; `src/storage/retention.ts:682–705`; `src/actors/log-store.ts:98–171`; `src/actors/manager.ts:5264–5265`; `src/actors/manager.ts:6430–6442` | Archive/removal or best-effort copy orphans a referenced/uncopied run. **PO-1/PO-2 OPEN.** | T2a/b/c each field × live/closed/archive; T8 write-and-close failure vs unchanged completed archive; H1b/H1c. |
| **R4 policy/locks** | Explicit per-action handles and exact 0/1/2 root-fd-bound locks; `src/actors/registry-store.ts:459–527`; `src/residency/retention.ts:254–264`; `src/storage/retention-cli.ts:531–596` | Caller-only checks race writer publication; all-registry locking stalls fleet. Root replacement still **B-1 OPEN**. | T1 path census, T2 all callbacks, T3 release, H3 busy writer control. No action locks all discovered roots. |
| **R5 fd/cache** | Fresh final fd bytes + exact generation, ≤2 action entries; `src/storage/retention.ts:726–783`; `src/actors/registry-store.ts:69–85` | Stat/open/read swap or aliased coarse stamp establishes false absence. | T4 same-size/coarse-time/swap veto vs stable read; H1c registry-swap E2E; cold import/first-use/graph/build evidence. |
| **R6 late swaps/holds/compaction** | Pinned parent publication, post-publish graveyard preservation, sticky stop; `src/storage/verified-remove.ts:473–529`; `src/storage/retention.ts:802–916`; `src/storage/retention-cli.ts:41–97` | Last caller check bypassed; old history lost; new tail overwritten; later candidates continue. **B-1/B-2/B-3 OPEN** for remaining atomicity windows. | T2c/T9 seams and unchanged controls, H1c actual custody + E2E-POST. Raw late-hold red cannot be waived by an acknowledged-hold green. |
| **R7 clocks/gates/platform** | Conservative newest/uncertain sets and fresh names; strict platform/uid/facility refusal; `src/storage/retention.ts:920–1020`; `src/actors/manager.ts:1832–1865`; `src/storage/retention-cli.ts:41–97`; `src/storage/verified-remove.ts:412–427` | Future timestamp poisons later rotations or newest deletion; fallback bypasses no-uid/platform/state/hold gates. | T6 future/backward/uncertain vs ordered control; T11 Windows helper still refuses; hosted valid Linux control after gates. |
| **R8 safe tests/release** | Recorded inode/path mutation map and owned quarantine release; `src/storage/verified-remove.ts:473–529`; `src/actors/registry-store.ts:518–525`; `tests/retention-apply-boundary.test.ts:123–125` | A test quietly deletes fixtures or validates release merely by callback invocation. | T1–T9 exact survivors + controls; T3 token/dev/ino/reacquire/zero touches; H1/H2 actual deletes only in hosted CI. |
| **R9 measurement** | §8 per-action spans and concurrent writer distribution; `src/actors/registry-store.ts:7`; `src/actors/registry-store.ts:509–511`; `src/residency/retention.ts:251–253`; `src/actors/manager.ts:5279–5304` | Sampled budget falsely advertised as syscall bound; writer timeout/starvation unmeasured. **B-6 OPEN.** | H3 representative + injected-slow paths, actual writer saves, owner disposition on every residual; E2E-POST no writer timeout in tested envelope. |
| **R10 enablement** | §10 coordinated final flip and gates; `src/storage/retention-cli.ts:126–159`; `src/storage/retention-cli.ts:628–632`; `src/actors/manager.ts:306–314` | One public/daily surface enabled early, skipped hosted proof or open findings mistaken for approval. | API9 remains refused through preparatory slices; H1c exact flip head, all named reviews/audit/receipts, E2E-POST and paired disablement on failure. |
| **API9 earlier comment** | Preserve first-statement refusal/literal dry-run until coordinated flip; `src/storage/retention-cli.ts:154–156`; `src/storage/retention-cli.ts:190–193` | Direct exported `dryRun:false` bypasses CLI refusal. | Direct API9 on mesh passing all other gates, zero callbacks/changes; dry-run allowed control and getter-mutation regression (`tests/retention-apply-boundary.test.ts:222–248`). |
| **FD descriptor addition** | Bound lease probe plus copied-snapshot proof; `src/storage/verified-remove.ts:131–144`; `native/fabric-mesh-lease.c:240–270`; `src/actors/log-store.ts:98–152` | Foreign preopen survives chmod and writes late; closed-after-copy fd loses uncopied bytes. **B-3 includes exact temporal test/ruling.** | T5/T8 and hosted H2 real foreign UID, late data retained/refusal; no-opener and complete-unchanged controls actually delete. |

**Round-2 ledger deltas (binding on the rows above):**

- **R1** adds **B-8 OPEN**. A fresh, forged or replayed marker is not provenance, so every temp-root-scan T candidate RETAINS (T2a).
- **R2** adds producer cutover receipts and post-cutover census from §3; B-5 stays OPEN.
- **R4** adds the module-private capability from §4. Forged, stale, reused, cross-target and cross-policy handles RETAIN, and a genuine handle is the allowed control (T3, T2a).
- **R10/API9** add H1c's post-flip direct exported API and actual daily owner path (§10 items 7–8). Argv-only or `sweep-main` evidence does not count.
- **R6/FD** are unchanged: B-1, B-2 and B-3 stay OPEN under R2-C3, R2-C4 and R2-C5.

## 8. Measurements and named post-merge E2E

### Measurement — fabric-store owns the report

Instrument each action's root/custody acquisition, move, descriptor probe, registry read, archive re-hash, publication/removal, rollback and release. Record hold duration, busy skips, helper cost and writer wait/timeout counts. Include root-fence cost and refused Main publication once B-1 has a mechanism.

H3 uses representative local ext4 fixtures on hosted CI and the fabric-store host, plus injected delays at each phase; use a latency filesystem fixture where available and label hook-only slow cases honestly. Real writers perform saves through the current 5-second registry timeout. Report p50/p95/max, fixtures, head and sample count. Age/entry/expiry budgets are sampled safeguards between syscalls, **not hard syscall bounds**. A slow filesystem can overrun them. Any accepted writer-timeout/publication residual needs fabric-store's recorded disposition; no number or PASS is promised here.

### E2E-POST: `retention-apply-10678-postmerge`

**Owner:** fabric-store. **Starting state:** a disposable PRIVATE mesh, valid deep state document, approved epoch, canonical Main/resident owners, exact P/S pair, old eligible and protected T/Rr/Ar/backup candidates and concurrent writers. Use a real Main/resident start and built public CLI, not only handcrafted helper inputs.

Two separate legs prevent a sticky veto from hiding the reclamation control:

1. **Safety leg:** capture inode/path/bytes ledger; drive built CLI dry-run, then inject late target/root/backup swaps, references, descriptor writes and hold at the actual action seams; apply. Every target remains at its original path or its reported actual graveyard, with **zero target deletes**. Later candidates also survive sticky stop. Verify both raw-hold acceptance and acknowledged hold; an unresolved raw-hold case is FAIL.
2. **Allowed leg:** fresh unchanged, eligible fixture without holds/openers/references; dry-run plan then apply. Compare exact integer `changes[].beforeBytes`, `changes[].afterBytes`, `bytesBefore` and `bytesAfter` (`src/storage/retention-cli.ts:600–601`) with independently measured target-file bytes and surviving tails. Reclamation is the before-minus-after sum, not before alone and not whole-host free-space noise. `FREED_GIB` rounds to three digits (`src/storage/retention-cli.ts:186`) and cannot prove small exact-byte reclamation.
3. In both legs, writers save without timeout in the reported measured envelope; no leaked canonical lock names. Retained late-written bytes match the writer's data, not merely the original inode id.

Post the exact installed/landed head and receipts on the issue. On any failure fabric-store disables public apply/CLI and daily sweep together, keeps the gap open and reports the preserved graveyards. A disposable E2E is not permission to run deletes locally in this design lane.

## 9. Implementation slices — exact file ownership

Every file below belongs to exactly one slice. Later slices **run**, not edit, earlier tests. Paths marked “new” do not exist on the pinned main. Costs are design estimates in agent-hours, not measured ETAs or a promise to close B-1/B-2/B-3 within this budget.

### S1 — custody primitives, report-only census, hosted receipts (12–14 h)

Exclusive files:
- `src/actors/registry-store.ts`
- `src/storage/registry-custody.ts` (new)
- `src/storage/descriptor-custody.ts` (new narrow shared verifier)
- `src/storage/verified-remove.ts`
- `native/fabric-mesh-lease.c`
- `native/toolchain.json`
- `src/mesh/reader-proof.ts`
- `src/storage/retention-census.ts` (new)
- `scripts/build.mjs`
- `scripts/hosted-real-delete-receipt.mjs` (new)
- `.github/workflows/test.yml`
- `tests/retention-custody-census.test.ts` (new)
- `tests/registry-custody-release.test.ts` (new)
- `tests/registry-custody-read.test.ts` (new)
- `tests/retention-descriptor-custody-recorded.test.ts` (new)
- `tests/retention-census-cli.test.ts` (new)
- `tests/retention-apply-platform.test.ts` (new)
- `tests/hosted/retention-descriptor-custody.test.ts` (new)

Content: bound primitives, helper ABI/trusted fd execution, fresh-read policy, non-authoritative census and CI receipts. Unwired retention authority is fail-closed; no new destructive action becomes available. Native hash generation/build changes run only on an authorized build/hosted path. Tests T1/T3/T4/T5/T10/T11/H2; E2E H2 actual sweep plus fleet census. Startup checks and fresh build required for implementation, not this docs-only lane.

### S2 — namespace and resident callbacks (8–10 h; after S1)

Exclusive files:
- `src/storage/retention.ts`
- `src/residency/retention.ts`
- `src/residency/legacy-run-archive.ts`
- `src/actors/child-completions.ts`
- `tests/retention-fence-recorded.test.ts` (new)
- `tests/retention-backups-clock.test.ts` (new)
- `tests/retention-compaction-publish.test.ts` (new)
- `tests/hosted/retention-deleters-real-delete.test.ts` (new)

Content: explicit callback policies, unknown/legacy retention, all-four-field predicates, parent-fd compaction, clock safety, wrappers, completion/request/slot cases. Where exact owner/lifecycle proof is unavailable, retain. Tests T2a/T6/T9 plus S1 tests; E2E H1a and live-resident cases, never treat its unresolved root race as PASS.

### S3 — admission narrowing and source/archive barriers (8–10 h; after S2 and census ruling)

Exclusive files:
- `src/agents/manager.ts`
- `src/actors/log-store.ts`
- `src/residency/host.ts`
- `src/residency/protocol.ts`
- `src/fabric-runtime-state.ts`
- `tests/run-root-validation.test.ts` (new)
- `tests/retention-archive-complete.test.ts` (new)
- `tests/retention-owner-fence-recorded.test.ts` (new)
- `tests/hosted/retention-owners-real-delete.test.ts` (new)

Content: ordinary override rejection, canonical config/producer checks before construction, proved private resident/nested binding, no marker-based ONE-SHOT authority while B-8 is open, and complete/incomplete manifest with independently enforced source barrier. The unchanged older actor caller cannot delete on incomplete archive; missing exact prune handle retains. Tests T7/T8/T2b plus S1/S2 tests; E2E H1b real Pi actor/resident/archive/close and backup controls. **Breaking:** merge only after every census hit and environment-only override cost has an owner disposition.

### S4 — actor/mesh wiring, measurements and final coordinated flip (10–13 h + measurement wall time)

Exclusive files:
- `src/storage/retention-cli.ts`
- `src/actors/manager.ts`
- `tests/retention-apply-boundary.test.ts` (existing; flip expectation changes only in final enabling head)
- `tests/retention-mesh-fence-recorded.test.ts` (new)
- `tests/hosted/retention-mesh-apply-real-delete.test.ts` (new)
- `tests/hosted/retention-registry-contention.test.ts` (new)
- `docs/retention-apply-operator.md` (new)

Content: typed archive caller, exact manager/log-store policy wiring, CLI per-action owner attribution, sticky stops, hold protocol, Linux-only platform boundary and fail-closed cost alarms. Own **all** `src/actors/manager.ts` edits here, including the daily argv; assigning its archive caller to S3 would violate disjointness. Tests T2c/API9/H1c/H3, all earlier suites and platform check. E2E candidate-head H1c and postmerge E2E-POST.

**Order:** S1 → fleet census/ruling → S2 → S3 → fleet producer cutover receipts and post-cutover census/ruling (§3) → S4. The S4 enabling head merges only after that ruling. The scoped implementation estimate was **35–43 agent-hours** before round 2. Round 2 adds about 1 h to S1 (capability primitive and T3 cases) and 2–3 h to S4 (two more H1c entrypoints), for a total of **38–47 agent-hours**, excluding reviews/audit, hosted queue, disposition waits and unresolved B-1/B-2/B-3 design work. Reprice/reslice before implementation if their mechanism needs additional files; do not quietly expand an enabling PR. Until then, preparatory slices may tighten existing in-process actions but cannot be described as #10678-complete.

## 10. Enablement gate

Before the final flip head may merge:

1. Every acceptance-blocking finding in §12 is closed by a tested mechanism, or by an explicitly filed and accepted scope change. No disposition silently waives #10678.
2. CODE_REVIEW and named deletion SECURITY_REVIEW pass on that head.
3. Independent ACCEPTANCE_AUDIT passes on frozen bytes; a new Luna max auditor, not this designer/coordinator.
4. Hosted CI passes with non-skipped real-delete receipts, native pin/startup/build checks and Windows refusal.
5. Measurement report and residual dispositions are recorded.
6. A tested disabling/revert plan and operator/graveyard evidence path are ready.
7. Every fleet host has a producer cutover receipt, and the post-cutover census ruling is recorded (§3, R2-C7).
8. H1c passes, with no skips, both safety and allowed legs through all three entrypoints on the exact flip head: CLI, direct exported post-flip `sweepMeshRetention(...,{dryRun:false})`, and the actual daily ActorManager owner path (R2-C6).

The **same enabling commit** changes the public API refusal (`src/storage/retention-cli.ts:154–156`), internal literal dry-run (`src/storage/retention-cli.ts:190–193`), CLI refusal (`src/storage/retention-cli.ts:628–632`) and daily argv (`src/actors/manager.ts:306–314`). Read `options.dryRun` once at entry. Preserve measured daily cadence, existing hold/epoch/state controls and all uncertainty vetoes.

An enabling PR head may be tested in disposable hosted CI before merge, but it must not land with unresolved acceptance. After landing/install, fabric-store owns E2E-POST and paired disablement on any failure. This design does not authorize a live apply/sweep.

## 11. Challenge record

Embedded here so the single tracked deliverable is durable without `.local/` access. Full round artifacts are retained under `.local/design-draft-r1.md`, `.local/challenge-r1.md` and `.local/design-draft-r2.md`; the lead can attach them to the issue.

| ID / severity | Concrete challenge and required control | Disposition |
|---|---|---|
| C1 blocker | Untagged terminal T run can still be an actor's `lastRunId`; optional status actorId is not one-shot proof (`src/agents/manager.ts:775`; `src/agents/manager.ts:1661`; `src/actors/manager.ts:3900`). Preserve genuine proved one-shot reclamation. | Adopt: unknown/legacy retains; marker authority conditional on producer/reference proof, not membership alone. B-4. |
| C2 blocker | Pin old S, replace path, publish X into new S, read old S and delete X. Legitimate writers/lifecycle and eligible reclaim must remain possible. | Adopt OPEN B-1. Existing fence is only a candidate. No path-check “fix”. |
| C3 blocker | Draft enabled after only two findings closed while acknowledged same-UID/raw-hold/cost residuals remained. Keep safe Linux allowed controls. | Adopt: every acceptance blocker gates all apply surfaces, §10. |
| C4 blocker | Rejection slice preceded fleet census; supported configs break before impact/ruling. | Adopt: S1 report-only fleet census precedes S3 rejection; fixture not census. |
| C5 major | Required hosted real-delete flag/job absent; skipped tests can look green. | Adopt: exact hosted inventory/receipt, required Linux flag and Windows step. |
| C6 major | Preopened fd makes probe refuse before internal final recheck; draft claimed it exercised a later write seam it could not reach. | Adopt: report actual early refusal; preserve no-opener delete control; exact later seam/threat remains B-3, not fake evidence. |
| C7 major | Prior-art row conflated `--clean` and `--remove`; Cargo licence unverified. | Adopt precision, omit Cargo. Independent live tmpfiles.d aging text does also specify exclusive lock-and-skip; this corrects the collaborator's narrower reading without conflation. |
| C8 major | Existing Windows helper allowance survives a nominal Linux-only flip. | Adopt explicit S4 code boundary and S1 Windows T11. |
| C9 minor | Hosted-test globs and “corresponding cases” overlapped slice ownership. | Adopt exact file lists; no editing an earlier-owned test later. |

Coordinator K1–K9 source/structural corrections adopted: correct numbering; fully qualified anchors; exact disjoint files; integer before-minus-after bytes; first-party session-id producer path; copy-generation proof; root-fd acquisition order; all-open-findings gate; Decision C for static/helper tests; bound helper ABI.

Post-resolution integration corrections (not a second design review): preserve existing same-fd helper hash/exec rather than the designer's erroneous “hash path then execute” description; launch the actual Pi CLI rather than `node dist/index.js`; do not claim affected-test selection inherently excludes hosted files; allocate the actor manager file entirely to S4; qualify actor-row membership and private host-binding as unproved; use broad ordinary-override rejection instead of unproved namespace-pattern exclusion. Round 2 then challenged these corrections together with the rest of the revision.

### Round 2 (collaborator `52a2be9c7a1341898d6a40c795f95b6b`; frozen head `4a543fbd1488fdd30891a3413dcf76a88930f587`; record `.local/challenge-r2.md`; verdict CHANGES REQUIRED)

| ID / severity | Challenge | Disposition (final; not re-challenged) |
|---|---|---|
| R2-C1 blocker | `status.json` is rewritten in the same-UID mutable run tree. A forged, replayed or stale marker proves neither a non-actor launch nor absence of references outside Π(N). | **Adopt.** Marker is never authority; every temp-root-scan T candidate, marked or not, RETAINS (§3, §4). Provenance requirements are stated but no mechanism selected, so **B-8 OPEN**. Producer-only testing is insufficient; T2a covers forgery/replay/later reference. Option (a) stands; classification does not list owner registries. |
| R2-C2 major | Fabricated owner-locked data can claim custody without locks. | **Adopt.** Module-private `WeakMap` capability (§4), minted only after real acquisition/evidence validation, exact root/lock/owner/token/lifecycle/target binding and single use. Forged/stale/reused/cross-target/cross-policy cases reject; genuine handle allowed (T3/T2a). Registry minting still needs B-1 generation; it does not close B-1. |
| R2-C3 blocker | Registry locks do not fence root lifecycle. | **Adopt; B-1 OPEN.** No new mechanism; neither existing Main-publication fence nor global lock is promoted to proof. All apply surfaces remain refused. |
| R2-C4 blocker | Acknowledged hold does not preserve raw late-hold safety. | **Adopt; B-2 OPEN.** Acknowledged-hold green never substitutes for raw race. Needs mechanism or explicit tracked acceptance change (draft 2); neither assumed. |
| R2-C5 blocker | Instantaneous lease probe leaves same-UID opener/name replacement after it. | **Adopt; B-3 OPEN.** H2 early refusal stays reported as early; no-opener control does not close the later seam. |
| R2-C6 major | CLI plus pre-flip API refusal does not prove post-flip direct API or daily wiring. | **Adopt.** Existing S4 H1c file adds built direct API and actual daily slot→flock→detached-child path, each with safety/allowed legs. §10 item 8 gates on them; argv-only and `sweep-main` substitutes excluded. |
| R2-C7 major | Census misses environment-only or old-producer launches. | **Adopt.** Fleet cutover/quiescence receipts plus post-cutover census/ruling (§3; §10 item 7). Pre-cutover/unknown data RETAINS; **B-5 OPEN**. |

The collaborator's independent prior-art recheck found no missed adequate simpler fit; Kubernetes GC is recorded as adjacent, not adopted. No R2 finding is rejected. Coordinator integration filled the actual designer id, qualified the daily anchors, clarified zero-lock capability minting/scoped access, and distinguished temp-root scans from independent archive transfer. These final responses were not re-challenged.

## 12. Open findings

**Each B item keeps all apply surfaces refused.** “Adopted finding” does not mean implemented or independently proven.

- **B-1 root lifecycle:** no proved exact P/S/Ar/Rr path→root owner binding and complete remover/replacer participation. Needed test: replace root after lock, successor writer publishes X; zero target deletes plus normal eligible control. No global lock workaround.
- **B-2 raw late hold:** final hold stat→unlink race remains. Acknowledged `--hold` is a proposal, not a waiver of raw-hold acceptance. Need mechanism or explicit acceptance ruling/tracking issue before enabling.
- **B-3 descriptor/name residual and exact temporal regression:** different-UID preopens are caught/preserved by proposed probe/hash checks, but actual internal post-final-recheck sequence and all supported same-UID writers remain unproved. Arbitrary same-UID/root process risk is not exempt by default. Lock-release successor races also cannot be declared impossible merely because a preceding identity check passed.
- **B-4 ownership/reference generation:** PO-1–PO-3, internal resident/nested admission, marker non-reference property, legacy/duplicate actor-id or run-id custody and historical cohorts need a whole-path proof. A row match in the pair is insufficient. Until established, RETAIN rather than publish false absence.
- **B-5 compatibility:** broad override and canonical-config narrowing require fleet/environment census and fabric-store ruling before the breaking slice. After S3, every fleet host needs a producer cutover receipt and post-cutover census ruling before the flip (§3, R2-C7). Pre-cutover or unknown-generation data stays RETAIN.
- **B-6 writer/root-fence cost:** representative/slow measurements, wait/timeout distributions and every accepted residual disposition are absent.
- **B-7 availability cost:** unsupported/missing leases or helper, reader-tail retention, unmarked legacy accumulation and tighter in-process refusal need measured refusal counts and owner-accepted costs/alarms. No startup/cache budget increase to hide them. Round 2 adds retained marked T runs (B-8) and inability to mint registry-locked handles while B-1 is open.
- **B-8 one-shot provenance (R2-C1):** no trusted generation-bound provenance meets properties (i)–(iv) in §3. Every temp-root-scan T candidate RETAINS; marker or producer-only testing grants nothing.

**Unresolved disagreement:** the designer argued registry writers need not take a separate root-lifecycle fence once every remover/renamer does, because their registry lock then binds writes to the still-canonical root. The collaborator requested writer participation. No re-review settled this. Resolve by tracing writers that create/reopen roots; they must not publish into a successor while an action trusts the old generation. B-1 remains open regardless.

**Verdict:** CHANGES REQUIRED on round 1 and CHANGES REQUIRED on round 2. Round-2 dispositions are the final response and were not re-challenged; no third round is allowed. Remaining principal matters are in the final section. This is useful design evidence and an implementation breakdown, **not an enabling design or acceptance PASS**.

## 13. Scope recommendations and tracking drafts

Recommend **no cut of data-safety, direct-API refusal, measurement or review/audit requirements**. Keeping ambiguous legacy runs is required fail-closed behaviour, not a safety cut. The only feature deferral recommended is Windows apply: keep it dry-run-only until an equivalent handle-custody proof exists. If that narrows the intended platform acceptance, the lead must obtain acceptance of the filed cut first. Fabric-store filed the tracking issues below, as reported in its 2026-10-14 04:32Z handoff.

For each draft: **Base: main `4e7c7de38c13e1a8ce79112706c8a54c4adacd8e`**, owner fabric-store, Deadline none. If implementation later exists only in an unmerged PR, replace Base with that PR and exact head before assigning its lane.

1. **Tracking issue:** [smarty-dev#11850](https://github.com/Smarty-Pants-Inc/smarty-dev/issues/11850). **Title: #10678 registry-root lifecycle fence and writer-generation proof (B-1).** Goal: keep exact-one/two registry custody valid through supported root removal/replacement. Cause: pinned fd reads old root while path-based writer can publish into a successor. Acceptance: complete PO-4 remover inventory; exact P/S/Ar/Rr ownership binding; writer bootstrap/generation participation; nonblocking acquisition; root-replacement + allowed-control tests through real CLI/resident; measured publication/hold cost. Next action: trace existing Main-publication fence and every root creator/remover. No enablement until resolved.
2. **Tracking issue:** [smarty-dev#11851](https://github.com/Smarty-Pants-Inc/smarty-dev/issues/11851). **Title: #10678 late-hold primitive: retain raw-hold safety or approve acknowledged protocol (B-2).** Goal: late hold gives zero target deletes. Cause: raw file creation does not join action gate. Decision requested: design a mechanism preserving current acceptance, or explicitly narrow supported exclusion to acknowledged `--hold`, retaining raw file best-effort only if approved. Acceptance: exact between-final-check/unlink tests and unchanged control; one explicit issue ruling before changing the contract. No cut is assumed here.
3. **Tracking issue:** [smarty-dev#11852](https://github.com/Smarty-Pants-Inc/smarty-dev/issues/11852). **Title: #10678 descriptor custody and post-probe writer census (B-3/B-4).** Goal: late data survives or deletion refuses. Acceptance: trace all terminal-tree/registry/name writers; foreign preopen/chmod/late-write and write-and-close-after-copy tests; exact requested temporal proof; supported same-UID writers fenced or proven quiescent; explicit owner disposition for arbitrary same-UID/root residual; no metadata-only absence proof. Next action: writer/descriptor census and reachable race E2E design.
4. **Tracking issue:** [smarty-dev#11853](https://github.com/Smarty-Pants-Inc/smarty-dev/issues/11853). **Title: Windows retention apply: exact registry and handle custody (proposed platform deferral).** Reason: Linux write leases and `/proc/self/fd` do not prove Windows writable-handle custody. Goal: safely reclaim with equivalent Windows reference/lifecycle/handle proof. Acceptance: hosted Windows real-delete matrix, preopened foreign writable handle across permission change, exact survivors/reclaimed bytes, bounded saves, named reviews/audit; lift Windows refusal only then. Next action: design the equivalent mechanism. Link this issue from #10678 and its operator/platform note before any accepted scope cut.
5. **Tracking issue:** [smarty-dev#11854](https://github.com/Smarty-Pants-Inc/smarty-dev/issues/11854). **Title: #10678 trusted one-shot run provenance (B-8).**
   - **Goal:** reclaim genuinely one-shot T runs.
   - **Cause:** same-UID writers can alter/replay a `status.json` marker.
   - **Acceptance:** properties (i)–(iv) in §3; forged/replayed/later-reference candidates RETAIN; genuine allowed control through real Main launch in hosted CI; no per-run owner-registry record.
   - **Next action:** evaluate mechanisms against the same-UID threat, or explicitly accept the retention cost. No mechanism/cut is assumed here.
6. **Tracking issue:** [smarty-dev#11855](https://github.com/Smarty-Pants-Inc/smarty-dev/issues/11855). **Title: #10678 fleet producer cutover and post-cutover census (B-5, R2-C7).**
   - **Goal:** no unadmitted producer generation remains when apply enables.
   - **Acceptance:** every host's cutover receipt, environment-only embedder attestations, post-cutover census dispositions, pre-cutover/unknown data RETAIN.
   - **Next action:** inventory producer processes on each fleet host after S3 installs.

## Open disagreements

These matters remain for the principal after two rounds, both **CHANGES REQUIRED**. The round-2 response was not re-challenged. Every unresolved acceptance blocker keeps CLI `--apply`, direct exported apply and daily apply refused together.

1. **B-1 writer participation** (round 1, restated R2-C3): designer argues remover/renamer participation plus registry locking can bind writers to the canonical root; collaborator requires writer participation too. Unsettled: trace all root creators/reopeners and prove exact generation binding (draft 1).
2. **B-2 raw late hold:** mechanism or explicit tracked acceptance change (draft 2). No narrowing is approved.
3. **B-3 post-probe same-UID/name TOCTOU:** reachable mechanism/test, or explicit accepted residual and scope ruling on arbitrary same-UID/root processes (draft 3). Early H2 refusal does not prove the later seam.
4. **B-8 one-shot provenance:** trusted generation-bound mechanism, or explicit acceptance that Main one-shot T bytes are not reclaimed (cost B-7; draft 5). A mutable marker is never authority.
5. **B-5 cutover:** prove quiescence receipts achievable for environment-only embedders, or keep meshes without receipts dry-run (draft 6).
6. **Windows apply deferral** (draft 4, smarty-dev#11853) remains a proposed cut; lead has filed it and must obtain the owning acceptance before any narrowing is treated as approved.
7. Collaborator has not reviewed final R2-C2 capability, R2-C6 entrypoint or R2-C7 barrier specifications. No disagreement about adopting those findings is recorded. B-4, B-6 and B-7 also remain OPEN as listed in §12.

Tracking drafts 1–6 are filed as smarty-dev#11850, #11851, #11852, #11853, #11854 and #11855 respectively; fabric-store owns their follow-through.
