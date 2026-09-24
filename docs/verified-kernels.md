# Verified policy kernels

Fabric executes JavaScript compiled from the registered Bend kernel entry points.
Production acceptance points and state transitions call that generated code;
there is no parallel reference implementation standing in for production.
TypeScript retains host integration, source observation, optimized
candidate production, and diagnostics. There is no handwritten fallback when a
kernel rejects a candidate.

`LAWS.bend`, `proofs/resource-spec.bend`, `proofs/state-spec.bend`,
`proofs/provider-spec.bend`, `proofs/authority-spec.bend`,
`proofs/lifecycle-spec.bend`, and `proofs/storage-spec.bend` define the specification
review boundaries.
`PROOF.bend` and its imported proof modules supply the closed proofs.
Review laws independently of implementations; never weaken a claim
to make an implementation check. Compiler checking does not replace independent
specification review. Independent predicates prevent a change to the kernel's
condition builder from silently changing its law.

## Acceptance ledger

Policy kernels cover the following areas through production bridges. The proof boundary is a set
of small policy kernels, **not a proof of every subsystem invariant**.

| Area | Executed/checked policy | Authoritative production path | Evidence |
| --- | --- | --- | --- |
| Effect independence | Complete-source normalization preserves every conflict; exact outputs preserve identities in both directions and obey bounds; effect-group traversal checks all pairs | `verified/resources.ts`, `components/effect-policy.ts`, `components/effect-scope.ts`, `core/action-registry.ts` | `resource_conflict_preserved`, `resource_refinement`, `resource_output_valid`, `resource_groups_exact`; source/ABI/registry/component regressions |
| Component lifecycle | Publication eligibility requires current epoch/owner, non-retirement and an open supervisor; one-use disposer admission; cleanup failure chooses quarantine | `components/supervisor.ts`, `components/effect-scope.ts` | `transition_sound`, `cleanup_failure_quarantines`, `consume_retires`; async lifecycle and inverse-stack tests |
| Capability authority | Exact parent-subset derivation, authorized membership, release refusal and positive acceptance over complete grants | `verified/authority.ts`, `core/action-registry.ts` | `authority_derive_safe`, `authority_child_use_safe`, `authority_released_refused`, `authority_derive_complete`; provenance and snapshot tests |
| Binding lifecycle | Admission and exact count transitions; retirement/revocation preserve holds and work; close reserves a generation only after owner, holds and calls clear | `core/provider-bindings.ts`, `core/provider-operations.ts` | `lifecycle-spec.bend` laws; real-settlement, repeated-release, shutdown, quarantine and finalizer tests |
| Compaction | Every successful proposed cut satisfies eligibility, prior-marker ordering, estimated tail budget and every matched call/result span; rendered byte/estimated-token bounds; retained-plus-omitted sample accounting | `compaction/hook.ts`, `compaction/bounds.ts` | `span_sound`, `spans_sound`, `cut_sound`, `summary_bounds`, `sample_accounting`; compaction and bridge tests |
| Memory | Active selection cannot admit a non-member unless all-branch selection is explicit; source/lineage bindings must both match; chunks have exact lengths, contiguous offsets, truthful completion and progress; truncated coverage cannot be complete | `memory/normalize.ts`, `memory/expand-service.ts`, `memory/digest.ts`, `memory/index.ts` | `active_lineage_only`, `explicit_all_lineages`, `pointer_sound`, `chunk_sound`, `coverage_sound`; source-bound pagination/lineage/integrity tests |
| Entropy normal forms | Canonical arguments stay original; candidate acceptance requires a proved plan, an actual change, and acceptance by the unchanged schema | `entropy/normal-form.ts` | `normalization_sound`, `canonical_identity`; identity/idempotence/forged-plan tests |
| Storage CAS | Exact key/value/identity forwarding, stale refusal, key and high-water successors, missing-delete no-op, safe-integer overflow refusal | `verified/storage.ts`, `mesh/store.ts` | `storage_safe`, `storage_put_complete`, `storage_delete_complete`, `storage_stale_refused`, overflow laws; eviction, corruption, request-mutation and writer-race tests |
| State and Schema | Pending protocol-2 heads need a commit marker; valid committed heads survive marker eviction; certificate checks all hold; consumption produces an inactive token | `state/store.ts`, `schema/controller.ts` | `head_sound`, `pending_without_marker_hidden`, `committed_head_visible`, `certificate_sound`, `consume_once`, `consume_retires`; state/schema protocol tests |

`all_sound` and `all_complete` establish the conjunction checker by induction,
not just a finite truth table. `spans_sound` separately lifts per-span evidence
over arbitrary finite lists. Lifecycle `begin_open_complete`,
`retain_open_complete`, and `inspect_open_complete` establish exact successful
outcomes from the independent `Open` predicate across every phase, cleanup mode
and counter value, alongside the existing safety and exact-transition laws.

### Proof-backed testing

Bend checks universal policy properties. JavaScript tests check the emitted ABI
and host boundaries with fixed, distinguishing examples. Cartesian products of
already-proved conditions are unnecessary:

- Resource refinement and conflict preservation use the universal laws, not a
  second handwritten conflict interpreter and a source/proposal cross-product.
- Authority derivation, provider tickets, storage CAS and lifecycle decisions
  retain positive and negative wire examples with unequal fields and counters.
- Certificate checks retain each host diagnostic position and multiple-failure
  precedence. Conjunction laws replace the Boolean matrix.

Keep encoding, numeric-limit, limb-carry, mutation/aliasing, artifact-provenance
and packaged-runtime probes: the compiler and TypeScript codecs are trusted
boundaries, not proved implementations. Keep real cancellation, settlement,
cleanup ordering, locking, corruption and migration regressions too.

Not every enumeration is redundant. The addressed-sampling sweep checks a
TypeScript producer and its omitted-entry identities; proved accounting alone
does not prove that producer. Component scheduling and diagnostic projections
also retain their host-level tests. Before pruning a decision matrix, identify
which policy assertions the closed laws cover, including positive completeness,
and retain focused tests for the remaining host observables. A safety law alone
can still permit an implementation that denies valid requests.

### Universal provider dispatch

All registry action routes execute `proofs/provider-kernel.bend` plans:
ordinary invocation, scoped acquisition, speculative launch, and cached-result
replay. Seven laws establish exact authority/descriptor/payload-slot forwarding,
single-use consumption, replay refusal, cancellation, revocation, and admission
completeness. The state-plan specifications are reused unchanged.

Policy, dispatch, authority, lifecycle and storage kernels are separate generated
libraries bound by the version-2 artifact receipt. The operation interpreter loads
on first use,
not idle startup. Shared full-text plan definitions and lemmas in
`proofs/state-*.bend` support the provider proofs; they do not establish storage
transaction correctness or expose a separate state-provider API. See
[provider capabilities](provider-capabilities.md) for the runtime contract and
its trust boundaries.

### Complete-resource conflict preservation

The footprint bridge proves the normalization-to-conflict-preservation
property over arbitrary finite declarations and arbitrary proposed outputs:

```text
conflict(A, B)
  => conflict(normalize(A, proposalA), normalize(B, proposalB))
```

`proofs/resources.bend` is the executable implementation. Its checked functions
are exported through `proofs/kernel.bend` and compiled into the same generated
library as the other policies. There is no second TypeScript overlap algorithm
that can grant independence.

The production path is:

1. `src/verified/resources.ts` snapshots and encodes the **complete original**
   declaration. No resource-count or name-length limit is applied during encoding.
2. TypeScript proposes a small deduplicated list. This producer is untrusted for
   policy correctness: it may omit, shorten, reorder, duplicate, or add names.
3. The compiled checker independently verifies nonempty metadata, the 64-resource
   and 256-code-unit bounds, wildcard exclusion, and **both directions of set
   inclusion** between the original and proposed lists.
4. An accepted exact scope retains the checked identities. Every rejected proposal
   becomes the compiled `Unknown` scope; an unknown source cannot become exact.
5. Component lifetime and registry call policy compare complete effect declarations
   through the compiled conflict/group operations. TypeScript summary maps and
   flags provide diagnostics only. They cannot turn a compiled conflict into an
   empty conflict list, even if the diagnostic data is inconsistent.

In particular, a proposer that silently drops resource 65 fails the full-list
coverage check. The theorem does not assume that TypeScript reported the correct
count, overlap, validity, or unknown-scope flags.

The independent specification uses equality witnesses, set membership, structural
list bounds, and shared-resource witnesses. It does not copy producer Boolean flags.
The 14 resource laws establish (the root claims name the actual exported entry
functions, so changing a forwarding wrapper also breaks the proof):

- Source interpretation preserves names or widens to unknown; exact sources are
  nonempty and contain no empty/malformed or wildcard identity. Every well-formed
  source remains exact before normalization, without a count or name-length cap.
- Exact normalization preserves resource membership in **both** directions.
- Every exact output meets the resource/name bounds and excludes wildcards.
- Conflict computation is sound and complete for named/unknown active scopes;
  quiet effects cannot conflict, and both-commutative effects remain independent.
- Normalization cannot erase a conflict, for **any** proposed output.
- Valid bounded set-equivalent proposals are accepted, preventing a deny-all
  implementation. Identity proposals are unchanged.
- Rechecking a result with the same proposal is idempotent.
- Group traversal is equivalent to checking every effect pair, not just the first.

Precision has an explicit premise: the candidate must be valid, bounded and
set-equivalent to its source. A broken producer can still cause conservative
false positives by supplying a bad proposal; it cannot create false independence.
The proof does not claim that an arbitrary producer finds a good proposal.

Identities are encoded as complete UTF-16 code-unit lists, including NUL and lone
surrogates. There are no lossy hashes, Unicode normalization, prefix clipping or
separately assigned integer IDs. Non-string values encode an invalid empty name;
missing/non-array resource collections encode missing scope. The decoder checks
the code-unit ABI. Tests cover every UTF-16 unit, adversarial proposals, the
64/65 and 256/257 boundaries, and 20,000-entry declarations.

The remaining trust boundary is faithful snapshot/ABI conversion and effect-kind/
ordering encoding, Bend's checker/compiler/Base, bundling and the JS runtime,
and truthful provider declarations. The TypeScript wire codec is tested, not
itself formally verified. Hash receipts and CI bind the entire transitive proof
source set to its generated artifact; finite tests are not substitutes for the
universal preservation proof.

### Producer/checker boundaries

The compactor's optimized TypeScript selector still proposes cuts. `computeCut`
checks every successful result, including the legacy and compact-all paths,
before it can become `firstKeptEntryId`. Checks use the live entries and complete
call/result span set. Span batches of at most 128 bound backend list
construction; every batch must pass. Candidate-selection optimality is still
test-backed, not formally proved. Token guarantees concern the structural
estimator and supplied calibration, not an undocumented provider tokenizer.

Memory checks the actual outgoing page **after** envelope trimming. The bridge
compares each text chunk with the selected normalized source slice, checks its
range through Bend, and checks the proposed continuation against the resulting
cursor. Source reads/hashes, parent-graph reconstruction, normalization,
Unicode slicing and page composition remain host responsibilities. The exact
claim concerns normalized text, not byte-for-byte JSONL reconstruction. A source
or lineage change fails closed; old pointers are never reinterpreted.

Entropy's schema validator and plan derivation remain TypeScript. Their actual
results feed the compiled acceptance decision; the original object is returned
on refusal. Validated output plus the canonical-identity gate establishes the
operational idempotence argument under the validator assumption. This is not a
proof of TypeBox or of JavaScript numeric-string conversion.

Schema's eight evidence facts are a fixed-length tuple at the adapter boundary.
The compiled conjunction checks all eight before mutation. The consumed status
comes from the compiled token transition and is persisted with the existing
compare-and-swap. At-most-once use across processes depends on that CAS protocol
and the mesh store, not on an in-memory Boolean alone. Filesystem rollback and
postcondition commands are not formally verified.

### Lifecycle limits

Provider bindings execute the compiled event reducer for admission, holds,
in-flight work, retirement, revocation and close reservation. Independent laws
prove exact count updates and prevent close while ownership remains. The host
interprets commands, observes actual promise settlement and releases private
one-use leases. Those observations and interpreter effects remain trusted.

The component supervisor still uses compiled eligibility and cleanup predicates.
Its epoch allocation, publication sequencing, inverse-stack execution and fairness
remain TypeScript protocols checked by integration tests. Neither reducer proves
that provider promises account for every subprocess or remote effect.
Arbitrary inverses, ambient effects, liveness, schedule confluence, and
author-defined observational equivalence are not claimed as Bend theorems.

## Reproducible bridge

The contributor toolchain pins **Bend 2.0.26**. Installed Fabric needs neither
Bend nor a Bend loader. Linux CI downloads that exact release archive and checks
its SHA-256 before running proofs. Windows tests execute the checked-in generated
JS and ABI; native Bend currently requires Linux, macOS, or WSL.

```sh
bend PROOF.bend --check-only
bun run proof:generate    # prove, compile, regenerate JS + declarations + receipt
bun run proof:check       # reprove, reproduce byte-for-byte, reject negative mutations
bun run proof:artifact    # compiler-free source/bridge/artifact freshness check
bunx vitest run tests/verified-kernels.test.ts tests/verified-resources.test.ts tests/verified-providers.test.ts tests/verified-artifact.test.ts
bunx vitest run tests/verified-authority.test.ts tests/verified-lifecycle.test.ts tests/verified-storage.test.ts
bun run typecheck
bun run build
bun run proof:dist        # probe the actual bundled registry/compactor and kernel
```

`BEND_BIN` may explicitly select a trusted compiler executable. Generation sets
`BEND_NO_TELEMETRY=1`. It never installs or updates Bend automatically.

Bend's CLI emits executable JavaScript, not a library switch. A pure `IO.pure`
main keeps the kernel definitions reachable for compilation. The build bridge:

1. Checks the entire root proof file and its imported laws.
2. Refuses unsafe declarations, holes, foreign effects, remote imports, and
   untracked local proof dependencies.
3. Compiles the same kernel source that those laws import.
4. Parses the emitted program and requires the exact pinned CLI footer.
5. Removes only its two invocation statements and exports the compiler-produced
   definitions through Bend's own trampoline, checking names and arities.
6. Tree-shakes unused runtime code and rejects host IO/imports before compacting
   compiler-local identifiers and syntax. Public ABI export names stay stable.
7. Writes generated JS, ABI declarations, and a SHA-256 receipt over the proof
   sources, ABI, bridge, and generated artifacts.

No algorithm is translated into handwritten JS. The small bridge, ABI
conversions, Bend checker/compiler/Base, esbuild, and JS engine are part of the
trusted computing base. Numeric adapters reject NaN, infinities, fractions and
unsafe integers. Comparison-only primitives accept JS-safe integer `BigInt`s.
Arithmetic guards respect the pinned backend's immediate `Nat` bound of
`2^48 - 1`; chunk/sample sums and lifecycle increments fail closed before overflow.
Storage revisions use canonical radix-`2^32` limbs to cover the full JS-safe integer
range. Booleans and tagged records/lists follow the emitted
ABI. Tests exercise those representations on both CI platforms.

Every build checks artifact freshness before bundling. Linux CI and `prepack`
add fresh checking and byte-for-byte regeneration, so editing a receipt is not a
substitute for proving the code. The standalone generated JS, declarations and receipt are
copied into `dist/verified/generated/`. Laws, proofs, kernel source and Bend's
license are included in the package for inspection. The receipt is a freshness
record, **not a standalone independently verified proof certificate**.

Negative probes delete a proof or deliberately break footprint bounds, source
coverage, identity matching, source validation, group traversal, positive
acceptance, epoch checks, tool-pair closure, chunk completion, canonical identity,
commit-marker visibility, and one-use consumption. The pinned compiler must reject each
mutation while the specification remains unchanged.

## What remains outside the claims

- Semantic remembering, arbitrary prose retention, retrieval ranking quality,
  secret detection, and recovery after source deletion.
- Full correctness of parsers, schemas, hashing, source observations, graph
  reconstruction, or filesystem transactions.
- Full lifecycle confluence, eventual settling, or rollback correctness for
  arbitrary provider code.
- Validation of user intent, success of host operations, and security against a
  malicious trusted host or provider.

These boundaries are deliberate. Executed kernels and mandatory result checks
remove the parallel-model gap for the specified decisions; they do not turn
unverified host observations or adapters into proved facts.
