# Provider capabilities

Every registry action uses the same executable, proved dispatch-plan kernel.
This covers state, mesh, memory, schema, agents, components, compact, prewalk,
Jev, Pi tools, captured extensions, MCP, and third-party registrations. There
is no opt-in flag or provider-supplied “verified” label. Additional compiled
kernels own capability attenuation, binding lifecycle transitions, and mesh CAS
revision decisions.

Verification covers dispatch decisions, not arbitrary provider implementations.
Native callers bypassing the registry remain outside this boundary.

## Dispatch and authority

`ActionRegistry` resolves an issued capability view or unrestricted host/root
authority, prepares and validates arguments, and applies authorization and
approval policy. The lazy `src/core/provider-operations.ts` interpreter then:

1. Snapshots structured-cloneable arguments in a private payload slot.
2. Issues a ticket bound to the provider binding UUID, generation, operation kind,
   exact action ref, descriptor hash, and payload-slot identifier.
3. Consumes the ticket synchronously before asynchronous observations.
4. Re-describes the bound provider, checks cancellation and authority, and runs
   `providerTake` from the generated `proofs/provider-kernel.bend` artifact.
5. Dispatches the action and slot selected by the plan, or refuses the call.
   No asynchronous gap separates that decision from the provider invocation.

Ordinary invocation, scoped acquisition, speculative launch, and cached-result
replay use this path. Speculation requires read risk and a `none` effect; an
eligibility callback cannot override that floor. Cache tokens include the
binding, descriptor hash, and capability-view identity. Replay is revalidated
before disclosure.

Preparation outputs are snapshotted before validation. Approval, tracing and
speculation callbacks receive separate copies, so they cannot change the owned
validated payload. Shared backing memory is refused, including buffers hidden
inside maps, typed views, and native WebAssembly memory. This is a tested host
adapter contract, not a Bend proof of JavaScript cloning or schema validation.

Tickets are one-use. This is not distributed exactly-once execution or an
idempotency guarantee for independently retried calls. Arguments must be
structured-cloneable data; native function-bearing arguments are refused.
Native aliases resolve to the descriptor's canonical action before dispatch,
and that exact canonical target is rechecked.

## Capability views

Capability views are immutable, issuer-owned, in-process authorities. Copies,
forged records, views from another registry, released views, and revoked derived
views are refused. Portable semantic digests describe capabilities; they are
not bearer grants. Another runtime must resolve and issue its own view.

The compiled authority reducer derives children only from exact parent grants.
Each grant includes its ref, provider, binding identity, generation, and descriptor
identity. Use and release consult private canonical state; public view fields are
presentation data. A child view cannot widen its parent's exact action grants. Parent release
revokes existing children and drops their binding holds. Release leases
explicitly: garbage collection is not a resource protocol.

Rolling replacement and withdrawal permit retained old generations.
`registry.revokeProvider(name)` revokes the current generation and cancels its
admitted callers. Releasing a view revokes that view's borrowers, not other
views or root authority. A generation whose close has begun cannot admit work.

An action grant such as `memory.recall` authorizes that action's argument surface.
It does not become a key-, path-, tenant-, or collection-scoped grant. A restricted
guest must not also receive ambient bypass APIs.

## Cancellation and cleanup

Caller cancellation does not discard the underlying provider promise. Catalog
and argument-preparation work and lifecycle finalizers are tracked too. Ordinary
conflict footprints remain until the actual call settles, not just its caller.

Late canceled acquisitions are disposed. Successful supervised acquisitions
transfer disposal synchronously into their owner's inverse stack, preserving
owner-before-resource cleanup. Unadopted acquisitions retain automatic abort
cleanup; failed ownership transfer disposes before publication. Cleanup callbacks are captured once,
shared between cancellation, explicit disposal, and shutdown, and retained
through actual completion. Invalid acquisition results and failed cleanup or
close revoke and quarantine the binding. Failed cleanup is never reported as successful.

The compiled lifecycle reducer owns binding phases, hold/work counts, ordinary
versus cleanup admission, and the transition that reserves close. Revocation and
owner release preserve outstanding work. Close is reserved synchronously before
arbitrary finalizers; neither new work nor another close can enter that generation.
Catalog finalizers are awaited through their actual completion, including returned
promises. Shutdown revokes views and releases their real leases; it never invents
a zero hold count while a lease remains owned.

Registry shutdown budgets scoped drain and binding drain separately, one second
each. Non-cooperative work is not killed or declared finished. Inspect
`registry.providerStatus()` for retiring/in-flight bindings and retained errors.
Providers are not closed under tracked work; successful late settlement can
complete their close. Manual disposal can still await a non-cooperative disposer.
Externally owned providers retain their session ownership exemption.

A provider promise settling does not establish that its subprocesses or remote
work have stopped. Revocation cannot undo an already-started irreversible effect.
In-flight provider work can continue after caller cancellation; dispatch admission
does not recheck authority at every internal commit. The registry provides no
generic rollback guarantee.

## Storage transitions

`MeshStore.put/delete` execute `proofs/storage-kernel.bend` plans. Requests capture
the key, expected revision, JSON value and writer identity before waiting for the
lock. The interpreter reads the locked slot and applies the plan's exact key,
payload, identity and successor revisions. CAS refusal, missing-key deletion and
revision exhaustion perform no replacement.

Live-key updates remain `+1`. Successful deletion consumes a revision. New or
recreated keys allocate above a persisted store-wide high-water mark, so bounded
tombstone eviction does not cause previously issued positive revisions to recur.
The JSON envelope remains `format: 1` for existing readers; `revisionFormat: 2`
requires its high-water mark. Every writer must follow the current protocol.
Older writers, external edits, deletion or rollback of the storage file are
outside the freshness guarantee.

Legacy migration seeds the clock from retained entries and tombstones. History
already discarded by older writers cannot be reconstructed. Unrecoverable or
empty damaged snapshots remain in place: reads tolerate them, while writes refuse
until a valid snapshot is restored. Complete concatenated snapshots retain their
existing newest-complete-snapshot recovery behavior. Invalid, missing, regressed
relative to retained history, or exhausted clocks fail closed on mutation.

Revisions use radix-`2^32` limbs, keeping arithmetic inside Bend's immediate-Nat
domain while representing every nonnegative JS-safe integer. The JSON/UTF-16
codec, faithful slot observation, history preservation, cooperative lock, atomic
rename and cache publication remain host assumptions. These plans do not prove
crash durability, multi-write state transactions or schema filesystem rollback.

## Verification boundary

Seven laws cover the executable provider wrapper: `provider_plan_safe`,
`provider_ticket_consumed`, `provider_replay_refused`,
`provider_revocation_consumes`, `provider_revoked_refused`,
`provider_canceled_refused`, and `provider_admission_complete`. They use shared
full-text plan specifications. The proof forwards a payload-slot identifier,
not a formal representation of arbitrary JavaScript arguments or provider code.

| Guarantee | Evidence | Boundary |
| --- | --- | --- |
| Exact, one-use dispatch plans | Executable laws, pinned generation and artifact receipt, ABI tests and negative mutations | Compiler, codec, and interpreter remain trusted |
| All registry action routes use the gate | Ordinary/scoped/speculative/replay integration tests and bundled smoke | Direct native calls bypass the registry |
| Exact authority attenuation/use/release | Executable subset, membership, positive acceptance and release laws; forged/cross-registry/retained-generation tests | JS issuer ownership, tuple codec, hashing and parent-abort delivery remain trusted |
| Binding lifecycle transitions | Executable admission, count-preservation, exact release and close-reservation laws; real-promise and finalizer tests | Host event delivery, private maps and correspondence to provider-owned work remain trusted |
| Validated/approved argument fidelity | Private snapshots and adversarial mutation/shared-memory tests across ordinary, scoped and speculative routes | Structured cloning, schema validation and the payload-slot interpreter remain trusted |
| Cancellation preserves ownership | Ignored-abort calls, retained conflicts, late acquisition, cleanup, quarantine and closing-generation tests | Provider-owned background work remains implementation-specific |
| Mesh CAS/revision plans | Executable safety, positive put/delete/no-op, stale refusal and overflow laws; mutation, eviction, restart and concurrent-writer tests | Faithful locked observations, persistent high-water history, codecs and filesystem effects remain trusted |
| Cache provenance | Descriptor/view identity, replay validation and read-only eligibility tests | Result and context semantics remain provider-specific |
| Packaging and lazy loading | Artifact checks, stable lazy entry, cold-import and first-use tests | Supported-platform CI and independent review remain necessary |

The host issuer, descriptor truthfulness, hash collision resistance, UUID
uniqueness, UTF-16/ABI codec, private-slot fidelity, interpreter, lifecycle event
observation, approval hooks, metadata/preparation hooks, compiler and JavaScript
runtime are trusted assumptions. Metadata hooks are not proved effect-free.
Compiler checking does not replace independent specification or adapter review.

State and schema use the mesh CAS primitive, but their multi-write transaction
protocols are not proved by either dispatch admission or the storage reducer.
Memory collection/lineage filtering and read-disclosure noninterference require
separate reasoning. Agent/process ownership and remote commit/cancellation remain
provider responsibilities. Pi tools, shell commands, captured extensions, MCP,
and managed or third-party implementations retain their native authority.

See [verified policy kernels](verified-kernels.md) for additional subsystem laws
and their precise host assumptions.

## Checks and performance

Use targeted checks, never the full suite:

```sh
bun run proof:generate
bun run proof:check
bunx vitest run tests/verified-providers.test.ts tests/verified-authority.test.ts tests/verified-lifecycle.test.ts
bunx vitest run tests/provider-bindings.test.ts tests/speculation-registry.test.ts
bunx vitest run tests/verified-storage.test.ts tests/mesh-store.test.ts tests/state-provider.test.ts tests/schema-enforcement.test.ts
bunx vitest run tests/verified-kernels.test.ts tests/verified-resources.test.ts tests/verified-artifact.test.ts
bun run typecheck
bun run build
bun run proof:dist
bun run assert:lazy-graph
bun run benchmark:provider-dispatch
bun run benchmark:startup . ../pi-fovea ../pi-contour
```

The contributor toolchain pins Bend 2.0.26. `BEND_BIN` can select a trusted
compiler executable. Normal builds verify artifact hashes without Bend.

The dispatch benchmark measures first use and warm full-registry read calls
without provider I/O. Each dispatch observes its descriptor again; measure real
metadata-hook and network costs for the intended adapter. Synthetic results do
not establish a provider-specific latency budget. The startup benchmark uses
fresh processes and reports import/registration, not full-session boot. CI
checks structural lazy-loading budgets, with no millisecond thresholds.
