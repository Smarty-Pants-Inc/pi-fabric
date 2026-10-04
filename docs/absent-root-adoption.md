# Aged absent-root adoption (F3059)

`ParticipantDirectory.lineageAlive()` retains the strict message-delivery contract: unknown is not an address and only root-owned clean closure proves routing death. `lineageAdoptable()` is the separate authority used only by orphan adoption. Raw Main participant/session presence, including stale or malformed presence, vetoes adoption. Its additional proof covers **absent** Main advertisements; it does not convert ordinary 15-second expiry into death.

For that additional proof, the directory must retain a finite actor-presence commit timestamp (modern participant files/shared records or legacy `actors/` presence). Every relevant presence renewal and host expiry must be older than `max(10 minutes, 2 × longest known presence/host lease TTL)`. Any live lineage host, including the root's resident host, or a fresh file-only host lease vetoes inheritance. Invalid/incomplete reads veto too. When no evidence is retained, the lineage stays unknown indefinitely; an actor's creation date is not evidence of its last renewal. Existing root-owned clean-close receipts remain stronger adoption evidence, but do not override a live or recently lapsed resident host. Message delivery intentionally continues to recognize explicit clean closure independently of resident liveness; the relaxed aged-absence rule never participates in routing.

Mesh CAS tombstones retain version counters, not wall-clock death/reaped-at timestamps; those counters are never treated as an age proof.

The proof is read twice from fresh shared state and strict participant/lease-file scans. Main resume publishes a fresh host lease under mesh custody before first local root activation/publication, even when there is no close receipt to invalidate. `ActorManager.#confirmAdoption` is unchanged: registry then mesh lock, fresh ownership and lineage re-check, expected-root compare, and durable registry claim. Returning hosts already pause consumption until their directory publication and registry ownership reconciliation. Project/residency/role placement remains unchanged: a durable orphan belongs only to a same-project project-agent resident host, never another project or a worktree agent.

This protects smarty-dev#4623's 20-second starvation lapse: raw Main presence stays a veto, a live resident file lease stays a veto even with missing Main presence, and recently expired host leases wait the full grace. The rule depends on the existing host/mesh consumption and registry fences, not on treating elapsed time alone as process-exit evidence.

## Isolated native proof

Build first, then run at low priority:

```sh
nice -n 19 node scripts/prove-absent-root-adoption.mjs artifacts/real-pi-$(git rev-parse --short=8 HEAD)
```

The script makes two fresh offline Pi RPC Mains and real resident/worker processes in an isolated `$TMPDIR` mesh and empty HOME/profile, with synthetic inference only. A creates a durable actor and crash-exits; its owned resident/launcher is stopped using process-birth checks; the native directory reaper withdraws A's Main record. B is a same-project project-agent and starts its own resident host. Before grace A stays alive; after grace the registry records B's root and the actor runs exactly one delivered event. JSONL RPC evidence, native activation receipts, registry readback and result are retained in the specified output directory; all owned processes are stopped/joined and scratch deleted.

The ten-minute floor can be shortened only when `NODE_ENV=test`, by the constructor's `lineageDeathGraceMs` test seam or `PI_FABRIC_TEST_LINEAGE_DEATH_GRACE_MS` for offline CLI proofs. The two-TTL minimum is never disabled (the native proof waits at least 60 seconds). Production ignores both shortening seams.
