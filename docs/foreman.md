# Fabric Foreman

`/skill:fabric-foreman <goal>` puts a fast typed decision loop above Main's slower coding loop. Choose `agent_settled` for economical decision points or `turn_end` for per-turn assessment. Main continues working while the Jev program watches; no second generative model runs for every observation.

This adapts [thruwire/foreman](https://github.com/thruwire/foreman)'s ten-question semantic supervision and deterministic policy to Fabric's existing lifecycle. Comparison baseline: upstream commit `3de1556a59b7a7e14daa1f89b2fc49080bbb8cce`, especially its [README](https://github.com/thruwire/foreman/blob/3de1556a59b7a7e14daa1f89b2fc49080bbb8cce/README.md), [runtime](https://github.com/thruwire/foreman/blob/3de1556a59b7a7e14daa1f89b2fc49080bbb8cce/docs/runtime.md), and [steering](https://github.com/thruwire/foreman/blob/3de1556a59b7a7e14daa1f89b2fc49080bbb8cce/docs/steering.md). It is not a port of the Codex App Server transport or a claim of assessment accuracy.

## Primitive audit

Fabric already has the required primitives for **session-bound, lifecycle-driven supervision**. No new runtime API, event name, registration, or configuration switch is needed.

| Foreman responsibility | Fabric primitive / implementation | Boundary |
| --- | --- | --- |
| Independent long-running controller with local state | `jev.spawn`, `JevProgramManager` in `src/jev/manager.ts` | One persistent QuickJS program; Main does not wait. Session-owned, not restart-durable. |
| Observe coding progress at a decision point | `observe.events`, `program.nextEvent()`, `JevObservationHost` in `src/jev/observation.ts` | `turn_end` and `agent_settled` are the same local event names used by actors; no polling required. |
| Bounded observation, history, and instruction evidence | Explicit `observe.include`, queue/age/character bounds; schema-bounded program input and local history | Settlement itself carries no transcript. Cache opted-in turn evidence; never assume hidden history access. Repository instructions are an explicitly supplied snapshot. |
| Ten parallel semantic questions | One `jev.evaluate` request with ten `noul` questions; `src/jev/client.ts` validates answers | Implementation, tests, requirements, verification, readiness, progress, stuck, off-track, instruction drift, and human need. Code selects actions; Jev does not. |
| Continue, steer, or request another pass | `program.advise` / `jev.advise` with `steer` or `followUp` and explicit `triggerTurn` | Same delivery semantics as actors, with additional Jev freshness and feedback gates. Advice is asynchronous, not a pre-tool veto. |
| Coding workers and independent verifiers | Existing `agents.spawn` / `agents.run`, tool allowlists, worktrees, status/log/wait | Main is the worker in this profile. Main can launch a verifier and execute approved targeted checks; the observer does not acquire worker-launch authority. |
| Stop/retry an owned child | Existing `agents.stop`, then an explicitly bounded fresh `agents.spawn` | Stop is not rollback; retries are policy, not a primitive. The default sidecar does not stop Main or retry workers automatically. |
| Structured progress and inspection | `program.emit`, `jev.status`, terminal envelopes | Latest 64 events in memory, not Foreman's atomic state file and append-only durable run store. |
| Persistent coordination when needed | Existing `mesh.put` CAS state, `mesh.publish`, durable actors and participant subscriptions | Requires explicit capabilities and trusted mesh configuration. A Jev run is not a mesh participant or a durable subscription target. |
| Bounded cost and interruption | Program limits, host ceilings, `jev.stop`, shared Main halt path | Main abort/Escape, tree navigation, and provider retirement cancel observers. Token usage is post-request, not a hard dollar cap. |

`FabricRuntimeState.dispatchHostEvent` feeds both actors and Jev from the same lifecycle, while `haltAdvisors` stops both. Jev is available with mesh disabled. Normal reasoning actors may receive a sanitized recent transcript and use broader host events; Jev deliberately requires an explicit text projection and exposes a smaller event allowlist. That difference is an authorization boundary, not a reason to add implicit transcript access.

The public types and host APIs are exported by `pi-fabric/jev` (`src/jev.ts`); guest declarations live in `src/jev/guest-types.ts`, and provider actions in `src/providers/jev-provider.ts`. See [Jev](jev.md) for their exact contracts and [agents](agents.md) for worker/actor control. All effects keep the normal approval, authorization, and capability-pinning paths.

## Profile behavior

The skill ships an executable starter in both kernel trees. The Python entry point submits the same TypeScript Jev artifact; it does not change the configured outer kernel. The program grants only `jev.evaluate` and, in steer mode, `jev.advise`.

1. Main derives a bounded goal, acceptance ledger, and applicable instruction excerpt; the user consents to sharing them and selected future turn text with TypeSafe.
2. The program consumes `turn_end` into a four-turn window. At the selected cadence it requires the latest turn to match the current revision, remain within 30 seconds, and be untruncated. Settlement metadata alone cannot establish completion. Duplicate settlement of the same revision costs no second evaluation.
3. One request scores all ten questions. Human need and bounds outrank drift/stuck correction; readiness, verification, and ordinary progress come later. Fixed policy messages request one corrective step, independent verification, continued implementation, or human attention. One initial correction has a 30-second grace period before persistent drift becomes an escalation outcome.
4. `FINISH_REVIEW` is only a candidate for Main's acceptance review. Main must inspect independent verifier findings and actual targeted test/build results before claiming completion. Semantic scores and verifier prose are not passing checks.
5. The observer retires on new input, escalation, a finish-review candidate, inference failure, or its budgets. Terminal `completed` means the program returned valid output, not that the software job is complete. Main inspects `result.outcome`.

Requested limits are 15 minutes, 20 assessments, 600 host calls, 40,000 reported tokens, and 120 consumed events, clamped to trusted host ceilings. Inference failure ends the profile with a labeled escalation, without spinning on 429/529 or guessing an answer. Read [Jev's budget and auth rules](jev.md) before launch. No live API test is necessary to install or verify the skill.

### Delivery is deliberately bounded

The profile uses `delivery: "steer", triggerTurn: true, maxAdvice: 1`, or record-only with neither delivery nor the `jev.advise` grant. Across **all** Jev observers, Fabric permits at most one delivery attempt per external user input. Automatic continuations do not re-arm it. Classification may continue, but later messages may be suppressed; progress events record the actual delivery result. Do not bypass the guard with `agents.steer`, repeated launches, or another transport.

Escalation and limit outcomes stop the observer, not Main. Even an accepted pause directive is advisory; a suppressed one cannot be assumed to have paused anything. This profile is not a safety gate or a fully autonomous stop/retry factory. Child control is already available when separately authorized, but should have explicit ownership, concurrency, grace, retry, deadline, and cleanup rules. Do not stop unrelated peers or route Main controls around advice policy.

### Intentional differences from upstream

- Main replaces the Codex App Server worker. Existing Pi/Claude runners can be composed separately; no new Codex App Server transport is implied.
- Turn/settlement boundaries replace streamed subprocess output and periodic observations, as requested. Silence causes no inference.
- Bounded selected evidence replaces automatic Git/shell observation. The program has no shell authority and no secret/history access. If the instruction snapshot changes, review and explicitly replace the observer; do not assume it refreshed.
- Jev programs do not survive host restarts. In-memory status is not durable recovery. Existing mesh primitives can store explicit checkpoints, but they do not resume a lost QuickJS context or make effects exactly once.
- The skill does not weaken feedback prevention to reproduce an unbounded factory loop. Use a reasoning actor when persistent conversational supervision with its different context/lifecycle contract is desired.

## Acceptance ledger and verification

| Check | Evidence |
| --- | --- |
| Both kernel commands load, remain user-only, resolve pointers, and submit the same artifact | `tests/kernel-skills.test.ts`, `tests/skill-docs.test.ts`, `tests/foreman-skills.test.ts` |
| No inference before a boundary; both cadences use opted-in evidence and batch ten Noul questions | `tests/foreman-skills.test.ts` through the real Jev provider and QuickJS runtime with synthetic transport |
| Deterministic continue, resume, steer/grace, verify, finish-review, human escalation, and bounds | Same executable skill tests, not a reimplementation of the policy |
| Missing/truncated/stale context, duplicate settlement, inference errors, and cancellation never imply success | Skill tests plus `tests/jev-observation.test.ts` |
| Approval/capability gates, feedback suppression, shared host event delivery and halt | `tests/jev-observation.test.ts`, `tests/jev-host-integration.test.ts` |
| Packaged compiled public API and event-driven advice still work | Fresh `bun run build`, then `bun run test:jev:dist` |

Run only these targeted checks, not the entire repository suite. Threshold accuracy needs separate, consented evaluation on representative tasks; deterministic tests establish control flow, not semantic correctness.
