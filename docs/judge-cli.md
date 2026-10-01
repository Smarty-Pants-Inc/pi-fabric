# fabric-judge: bounded, advisory item judgments

**Stacked on #214** (the #2890 shared decision ledger and route hook). This is a
factory-callable JSON seam, not a controller, daemon or remediation service.
It launches a capability-restricted process agent on ambiguity and therefore
requires a **named Sol-max security pass** before installation/live admission.

## Contract and packaging

`package.json` registers `bin/fabric-judge`, which executes `dist/judge-cli.js`.
Use Node 24+, the package's declared Pi/TypeBox peers, and an explicitly selected
Pi binary (the proof uses Pi **0.87.1**). No Pi Main/session is required.
The executable reads one JSON document from stdin (32,768 bytes maximum, stdin
must close within 5 seconds), and writes exactly one terminal JSON envelope to
stdout, after owned worker cleanup. Exit 0 means a completed non-unknown judgment;
exit 2 means unknown/rejected. Do not interpret the exit status as authority to act.

```json
{"questionClass":"item-stalled","itemRef":"org/repo#1","evidenceRefs":[{"url":"https://example.test/source","revision":"snapshot-1","observedAt":"2026-10-01T00:00:00Z"}],"evidence":{"facts":{"complete":true},"excerpts":["Redacted snapshot excerpt."]},"allowedVerdicts":["moving","stalled","dependency","agent_decision","human_decision","unknown"],"timeboxMs":30000,"budget":{"maxEvaluations":1,"maxAgents":1,"maxTokens":10000},"requestKey":"class/item/snapshot/policy"}
```

All request and nested structural objects reject additional fields. Facts are
bounded JSON data (depth 6, 128 entries/container, strings 4,096 characters), not
configuration; dangerous prototype keys are rejected. Excerpts: at most 32, each
4,096 characters. Refs: at most 32 distinct HTTPS URLs (2,048 characters), no URL
credentials; revision 128 characters; observedAt a UTC timestamp. Item/request key:
512/256 characters. Verdicts must be a unique subset of the class enum, including
`unknown`. Timebox: 1–300,000 ms; evaluation/agent budgets: 0–1 each; token budget:
1–20,000. USD budgets are deliberately unsupported, not silently ignored.

The output includes `verdict`, `confidence` (0–1 or null),
`confidenceProvenance` (`jev-distribution`, `agent-self-report`, `unavailable`),
`evidenceLinks`, `nextAction`, `decisionId`, `cost`, `status`, `reasonCode`.
Citations must exactly match supplied refs; non-unknown agent verdicts require
at least one citation. The fixed recommendation mapping is:

| Verdict | nextAction.kind | owner |
|---|---|---|
| moving | none | dev-lead |
| stalled | review_stall | dev-lead |
| dependency | wait_dependency | dev-lead |
| agent_decision | request_agent_decision | dev-lead |
| human_decision | request_human_decision | dev-lead |
| unknown | collect_evidence | knowledge-lead |

`nextAction.targetRef` is always the supplied itemRef. Recommendations are NOT
shell commands, launches, GitHub updates or inbox writes. The caller must verify
actual evidence, protection, holds and admission before any separate action.

## Trusted policy and ladder

Set `FABRIC_JUDGE_CONFIG` to an operator-controlled JSON file (at most 8 KiB).
Never derive this path, model, policy, binary or endpoint from item text.

```json
{"policy":{"version":"knowledge-item-stalled-v1","role":"Sol","pin":{"model":"provider/EXACT_APPROVED_SOL_MODEL","effort":"max"}},"piBinary":"/absolute/path/to/pi"}
```

The Sol role's exact model identity is approved by the trusted policy owner;
Fabric does not guess a model ID from the word Sol. `high` is allowed ONLY when
the trusted policy also sets `protectionKnownClear:true`; otherwise `max` is
mandatory. Untrusted evidence cannot lower either pin. Optional `ledger` must
be an absolute host-owned path (default: the #214 `fabric/model-routing.jsonl`
under Pi's agent directory). `jevModel` uses the existing Jev route resolver and
credential mechanisms. No new secret store/loader or alternate-account retry.
`fixtureEndpoint` is an explicitly trusted **loopback-only HTTP fixture** option;
it uses a dummy fixture key, never a host credential. It is not a production
endpoint/authentication override.

1. Validate, allocate the #214 decision ID, fsync exactly one decision via
   `appendRouteRecord` **before any inference**. A pre-record failure means no
   dispatch. The record contains item/key/ref revisions, policy/rubric/schema
   versions, pinned model/effort, budget and deadline, not raw evidence text.
2. Evaluate exactly one finite Jev Choice with `JevClient.evaluate`, bounded by
   2.5 seconds and the total deadline. Accept only when probability **and**
   confidence are >= .90. Jev citations/recommendations are host-derived, not
   generated instructions. A malformed answer/refusal/timeout/error is terminal
   UNKNOWN, not a cross-provider retry.
3. Only a valid low-confidence distribution can start ONE `AgentManager` ->
   `ProcessTransport` -> worker. Its own manager config sets timeout and remaining
   `maxTokensPerChild`; no ineffective smaller per-call timeout override. Exact
   model/effort admission is verified before prompt. Startup and resume retries
   are disabled for judgment runs.
4. Worker: `tools:[]`, `extensions:false`, `recursive:false`, full-code off,
   empty private cwd, no ambient skills/prompts/context/themes/project resources,
   no auto-compaction; fixed system prompt replaces ambient system instructions.
   Only the reviewed reply and route hooks load. The evidence packet is JSON
   wrapped/labelled UNTRUSTED DATA. The result must be one schema-validated
   `fabric_reply`; free text is not a verdict. Central citation/finite-action
   validation runs again before acceptance.
5. Same decision ID joins Jev attempt, child route header/receipt/outcome,
   agent attempt, and final judgment outcome. The #214 hook emits
   `X-Smarty-Route: item-stalled/<encoded-pin>-<effort>/judgment-agent:<decisionId>`.
   The pre-recorded host-only dispatch option prevents a second decision row.
   The outcome has `truth:null`: self-judgment is not an independent training label.

UNKNOWN reason codes include `incomplete_evidence`, `evaluation_budget`,
`agent_budget`, `token_budget`, `timeout`, `cancelled`, `refusal`, `invalid_schema`,
`invalid_citation`, `budget_wait`, `agent_failed`, `inference_failed`,
`record_failed`, `outcome_record_failed`; rejected transport/configuration inputs
use `invalid_input`, `invalid_config`, `invalid_policy` before inference (no
accepted decision record when there is no valid request/trusted policy). No
automatic human ask follows an error. Cancellation stops owned workers and
awaits exit; cancelling an observation alone is never considered cleanup.

## Proof and explicit limits

```sh
bun run build
nice -n 10 python3 scripts/prove-judge.py --out "$TASK_OUT/python-proof" --pi /absolute/pi
```

The Python script (no Pi Main, clean fixture environment) calls the packaged bin
twice with the same synthetic `item-stalled` packet: high Jev acceptance, then
valid low confidence -> a real Pi 0.87.1 RPC process using a faux OpenAI-compatible
local model and the actual reply/header hooks. It asserts exact one terminal JSON,
one decision per call, same IDs across ledger/header/outcomes, admitted pin,
only `fabric_reply`, and untrusted injection text excluded from system authority.
It saves both envelopes, ledgers, stderr and a combined `proof-output.json`.

This is **not live Knowledge calibration, production-model quality or ALL-work
mission proof**. The trusted policy/quality gate, factory admission/#2876 capacity
reservation, stable request/run deduplication/reconciliation, controller consume
and revalidation, live gateway metering, later independent truth labels and
actual remediation/inbox authorization remain external integration gates.
`requestKey` is recorded for the controller; the judge does not replay/dedupe it.
The deterministic evidence/controller rung is outside this deliberately bounded
Jev -> judgment-agent PR1.

Token usage includes Jev and child input/output/cache usage. Guards observe
reported usage after responses and can overshoot; this is NOT a gateway hard-token
or dollar reservation. Total USD pricing is unavailable, so cost.usd stays null
and basis unknown; individual child reported charges remain in attempt/route
records. Budget/capacity errors are named waits, never cheaper-model or account
fallback. Native Pi/provider availability retries are inherited from trusted Pi
configuration and remain within the total deadline; this slice disables Fabric
startup/resume retries, not upstream retry or reservation semantics. Tool
allowlisting is **not a same-UID OS sandbox**. Worker-owned temporary
transport files are not remediation writes; only the shared ledger persists on
normal completion. An unresolved worker keeps its owned receipt for reconciliation.

The judge has its own build entry and is forbidden from the extension's static
startup graph; it is not registered as a session hook/provider or idle service.
