# Opt-in task model routing (shadow only)

`agents.spawn` accepts `model: "auto"` for **new session-owned process/Pi tasks only**.
It makes one typed Jev Choice and records it. It **never changes the model or effort
launched**: PR1 always runs the role's explicit pin, even when Jev recommends a
cheaper candidate at high confidence. This is evidence collection, not live routing
or measured savings. Mains, actors, durable agents, handoffs, explicit-model calls
and the fleet's direct task CLI are not routed.

```ts
const child = await agents.spawn({
  task: "Extract the public version number from the named file; report only that number.",
  model: "auto",
  routeClass: "bounded-lookup",
  transport: "process",
  runner: "pi",
  pinModel: "my-provider/gpt-6.1-sol", // registered canonical model or role alias
  pinThinking: "high",
  // Map trusted issue/PR state to this flag, NEVER infer it from task text.
  protected: false,
});
```

Bounded lookup means a checkable one-file lookup, extraction or formatting task;
not code changes, design judgment, review evidence, secrets or client data. Callers
must set `protected: true` for review, security, audit, named passes and all
`needs-security-pass` work. An omitted/unknown flag or unknown class is excluded
before Jev. Protection is a caller-supplied trusted-state snapshot, **not an
authenticated label oracle**. No prompt-based override or cheaper live route exists.

## Pins and finite candidates

Both pins are required. Call fields take precedence over dedicated role settings
in the usual `fabric.json` configuration. The parent's model, global `agents.model`
and Fabric's default medium effort do **not** qualify as role pins.

```json
{
  "agents": {
    "modelRouting": {
      "pinModel": "my-provider/gpt-6.1-sol",
      "pinThinking": "high",
      "shadowCandidates": [
        { "model": "my-provider/gpt-6.1-sol", "effort": "medium" },
        { "model": "my-provider/gpt-6-luna", "effort": "medium" }
      ]
    }
  }
}
```

Omit `shadowCandidates` for the pin-only default. The pin is always included and
identical model/effort pairs are deduplicated. Shadow candidates must use exact
registered, authenticated keys visible to Main; at most 16 additional candidates
are evaluated. Unavailable candidates select the pin with `invalid-candidates`.
Configuring a shadow candidate grants **no permission to run it**. Paul's rule
still applies: no Sol role at medium or below without measured parity and his
approval. This PR has no switch to enable live routing.

Jev receives only class, clear protection status and finite model/effort choices:
no task text, prompts, history or free-text explanation. One request, no retry,
2.5-second absolute deadline (including credential resolution/network work).
Both confidence and chosen probability must be >= 0.90. Low confidence, malformed
answer, backend error or timeout records a fixed reason and uses the pin. Caller
cancellation still cancels launch. The existing Jev provider/client supplies the
backend; `jev.enabled: false`, Schema enforce's unavailable Jev programs, or owner
retirement yields `jev-error` and pinned dispatch rather than relaxing that gate.

The handle includes `routeDecision` with `{ model, effort, confidence, probability,
reasonCode, decisionId, ... }`. `model/effort` describes the accepted **would-be**
choice (or pin on fallback); `shadowChoice` preserves Jev's raw finite choice even
below threshold. `handle.model/thinking` and the worker's admission still use the pin.

## Durable join and child header

Before the transport dispatches, Fabric writes a `decision` row to
`<requested cwd>/.pi/fabric/model-routing.jsonl`, outside the ephemeral agent run
directory, using append-only writes and `fsync`. This records the decision ID,
Main/child native session IDs, class, role pin, candidates, shadow choice,
confidence, probability, fixed reason, latency and time. A seeded native child
session binds the recorded child ID to Pi, not just to the process transport.
Terminal `outcome` rows join on `decisionId`, with status, verified admitted model
and effort, observed model, token/cache/cost counters when known and time.
Pre-admission failures record null admission, not the requested model. Confirmed
launch failures get an outcome; an unconfirmed launch is retained by the existing
manager's cleanup obligation rather than falsely reported as a completed child.
Outcome writes use the manager's terminal-save retry/retention fence. A process
crash before terminal settlement still requires a later outcome reconciler; PR1
does not add one or invent quality/price estimates.

If the decision write fails, pinned work still dispatches with `record-failed` in
the handle and header. If storage stays unavailable, no durable record can be
promised; the retained terminal run lets the owner diagnose/reconcile the gap.
There is no unlogged cheaper execution. Keep the state directory for Light's
later outcome join; deleting the caller's cwd deletes this local ledger.

Each provider request in the child has:

```
X-Smarty-Route: <class>/<percent-encoded-provider-model>-<effort>/<reasonCode>:<decisionId>
```

This identifies the threshold-accepted would-be choice, or the pin on fallback.
Actual model attribution comes from the provider/model row and the outcome, not
from treating a shadow header as a live route. The header has no prompts or
free-text reasons. A standalone explicit `-e` child hook mutates Pi's
`before_provider_headers` map in place; it also loads for `extensions: false`.
Worker launch strips inherited route metadata before passing this child's own
header, so unrelated nested explicit-model tasks are not misattributed.

**Required before merge/install:** a named **Sol-max security pass** on the
header/identity plumbing and local ledger trust boundary. Unit tests and an
isolated real-Pi localhost capture are evidence, not that security approval or
Light's production gateway receipt/weekly quality-and-spend acceptance.
