# Native wake attribution

Fabric records wake diagnostics without changing message text, delivery policy, or turn authority.

## Admission boundary

Only newly admitted `message_start` sources since the preceding native `context` boundary are counted. Re-reading history or calling `context` again without an admission does not create another record.

- Exactly one confirmed Fabric source: `type:custom`, `customType:pi-fabric.wake-cause`, with `{cause, from, topic?, key?}`. This is an exact single-origin view.
- Exactly one raw-user source without a native capability-1 origin receipt: `pi-fabric.wake-diagnostic`, with `{cause:"unattributed"}` or `{cause:"ambiguous", candidates:[...], basis:"unconfirmed-raw-input-attempts"}`. Attempts are not admissions.
- Several sources: **only** `pi-fabric.wake-diagnostic`, with `{cause:"multiple", exact:false, causes:[...]}`. The ordered list retains each source and its own `exact` boolean. An exact list member does not make the aggregate exact. A human or otherwise unattributed raw user is retained as an inexact member, never overwritten by a later Fabric message.

Example: human input followed by an admitted Fabric steer in one boundary:

```json
{"cause":"multiple","exact":false,"causes":[{"cause":"unattributed","exact":false},{"cause":"steer","from":{"id":"agent:worker","name":"Worker","kind":"agent"},"exact":true}]}
```

Batched Main followUps, root mesh inbox events and local lifecycle events retain all their causes. No single cause is promoted to represent the batch, so the aggregate is never marked exact.

## Trust boundary

`wakeCause` and `wakeCauses` supplied in message details or sender commands are not receiving evidence. Control admission derives identity, delivery kind, topic and event key from the authenticated `MeshEvent` envelope. Main ignores plain `request.wakeCause`. Resident admission ignores the record's optional diagnostic: host notices use the authenticated resident writer; positively classified actor output retains the existing delegated actor identity. Resident diagnostic topic/key describe the receiving storage route and envelope key.

Trusted local lifecycle/completion adapters retain their categories with private, nonserialized in-process admission snapshots. These do not add authority or cross a control transport. Local lifecycle snapshots come from the broker's admitted observed events, never event payload diagnostics.

Custom-message capture requires the private producer receipt registered against the native message's details object. Native Pi preserves that reference. A serialized copy, foreign details dictionary, or mutation of the exposed diagnostic cannot forge or alter the receipt. Receipts are consumed once; historical metadata is not backfilled.

## Reading evidence

`scripts/read-wake-causes.py` allowlists cause records (`cause`, `from.id/name/kind`, optional `topic`, `key`, `exact`) and diagnostics (`cause`, optional `exact`, validated candidate/causes lists and fixed ambiguity basis). Extra fields at every nesting level are dropped. Malformed required fields still fail validation. `multiple` is a diagnostic, not an exact single Fabric turn. Historical output names `rawUserDiagnostic*` include multiple-source boundary diagnostics; do not interpret every diagnostic as a human input or an autonomous Fabric wake.

Producer metadata (`custom_message.details.wakeCause` or batched `.wakeCauses`) and actual admission entries are alternative views: never sum them. Admission does not prove provider success.

## Test-only native probe observation

`scripts/probe-wake-causes.mjs` observes RPC events and native journal/mesh filesystem events. Expected arrivals have one finite timeout, not a sampling loop. Absence has no positive event: quiet windows (1-second passive admission, optional 121-second human idle precondition, and bounded final shadow/idle coverage) use one named deadline each, with immediate checks on RPC/filesystem arrivals. These are **test-only bounded one-shot waits**, not production polling. The helper `scripts/wake-probe-observer.mjs` closes all listeners/deadlines on success, failure or shutdown.
