# Factory-owned actor sessions

A factory actor is still a Fabric actor. Session mutation is fenced to the **owning Pi host identity**, not merely the OS machine on which a caller happens to run. Do not edit, truncate, or remove a live actor's `session.jsonl` from a shell, and do not bypass the ownership check.

## Reset one actor by name

Execute this TypeScript body through `fabric_exec` in the existing Pi session that owns the factory actor (the `smarty-factory-host@` owner for the incident in smarty-dev#3238):

```ts
return await agents.resetSession({ id: "code-shards6-4-security-astra" });
```

The general call is **`await agents.resetSession({ id: "<actor name>" })`**. `id` accepts an exact actor name, a unique ID prefix, or the full actor ID. This is the live actor action, not a global-template operation. `agents.actorStatus({ id: "<actor name>" })` identifies the actor before the reset; retain its returned ID and reset response as operator evidence.

The `agents.resetSession` action forwards the supplied name unchanged to `ActorManager.resetSession`, which resolves that name and checks the host's ownership. A different owner receives `Fabric actor is owned by another host: <id>`. Running a new Pi on the same OS host does **not** make it the owner, and this action does not proxy reset requests to a remote factory. Route the above body to the existing owning Pi rather than issuing it from code-lead's Pi. No factory shell reset command or remote reset API is implied here.

An in-flight activation finishes first. At the serialized boundary, Fabric archives the journal as `session.jsonl.<UTC stamp>.bak` (the two newest backups are kept), seeds a new native header, logs one `fabric-host` message with `reason: "session reset (requested)"`, and publishes presence. Instructions, topics, model/effort bindings, queued work, and the mailbox are unchanged. Inspect the reset entry with:

```ts
return await agents.messages({ id: "code-shards6-4-security-astra", limit: 20 });
```

Tests in `tests/actor-session-reset.test.ts` cover the exact-name lookup, owner refusal, archive/log behavior, and waiting for an in-flight run. This documents the supported owner control call; it does not claim that the production actor has been reset.

## Activation inference and window alarms

Factory reviewers that select `inferenceContext: "activation"` start each child from their instructions and new message, not their accumulated audit history. The worker isolates inference **before Pi startup**, then retains the current native entries in the full journal after exit. This protects preflight as well as the actual model request from oversized previous tool results. All tool exchanges in the current activation remain in context.

After all context transforms and native `before_provider_request` handlers, the worker checks the final JSON request (including system/messages and tool schemas) against the selected model's context window using Pi AI's text estimator. It dispatches only the admitted JSON snapshot; unsupported non-JSON payloads fail closed. A later payload reduction can make an otherwise oversized activation fit. A single unfittable activation fails once and immediately emits an owner-visible `actor.alarm` on `ops.owner` plus a triggered host follow-up; it does not wait for three failures or enter an automatic retry loop. Reduce/split an oversized new message; resetting history cannot make that message fit. Full-history actors keep native history/compaction semantics.

If a hard-killed worker or retention failure leaves `.activation-<run id>.jsonl` beside the actor journal, preserve that file for recovery. Normal terminal retention appends its entries before publishing status and removes the isolated file only after successful retention. Never manually merge or delete it while an activation is running.

See [actor inference and reset semantics](agents.md#inference-history-per-activation).
