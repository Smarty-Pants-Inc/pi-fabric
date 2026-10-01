# Fabric turn provenance

Fabric supplies structured admission metadata with user-message injections and custom-message deliveries whose sender has a recorded verification method or is an explicitly registered in-process producer. Deliveries without that admission evidence carry no claim. Message text, names quoted in a report, and payload fields cannot select a human channel or principal.

```ts host
provenance: {
  v: 1,
  channel: "fabric",
  sender: {
    id: "agent:worker-id",
    kind: "agent",
    name: "Worker",
    verified: "mesh"
  },
  via: "followUp"
}
```

The sender comes from the mesh command/event envelope or the registered local producer. Mesh events record their admission method in `event.verification`; control and legacy relay delivery carry that record through to Main and its durable journal. A recorded `bridge` method yields `kind: "remote"`, even without an identity marker. An absent record (including commands from pre-change bridge processes) yields no claim: neither `from.verified` nor `data.bridge` grants verification. In-process producers explicitly supply `mesh`; it is never the default. Host-generated summaries, reload requests, shell notices, and prewalk directives identify the emitting Fabric runtime; reporting an actor failure does not impersonate the failing actor.

Records inbox batches, including peer-authored and imported `github:*` records, have author labels but no authenticated admission envelope, so they carry no claim, never the receiving Main's identity. The principal's typed `/fabric prewalk <task>` and dashboard input to the local Main also carry no Fabric claim; Pi assigns their input channel. Only the generated prewalk notices and directives claim Fabric.

`via` is `steer`, `followUp`, `actor`, or `replay`. Custom-message delivery keeps its existing `deliverAs` and `triggerTurn` options, including passive messages and `nextTurn`. On capable hosts, turn-start work, records, completion, and shell inbox insertion, plus skill/proxy notices, use the same delivery adapters with `deliverAs: "nextTurn", triggerTurn: false`; Pi consumes these messages after the hooks and before the first inference, without an extra wake. Hosts without `pi.hostCapabilities?.turnProvenance === 1` retain the original `before_agent_start` hook-result messages: legacy Pi consumes its `nextTurn` queue before the hooks, so enqueueing there would delay delivery until another prompt. Hook-result delivery joins the first inference without a wake or a stale queued copy. On capable local hosts, mixed-sender follow-up, lifecycle, and work-inbox batches are split into sender-homogeneous FIFO messages, including at turn start. Legacy hosts retain their existing batching.

## Host API compatibility

The Fabric adapter requires the explicit Pi extension capability `pi.hostCapabilities?.turnProvenance === 1`. On that host, **both** `pi.sendUserMessage(text, { deliverAs, provenance })` and `pi.sendMessage(message, { deliverAs, triggerTurn, provenance })` receive the v1 claim. The deprecated `pi.supportsProvenance` flag does not activate this adapter. The current pinned Pi dependency predates the settled capability; older hosts keep legacy calls and turn-start hook-result delivery.

JavaScript function arity cannot prove option support. Fabric makes no probing delivery, version guess, catch-and-resend, or silent conversion of passive custom messages to triggering user messages. Without the explicit capability, it passes the original arguments unchanged and records one compatibility warning per Pi process, including across extension reloads. A user injection that previously used one argument still uses one argument.

## First receipt and recovery

Pi writes `turnId` and `receivedAt` at the first receipt. Fabric never creates either field. The `main-followups` journal stores the original verified sender and admission metadata before acknowledging a durable message. Its `sentAt` records Fabric send time; it is not Pi receipt time.

After restart or reload, an unreceived journal item goes to Pi with the original verified sender and `via: "replay"`. Fabric sends no `turnId` or `receivedAt`, including stamps found in an older journal. Pi creates a new authoritative receipt and ignores any receipt fields supplied in extension claims. Its admission record and Fabric send time remain unchanged. An item already held by any persisted session branch is never re-injected. During a live reload, a handed-over item still in Pi's queue or in-memory session is also left alone. Compaction, fork, export, and session-entry stamping remain Pi responsibilities.

Only Pi assigns `keyboard`, `terminal`, and `voice`. Unattested pane writes are terminal input, with no human principal. Fabric never claims `keyboard` or `voice`, never supplies a principal, and does not bind voice from global settings or message text. Consumers treat absent provenance and unknown versions as UNKNOWN.

## Trust boundary

Pi accepts a Fabric claim only from an extension listed in the host's **global** `settings.json` under `turnProvenance.fabricExtensions`. The profile uses `PI_CODING_AGENT_DIR`; trust is never configured through project settings. For the fleet's release layout, the global entry is:

```json
{
  "turnProvenance": {
    "fabricExtensions": ["/home/paul/.local/share/smarty-dev/fabric/releases/"]
  }
}
```

Without trust, or with an invalid claim, Pi still delivers the message and records `terminal`. The host capability advertises API support, not installation trust. Bridged messages use `sender.kind: "remote"` and `sender.verified: "bridge"`; local participants retain their admitted kind. A user-message injection with no known calling participant carries no claim.

Process workers receive prompts over RPC. RPC cannot claim a channel, so those turns record `terminal` (UNKNOWN to k3) until the worker's Pi delivers through the trusted Fabric extension's `sendMessage` or `sendUserMessage` API. Fabric does not stamp worker RPC prompts.

Mesh verification uses Fabric's existing trusted local runtime and filesystem admission boundary. The bridge additionally validates remote participant ownership at commit. These metadata fields do not provide cryptographic isolation from a process that can edit mesh state, journals, or the Pi session file. They attribute the emitting participant; they grant no approval or authority.
