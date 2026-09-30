# Fabric turn provenance

Fabric supplies structured admission metadata with every user-message injection and custom-message delivery to Pi. Message text, names quoted in a report, and payload fields cannot select a human channel or principal.

```ts
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

The sender comes from the mesh command/event envelope or the registered local producer. A mesh bridge adds `verified: "bridge"` to the admitted envelope after its identity and ownership checks. Fabric preserves that marker through control routing and its durable journal. Host-generated summaries, reload requests, shell notices, and prewalk directives identify the emitting Fabric runtime; reporting an actor failure does not impersonate the failing actor.

`via` is `steer`, `followUp`, `actor`, or `replay`. Custom-message delivery keeps its existing `deliverAs` and `triggerTurn` options, including passive messages and `nextTurn`. On capable local hosts, mixed-sender follow-up, lifecycle, and work-inbox batches are split into sender-homogeneous FIFO messages. Legacy hosts retain their existing batching.

## Host API compatibility

The Fabric adapter requires the explicit Pi extension capability `pi.supportsProvenance === true`. That capability must mean that **both** `pi.sendUserMessage(text, { deliverAs, provenance })` and `pi.sendMessage(message, { deliverAs, triggerTurn, provenance })` accept and persist v1 provenance. The current pinned Pi dependency predates that capability. The Pi owner must expose the capability and custom-message option alongside the user-message option before this adapter activates.

JavaScript function arity cannot prove option support. Fabric makes no probing delivery, version guess, catch-and-resend, or silent conversion of passive custom messages to triggering user messages. Without the explicit capability, it passes the original arguments unchanged and records one compatibility warning per Pi process, including across extension reloads. A user injection that previously used one argument still uses one argument.

## First receipt and recovery

Pi writes `turnId` and `receivedAt` at the first receipt. Fabric never creates either field. The `main-followups` journal stores the original verified sender and admission metadata before acknowledging a durable message. Its `sentAt` records Fabric send time; it is not Pi receipt time.

After restart or reload, an unreceived journal item goes to Pi with the original sender and `via: "replay"`. Its admission record and send time remain unchanged. An item already held by any persisted session branch is never re-injected. During a live reload, a handed-over item still in Pi's queue or in-memory session is also left alone. Compaction, fork, export, and session-entry stamping remain Pi responsibilities.

Only Pi assigns `keyboard`, `terminal`, and `voice`. Unattested pane writes are terminal input, with no human principal. Fabric never claims `keyboard` or `voice`, never supplies a principal, and does not bind voice from global settings or message text. Consumers treat absent provenance and unknown versions as UNKNOWN.

## Trust boundary

Mesh verification uses Fabric's existing trusted local runtime and filesystem admission boundary. The bridge additionally validates remote participant ownership at commit. These metadata fields do not provide cryptographic isolation from a process that can edit mesh state, journals, or the Pi session file. They attribute the emitting participant; they grant no approval or authority.
