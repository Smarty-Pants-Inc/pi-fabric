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

Only Pi assigns `keyboard`, `terminal`, and `voice`. Unattested pane writes are terminal input, with no human principal. Fabric never claims `keyboard` or `voice`, never creates a human principal, and does not bind voice from global settings or message text. A relay may carry the original host-admitted principal unchanged, as described below. Consumers treat absent provenance and unknown versions as UNKNOWN.

## Originating principal on relays (#821)

A verified Pi receipt may name an originating requester:

```ts host
principal: { id: "paul", binding: "voice-call" }
```

`FabricPrincipal` records attribution, **not authority**. Its bindings are `herdr-client`, `voice-call`, and the reserved `org-agent` binding. Fabric accepts a human origin only from a Pi-stamped v1 `keyboard`/`herdr-client` or `voice`/`voice-call` receipt. A Fabric receipt must have an admitted `mesh`/`bridge` sender. All origins require Pi's `turnId` and `receivedAt`; an unstamped extension claim is not a receipt. An `org-agent` origin must already be explicitly enrolled in trusted receipt metadata; there is no lookup by agent name, text, environment role, or payload, and no default org identity mapping in this PR.

Cheap `message_start` and `context` observers capture the requester from actual input metadata. `before_agent_start` clears the previous scope; unattributed user/custom deliveries clear it too. Only Fabric's passive skill/proxy/shell-awareness notices are excluded as non-request inputs. The inference input reconstructs scope after a live extension reload. No journal engine or optional engine is imported at registration.

Model-authored `principal`, `provenance`, `sender`, and nested `data` fields cannot create or upgrade this scope. Agent send/spawn and mesh publish providers snapshot it separately from payload data before constructing outgoing host-owned deliveries. The immediate sender changes at each hop; `principal` does not. No scope means no principal claim. Mesh/control events put principal in the event envelope, not command data; receiving control hydrates the command only from that admitted envelope. The bridge forwards only recorded admissions. Host-originated lifecycle/alarm routing does not borrow an ambient principal.

Task launch stores its admission in a private sidecar next to `task.txt`; steering records keep it separately from message/data. A stable `worker/principal-delivery.js` extension consumes one private, worker-generated delivery id through `/fabric-delivery` and injects through Pi's trusted API. RPC prompts cannot claim provenance: arbitrary RPC/prompt fields and command arguments never provide admission metadata. The extension strips old receipt stamps. Hosts without the explicit capability retain legacy unattributed behavior, including worker prompts. Global installation trust must include the stable worker hook's release path as well as the main extension.

Main's follow-up journal and private actor queues persist the principal with the verified admission before acknowledging it. Replay preserves that snapshot and send time, changes only `via` to `replay`, and lets Pi stamp first receipt. Same-sender deliveries with different principals are separate capable-host batches. Actor activations pass the queued admission into their task launch. Automatic actor replies retain a principal only while every admitted steering and follow-up input has that same principal. Foreign or UNKNOWN input cumulatively clears the activation attribution before admission; recoverable activations persist the downgrade first, and a failed persistence write rejects the input. Ownership replay and owner restart preserve the downgrade with the mailbox item, including when the native session is reused. Recovery records without a supported activation-lineage version carry no principal. Coalescing takes the admission of the selected payload and clears any older claim when that payload has none. Host failure alarms carry no principal. Resident owners preserve the same admission through their private records and controls.

`FabricPrincipalAuthorityCheck` is a typed integration port exported with `FabricPrincipal` from `pi-fabric/mesh`. It describes a principal/action/target check and an `allow`/`refuse`/`unknown` result; **no policy or default refusal rule is installed here**. Knowledge-lead's #808 authority mapping and dev-lead's role/AGENTS rules are prerequisites for the unauthorized-write counterexample (issue acceptance 3b). Attribution alone never authorizes a write.

Positive host-side proof requires a Pi build advertising `hostCapabilities.turnProvenance === 1` that accepts relayed Fabric principals and exposes its stamped receipts. A bare version string (including `0.87.1`) is not evidence of that capability. The pinned legacy SDK and a stock installed host may lack it; a real-process probe must report that blocker. It must not fabricate receipts or force a capability flag.

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
