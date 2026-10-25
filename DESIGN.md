DESIGN (design-pair, challenge record: pending — external-lead-owned; ready for handoff before challenge)

Handoff status: this lane's design artifact is complete. The parent commits and sends the full text plus commit head to lead `fabric-store`; another lead briefs the Opus 5.5 xhigh challenger and returns findings. This lane must not spawn or brief a challenger. External challenge is not a blocker to file completion or transmission; findings receive at most two resolution rounds. Implementation waits for resolution of actual open design choices, not further research in this lane.

## Previous designs

This is an additive Fabric slice for [smarty-inbox #342](https://github.com/Smarty-Pants-Inc/smarty-inbox/issues/342), not a replacement actor system.

| Existing design | Keep | Change / limit relevant here |
|---|---|---|
| [NATS link](docs/mesh-bridge-nats.md) | Exact allow-list, credential-machine checks, existing actor discovery, mapped control IDs, ledger, cancellation and owner ACK return. | The callback needs authenticated source-machine evidence that the current file event does not retain. No pod wire change. |
| [A′ bridge threat model](docs/bridge-nats-threat-model.md), especially R1, R6–R8, R11 and T11/T12 | Source identity comes from verified presence on the credential-bound machine; the hub's same-UID trust boundary remains explicit. | A file event's `verification: "bridge"` is **not independently authenticated evidence** for this new callback. Add a bridge-owned durable evidence projection, not a trusted payload field. Hop dedupe is not application exactly-once. |
| [Durable residency through Pi](docs/residency-runtime.md) | Existing resident owner, locks, owner incarnation, readiness, wake, drain and release checkpoints. | Load a native callback in the resident, which runs with `--no-extensions`. Native callback promises must retain release custody without an AgentManager worker. Durable identity does not promise transparent resumption of an interrupted ask. |
| [Actor registry storage](docs/actor-registry-storage.md) | Format-1 metadata, exact IDs, locked saves, history storage, foreign-row preservation. | Native rows must be unmistakably non-model on load, save and import. A new optional field on an otherwise valid Pi row is insufficient, especially with older owned-row serializers. |
| [NATS authentication #7504](docs/designs/nats-auth-7504.md) and [challenge R1](docs/designs/nats-auth-7504-challenge-r1.md) | Network credential identity is distinct from same-host/same-UID isolation; enumerate actual bootstrap paths rather than only the initiating Main. | Do not introduce credentials, an issuer, or another auth transport here. Older proposals in this document are not evidence that the deployed Path B bridge has per-session cryptographic authentication. |
| [Retention apply #10678](docs/designs/retention-apply-10678.md), notably R2-C1/C2 | A serialized label is not a capability; process-local object identity is not durable proof; unknown ownership fails closed. | Its deletion machinery is unrelated and stays untouched. Apply the lesson to native execution markers and recovered ingress evidence, not the proposed retention framework. |

Source inspected at hub `7ac1f5dcfc2d1b624657bbc5fa0114a31f99f155`. Frozen pod revision resolves locally to `f33937156f587f2479c651ae301de44ec0f0592a` (`fix(nats): recover verified clients after server restart`). This is a design proposal: no implementation, compatibility test, application effect or security approval is claimed.

The named `src/mesh/types.ts` does not exist at this head. Actual event types are in [`src/mesh/event-log.ts`](src/mesh/event-log.ts), re-exported by [`store.ts`](src/mesh/store.ts); bridge contracts are in [`bridge-nats-types.ts`](src/mesh/bridge-nats-types.ts).

## Decision

Add an **ask-only, host-registered native execution mode to the existing durable ActorManager**. Bind the handler to an exact `(rootId, scope, actorId, handlerKey)`; never select it by a message string, actor name, arbitrary runner, or caller-supplied module path.

For this slice:

- The execution owner is the hub's existing file-backed ResidentHost. Native business execution on a NATS host, session-only native actors, topics, host-event subscriptions, directives and native task runners are not supported.
- The native actor is discoverable and remotely addressable through the existing actor participant and `agents.ask` path. No new provider, channel, endpoint, daemon, delivery loop, dependency or model run.
- The bridge writes a small immutable ingress receipt beside its existing ledger. The receiving control plane reads the receipt through a **host-pinned reader** before claims, cache replay or cancellation. The callback context is reconstructed from this evidence, not from `event.from.verified`, `verification`, `sender`, `principal`, `data`, or a serialized context object.
- Keep native asks ephemeral in the actor queue, as current asks already are. Do **not** add callerless replay or persist a trusted callback context. A process crash leaves the ordinary control outcome completed or indeterminate; the application retries with the same `opKey`.
- Application RPC parsing, seat authorization and atomic durable application deduplication remain the handler's responsibility. Fabric guarantees faithful opaque data transport, authenticated ingress context within the stated trust boundary, native-only dispatch and existing control correlation—not exactly-once arbitrary external effects.

### Frozen public exchange

```ts
const reply = await agents.ask({
  id: pinnedActorId,
  message: 'smarty-inbox.rpc.v1',
  data: { v: 1, requestId, operation: 'add' | 'list' | 'resolve', opKey, args },
});
// reply is the existing FabricActorMessage wrapper:
reply.data; // { v: 1, requestId, ok, status, result | error }
```

No identity fields, handler key, module, ingress receipt, source machine or seat selector are added to this request. No model/thinking override is required. The `status` vocabulary and application result/error schemas belong to smarty-inbox; Fabric must not invent them. Application failures are successful transport replies containing `ok: false`. Pre-handler transport/native-admission failures reject the ask by the existing control failure path, not a fabricated application envelope.

## Prior art

No third-party code is proposed for copying and no new external dependency is selected. Bounded non-GitHub documentation/package searches were performed for this draft on 2026-10-25. GitHub calls were explicitly prohibited; repository links below identify sources but were not fetched.

| Candidate | Link / licence evidence | Fit and reason not to introduce it |
|---|---|---|
| Existing Fabric ActorManager + control + A′ ledger | [manager](src/actors/manager.ts), [control plane](src/topology/control-plane.ts), [ledger](src/mesh/bridge-nats-ledger.ts); repository MIT (`package.json`). | Best fit. Already supplies identity, ownership, serial drains, claims, cancellation, ACKs, resident wake and retry correlation. Add one native dispatch branch rather than duplicate these. |
| Dapr actors | [official overview](https://docs.dapr.io/developing-applications/building-blocks/actors/actors-overview/), [source](https://github.com/dapr/dapr/tree/master/pkg/actors); Apache-2.0 verified from [pkg.go.dev licence](https://pkg.go.dev/github.com/dapr/dapr/pkg/actors?tab=licenses). | Stateful identities and turn-based access are the right model. Adding the runtime/state/placement integration would replace machinery already present and violate this bounded slice. Its turn serialization alone would not make external inbox effects exactly once. |
| Cloudflare Durable Objects / workerd | [official concepts](https://developers.cloudflare.com/durable-objects/concepts/what-are-durable-objects/), [workerd](https://github.com/cloudflare/workerd); `workerd` package metadata from [registry](https://registry.npmjs.org/workerd/latest) declares Apache-2.0. This does not license the hosted service. | Unique durable identities and transactional storage fit conceptually. A separate runtime or hosted service does not preserve the existing actor and bridge paths; no reason to add either for one callback. |
| NATS JetStream deduplication / ACKs | [JetStream semantics](https://docs.nats.io/using-nats/developer/develop_jetstream/model_deep_dive); installed `@nats-io/jetstream` and `@nats-io/transport-node` metadata declare Apache-2.0. | Already used. Reuse message IDs and claims for transport replay; do not confuse their bounded horizon with permanent application `opKey` identity or a transaction across external effects. |
| GOVA, *Graph of Virtual Actors: a Big Data Analytics Architecture for IoT* | [arXiv 1703.02510v1](https://arxiv.org/abs/1703.02510v1). API abstract inspected; paper/reuse licence **not verified**, no code adopted. | Search surfaced virtual-actor architecture, not a smaller implementation of this hub boundary. Full paper was not reviewed and is not evidence for this design's security claims. |

The arXiv query `all:"virtual actors"` returned nine hits (first three inspected; two concerned simulated avatars). Hugging Face model search `durable actor` returned `[]`. These narrow searches do not prove absence of other implementations. The challenger should independently recheck prior-art fit/licences; unrestricted GitHub source search remains outside this writer's authorization.

## Actual implementation seams

1. `ActorManager.create()` currently rejects any runner except `pi`, `pi-durable`, `claude`, resolves models during creation, and generates a random actor ID. `#loadActorsNow()` normalizes other runners to `pi`. Registering a hosted custom runner does not solve either problem.
2. `ask()` uses `#enqueue()` and `#drain()`. The drain performs model binding, Pi session setup and `agents.run()` before constructing an outgoing `FabricActorMessage`. Branch **before all those operations**, not after an empty/fake model run.
3. `#persistQueue()` explicitly excludes items with `resolve` or `reject`. This is useful here: an interrupted ask does not silently become a callerless native activation on restart.
4. Both `AgentMessageRouter.acceptControl` and `ResidentHost.#acceptControl` call `actors.ask(...)` and return `{accepted:true, messageId:result.id, result}`. Preserve that wrapping. Both need the same native admission/context port, not separate interpretations.
5. `FabricControlPlane.#acceptCommand()` consults owned/seen claims and processes cancellation **before** calling the handler. Checking authenticity only inside the callback would permit claim poisoning, unauthorized cancellation and cached-result disclosure.
6. `NatsBridgeCarrier.#inboundCommand()` verifies the credential-bound delivery machine, sender presence, return host and allow-list; its claim already stores `sourceMachine`, `requesterId`, `from`, mapped/source command IDs, target, deadline and owner. The missing piece is a bounded durable read projection usable by another process, with the exact forwarded body binding.
7. `MeshBridge.#outbound()` reconstructs `BridgePublish` and replaces inbound identities from mirrored presence. `StoreBridgeSide.#publication()` reconstructs the final publication; `event-log.ts` reconstructs stored metadata and derives verification from **caller-set** `input.from.verified`. Arbitrary metadata cannot simply be added at the carrier and assumed to survive or be authentic.
8. Resident bootstrap imports custom runner modules only for agents (`runnerModule` / `#recoverHostedRuns`); this is not actor registration. Main runtime composition is `src/fabric-runtime-state.ts`; both actor scopes pass through `ActorDirectory`.

## 1. Concrete host API and persistent actor identity

Add an import-light host subpath `pi-fabric/native-actors` (`src/native-actors.ts`) over `src/actors/native-handler.ts`. It is **not** exported into guest declarations or a `fabric_exec` action.

Proposed API (names are implementation targets):

```ts
interface NativeActorBinding {
  actorId: string;             // exact 32 lowercase hex; supplied deployment pin
  rootId: string;              // exact canonical session:<uuid> of hub service owner
  scope: 'session' | 'project'; // storage scope; residency is always durable
  name: string;
  handlerKey: string;          // e.g. smarty-inbox.rpc.v1; immutable ABI identity
  residentModule: string;      // absolute installed ES-module path, host supplied
  ingress: { link: string; ledgerDir: string }; // absolute trusted link ledger directory
}
interface NativeActorRequest {
  message: string;
  data: unknown;               // immutable JSON snapshot, never identity
}
interface NativeActorContext {
  actorId: string;
  source: 'nats-bridge';
  sender: { id: string; kind: 'main'; sessionId: string };
  sourceMachine: string;
  link: string;
  commandId: string;           // hub mapped control ID
  sourceCommandId: string;     // pod control ID, NOT requestId or opKey
  sourceEventId: string;
  deadlineAt: number;
  signal: AbortSignal;
}
type NativeActorHandler = (
  request: Readonly<NativeActorRequest>,
  context: Readonly<NativeActorContext>,
) => Promise<{ data: unknown; text?: string }>;
registerNativeActorHandler(binding: NativeActorBinding, handler: NativeActorHandler): () => void;
```

Registration is a small process-local map, using the existing runner-registry singleton pattern only to survive duplicate installed module copies. It performs no actor creation, module import, model lookup, database open or background work. Key by the full tuple, not just `handlerKey`. Reject duplicate/conflicting registrations; unregister removes only its own entry and never converts its actor to Pi. Seal a manager's registration snapshot for its lifetime; replacement requires a normal quiescent owner restart, not hot swapping an in-flight callback.

An explicit host extension registers this binding during its own initialization, before Fabric activates the owning runtime. Its installed resident module exports:

```ts
export const handlerKey = 'smarty-inbox.rpc.v1';
export const handle: NativeActorHandler = /* application-owned callback */;
```

The module exports code only; it does not get to select actor IDs, roots, ingress directories or grants in the resident. Resident composition supplies those from its accepted host configuration and verifies the exported key. No wildcard handlers, code strings, dynamic guest import path, registry-driven arbitrary module import, fallback module or filesystem discovery scan.

### Birth, reconnect and registration availability

- Add host-only `ActorManager.ensureNativeActor(binding)` and an `ActorDirectory` scope-aware override. This is not `create()` with a special model or runner flag. Reuse registry custody, name conflict checks, ownership checks, metadata save and participant publication. No generated ID: the deployment supplies `actorId` once.
- Only the matching durable resident owner may materialize the row. Main registration is declarative: `fabric-runtime-state.ts` captures matching bindings and carries their **serializable declarations** in `ResidentHostConfig.nativeActors`, before `ResidencyClient` starts that root. The Main's observational manager must recognize native rows without becoming their executor.
- On first birth, refuse an existing model row with the same ID, an occupied name, another registry/root's same ID, or a different handler binding. Do not convert, delete/recreate or adopt by name. On restart, require exact identity/marker/config agreement; return the existing actor ID. Explicit stopped rows stay stopped.
- Native bindings are hub-file-only. A NATS execution host or a foreign root may display the actor but may not load or execute its handler. Disable generic orphan adoption for native rows. Moving their owner or ID is a separate explicit migration, outside this slice.
- `ResidentHost.start()` loads configured native modules **after owner custody acquisition, before readiness/control start/queue resume**. Only a root with configured native actors pays this load. Use the existing absolute-path dynamic-import technique, but a separate typed native loader, not `loadRunnerModule` or `AgentManager`.
- Accepted configuration includes binding/root/actor/module/ingress pins and participates in existing whole-config wake equality. A resident started without the originating extension must still load the callback from this configuration. Module missing, import failure, wrong key/export, mismatched binding or unsafe configured path blocks native readiness; no automatic alternate generation path.
- `residentModule` points into an immutable installed application generation. Validate absolute regular-file/owned/non-group-or-other-writable module and trusted parent chain under the existing host trust model. Do not claim this defeats same-UID code replacement. Keep the old installed generation available for the existing dormant config; a deployment updates config only through the ordinary quiescent restart route.

### Persisted marker and no downgrade

Store the native row in the existing metadata registry with:

```ts
execution: { kind: 'native', version: 1, handlerKey: string },
runner: 'native',
instructions: null,
residency: 'durable', events: [], topics: [],
delivery: 'mailbox', responseMode: 'text', triggerTurn: false,
coalesce: false
```

`instructions: null` is an intentional fail-closed compatibility guard: the current old loader requires a string and skips such a row before normalizing its runner. Native rows are a typed internal variant, not a fabricated `FabricActorRequest` with empty instructions. Ordinary model-row layout stays unchanged.

Required loader/serializer rules:

- Check for **any** `execution` field or reserved `runner: 'native'` before legacy model defaults. Recognized valid native rows load as native; unknown/malformed execution or inconsistent native sentinels remain quarantined with a diagnostic, never normalize to Pi.
- Recognition does not authorize execution. Exact host registration and matching resident config are separately required. A native row with no handler is unavailable, not model-backed.
- Preserve the execution marker in every owned save, reload and foreign-row merge. Keep quarantined raw rows rather than dropping/replacing them during an unrelated save.
- Host pins reserve the actor ID. Even if its marker disappears, a model-shaped row at a configured native ID is a mismatch and cannot execute. No automatic repair of conflicting bytes.
- `definition()` / export of native actors returns a named unsupported-operation error rather than a model template. `GlobalActorRegistry.create/update/toRequest` and its on-disk loader reject or quarantine any native/unknown execution marker **before whitelist reconstruction**; imports cannot discard the marker and call ordinary create. Check provider input at runtime too, not just TypeScript types.
- Public create/import/setModel/setThinking/setTools/setInstructions/setInferenceContext/setEvents/setTopics/setCoalesceKey/setDeliveryPolicy cannot convert or configure native business execution. Existing stop/status/remove controls may remain, with native custody rules below. No new model-setting capability is advertised for native actors.
- Older binary **execution** on a native-bearing owner is unsupported. Test the pinned old loader's refusal, but still require coordinated root-writer cutover: an old serializer can otherwise discard unknown rows. Rollback removes the bridge allow entry/withdraws native routing and stops the owner; it never interprets the service as Pi. No claim of arbitrary mixed-generation registry safety.

## 2. Authentic ingress without widening the generic bridge wire

### Chosen minimum: ledger-owned receipts, not additional MeshEvent identity fields

Do **not** carry `sourceMachine` as a newly trusted `MeshEvent`/`BridgePublish` field. Instead, extend the existing bridge ledger with an immutable receipt projection, read by the host through its configured `(link, ledgerDir)`.

This directly addresses the reconstruction seams: the existing `data.bridge = {from: link, id: sourceEventId}` and mapped outer `commandId` already survive `MeshBridge.#outbound`, `StoreBridgeSide.checkBridgePublish/#publication` and event-log publication. They are **lookup hints only**. The actual machine/session facts live outside public `MeshStore.publish` state. Generic stdio bridges gain no receipt writer and no power to mint credential-bound context. No bridge protocol-version or pod serialization change is needed.

Add `src/mesh/bridge-nats-ingress.ts` with a bounded read-only verifier and a ledger-owned projection writer. The writer is used only by the real `startNatsLink` composition after `NatsBridgeCarrier`/identity-gate admission. It is not a `MeshStore` method or provider action. Tests inject a recording implementation only at the same native port boundary.

### Receipt contents and ordering

Path: `<configured ledgerDir>/native-ingress/<mappedCommandId>.json`, where the filename grammar is the existing 64-hex mapped ID. Never accept an incoming path. A receipt contains:

- version and link;
- mapped and original command IDs, source envelope ID;
- verified sender ID and canonical session ID;
- credential-bound source machine;
- target ID, owner host/identity, addressed owner incarnation;
- operation, mapped cancellation target when applicable, original return host;
- effective deadline and ledger expiry;
- a canonical SHA-256 binding of the **exact final control semantic body** plus carried principal, outer destination, sender ID/kind and source bridge stamp. This includes nested application data and every retained command field; no selective exclusion of `message`, `args`, bindings, deadlines or return address.

The canonical expected body is constructed from the same pick/mapping used for the emitted to-file event, then the fixed `{bridge:{from:link,id:sourceEventId}}` stamp that MeshBridge will add. Exclude only file-local `id`, sequence, commit time, and display name; those are rewritten/diagnostic, not authority. Use one narrow helper for this canonicalization and its receipt verification, not subtly different producer/consumer recipes. Any future field affecting delivery must enter this binding; unsupported extra outer fields fail native admission.

Order is:

1. Raw NATS delivery machine + live presence + exact allow gate + target and `replyTo` validation.
2. Existing durable ledger claim. For this direction retain the resolved forwarded body/fingerprint as claim evidence as well; this permits same-byte projection repair on a later valid redelivery.
3. Durably publish the immutable receipt (owned 0700 directory, 0600 regular file, no-follow reads/writes, atomic durable publication and parent namespace barrier). An existing different receipt is corruption/conflict, never overwritten. Receipt/ledger write failure prevents event exposure and source ACK.
4. Existing MeshBridge to-file durable append. The receipt may exist before an append; it grants no independent execution. The ordinary actor control command must still be admitted and claimed.
5. Existing source ACK-after-append rule, owner outcome, mapped reply and ACK forwarding stay unchanged.

Receipt creation is idempotent and under the ledger's one-writer custody. Add no second lock owner, RPC or background mirror. Attach expiry/cleanup to existing ledger maintenance; never prune a receipt before its claim horizon. An expired receipt cannot admit new execution. Missing or corrupt evidence is denial; the owner must never rebuild it from a public event. A missing projection may be regenerated only by the ledger-owning bridge on a freshly verified delivery, bound to the original claim's same bytes and identity. Existing pre-upgrade claims without this evidence cannot invoke native handlers.

### Canonical session identity

The native RPC sender must be a Main root with a canonical lowercase `session:<uuid>` ID. Derive `context.sender.sessionId = sender.id.slice('session:'.length)`. Do not use the raw envelope's or presence record's `sessionId` as authority. Actor/agent senders are not admitted for this slice. Preserve compatibility when optional wire identity fields are absent.

Distinguish three cases explicitly; the proof must not misreport safe normalization as packet rejection:

- **Mismatching authoritative assertions are rejected.** A sender lookup ID not verified on the credential-bound delivery machine, a different asserted credential machine, or a receiving event/receipt session inconsistent with its canonical root fails native admission. Modifying data or identity after the receipt binding likewise fails before claim/handler.
- **Optional untrusted envelope identity is ignored, not promoted.** The existing carrier reconstructs `from` from verified presence. Extra `envelope.from.sessionId`, name, kind or purported machine fields are not credentials and need not make an otherwise valid request fail. A test with such extras must observe the actual canonical identity or refusal and zero unauthorized effects; it must not insist that the carrier rejects a packet it safely normalizes. No new fields are required from the older pod.
- **Application identity claims are application input, never context.** Identity/seat fields smuggled into RPC `data` are rejected by the handler's closed application schema or authorization, not claimed as a generic Fabric payload-validation guarantee. Arbitrary legitimate opaque application data is still delivered unchanged.

Carry canonical root derivation into NATS identity/mirror construction (`bridge-nats-identity.ts`, root projection in `bridge-nats-file.ts`/`bridge-nats-presence.ts` as needed). An optional presence `sessionId` is normalized from the verified canonical root ID; it cannot override it. The final authoritative callback/receipt pair must be consistent; the receiving verifier independently derives and checks it. Names are diagnostics only. Tests must poison **presence.sessionId**, not merely the envelope, and show canonical normalization or refusal, never acceptance of the forged session.

### Trust boundary, stated precisely

This protects against model/pod payload forgery, wrong credential machine, ordinary/public mesh publication pretending to be bridge ingress, generic stdio peer metadata, and persisted context-label forgery. `MeshStore.publish` alone cannot create a valid receipt for altered request bytes. Replaying an exact witnessed command can only replay that same authenticated command, with its original return address, through existing control dedupe.

It does **not** cryptographically isolate arbitrary same-UID hub processes, trusted host modules, direct disk writers or holders of the bridge credential. They can read/write the private ledger/receipt files or change trusted code. Filesystem modes, checksums and a WeakMap are not claimed to prevent that. This is the existing A′ same-UID boundary, not a new assertion that local callers are cryptographically authenticated. If #342 requires resistance to hostile same-UID disk writers, this design is blocked: recommend a separately authorized UID/service boundary rather than add an HMAC secret readable by the same attacker.

## 3. Control admission, runtime capability and direct local calls

Add a narrow host-only `nativeActorIngress` port to `FabricControlPlaneOptions`, composed from the actor bindings. It knows all configured/reserved native IDs and persisted native/quarantined rows, including when the callback failed to load.

Its two operations are conceptually:

```ts
requiresNativeAdmission(targetId: string): boolean;
admitNativeControl(event: MeshEvent): NativeControlAdmission; // opaque issued handle
```

`NativeControlAdmission` is process-local and backed by a private WeakMap, binding the complete checked snapshot, exact target and command. It is not JSON, a public field or a TypeScript-only brand. Both host adapters pass this handle separately to `ActorMessageBindingOptions.nativeAdmission`. No exported mint-from-asserted-context helper. The callback gets only a frozen read view plus signal, not the handle or receipt path.

In file `#acceptCommand`, validate native evidence **before** consulting `#ownedCommands`, `#seenRecord`, cancellation, stale-incarnation ACK routing, or attempting the shared claim. Bind cached acceptance to this authenticated input on every replay. This is mandatory even though the callback is also gated. Do not add native logic to the pod NATS sender.

- `ask` with authentic receipt: proceed through existing deadline/incarnation/claim machinery and then existing adapters.
- `cancel`: require its own authentic bridge receipt, matching same requester/machine/target lineage and mapped original command, then reuse the existing active-command abort.
- `steer`/`followUp`/topic-triggered activation of a native actor: refuse, no enqueue. Existing public `tell` is not an alternate native RPC route.
- Unauthenticated event: drop with a bounded diagnostic, no result or refusal to an untrusted `replyTo`, no claim creation, no cancellation. Legitimate bridge pre-publication refusals still use the existing `notRun` ACK path.
- Local `actors.ask`, direct router calls, and local provider `agents.ask` carry **no native admission** and reject native execution. Neither `{verification:'bridge'}` nor copying a prior context/handle-shaped object grants it. This slice deliberately supplies no local identity-emulation API.
- A genuine handle is bound to one queued invocation. Reusing it with different data/target or directly for a second invocation fails. Duplicate control deliveries must take control dedupe, not reuse the handle to enqueue again.
- `commandFromEvent()` must never promote unknown raw fields such as `nativeAdmission`, `nativeContext`, `sourceMachine`, `sender` or `sessionId` to a trusted context. Its current spread is not an authentication mechanism. The control plane attaches the handle out-of-band only after the verifier returns it.

At dequeue the native branch revalidates the issued handle, its exact frozen input, ownership, binding, cancellation and deadline immediately before invoking the callback. Long queue waits cannot revive expired evidence. Do not require the old sender still to have live presence at this later instant: the immutable receipt proves ingress-time identity; policy and revocation of application access belong to the application at effect time.

## 4. Native dispatch, reply and lifecycle custody

Reuse `ActorManager`'s existing serial queue and `#draining`/`#inFlight` ownership, not an independent executor.

1. Native detection occurs before model binding in both creation and ask preflight (`AgentsProvider`, router, resident). Model/thinking/route overrides fail explicitly. An unconfigured native marker must not touch model preparation while producing its refusal.
2. Native admission checks run before enqueue. Require a direct **ask** item with resolve/reject and genuine handle. Hard-code no batching, coalescing, `dedupeKey`, records, validWhile guest evaluation, child-completion subscriptions or model capabilities for native rows.
3. `#drain` keeps the existing ownership/lease/release checks, in-flight registration, AbortController and one-at-a-time drain. Dispatch to a small `#runNativeAsk` branch before Pi session repair, binding, `#runRequest`, model route, `agents.run` or worker launch. An exhaustive branch is required: missing handler/invalid native state throws, never falls through.
4. Await the **actual callback promise**. Record native preparing/running/idle diagnostics using existing actor status, without inventing an AgentManager run ID or fake usage/model metrics. Handler exceptions become bounded control errors, never prompts or retries through a model.
5. Wrap the validated JSON return as an ordinary `FabricActorMessage`: new Fabric message ID, exact actor ID/name, `direction:'out'`, `source:'direct'`, timestamp, optional text, and `data` exactly equal to the callback's response. Existing host adapters/control ledger/ACK carry it unchanged. Enforce current event/ACK size bounds before releasing output; an invalid/oversized callback result is a native error.
6. Do not publish application reply bodies or native identity context on general `fabric.actor.output` or lifecycle broadcasts. Keep direct reply/ordinary private history and bounded diagnostics; mailbox policy means no followUp to the owning Main. This avoids turning one seat's RPC result into ambient model input. Shared on-disk history remains within the stated hub trust boundary.
7. A native promise retains `#draining`, `#inFlight`, AbortController and therefore `inFlightCount`/`releaseCustodyCount` until settlement. Stop, halt, cancellation and deadline abort the signal; abort is not proof the callback stopped. Queued cancellation removes the item before invocation.
8. Cooperative `checkpointForRelease`, owner handover, dormancy and removal cannot report success while a native promise still runs. If existing close grace expires, return a named busy/unsettled native-custody failure; do not clear the drain or release the host fence in that still-live process. No replacement handler/owner may start until actual settlement or existing confirmed whole-process termination. Native callbacks must not detach untracked effects.
9. Callback cancellation cannot undo an already committed transaction. A timeout after commit is an uncertain caller observation; the next application retry uses the same `opKey`. Do not claim `notRun` after callback entry just because its signal was aborted.
10. Native actors have `events:[]`, `topics:[]` forever in this slice. Source subscription setters and registry imports cannot enable host/mesh dispatch. Direct `fabric.steer`, lifecycle delivery, child completion and restored callerless queues for native actors are refused/quarantined before callback or model execution. Normal native actor discovery/participant heartbeat is retained.

### Crash/restart semantics

The actor definition and ID are durable; an ask's JavaScript continuation is not. Keep the existing exclusion of asks from queue snapshots. No trusted context is serialized into a queue, archive or global template. On reconstruction, no callerless native activation is inferred from a saved `provenance`/`sender` label. Any legacy/forged native queue record is quarantined, not replayed.

Control outcomes already saved can be replayed after receipt revalidation. A claim without saved outcome is indeterminate under current control semantics; **do not clear the claim to force a rerun**. A client may issue a new ask (new transport command ID, normally new `requestId`) with the same application `opKey`. The app transaction decides whether it is already done. Restart acceptance must show both stable actor identity and this honest interrupted-request behavior.

## 5. Fabric guarantee versus application responsibility

| Fabric guarantees in this slice | Handler / smarty-inbox must guarantee |
|---|---|
| Exact pinned actor and host-only native registration; no model fallback. | Validate message `smarty-inbox.rpc.v1`, version, operation, requestId, opKey, args and response schema; reject identity-smuggling/unknown fields under its closed schema. |
| Verified ingress Main ID and canonical session ID, source machine and link in out-of-band callback context. | Map the authenticated `(link, sourceMachine, sender.id)` to an authorized seat using application-owned configuration. Never choose a seat from text/data/args/from labels or model claims. |
| Opaque application data snapshot, existing reply wrapper and transport correlation. | Echo the current `requestId` exactly, correlate result to this call, redact errors, authorize each add/list/resolve against the mapped seat, and scope item lookups to that seat. |
| Existing one-active-drain-per-actor and transport claim/ACK replay. | Atomic durable idempotency and one application effect across *different* control commands, process crashes and concurrent retries. |
| No cross-seat return-address rewriting through authenticated replay. | Store/read results only in the authenticated seat's namespace. Equal opKeys from different seats are different operations. |

Required application idempotency contract for the integration fixture and actual service:

- Application identity is `(seat, operation, opKey)` (or the app's existing equally strong documented namespace), **not** `commandId`, actor activation ID or `requestId`.
- In one database transaction, uniquely claim that identity, check a canonical semantic args hash, apply the effect and persist its stable result/item ID. A second retry with the same identity/args returns that result; different args under the same identity is a conflict without another effect.
- Store the stable operation result, not an old full response envelope. Each retry wraps it with its own incoming `requestId`; otherwise a correct dedupe result breaks call correlation.
- List is seat-filtered; resolve checks seat membership before any idempotent success lookup or mutation. Cached results do not bypass authorization.
- If the real effect is outside the transactional store, the handler needs the application's existing outbox or downstream idempotency capability. Fabric does not supply an atomic transaction with that external system.

The Fabric PR may use a small durable transactional **test handler** to prove the boundary without implementing smarty-inbox's production validation or dedupe. A fake handler that increments a JS counter is insufficient for the restart/effect claim. Production application acceptance remains blocked until the actual handler satisfies this contract.

### Distinguish the four IDs

| ID | Lifetime / meaning |
|---|---|
| `sourceCommandId` / mapped `commandId` | One control attempt and its transport redeliveries; bridge maps by verified requester. |
| Fabric reply `id` / activation item ID | Host execution/result bookkeeping; not business identity. |
| Application `requestId` | One application call's correlation; may change on a retry. |
| Application `opKey` | Stable business retry identity, seat-scoped; must survive transport retries and new calls. |

The frozen provider was read with `git show`; the compatibility investigator independently confirms that **each public `agents.ask` invocation generates a new command/idempotency UUID and ignores caller-supplied `idempotencyKey`**. Only its internal dormant-wake resend reuses that attempt's key; `requestResult` returns `acceptance.result`. Therefore repeated calls with the same `opKey` require application deduplication across distinct command IDs. Control dedupe cannot provide that guarantee. Native presence must remain `kind: 'actor'` with the `ask` capability, because the actual frozen provider requires both.

## 6. File-level implementation plan

Keep this a single bounded native feature, with no changes to verified policy kernels or third-party dependencies.

| Files | Concrete delta |
|---|---|
| `src/native-actors.ts` (new), `src/actors/native-handler.ts` (new), `package.json`, `scripts/build.mjs` | Import-light host registration API, frozen exact bindings, native module export contract, stable compiled subpath. No root-entry eager application import. |
| `src/actors/types.ts`, `src/actors/manager.ts`, `src/actors/directory.ts` | Internal native row/marker type; exact-id ensure; discriminate before model paths; ephemeral ask admission; serial native branch; shared release custody; deny subscriptions/adoption/model setters; marker-preserving saves and quarantines. |
| `src/actors/global-registry.ts`, `src/providers/agents-provider.ts` | Reject native/unknown-marker template/import/export downgrade before reconstruction; local native asks refuse without model preparation. |
| `src/fabric-runtime-state.ts`, `src/residency/protocol.ts`, `src/residency/client.ts`, `src/residency/host.ts` | Copy host declarations into validated resident config; load configured module under owner custody before readiness; register both actor scopes; no Main-only closure dependency; native close/release busy fence. |
| `src/mesh/bridge-nats-ingress.ts` (new), `src/mesh/bridge-nats-ledger.ts`, `src/mesh/bridge-nats-types.ts`, `src/mesh/bridge-nats-carrier.ts`, `src/mesh/bridge-nats-link.ts` | Durable receipt projection of genuine admitted claims; retain exact forwarded semantic input; same-byte retry; read-only bounded verifier. No application parsing. |
| `src/mesh/bridge-nats-identity.ts`, root projection in `src/mesh/bridge-nats-file.ts` / `src/mesh/bridge-nats-presence.ts` | Canonical root session derivation and contradictory-session refusal. No promotion of remote metadata. |
| `src/topology/control-plane.ts`, `src/providers/agents-message-router.ts`, `src/residency/host.ts` | Native guard before claims/replay/cancel; opaque admission handle separate from command data; identical host adapters; existing result/ACK semantics. |
| `src/topology/records.ts` (if required by its existing capability builder) | Native actor advertisement: ask/status/lifecycle only, no model bindings or business tell/subscription capability. Do not change the old pod's parser. |
| `src/mesh/bridge.ts`, `src/mesh/event-log.ts`, `src/mesh/store.ts` | **No credential-context widening planned.** Tests exercise their real reconstruction/publication paths. If implementation instead adds metadata, it needs fresh paired review of preservation and transport-source authority; merely copying fields is not this design. |

The application supplies its installed callback module and deployment binding/seat map. Do not put smarty-inbox's application transaction, credentials or seat secrets in Fabric. No additional polling/timers: existing actor/control/bridge events and ledger maintenance drive work.

## 7. Acceptance plan (all unexecuted)

Keep unit tests where they prove a trust boundary or otherwise expensive crash/race path; the decisive test is a real mixed-version request through the user-facing provider. Never run the full test suite.

| ID / proposed evidence | Required allowed and refused behavior |
|---|---|
| N1 `tests/native-actor.test.ts` | Register exact host binding, birth ID, ask through genuine control admission, return existing wrapper/data. Spies **and throw-on-use sentinels** on model resolver/preparation, AgentManager.run/spawn and worker launch remain zero for create, successful ask, rejection, missing callback and restart. Ordinary model actor control still works. |
| N2 `tests/native-actor-registry.test.ts` | Reconstruct ActorManager/ActorDirectory over the same registry: same exact ID/marker. Missing registration/module, malformed/unknown marker, dropped marker at reserved ID, wrong root/handler/scope, attempted ordinary-row conversion all fail closed. Exercise actual owned save/foreign merge plus public definition/export/import/global update; no downgrade to Pi. Load the native row with the pinned old loader as a negative compatibility control. |
| N3 `tests/bridge-nats-native-ingress.test.ts` | Real `NatsCarrierSide → MeshBridge → StoreBridgeSide → MeshStore → FabricControlPlane` path, not hand-authored trusted context. Valid machine/session reaches callback, including absent optional old-pod fields. Wrong credential-bound machine/sender and mismatching authoritative session, target, owner, link, return address, command ID or receipt-bound message/data/args/deadline/principal fail before claim/handler. Extra untrusted envelope fields and poisoned optional presence.sessionId yield canonical normalization or refusal, never forged callback identity. Forged public `MeshStore.publish` verification/context without matching evidence fails. Identity-smuggling RPC data is separately rejected by application schema/authorization. |
| N4 same suite, persisted evidence | Separate bridge and receiver processes/recreated verifier; authentic receipt works after reconstruction. Missing/corrupt/truncated/expired/symlink/foreign-mode receipt denies. Receipt-only/no event executes nothing. Crash after claim/before projection and after projection/before append, then genuine redelivery: no new identity or effect. A generic stdio bridge cannot mint context by copying metadata. Exact replay returns only the original authenticated result/address. |
| N5 `tests/native-actor-custody.test.ts` | Pending native promise with no AgentManager run retains `releaseCustodyCount`; strict release, dormancy, stop/remove/close cannot claim completed custody while it ignores abort. Queued cancel invokes zero callbacks; in-flight cancel observes signal. Callback commit then abort yields uncertain observation, not `notRun`. Handover starts no successor until settlement/confirmed termination. |
| N6 `tests/native-actor-sources.test.ts` | Attempt tell/followUp/steer, topics, host events, lifecycle, child completion, requeue archive, native row with subscriptions and forged saved context. No handler/model entry. Ordinary actor subscriptions remain unaffected. |
| N7 `tests/native-actor-residency.test.ts` + built resident fixture | Actual detached Pi resident bootstrap without the Main extension, configured module load before readiness, reconstructed same actor ID and ask result; wrong/missing module has zero model calls and no native readiness. Dormant/relaunch config pins work or refuse as designed. An interrupted ask is not callerlessly replayed. POSIX flow; Windows durable-launch limitation declared, not counted as pass. |
| N8 `tests/native-actor-pod-compat.test.ts` + candidate canary | Run the **actual provider/router/control sender source or built bundle at `f33937156f587f2479c651ae301de44ec0f0592a`** in a separate process against the candidate link and hub. Real authenticated test broker and actual native resident. Drive public `agents.ask` with the exact frozen wire. Validate `.data` (not an extra wrapper), native context, actor ID, correlation, discovery and cancellation. Handwritten v1 envelope fixtures do not satisfy N8. |
| N9 N8 application fixture | Two configured seats on distinct pinned sessions/machines. Add/list/resolve are seat-scoped; attempt guessed other-seat item ID, equal cross-seat opKey, spoofed identity/seat fields and cached-result fishing. No unauthorized read/mutation/result; authenticated context never becomes payload identity. |
| N10 N8 retries/effects | Inject and count suppression of the **response envelope after durable app commit** so the caller actually misses its ask result. Lost broker `+ACK`/PubAck alone is not this failure. Exercise same-control redelivery separately, then a new actual old-provider `agents.ask` retry with unchanged opKey and a distinct command UUID. Concurrent calls with same seat/operation/opKey and distinct requestIds return one stable application item/result, each echoes its own requestId, durable effect count is exactly one. Restart bridge/actor managers and app fixture before retry; still one. Same key/different args conflicts. Same key/different seat is isolated and independently delivers. |
| N11 startup/build | Cold import/idle plus first-use native module fixture; no application/optional engines pulled into normal eager static closure or idle registration. Run build artifact and lazy graph assertions without raising budgets. |

### Mixed-version fixture custody

The old revision exists in this local Git object database. The parent-provided [compatibility investigation](.local/342-compat-path.md) confirms old/current `bun.lock` bytes are identical and records old provider/router/control/runtime SHA-256 pins. It identifies a viable private-scratch materialization of the old runtime with installed dependencies shared under explicit provenance. This is inspection evidence, **not an executed mixed-version proof**.

The implementer should stage that exact sender bundle/source and required assets into the isolated fixture through an authorized build step, record full SHA and artifact digest, and launch it separately so module resolution cannot accidentally load the candidate provider. Pin the candidate hub/link/resident artifact separately. Use the project's existing broker/resident fixtures where possible. Do not modify the real pod, upgrade its provider, substitute a current sender, or count a source-shape assertion as cross-version execution.

The current `tests/bridge-nats-file.test.ts` proves useful reconstruction/ACK plumbing but constructs envelopes and manually publishes the file ACK; it does not prove N8. Keep/extend those targeted trust-boundary tests and add the actual provider/resident boundary.

Implementation verification, after choosing only affected files:

```sh
bun run typecheck
bunx vitest run tests/native-actor.test.ts tests/native-actor-registry.test.ts tests/bridge-nats-native-ingress.test.ts tests/native-actor-custody.test.ts tests/native-actor-sources.test.ts
bunx vitest run tests/native-actor-residency.test.ts tests/native-actor-pod-compat.test.ts
bun run assert:lazy-graph
bun run build
```

These new paths are proposed, not present or passing. Required broker/old-bundle/resident prerequisites must be explicit in CI; absence is blocked or an expected skipped platform coverage report, never a successful compatibility receipt. The lead owns any test that launches/terminates processes or performs fixture deletion under the applicable hosted-test rules.

## 8. Failure table and rollout

| Condition | Behavior |
|---|---|
| Native ID known, no matching host registration/module/config | Unavailable, zero model work. Preserve row and diagnostic. |
| Authentic command, owner incarnation changed | Existing stale-incarnation refusal; no callback. Existing permitted dormant retry only; no new automatic effect retry. |
| Forged public mesh event/context, missing receipt, stdio metadata | Drop before claim/replay/cancel; no result disclosed and no native/model invocation. |
| Application rejects schema/seat/opKey conflict | Application `ok:false` reply, current requestId, no forbidden effect. |
| Callback committed, ACK lost | Existing transport replay if possible; otherwise new ask with same application opKey returns stable application result. |
| Crash between callback effect and saved control outcome | Control outcome indeterminate, not exactly-once inference; app dedupe prevents repeat effect on explicit retry. |
| Callback ignores cancellation | Keep release custody; fail busy rather than launch a replacement concurrently. |
| Old binary / imported native marker | Non-model sentinel + explicit import refusal; coordinated deployment/rollback, never automatic model downgrade. |

Roll out on the hub only: install candidate Fabric and immutable application module; quiesce that root's old writers; configure exact actor/root/module/ingress pins; start resident and read back marker and ID; add the exact actor to the existing link allow-list with ask/cancel and the already-authorized sender seats; run N8–N10 with test data. Do not widen seat destination classes or principal carry rights. Rollback withdraws the allow entry/route and stops the native owner under existing custody before replacing code. Preserve app dedupe records, actor identity and evidence; the pod remains frozen.

## 9. Open issues and challenge targets

1. **External-lead-owned challenge pending; file complete.** Parent commits and sends full artifact plus head to lead `fabric-store` now, before challenge. Another lead briefs Opus 5.5 xhigh and returns findings; this lane neither spawns nor briefs it. Findings receive at most two resolution rounds. Recommendation: challenge receipt authenticity/replay, no-downgrade paths and non-cooperative callback release first. Pending review is not missing design content or a reason to withhold the file; this artifact is complete but not yet independently approved.
2. **Production pins not supplied.** Exact pinned actor ID, owning hub root, installed application module and allowed seat/machine bindings must come from the existing #342 deployment record. Recommendation: require explicit pins before implementation canary; do not invent or discover them by name. Their absence does not block the API/test design.
3. **Application transaction is outside Fabric.** Production schema/seat authorization and opKey/effect transaction have not been inspected in this bounded task. Recommendation: application owner supplies that implementation and evidence; run N9/N10 against it before claiming end-to-end one-effect delivery. A Fabric fixture proves the boundary only.
4. **Threat scope needs confirmation in challenge, not silent widening.** Receipt files rely on existing trusted same-UID hub storage. Recommendation: retain the documented A′ boundary. If the principal requirement includes hostile same-UID hub writers, stop this slice for a separately scoped isolation design; no hash, metadata brand or same-UID-readable secret can honestly close it.
5. **No cross-version or built resident proof yet.** The old provider/control source has been read and the commit resolved, but not executed against new code (none exists). Recommendation: N8 is a release gate, not optional follow-up.
6. **Prior-art search restriction.** GitHub source searches were prohibited; external documentation/licence checks above are narrower. Recommendation: challenger rechecks adequate simpler fits within its own authorization; do not describe this as an exhaustive ecosystem search.

No unresolved choice is delegated to a principal without a recommendation. No new spending, access grant, credential, external dependency or production effect is authorized by this document.
