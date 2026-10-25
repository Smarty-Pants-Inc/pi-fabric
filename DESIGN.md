DESIGN (design-pair, challenge record: [.local/342-challenge-r1.md](.local/342-challenge-r1.md), source `/home/paul/lanes/fs-342hub/.local/342-challenge-r1.md`, SHA-256 `da4b1118eb5753b4bcc6aee611c920b27995dd1ec0b9ad16d45d640e49e2be3c`; status: R1 response, pending external recheck)

R1 response by the Astra xhigh **designer**, not an approval or judge verdict. The independent challenge contains B1/B2, N1–N5 and Q1/Q2; authoritative lead rulings are recorded in [.local/342-lead-r1.md](.local/342-lead-r1.md). Decisions and affected scope are revised throughout below. The parent commits the revised artifact and sends it with the pinned challenge to lead `fabric-store`, who owns the external round-2 challenge (maximum two rounds total). This lane neither spawns nor briefs a challenger. No deployment, service restart, credential change or native integration proof is performed by this revision. Parent integration after the Astra handback incorporates the lead's later canonical application SID correction, selected R2 schedule and synchronous-lock timer requirement, plus the consumed ingress implementation's exact data-copy inventory; the final artifact therefore differs from the designer handback hash.

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

Original design inspection baseline: hub source `7ac1f5dcfc2d1b624657bbc5fa0114a31f99f155`, **not** installed hub `ca4b1482119748394df5125c2a8d2853676f7e04`. The pinned challenge establishes that the former is 21 commits later, that the four cited bridge gate files are identical, and that the relevant session/project dormancy rule is unchanged; it does not establish whole-runtime equivalence. Frozen pod: `f33937156f587f2479c651ae301de44ec0f0592a`. Pod and installed hub **diverged**, sharing `7c8358ff8afb6c16ba7d5acdb3bb584c12804389`; the pod is not an ancestor of the hub (challenge records 32 pod-only / 354 hub-only commits). Revision grant/inspection HEAD: `f78625d137230b691ef837a484a89613d3d45a7f`.

Main reports a committed frozen-client helper and transactional fixture, 11 passing fixture tests, two passing actual-old-runtime smoke tests, typecheck and a fresh build in [.local/342-handoff.md](.local/342-handoff.md). Those receipts predate native source integration. Main's later R1 update reports 46 passing native-template guard tests (canonical job `tmp.axLyyAfsBu`), and the API writer's completed/consumed handback with its focused check still running at that update. These are bounded source-task results; no integrated native execution is claimed. True native N7/N8, production application effects and security approval remain unproved.

The named `src/mesh/types.ts` does not exist at this head. Actual event types are in [`src/mesh/event-log.ts`](src/mesh/event-log.ts), re-exported by [`store.ts`](src/mesh/store.ts); bridge contracts are in [`bridge-nats-types.ts`](src/mesh/bridge-nats-types.ts).

## Decision

Add an **ask-only, host-registered native execution mode to the existing durable ActorManager**. Bind the handler to an exact `(rootId, scope, actorId, handlerKey)`; never select it by a message string, actor name, arbitrary runner, or caller-supplied module path.

For this slice:

- The execution owner is the hub's existing file-backed ResidentHost, under **one supervised, headless, no-model actual Pi Main on Ryzen 1**, with **`scope: 'session'` only and `residency: 'durable'`**. Storage scope and execution residency are different axes. Project-scoped native actors, NATS-host execution, subscriptions, directives and native task runners are out.
- Production actor ID stays **`5027cfd59b1247b6bb4666970c481cc4`**. Its owning root is the canonical ID of a persisted native Pi session, never a newly generated per-boot identity.
- The native actor is discoverable and remotely addressable through the existing actor participant and `agents.ask` path. No new provider, channel, business endpoint, delivery loop, dependency or model turn. One supervised launcher keeps the real owner Main alive; it is not another actor runtime or transport daemon.
- Keep the A′ live-root gate unchanged. The bridge cannot wake a dormant file owner and gains no wake path. Root down means refusal even if its resident remains alive; session scope prevents ordinary native idle dormancy, not root-gate enforcement.
- The bridge writes a bounded immutable ingress receipt beside its existing ledger. The receiving control plane reads the receipt through a **host-pinned reader** before claims, cache replay or cancellation. The callback context is reconstructed from this evidence, not from `event.from.verified`, `verification`, `sender`, `principal`, `data`, or a serialized context object.
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

**Q1 settled by the lead:** snooze is OUT; the round-2 application cut and frozen add/list/resolve wire supersede the original issue goal for this delivery. No identity fields, handler key, module, ingress receipt, source machine or seat selector are added to this request. No model/thinking override is required. The `status` vocabulary and application result/error schemas belong to smarty-inbox; Fabric must not invent them. Application failures are successful transport replies containing `ok: false`. Pre-handler transport/native-admission failures reject the ask by the existing control failure path, not a fabricated application envelope.

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
  scope: 'session';             // ONLY this root's registry; residency is durable
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

**Application resident-module adapter (lead updates, 2026-10-25 16:21–16:35Z).** The application handler is [`Smarty-Pants-Inc/smarty-inbox` PR #344](https://github.com/Smarty-Pants-Inc/smarty-inbox/pull/344), full source commit **`1dc71f70610bc5ec69a1b80df0bcd4170b12ab7f`**, `packages/inbox/src/rpc-handler.ts`. Main verified the staged full-repository archive SHA-256 **`fdae85c6dae9f9e152b0fc34447add713d00c4491277995947a15f943eea8f82`**, checked its native Git tar commit ID, safely materialized it into private scratch, and read the actual handler constructor, caller validator and its test-owned-home/store setup. `createInboxRpcHandler()` returns `(trusted: {sid, machine}, data, signal?) => Promise<replyData>`. It has not been executed by this lane. This pin still contains the synchronous lock; the promised async-lock revision requires its own new source/artifact pin before canary. The constructor reads canonical installed home/config, not a request-selected store; executable fixture setup must follow its isolated-process tests, never invoke an uninspected default store. The adapter belongs to the installed application module, not Fabric's generic library:

```ts
// Application module imports its generation-pinned createInboxRpcHandler export.
const rpc = createInboxRpcHandler(); // application owns store/configuration setup
export const handlerKey = 'smarty-inbox.rpc.v1';
export const handle: NativeActorHandler = async (request, ctx) => {
  if (request.message !== 'smarty-inbox.rpc.v1') {
    throw new Error('Unsupported native RPC message');
  }
  return { data: await rpc(
    { sid: ctx.sender.id, machine: ctx.sourceMachine },
    request.data,
    ctx.signal,
  ) };
};
```

**Corrected contract (lead update 2026-10-25 16:27:30Z):** application `sid` MUST be **`session:<uuid>`**, from verified **`ctx.sender.id`**. The principal-lead reports `rpc-caller.ts:25/:38` rejects other forms. `ctx.sender.sessionId` remains the bare UUID in Fabric's generic context, but the adapter MUST NOT pass it as the application's SID. Add an adapter test asserting the exact prefixed ID is passed and the bare UUID is never passed. Never use `request.data.sid`, an environment seat label, or a caller-supplied context. The actor ID is not compiled into this adapter/handler. Prefer this actual, source-pinned export for N9/N10 once its complete runtime inputs are staged and its store is explicitly confined to test-owned temporary state; do not invoke an uninspected default store or describe the existing SQLite test fixture as production-handler proof.

### B1/B2: real owner Main, stable session, supervised restart and idle behavior

**B1 answer:** provide the missing actual Pi Main; do not treat `resident:<digest>` (`kind: agent`) as a Main. **B2 answer:** choose session scope, not a dormancy exemption or a bridge wake change. `ActorManager.#dormantIdleCandidate()` immediately returns false for `#actorScope === 'session'`; the resident therefore keeps this idle durable actor active instead of making it dormant and releasing on the 10-minute default `residentIdleExitMs`. `reconcileSessionOrphans()` does not substitute for the live Main. Prove this with a real bridged ask after more than the configured idle-exit interval, not just a registry inspection.

**Inspected Pi bootstrap (not a new/fake Main implementation).** Pi documentation was read at release `031c749962972b0cb966e443e921d64654627b20`, under `node/node_modules/@earendil-works/pi-coding-agent/`: `README.md`, `docs/sdk.md`, `docs/extensions.md`, `docs/cli.md`, `docs/cli-integration.md`, `docs/rpc.md`, `docs/sessions.md`, `docs/session-format.md`, `docs/input-admission.md`, and SDK examples `06-extensions.ts`, `11-sessions.ts`, `12-full-control.ts`. Exact implementation checks use that release's `dist/core/session-manager.js`, `dist/core/sdk.js`, `dist/core/model-resolver.js`, `dist/main.js` and `dist/modes/rpc/rpc-mode.js`.

- Do **not** specify bare `pi --mode rpc` with no model and assume it starts: this release's `main.js` exits for a noninteractive session without `session.model`. Nor invent a dummy provider/assistant transcript to satisfy that check.
- Reuse Fabric's already-built [SDK/RPC entry](src/worker/task-entry.ts), `dist/worker/task-entry.js` (existing build entry/artifact assertion). It imports the **pinned Pi binary's SDK**, calls native `createSessionManager`, `createAgentSessionServices`, `createAgentSessionFromServices`, `createAgentSessionRuntime`, then `runRpcMode`. It does not perform the CLI's model-required exit check. Native SDK construction permits `model: undefined` and sets thinking off when no authenticated model is available. This is source-supported, **not yet an executed no-credential service proof**.
- The installed, generation-pinned `inbox-owner-pi` executable is only an argv adapter: `exec <pinned-node> <fabric-generation>/dist/worker/task-entry.js <pinned-pi-package>/dist 1 "$@"`. No task/parent environment is inherited, no terminal-task profile is set, no provider extension is loaded. It is also the explicit `PI_FABRIC_PI_BINARY` for this owner's resident: otherwise the existing resident launcher would hit the same no-model CLI check. The resident continues to run its real `residency/pi-entry.js` through the existing launcher/owner-custody protocol, not a handcrafted `ResidentHost` in the Main.

**Native session provisioning and pinning, once at authorized installation:**

1. Choose one private service state directory and service working directory on Ryzen 1; validate ownership/modes, exclusive launcher custody and immutable code paths. Reuse an explicitly assigned **service** session if supplied, never discover/adopt a principal's session by name. Validate its native header/root and use that same file.
2. For a new service only, exclusively create an empty regular file `owner.jsonl` (0600), then call the pinned SDK's `SessionManager.open(ownerFile, sessionDir, serviceCwd)`. The inspected `_setSessionFile()` branch for an existing empty file calls native `newSession()` and `_rewriteFile()`, persisting the real native header even without a user/model message. The launcher writes **no session JSON or fabricated transcript entries**. Do not rely on `SessionManager.create()` plus `appendCustomEntry()` alone: ordinary metadata-only sessions defer their first flush; `persistCurrent()` is owned-journal-only, not a generic force-flush API.
3. Fsync the resulting regular file and directory, reopen through `SessionManager.open`, and verify a canonical UUID. Atomically persist the **configuration** pin `{sessionFile, rootId: 'session:' + getSessionId(), actorId}`. Only after this readback may native config/registration be exposed. On a bootstrap crash before the pin, an existing valid native file is reused, not overwritten. Once the pin exists, a missing/empty/malformed/wrong-ID file refuses startup; never create a replacement identity automatically. No `--no-session`, `--continue`, `--fork` or per-boot `--session-id` for the Main.

**Concrete launcher/supervision contract (implementation/deployment target, not installed here):**

- A thin Node launcher under one systemd service owns the child and its stdin. It starts `inbox-owner-pi --mode rpc --session <absolute-owner.jsonl> --no-tools --no-extensions --no-skills --no-prompt-templates --no-context-files --extension <pinned-owner-bootstrap> --extension <pinned-fabric-dist/index.js>` in a service cwd with **no trust-gated project resources**; all required configuration lives in the explicit isolated profile/pins. The inspected task entry has its own trust resolver, so do not assume the CLI's `--no-approve` implementation applies there. Unexpected project resources refuse the launcher preflight. It holds the pipe open, continuously drains RPC stdout and bounded stderr, correlates only read-only health/state commands, and exposes no arbitrary RPC command socket. **EOF requests native shutdown**; `/dev/null` stdin is not a persistence mechanism.
- The bootstrap factory validates the persisted root/actor/module/link pins and registers the binding **before any Fabric `session_start`/runtime activation**; the native module is loaded in the resident under custody before native readiness. The owner profile explicitly supplies file-mesh configuration with `mesh.actorScope: 'session'`, and the binding independently requires `scope: 'session'`; all required pins survive the serialized resident config. Runtime readiness requires exact root/actor/config readback, no selected model, resident readiness and live root+actor observations, not merely a child PID or an RPC `get_state` response.
- Child environment is a small explicit allow-list of host essentials and owner-specific mesh/profile/binary pins, not `...process.env`. Use an isolated `PI_CODING_AGENT_DIR` with no provider auth/model configuration, no credential-loader wrapper, no provider API keys, no ambient task/parent/session-ID override, no provider extensions, and `PI_OFFLINE=1`. The file-side owner/resident needs **no NATS or model-provider credentials**; the separate existing link retains its already-authorized broker credential. Never print inherited environment/auth contents. Unexpected model selection fails readiness rather than creating a service dependency on it.
- No initial prompt, RPC prompt/steer/follow_up, model tool run, root message handler or model warm-up. The bootstrap's input hook consumes/refuses unexpected user input without a turn (do not depend on a thrown input-hook error, which Pi may isolate); no root prompt is forwarded to the application. Only the exact actor is bridge-addressable for this RPC. Mailbox delivery, no native ambient input/output, disabled discovered extensions and zero credentials keep normal owner maintenance model-free. Tests must attempt root input and show zero model/provider calls, not merely an idle happy path.
- Follow the existing reviewed factory-host supervision shape in `/home/paul/.local/share/smarty-dev/factory/current/setup/systemd/smarty-factory-host@.service`: held-open RPC stdin and systemd restart. Do **not** copy its `--no-session`, model pin or shared credential profile. Target unit: dedicated owner instance, `UMask=0077` (including native session temporary files), `Restart=always`, `RestartSec=5`, bounded startup/stop timeouts, `KillMode=control-group`; enabled for boot through the host's user-service target with verified lingering (or the approved equivalent host supervisor). A user unit without boot activation is not a reboot solution. Installation owner verifies this on 10-26 after merge; no systemd/linger changes now.
- On launcher shutdown, close stdin and join native shutdown; on timeout, confirm whole-process termination before allowing a successor. Existing resident release/owner fences remain authoritative, including a detached resident surviving the Main. A timeout is not a clean-release receipt. On Main crash, systemd restarts the **same session file/root**; existing residency reconnect/custody recovers or refuses overlap. A full reboot restarts this same unit/config and its resident, not a new session. Missing configuration or old writer custody remains unavailable, never auto-repaired by changing IDs.

The isolated root-down acceptance test holds a test-only restart barrier in its launcher (so automatic restart cannot hide the negative), first settles prior asks, observes the root as non-live under the existing gate (including its ordinary stale-observation horizon), keeps the resident alive, then issues an actual old-provider ask: **definite refusal and +0 callbacks**. Restart the actual Main over the same native file, read back the same root and `5027cfd59b1247b6bb4666970c481cc4`, then require a successful reply. Previously admitted work is not retroactively revoked by this ingress gate; do not call that a new post-root-down admission.

### Birth, reconnect and registration availability

- Add host-only `ActorManager.ensureNativeActor(binding)` and an `ActorDirectory` override restricted to its **session** manager. This is not `create()` with a special model or runner flag. Reuse registry custody, name conflict checks, ownership checks, metadata save and participant publication. No generated ID: the deployment supplies `actorId` once.
- Only the matching durable resident owner may materialize the row. Main registration is declarative: `fabric-runtime-state.ts` captures matching bindings and carries their **serializable declarations** in `ResidentHostConfig.nativeActors`, before `ResidencyClient` starts that root. The Main's observational manager must recognize native rows without becoming their executor.
- On first birth, refuse an existing model row with the same ID, an occupied name, another registry/root's same ID, or a different handler binding. Do not convert, delete/recreate or adopt by name. On restart, require exact identity/marker/config agreement; return the existing actor ID. Explicit stopped rows stay stopped.
- Native bindings are hub-file-only and session-scope-only. A NATS execution host, project manager or foreign root may display the actor but may not load or execute its handler. Disable generic orphan adoption for native rows. Moving their owner or ID is a separate explicit migration, outside this slice.
- `ResidentHost.start()` loads configured native modules **after owner custody acquisition, before readiness/control start/queue resume**. Only a root with configured native actors pays this load. Use the existing absolute-path dynamic-import technique, but a separate typed native loader, not `loadRunnerModule` or `AgentManager`.
- Accepted configuration includes binding/root/actor/module/ingress pins and participates in existing whole-config wake equality. A resident started without the originating extension must still load the callback from this configuration. Module missing, import failure, wrong key/export, mismatched binding or unsafe configured path blocks native readiness; no automatic alternate generation path.
- `residentModule` points into an immutable installed application generation. Validate absolute regular-file/owned/non-group-or-other-writable module and trusted parent chain under the existing host trust model. Do not claim this defeats same-UID code replacement. Keep the old installed generation available for the persisted resident config; a deployment updates config only through the ordinary quiescent restart route. No native dormant-wake behavior is introduced.

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
- Older binary **execution** on a native-bearing owner is unsupported. Test the pinned old loader's refusal, but still require coordinated **SESSION-root writer cutover**: quiesce every old Main/resident/other writer of this one root's `meshRoot/actors/<fabricSessionId>` registry before exposing a native row. ActorDirectory still contains ordinary project machinery, but this native row is never put in its shared project registry; no fleet-wide/project-shared registry safety is claimed or required for this cut. An old serializer for this session can still discard unknown rows. Rollback withdraws the bridge allow entry/route and stops the owner under custody; it never interprets the service as Pi or reopens this native-bearing session with an old writer.

## 2. Authentic ingress without widening the generic bridge wire

### Chosen minimum: ledger-owned receipts, not additional MeshEvent identity fields

Do **not** carry `sourceMachine` as a newly trusted `MeshEvent`/`BridgePublish` field. Instead, extend the existing bridge ledger with an immutable receipt projection, read by the host through its configured `(link, ledgerDir)`.

This directly addresses the reconstruction seams: the existing `data.bridge = {from: link, id: sourceEventId}` and mapped outer `commandId` already survive `MeshBridge.#outbound`, `StoreBridgeSide.checkBridgePublish/#publication` and event-log publication. They are **lookup hints only**. The actual machine/session facts live outside public `MeshStore.publish` state. Generic stdio bridges gain no receipt writer and no power to mint credential-bound context. Main's additional source check resolves the stamp assumption for **this composition**: `MeshBridge.#outbound` uses `options.remoteName` and `event.id` for inbound/toLocal `stamp.from/id`, and `createNatsFileLink` sets `remoteName: options.link` (`src/mesh/bridge-nats-file.ts`). Do not generalize that equality to every bridge. No bridge protocol-version or pod serialization change is needed.

Add `src/mesh/bridge-nats-ingress.ts` with a bounded read-only verifier and a ledger-owned projection writer. The writer is used only by the real `startNatsLink` composition after `NatsBridgeCarrier`/identity-gate admission. It is not a `MeshStore` method or provider action. Tests inject a recording implementation only at the same native port boundary.

### Receipt contents and ordering

Path: `<configured ledgerDir>/native-ingress/<mappedCommandId>.json`, where the filename grammar is the existing 64-hex mapped ID. Never accept an incoming path. A receipt contains:

- version and link;
- mapped and original command IDs, source envelope ID;
- verified sender ID and canonical session ID;
- credential-bound source machine;
- target ID, owner host/identity, addressed owner incarnation. The actor's root remains `binding.rootId`; its owner host **and** owner identity are the existing deterministic `resident:${sha256(rootId).slice(0,24)}`, not the session root itself (`src/residency/protocol.ts`, `src/residency/actor-ownership.ts`, and `ResidentHost` participant publication). Bind the exact final `event.to` as well;
- operation, mapped cancellation target when applicable, original return host;
- effective deadline and ledger expiry;
- a canonical SHA-256 binding of the bounded **exact final forwarded control semantic body**, carried principal, outer destination, sender ID/kind and source bridge stamp. This includes nested application data and every retained command field; no selective exclusion of `message`, `args`, bindings, deadlines or return address. The complete resolved forwarded body is retained once in the private ledger for durable repair. The immutable receipt projection stores routing/identity metadata and semantic/integrity digests, **not another application-body copy**. Its private namespace is the authority boundary; a checksum is corruption detection, not independent origin authentication.

The canonical expected body is constructed from the same pick/mapping used for the emitted to-file event, then the fixed `{bridge:{from:link,id:sourceEventId}}` stamp that MeshBridge will add. The final event also contains `MeshStore.publish()`'s process-local **`sender` authority stamp** (`src/mesh/store.ts:578`): bind it using the same publishing process's `processSender()`, including its complete JSON semantics. It is not remote machine/session evidence. Do not ignore it, accept arbitrary replacement stamps, or reject every legitimate stored event merely because the carrier's pre-publication event lacked it; the persisted positive case and changed/missing-stamp counterexamples must pass. Exclude only file-local `id`, sequence, commit time, and display name; those are rewritten/diagnostic, not authority. Use one narrow helper for this canonicalization and its receipt verification, not subtly different producer/consumer recipes. Any future field affecting delivery must enter this binding; unsupported extra outer fields fail native admission.

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

**Q2 settled by the lead:** accepted caller scope is the existing per-machine sender pin + effective command deadline + item-18 distinct `org-kate`/`org-paul` machines and Secrets. Path B is a **credential-machine boundary**, not cryptographic per-session authentication: a holder of a machine's credential can assert that machine's pinned session presence. We do not claim to distinguish two hostile credential holders on that machine. No sender-source epoch, per-session key, new auth transport or auth redesign is added. Deployment must meet item 18; a shared `sandbox1` credential is not an accepted seat.

The command deadline is **not a short session-freshness proof**. Main inspected the actual frozen `f3393715:src/config.ts`: `MAX_AGENT_TIMEOUT_MS = 24 * 3_600_000`, the default equals that maximum, and `agents.timeoutMs` uses that default (lines 958/964/1062 at the frozen revision). Existing control code clamps the effective window; this slice carries/rechecks that effective deadline unchanged. No live seat timeout setting was inspected or inferred. Identity is ingress-time presence on the credential-bound machine; effect-time application authorization remains separate. No old-pod timeout/config change or re-pin is proposed.

This protects against model/pod payload forgery, wrong credential machine, ordinary/public mesh publication pretending to be bridge ingress, generic stdio peer metadata, and persisted context-label forgery. `MeshStore.publish` alone cannot create a valid receipt for altered request bytes. Replaying an exact witnessed command can only replay that same authenticated command, with its original return address, through existing control dedupe.

It does **not** cryptographically isolate arbitrary same-UID hub processes, trusted host modules or direct disk writers: those principals can read/write private ledger/receipt files or change trusted code. Credential holders can assert the identities allowed to their machine as described above; holding a remote machine credential is not, by itself, a grant of hub disk access. Filesystem modes, checksums and a WeakMap are not claimed to prevent same-UID code/disk forgery or credential-machine impersonation. This is the existing A′ same-UID boundary, not a new assertion that local callers are cryptographically authenticated. If #342 requires resistance to hostile same-UID disk writers, this design is blocked: recommend a separately authorized UID/service boundary rather than add an HMAC secret readable by the same attacker.

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
6. Suppress **both** `fabric.actor.input` (currently emitted by `ask()`) **and** `fabric.actor.output`, plus body-bearing lifecycle notifications, for native rows. Keep the direct response intact, but retain **no native request/reply body in actor history or archives**: sanitize the native history copy before `#logs.recordMessage` **and** `#unarchivedMessages`, including errors/drop/stop paths. Retain only message/actor IDs, timestamp, direction and bounded static outcome diagnostics—no text preview, data, sender context, args or result. Do not mutate/truncate the direct reply wrapper to achieve this. Mailbox policy means no followUp to Main. See the retention inventory below; control/transport copies remain and are not erased by this choice.
7. A native promise retains `#draining`, `#inFlight`, AbortController and therefore `inFlightCount`/`releaseCustodyCount` until settlement. Stop, halt, cancellation and deadline abort the signal; abort is not proof the callback stopped. Queued cancellation removes the item before invocation.
8. Cooperative `checkpointForRelease`, owner handover, close and removal cannot report success while a native promise still runs. Ordinary native idle dormancy is unavailable because scope is session. If existing close grace expires, return a named busy/unsettled native-custody failure; do not clear the drain or release the host fence in that still-live process. No replacement handler/owner may start until actual settlement or existing confirmed whole-process termination. Native callbacks must not detach untracked effects.
9. Callback cancellation cannot undo an already committed transaction. A timeout after commit is an uncertain caller observation; the next application retry uses the same `opKey`. Do not claim `notRun` after callback entry just because its signal was aborted.
10. Native actors have `events:[]`, `topics:[]` forever in this slice. Source subscription setters and registry imports cannot enable host/mesh dispatch. Direct `fabric.steer`, lifecycle delivery, child completion and restored callerless queues for native actors are refused/quarantined before callback or model execution. Normal native actor discovery/participant heartbeat is retained.

### N1/N2: body-copy and retention inventory

- **Native actor input/output notifications:** omitted, not broadcast. No body/context goes into ambient actor or lifecycle topics. This is an implementation requirement, not an existing default.
- **Actor history/archives:** metadata-only for native rows. Inspection of `src/actors/log-store.ts` shows a 100-message active ring (`ACTOR_MESSAGE_HISTORY_LIMIT = 100`), not an age TTL; `ActorManager.#recordMessage` also retains pending archive copies. Existing run/archive retention settings (`actorRunArchiveMs`, `terminalRunEventsAgeMs`, `terminalRunEventsMaxBytes`) are not a per-native-reply no-history switch. Use the native branch/sanitizer rather than inventing such a knob, a seven-day history guarantee or a new deletion policy. No native model session/run transcript is created. Ordinary actors remain unchanged; no historical deletion is authorized.
- **Hub control/file mesh, native ingress receipts and bridge ledger:** request control events, the ledger's additional complete incoming `toFile` resolved-body evidence and saved control outcomes/ACKs contain application bodies. Include that **ledger body copy** in #342's inventory, not just the application DB and broker. The immutable native receipt contains routing/identity metadata and full-semantic/integrity digests, **not another application-body copy**; its identity/correlation metadata still needs the same private custody/retention accounting. Receipt cleanup follows claim custody: existing `BRIDGE_NATS_HORIZON_MS = 24 * 60 * 60 * 1_000`, with claim expiry `max(deadlineAt, claimedAt) + horizon`; this can extend well beyond ingress and is not a short freshness TTL. Existing transport/ledger retention and access rules continue. Expiry denies new admission and permits normal maintenance; it does not certify immediate deletion of every file/archive copy. Metadata-only actor history removes an additional avoidable copy, not these required control/evidence copies.
- **Frozen pod broker inbox:** at `f33937156f587f2479c651ae301de44ec0f0592a`, `src/transport/nats/inbox.ts` pins inbox `maxAgeMs: 0`; `provision.ts` asserts exact stream policy with Limits retention. There is **no age bound** on replies. Inspected source default byte cap is **1,073,741,824 bytes (1 GiB) per machine inbox stream**, `DEFAULT_MAX_BYTES`; the normal runtime config branch uses that value (direct-options callers can specify a cap). A consumed/ACKed reply is not thereby removed under Limits retention. `org-kate`/`org-paul` inbox bodies may remain until byte eviction. Do not substitute the later hub's seven-day default. Live broker settings/replicas still require readback.
- **Decision/limit:** keep the frozen pod, **no re-pin** and no finite-age stream change it rejects. This corrects the retention fact; it is **not principal approval of sensitive/client-data retention** and does not settle the application's pending data-scope decision. Test canaries use approved synthetic data until that separate approval/readback exists. Native metadata-only history reduces an avoidable hub copy; it cannot make the broker age-bounded.

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
- List/resolve enforce the application's existing native caller and visibility policy before any idempotent success lookup or mutation. In the inspected real handler, approved peer cross-reads are part of native policy (`crossRead` may narrow them); isolation means **no unauthorized** read/mutation/replay disclosure, not a new Fabric-imposed ban on authorized shared views. Other-seat resolve probes must use genuinely unauthorized items, not the caller's permitted sent/delegated asks. Cached results never bypass authorization.
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
| `src/fabric-runtime-state.ts`, `src/residency/protocol.ts`, `src/residency/client.ts`, `src/residency/host.ts` | Copy session-only declarations into validated resident config; load configured module under owner custody before readiness; ensure only the native session row; no Main-only closure dependency; native close/release busy fence. No project-native support or bridge wake. |
| `src/mesh/bridge-nats-ingress.ts` (new), `src/mesh/bridge-nats-ledger.ts`, `src/mesh/bridge-nats-types.ts`, `src/mesh/bridge-nats-carrier.ts`, `src/mesh/bridge-nats-link.ts` | Durable receipt projection of genuine admitted claims; retain exact forwarded semantic input; same-byte retry; read-only bounded verifier. No application parsing. |
| `src/mesh/bridge-nats-identity.ts`, root projection in `src/mesh/bridge-nats-file.ts` / `src/mesh/bridge-nats-presence.ts` | Canonical root session derivation and contradictory-session refusal. No promotion of remote metadata. |
| `src/topology/control-plane.ts`, `src/providers/agents-message-router.ts`, `src/residency/host.ts` | Native guard before claims/replay/cancel; opaque admission handle separate from command data; identical host adapters; existing result/ACK semantics. |
| `src/topology/records.ts` (if required by its existing capability builder) | Native actor advertisement: ask/status/lifecycle only, no model bindings or business tell/subscription capability. Do not change the old pod's parser. |
| `src/mesh/bridge.ts`, `src/mesh/event-log.ts`, `src/mesh/store.ts` | **No credential-context widening planned.** Tests exercise their real reconstruction/publication paths. If implementation instead adds metadata, it needs fresh paired review of preservation and transport-source authority; merely copying fields is not this design. |

The application/deployment owner supplies the installed callback module, binding/seat map, owner-bootstrap extension and thin supervised launcher/argv adapter described above. Reuse `dist/worker/task-entry.js`; do not add another Main implementation or import optional application engines into Fabric startup. No service/secret changes in this revision; installation is a 10-26 post-merge step. Do not put smarty-inbox's transaction, provider credentials or seat secrets in Fabric. Existing actor/control/bridge events, supervision and ledger maintenance drive work, not a new business polling loop.

## 7. Native acceptance plan (unexecuted; prior fixture/smoke evidence is narrower)

Below, N1–N11 are **acceptance-test IDs**, not the external challenge's N1–N5 finding IDs. Keep unit tests where they prove a trust boundary or otherwise expensive crash/race path; the decisive test is a real mixed-version request through the user-facing provider. Never run the full test suite.

| ID / proposed evidence | Required allowed and refused behavior |
|---|---|
| N1 `tests/native-actor.test.ts` | Register exact host binding, birth ID, ask through genuine control admission, return existing wrapper/data. Spies **and throw-on-use sentinels** on model resolver/preparation, AgentManager.run/spawn and worker launch remain zero for create, successful ask, rejection, missing callback and restart. Ordinary model actor control still works. |
| N2 `tests/native-actor-registry.test.ts` | Reconstruct ActorManager/ActorDirectory over the same registry: same exact ID/marker. Missing registration/module, malformed/unknown marker, dropped marker at reserved ID, wrong root/handler/scope, attempted ordinary-row conversion all fail closed. Exercise actual owned save/foreign merge plus public definition/export/import/global update; no downgrade to Pi. Load the native row with the pinned old loader as a negative compatibility control. |
| N3 `tests/bridge-nats-native-ingress.test.ts` | Real `NatsCarrierSide → MeshBridge → StoreBridgeSide → MeshStore → FabricControlPlane` path, not hand-authored trusted context. Valid machine/session reaches callback, including absent optional old-pod fields. Wrong credential-bound machine/sender and mismatching authoritative session, target, owner, link, return address, command ID or receipt-bound message/data/args/deadline/principal fail before claim/handler. Extra untrusted envelope fields and poisoned optional presence.sessionId yield canonical normalization or refusal, never forged callback identity. Forged public `MeshStore.publish` verification/context without matching evidence fails. Identity-smuggling RPC data is separately rejected by application schema/authorization. |
| N4 same suite, persisted evidence | Separate bridge and receiver processes/recreated verifier; authentic receipt works after reconstruction. Missing/corrupt/truncated/expired/symlink/foreign-mode receipt denies. Receipt-only/no event executes nothing. Crash after claim/before projection and after projection/before append, then genuine redelivery: no new identity or effect. A generic stdio bridge cannot mint context by copying metadata. Exact replay returns only the original authenticated result/address. |
| N5 `tests/native-actor-custody.test.ts` | Pending native promise with no AgentManager run retains `releaseCustodyCount`; strict release and stop/remove/close cannot claim completed custody while it ignores abort; session-scope idle dormancy is refused. Queued cancel invokes zero callbacks; in-flight cancel observes signal. Callback commit then abort yields uncertain observation, not `notRun`. Handover starts no successor until settlement/confirmed termination. Add an actual approximately 11-second synchronous handler event-loop stall while the separate owner Main and observer continue: assert owner custody/presence survive if their real expiry budgets permit, otherwise name the timer that expires/fences and refuse the canary until the application async lock is ready. Compare actual expiry/effect-time checks, not just heartbeat intervals; no raised timer budget or mocked-clock pass. |
| N6 `tests/native-actor-sources.test.ts` | Attempt tell/followUp/steer, topics, host events, lifecycle, child completion, requeue archive, native row with subscriptions and forged saved context. No handler/model entry. Native ask success/error/cancel emits no input/output body notification; inspect actor history/archive bytes for absence of request/result/context bodies while the direct reply stays intact. Ordinary actor subscriptions/history remain unaffected. |
| N7 `tests/native-actor-residency.test.ts` + built resident/owner fixtures | Actual pinned Pi SDK/RPC Main and detached Pi resident through existing `worker/task-entry`/resident launcher, no provider credentials or selected model, native session persistence, config/module load before native readiness. Resident can load its configured handler without the originating Main extension **in that resident process**, but does not replace the live Main requirement. Wrong/missing module has zero model calls/no native readiness. Restart reopens the same native file/root and same actor ID; missing/empty pinned session refuses. Unexpected root input starts no turn. Interrupted ask is not callerlessly replayed. POSIX target; Windows durable-launch limitation declared, not counted as pass. |
| N8 `tests/nats-auth-path-b-native-actor-pod-compat.test.ts` + candidate canary | Run the **actual provider/router/control sender source or built bundle at `f33937156f587f2479c651ae301de44ec0f0592a`** in a separate process against the candidate link/hub, real authenticated broker, real headless owner Main and actual native resident. Drive public `agents.ask`; validate `.data`, native context, fixed actor ID, request correlation, discovery, cancellation and same-sender-only reply. Settle existing traffic, stop Main only while resident is alive, observe gate root-down, then ask: definite refusal, **+0 callbacks**. Restart Main from the same native file: same root/actor, success. With both healthy, wait **longer than configured `residentIdleExitMs`** (default 600,000 ms) with no application asks, then an actual bridged ask succeeds without any local wake. Test the exact adapter artifact used by both Main/resident. Handwritten envelopes, an in-memory owner or a resident-only test do not satisfy N8. |
| N9 N8 application fixture | Two configured seats pinned to distinct `org-kate`/`org-paul` machines and exact approved sessions after item 18. Rollout validator refuses `sandbox1` and duplicate machine assignments; a real negative ask from `sandbox1` is refused before callback. Add/list/resolve are seat-scoped; attempt guessed other-seat item ID, equal cross-seat opKey, spoofed identity/seat fields and cached-result fishing. No unauthorized read/mutation/result; authenticated context never becomes payload identity. |
| N10 N8 retries/effects | Inject and count suppression of the **response envelope after durable app commit** so the caller actually misses its ask result. Lost broker `+ACK`/PubAck alone is not this failure. Exercise same-control redelivery separately, then a new actual old-provider `agents.ask` retry with unchanged opKey and a distinct command UUID. Concurrent calls with same seat/operation/opKey and distinct requestIds return one stable application item/result, each echoes its own requestId, durable effect count is exactly one. Restart bridge/actor managers and app fixture before retry; still one. Same key/different args conflicts. Same key/different seat is isolated and independently delivers. |
| N11 startup/build | Cold import/idle plus first-use native module fixture; no application/optional engines pulled into normal eager static closure or idle registration. Run build artifact and lazy graph assertions without raising budgets. |

### Mixed-version fixture custody

Reachable, committed compatibility source is [`tests/helpers/frozen-f3393715.ts`](tests/helpers/frozen-f3393715.ts), not the untracked `.local/342-compat-path.md`. It archives unchanged old source/assets, rejects absent history or changed locks, checks exact hashes before sharing only third-party `node_modules`, and imports the actual old runtime/catalog/config. Its current-source type imports are erased; it does not substitute a candidate sender. It records source tree and archive digest at materialization. Frozen `src` tree inspected here: `98c0b004c04662de15b3d4e1440125d67892fcce`.

Pinned SHA-256 values from that helper:

| Frozen file | SHA-256 |
|---|---|
| `src/providers/agents-message-router.ts` | `544971ab607c1ad6d5f0ced43bdc540bea124e3e97c6eaa8e291d4eee1bdd7c8` |
| `src/topology/control-plane.ts` | `4101689148db8ba6df87e7a719574d9c6f8397c377b3e8f21a144f4ba711c8fe` |
| `src/providers/agents-provider.ts` | `7bf93ff6f412a94c87159ef58e837a94836d2299a8e78dd545b8166a3fabbed3` |
| `src/fabric-runtime-state.ts` | `6682fce72c33f4bd3e8caafe732ffae3cff8cd50b0094e94e42b906feea649d8` |
| `bun.lock` (old/current equality required) | `7b871eab624822df21b6d4628f420ffd09e5d7ac4b62eca5436a129fb4b63ebb` |

Main's two actual-old-runtime smoke passes and 11 fixture passes prove their bounded prerequisites, not N7/N8. Separately, the pinned external challenge's **Spike evidence** table records inspected component logs: two runner/identity tests, three corrected mixed-wire tests, and one source-level retention-policy check. The first mixed-wire expectation failed and was corrected; the passing spike uses fake presence/participants and an unconditional authorizer. It omits the real identity gate, file reconstruction/control/actor path and broker. Cite it as partial source/component evidence only, **never real-broker/native-actor proof**.

The implementer should stage that exact sender bundle/source and required assets into the isolated fixture through an authorized build step, record full SHA and artifact digest, and launch it separately so module resolution cannot accidentally load the candidate provider. Pin the candidate hub/link/resident artifact separately. Use the project's existing broker/resident fixtures where possible. Do not modify the real pod, upgrade its provider, substitute a current sender, or count a source-shape assertion as cross-version execution.

The current `tests/bridge-nats-file.test.ts` proves useful reconstruction/ACK plumbing but constructs envelopes and manually publishes the file ACK; it does not prove N8. Keep/extend those targeted trust-boundary tests and add the actual provider/resident boundary.

Implementation verification, after choosing only affected files:

```sh
bun run typecheck
bunx vitest run tests/native-actor.test.ts tests/native-actor-registry.test.ts tests/bridge-nats-native-ingress.test.ts tests/native-actor-custody.test.ts tests/native-actor-sources.test.ts
bunx vitest run tests/native-actor-residency.test.ts tests/nats-auth-path-b-native-actor-pod-compat.test.ts
bun run assert:lazy-graph
bun run build
```

These native acceptance paths are proposed; neither their existence nor their passing is asserted by this revision. The real-broker test uses the existing `nats-auth-path-b-*.test.ts` CI selection (and committed helper's full-history checkout prerequisite), rather than an unselected new test filename. Required broker/old-bundle/resident prerequisites must be explicit in CI; absence is blocked or an expected skipped platform coverage report, never a successful compatibility receipt. The lead owns any test that launches/terminates processes or performs fixture deletion under the applicable hosted-test rules.

### Fixture/CI versus installed candidate, and earliest live use

Lead update 2026-10-25 16:35Z forwards the org's 16:31 ruling: **#1572 stays off the factory host until the native canary passes**. On Ryzen 1 only Kate's authorized v1 restart and the bridge/hub run side-by-side, explicitly pinned to the candidate using `smarty-role --fabric-release`; do not replace the factory's Fabric generation as a prerequisite. The #342 production headless Main uses that side-by-side pin and becomes live only after canary. No Ryzen 1 restart/install occurs today.

| Acceptance work | Where / installation prerequisite |
|---|---|
| N1–N6 and N11 | Focused CI or isolated work-host fixtures; **no Ryzen 1 install**. N5's real-process stall/custody case also needs the built SDK/resident fixture and separate observer, not production services. |
| N7 real SDK Main/resident/bootstrap/restart | POSIX CI or isolated work-host fixtures with built candidate and pinned Pi SDK/argv adapter. **No production install/credentials**; real processes/native sessions mandatory. |
| N8–N10 frozen provider + authenticated broker + native resident + transactional fixture | CI/isolated fixtures with synthetic stores/generated broker credentials prove Fabric A13–A15; **no production install**. They do not prove the application's protected-writer/data policy. |
| N9/N10 with the real application export | Requires its complete pinned runtime inputs and isolated canonical-home/store setup from its own tests; the async-lock revision is required for contention canary. Can run isolated without Ryzen 1 installation. Source staging is not execution. |
| Live candidate N8–N10 / native canary | Requires **10-26 after Proof A**, authorized side-by-side candidate-pinned bridge/hub and test owner Main, item-18 machine/Secrets and live retention/allow-list readbacks, application async lock, and applicable review/security/data-scope gates. The test Main needed for canary is not a grant to make production service live first. |
| Production headless Main / real seat use | Only after native canary and accepted code/deployment gates. Keep #1572 off factory until then; no implicit fleet upgrade. |

**Earliest live date is conditional 2026-10-26 after Proof A, candidate installation and successful native canary**, not the 08:00Z pod image or a fixture pass. Selected plan: hub-only R2 around **10-26 22:00Z**, installed before **10-27 06:00Z**; no earlier clock time is promised. R1 is opportunistic only if green/approved by 10-26 15:00Z. Exact Proof A/canary/install timing remains with the lead.

## 8. Failure table and rollout

| Condition | Behavior |
|---|---|
| Native ID known, no matching host registration/module/config | Unavailable, zero model work. Preserve row and diagnostic. |
| Root Main non-live, even if resident lives | Existing bridge gate refusal, no newly admitted callback; restore the supervised same-root Main, not bridge wake. |
| Idle session-native actor past resident idle-exit interval | Stays active under existing session-scope rule; real bridged ask must succeed. No project-native dormancy/wake support. |
| Authentic command, owner incarnation changed | Existing stale-incarnation refusal; no callback. No new automatic effect retry or native dormant-wake guarantee. |
| Forged public mesh event/context, missing receipt, stdio metadata | Drop before claim/replay/cancel; no result disclosed and no native/model invocation. |
| Application rejects schema/seat/opKey conflict | Application `ok:false` reply, current requestId, no forbidden effect. |
| Callback committed, ACK lost | Existing transport replay if possible; otherwise new ask with same application opKey returns stable application result. |
| Crash between callback effect and saved control outcome | Control outcome indeterminate, not exactly-once inference; app dedupe prevents repeat effect on explicit retry. |
| Callback ignores cancellation | Keep release custody; fail busy rather than launch a replacement concurrently. |
| Old binary / imported native marker | Non-model sentinel + explicit import refusal; coordinated deployment/rollback, never automatic model downgrade. |

Roll out on Ryzen 1 only, **10-26 after merge**, not the pod's 08:00Z image path. No Main movement/restart, service enablement, deployment or secret change is authorized now.

1. Validate item-18 deployment evidence: exact approved Kate SID on machine `org-kate`, exact approved Paul SID on `org-paul`, and distinct already-authorized Secrets/permissions. The deployment validator—not an invented generic bridge-parser guarantee—refuses if any RPC seat entry names `sandbox1`, any machine appears on two seat entries, or any SID/machine differs from the approved seat map. No `send: ask` upgrade of an ambiguous existing/shared entry.
2. Quiesce old writers of this **session-root** registry only; install pinned candidate Fabric, immutable app/bootstrap modules, thin launcher and native SDK argv adapter. Provision/reuse and persist the actual owner Pi session as above. Keep actor `5027cfd59b1247b6bb4666970c481cc4`, session scope, module/ingress paths and root pin fixed. This is not a project-shared writer cutover.
3. Enable the one owner service for boot using approved host supervision; verify boot activation/lingering and restart policy, then start actual Main plus its existing resident. Read back native session/root, actor marker/ID, resident config and live root+actor observations before readiness. No model credentials or root prompts.
4. Add only that actor destination and necessary ask/cancel rights to the exact two validated seat entries. No destination-class or principal-carry widening. Run N8–N10 on synthetic data, including root down/+0 callbacks, same-root restart, idle past the interval, sandbox1 refusal, and actual frozen-provider reply path. Live broker retention readback and any sensitive-data approval remain separate gates.
5. Rollback withdraws the allow entry/route and stops the owner under existing custody before code replacement; confirm outstanding callbacks settled or the owning process terminated. Preserve app dedupe records, native session file/root, actor identity and receipt evidence. Never reopen a native-bearing session with an old serializer or silently repin the pod.

## 9. R1 disposition and remaining gates

These are designer responses to all nine external findings, **pending the lead-owned external recheck**, not self-issued closure/approval.

| Finding | Disposition and concrete change |
|---|---|
| B1 blocker | **Adopt.** One supervised actual no-model Pi Main; existing SDK/RPC bootstrap, native-persisted owner file/root, boot/restart supervision, credential-free owner/resident, unchanged live-root gate. N8 root down with resident alive = refusal/+0 callbacks; same-file restart = same actor/root and success. |
| B2 blocker | **Adopt.** `scope: 'session'` only, durable residency. Existing session rule avoids dormancy; no native project scope, dormancy exemption or bridge wake. Real bridge ask after `residentIdleExitMs` is mandatory. |
| N1 retention | **Adopt factual correction.** Frozen inbox age unbounded, source cap 1 GiB; no pod re-pin. Sensitive-data approval and live stream readback are not invented. |
| N2 body copies | **Adopt.** Suppress native input **and** output broadcasts; choose metadata-only actor history/archive copies, preserving direct/control reply. Document remaining hub/broker copies and inspected retention knobs. |
| N3 shared seats | **Adopt.** Exact org-kate/org-paul machine/SID entries after item 18; refuse sandbox1/duplicate-machine rollout, plus N9 sandbox1 negative. |
| N4 estimate | **Adopt.** Minimum cut and implementation/proof/review estimates in §10; original 3–5 h is not an honest completed-proof estimate. |
| N5 evidence/cutover | **Adopt.** Distinguish 7ac1f5 source baseline from installed ca4b1482 and divergent frozen pod; committed helper/pins reachable; spike partial only; session-root writer cutover, no project-shared claim. |
| Q1 scope | **Lead accepted:** snooze OUT; only add/list/resolve. |
| Q2 authority | **Lead accepted:** credential-machine sender pin/deadline plus distinct machines/Secrets, not cryptographic per-session proof. Same-UID and credential-holder residuals explicit. |

Remaining gates/unknowns, with recommendations:

1. External round 2 is owned by the lead; send this artifact and the pinned challenge, not a designer-briefed substitute. Recommend recheck B1 bootstrap/no-credential resident launch and B2 idle path first, then unchanged ingress-before-claim and custody boundaries.
2. Actor ID is fixed. Actual service root/file, immutable application paths, exact approved seat SIDs, item-18 broker permission/Secrets evidence and live allow-list/stream readback still come from deployment ownership. Recommend fail closed on missing pins, not infer them from names or modify services today.
3. Application production schema, protected-writer policy, opKey transaction and viewer readback remain application-owned/uninspected here. Recommend N9/N10 against that implementation before one-effect/product acceptance; fixture success cannot settle these claims or client-data retention approval.
4. Real credential-free Main/resident bootstrap, full native dispatch and real-broker mixed-version N7/N8 are **unrun**. The reusable source path avoids a fake runtime, but does not eliminate integration risk. Recommend make these release gates; if the pinned SDK adapter does not work, report a blocker rather than add provider credentials, a fake Main or weaken the gate.
5. Original bounded prior-art research remains above; no new ecosystem research was needed for these blockers. No exhaustive-search claim. The external challenge, not this writer, owns independent sufficiency judgment.

## 10. Minimum cut, remaining effort and delivery feasibility (designer estimate)

**Minimum A13–A15 cut is not just registration.** Retain all six security/execution boundaries: (1) exact native API/marker/import guards; (2) ask-only queue branch before all model preparation, with body-copy suppression and actual-promise custody; (3) real bridge receipt writer/verifier bound to final forwarded bytes; (4) admission **before claims, cached results and cancellation**, plus out-of-band handle through adapters; (5) resident config/module composition and the real supervised same-session owner Main; (6) actual f339 provider → authenticated broker → real link/file control → native resident → same sender reply, including restart/root-down/idle and retry/cancel negatives. These are about 20–25 source/build touchpoints in six coupled paths, not one runner callback. Trim unused project-native scope, native engines/runners, subscriptions, local emulation and generic framework options—not trust-boundary checks. No auth redesign, dependency, pod re-pin or verified-kernel change.

Remaining work at this R1 response (no integrated native execution; API/template handbacks and bounded checks are prerequisites, not completed actor/control/resident work):

| Work | Engineering hours, remaining estimate |
|---|---:|
| Integrate API/subpath/template guards; actor marker/load/save/ask-only branch, no-model/no-body/custody behavior (first two paths) | 4–6 |
| Receipt/ledger/final-body canonicalization plus pre-claim/replay/cancel control/adapters (next two paths) | 3–5 |
| Session-only resident composition, native-session bootstrap/launcher/adapter, readiness/failure behavior (fifth path) | 2–3 |
| Targeted boundary tests + real N7/N8/N9/N10 user-path proof, restart/idle/reply-path failures (sixth path; reuse committed helper/transaction fixture) | 4–6 |
| Integration corrections, fresh type/build/lazy checks, final evidence and independent review response | 3–5 |
| **Total** | **16–25 engineering hours** |

Thus implementation is approximately **9–14 h**; proof/fix/review is another **7–11 h**, not hidden inside the original 3–5 h. The already-reported 11+2 passing prerequisites are sunk evidence, not deductions from unrun native proof. Production application work, item-18 credential provisioning and host installation/reboot acceptance are outside this Fabric engineering estimate; dependencies on them remain explicit.

**Elapsed:** with two effective implementers on disjoint actor versus bridge/control work, one integration owner, available fixtures/broker and an external reviewer, allow roughly **10–16 elapsed working hours** from resumed integrated work; with one implementer, **16–25 h** before review/CI queues. Parallelism does not divide by two: resident/config/guard interfaces converge, and the real idle proof alone must exceed 600,000 ms at the default. Budget additional elapsed queue time rather than treating a missing broker, host artifact or reviewer as a skip/pass.

**Selected schedule (lead decision 2026-10-25 16:27:30Z): plan hub-only R2 around 2026-10-26 22:00Z, installed before 2026-10-27 06:00Z.** R1 is opportunistic only: if the PR is green and approved by **2026-10-26 15:00Z**, ride R1; otherwise R2. No proof gate is cut. PR around 10-26 14:00Z remains conditionally feasible if external design R2 resolves promptly, disjoint receipt/bridge, actor/registry and resident/custody paths proceed on 10-25, and real integration/proof finishes before PR opening. It is not a 3–5 h commitment, and 15:30Z merge is not promised. Starting the whole cut on 10-26 morning is not credible. Installation is after merge and is not counted as already done.

**Application lock risk (lead update 2026-10-25 16:28:47Z):** the application owner reports a synchronous `spawnSync('flock', ... '-w10')` store lock with approximately 11-second child timeout (`store.ts:1649–1662` → `lock.ts:15`). Abort cannot interrupt that resident event-loop stall. The lead has requested an async lock **before the native canary**. Source timer readback and the N5 stall case must distinguish separate Main-root liveness, resident host/registry custody, participant/host-lease expiry and control ACK/claim deadlines; an 11-second blocking callback is not presumed safe just because its application deadline is longer. Timer conclusions and executed proof remain explicit gates, not an authorization to enlarge timers.

**Source-read timer answer (native stall proof UNRUN):** the consumed/terminal investigation is `.local/342-native-timer-budget-result.md`; Main independently checked key freshness/default source and the staged application's synchronous lock. Timer files are unchanged from inspected baseline `f78625d137230b691ef837a484a89613d3d45a7f`.

| Boundary | Source/default and 11-second result |
|---|---|
| **Tightest consumer freshness: 10 s** since confirmed heartbeat | `src/topology/participant-directory.ts:99,882,2401–2416`: heartbeat 5 s; `canConsumeMesh` requires age **< 2×heartbeat**. Even a fresh 11 s block crosses it; start near next heartbeat crosses by about **6 s**. Claim/handler/outcome/ACK effect gates recheck (`src/topology/control-plane.ts:760–784,3153–3163,3184–3204,3371–3373`). |
| **Resident/actor lease: 15 s**, renewed every 5 s | `src/topology/legacy-root-liveness.ts:6`; `participant-directory.ts:882–883,994–1025,3590–3610`. Start at age near 5 s, return near age 16 s: can lapse about **1 s** before unblock. Separate healthy Main does not renew resident actor-owner lease. |
| OWNER custody / registry ownership | `src/residency/host.ts:3106–3224`, `src/residency/file-lock.ts:15–57`: retained kernel lock/token, **no TTL**. `src/actors/registry-store.ts:487–552`: other writer can time out after 5 s acquisition wait, not steal valid live holder. Heartbeat lapse is not positive lineage death or successor permission. |
| Launcher watchdog | `src/residency/launcher.ts:692–766`: default 180 s stale/30 s sampling/900 s cold allowance; 11 s alone does not trigger it. Custom shorter settings require readback. |
| Control deadline/claim/ACK | `src/topology/control-plane.ts:2936–3025,3140–3163,3274–3355`: absolute deadline/durable claim, not 5 s stealable running lease. Fresh default bridge window ≥30 s, but transit/queue age may leave <11 s. Abort cannot interrupt sync work. Long-budget result may recover after genuine refresh; no unconditional result-loss or safety claim. |

Thus **11 seconds loses freshness and can lose visible presence under defaults; it does not itself release OWNER custody**. N5 exercises fresh-renewal and worst-phase starts with separate observer/Main, no successor/duplicate callback, and honest delayed/refused admission/recovery. After the awaited/AbortSignal-controlled application lock arrives, repeat real contention and require heartbeats to continue with unchanged budgets before canary. No timer extension is proposed.

Named schedule risks: no-credential SDK wrapper behavior in both owner and existing resident launcher; native promise release with no AgentManager run; complete receipt binding across reconstruction and replay/cancel; quarantined marker preservation under session-writer restart; real broker identity/stream prerequisites; current candidate versus divergent f339 runtime compatibility; production handler/item-18 availability; external R2/R1 findings. A failed native user-path proof can require another fix/proof cycle; it is not converted into source-only acceptance.

No new spending, access grant, credential, dependency, principal data-retention approval or production effect is authorized by this document.
