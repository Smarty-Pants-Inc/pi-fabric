// Mixed FILE-mesh <-> NATS harness for the A' bridge (smarty-dev#15310, D3 gate 3, lane L4).
//
// One real authenticated nats-server (tests/helpers/nats-cluster.ts, pinned 2.14.7 in CI) runs with:
// - a real FILE-transport FabricRuntimeState Main on the hub file mesh (the Ryzen 1 lead stand-in);
// - a real NATS-transport FabricRuntimeState Main on machine-b (the moved v2 lead);
// - the A' link, started in-process through `startLink`, holding the machine-a credential and dialing
//   the broker through a NatsWireFaults relay, so forced disconnects, lost +ACKs and lost PubAcks hit
//   the link only. Both Mains dial the broker directly.
// Only the Pi host UI/model boundary is a fixture (pi.sendMessage is captured). Control, admission,
// presence, leases, ACKs and the broker are the product's.
//
// The link contract: `startNatsLink(config, { signal, log }) -> { closed, stop }` in src/mesh/bridge-nats-link.ts
// (L2). The config is L5's NatsLinkConfig (src/mesh/bridge-nats-config.ts): the harness builds its JSON and,
// when L5's module is in the checkout, validates it with L5's own parser (`validateLinkConfig`). Allow entries
// are L3's BridgeNatsAllowEntryInput (send/receive/receiveTopics), the only shape (integrator notes 4-6).
// `orgJson` is always this harness's fixture with fictional ids, never a real org.json. Until L2 lands,
// `loadNatsLink()` returns undefined and the link-dependent E2E files skip; `startLink` may also be injected.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { pathToFileURL } from "node:url";
import { ModelRegistry, ModelRuntime, type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { fauxProvider } from "@earendil-works/pi-ai";
import { vi } from "vitest";
import { CapturedToolCatalog } from "../../src/capture/catalog.js";
import { normalizeFabricConfig, type FabricConfig } from "../../src/config.js";
import { FabricRuntimeState } from "../../src/fabric-runtime-state.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../../src/mesh/store.js";
import { openFabricTransport } from "../../src/transport/index.js";
import { runTransportAdmin } from "../../src/transport/nats/admin.js";
import { hashToken } from "../../src/transport/nats/subjects.js";
import type { BridgeNatsAllowEntryInput } from "../../src/mesh/bridge-nats-types.js";
import type { FabricControlOperation } from "../../src/topology/control-plane.js";
import type { FabricParticipantInfo, FabricParticipantRecord, FabricKvHostLease } from "../../src/topology/types.js";
import type { FabricTransport, TransportEnvelope, TransportSent } from "../../src/transport/types.js";
import { createPathBHarness, PATH_B_MACHINES, PATH_B_PERMISSION_POLICY, teardownWithin, type PathBHarness } from "./nats-cluster.js";
import { NatsWireFaults } from "./nats-wire-faults.js";

export const BRIDGE_MACHINE = "machine-a";
export const NATS_MAIN_MACHINE = "machine-b";
export const DEFAULT_LINK_NAME = "v2";
/** Fixed canonical root sessions (`session:<uuid>` is the only id a bridge admits). */
export const FILE_MAIN_SESSION = "15310f11-aaaa-4000-8000-000000000001";
export const NATS_MAIN_SESSION = "15310b22-bbbb-4000-8000-000000000002";
/** A live file root the link does NOT allow-list. */
export const FILE_OUTSIDER_SESSION = "15310f33-cccc-4000-8000-000000000003";
/** A second allow-listed NATS root on the same machine as natsMain (commandId-collision and seat tests). */
export const NATS_PEER_SESSION = "15310b44-dddd-4000-8000-000000000004";
/** The fixture org.json's product owner seat (never booted; it only makes the pins file valid). */
export const OWNER_SEAT_SESSION = "15310a55-eeee-4000-8000-000000000005";
export const SEAT_PRINCIPAL = "Kate";
export const rootId = (session: string): string => `session:${session}`;

// ---------------------------------------------------------------------------------------------
// Link entry (L2). Config: L5's NatsLinkConfig JSON. Handle: { closed, stop }.
// ---------------------------------------------------------------------------------------------

export interface MixedLinkHandle {
  /** Graceful stop: fences mirrors, joins in-flight work, closes NATS. */
  stop(): Promise<void>;
  /** Settles when the link ends (stop or a fatal error). */
  readonly closed?: Promise<unknown>;
}

export interface MixedLinkStartOptions {
  readonly signal: AbortSignal;
  readonly log: (line: string) => void;
}

/** The NatsLinkConfig JSON the harness builds (L5 owns the schema; L5's parser validates it when present). */
export interface MixedLinkConfig {
  readonly version: 1;
  readonly name: string;
  readonly link: string;
  readonly file: { readonly mesh: string };
  readonly stateDir: string;
  /** Seat pins (L3): ALWAYS this harness's fixture; asserted never under $HOME. */
  readonly orgJson: string;
  readonly nats: {
    readonly servers: readonly string[]; readonly machine: string; readonly credentials: string; readonly caFile: string;
    readonly custodyStream: string; readonly livenessStream: string; readonly pathBMachines: readonly string[]; readonly operatorMachine: string;
  };
  readonly allow: { readonly file: readonly BridgeNatsAllowEntryInput[]; readonly nats: readonly BridgeNatsAllowEntryInput[] };
}

export type StartMixedLink = (config: unknown, options: MixedLinkStartOptions) => Promise<MixedLinkHandle>;

const CONFIG_MODULE = path.resolve(import.meta.dirname, "../../src/mesh/bridge-nats-config.ts");
/** L5's link-config module, when this checkout has it. */
export async function loadLinkConfigModule(): Promise<Record<string, unknown> | undefined> {
  return fs.existsSync(CONFIG_MODULE) ? await import(pathToFileURL(CONFIG_MODULE).href) as Record<string, unknown> : undefined;
}

/** L5's parsed NatsLinkConfig for this JSON (its parser refuses any drift), or the JSON itself before L5 lands. */
export async function validateLinkConfig(config: MixedLinkConfig): Promise<unknown> {
  const module = await loadLinkConfigModule();
  const parse = module?.parseNatsLinkConfig;
  return typeof parse === "function" ? (parse as (value: unknown) => unknown)(config) : config;
}

const LINK_ENTRY = path.resolve(import.meta.dirname, "../../src/mesh/bridge-nats-link.ts");

/** Whether the product link entry exists in this checkout (synchronous, for describe.skipIf). */
export const natsLinkPresent = (): boolean => fs.existsSync(LINK_ENTRY);

/** The product entry, absent until L2 lands. A missing module returns undefined; any other import error throws. */
export async function loadNatsLink(): Promise<StartMixedLink | undefined> {
  if (!natsLinkPresent()) return undefined;
  const module = await import(pathToFileURL(LINK_ENTRY).href) as Record<string, unknown>;
  const start = module.startNatsLink;
  if (typeof start !== "function") throw new Error("src/mesh/bridge-nats-link.ts does not export startNatsLink");
  return start as StartMixedLink;
}

/** Integrator's destination-class table (coordination note 3): a moved lead or a Ryzen 1 root/actor. */
export const ROOT_SEND_OPERATIONS: readonly FabricControlOperation[] = ["steer", "followUp", "ask", "cancel"];
/** A principal seat receives followUp and cancel only (L3 BRIDGE_NATS_SEAT_RECEIVE). */
export const SEAT_OPERATIONS: readonly FabricControlOperation[] = ["followUp", "cancel"];

/** One root allow entry (L3 BridgeNatsAllowEntryInput): it may send and receive `operations`. */
export function allowRoot(id: string, operations: readonly FabricControlOperation[] = ROOT_SEND_OPERATIONS, principal?: string,
  receiveTopics?: readonly string[]): BridgeNatsAllowEntryInput {
  return { kind: "root", id, send: [...operations], receive: [...operations], ...(principal === undefined ? {} : { principal }),
    ...(receiveTopics === undefined ? {} : { receiveTopics: [...receiveTopics] }) };
}

/** A file-side actor allow entry (owner root + name; the verified presence must match both). */
export function allowActor(rootId: string, name: string, operations: readonly FabricControlOperation[] = ROOT_SEND_OPERATIONS): BridgeNatsAllowEntryInput {
  return { kind: "actor", rootId, name, send: [...operations], receive: [...operations] };
}

// ---------------------------------------------------------------------------------------------
// Mains
// ---------------------------------------------------------------------------------------------

export interface CapturedMessage { readonly message: unknown; readonly options: unknown }
export interface Delivered {
  readonly text: string;
  readonly from: MeshIdentity | undefined;
  readonly delivery: string | undefined;
  readonly deliverAs: string | undefined;
  readonly details: Record<string, unknown>;
}

export interface MixedMain {
  readonly label: string;
  readonly sessionId: string;
  readonly id: string;
  readonly cwd: string;
  readonly transport: "file" | "nats";
  readonly config: FabricConfig;
  runtime: FabricRuntimeState;
  context: ExtensionContext;
  readonly messages: CapturedMessage[];
  invoke(ref: string, args: Record<string, unknown>, signal?: AbortSignal): Promise<unknown>;
  /** Pi-delivered Fabric agent messages (followUp/steer), oldest first. */
  delivered(): Delivered[];
  /** Start (or restart into) a fresh runtime with the same session; returns the new incarnation evidence. */
  start(): Promise<void>;
  stop(): Promise<void>;
}

function delivered(messages: readonly CapturedMessage[]): Delivered[] {
  return messages.flatMap(({ message, options }) => {
    const value = message as { customType?: string; content?: unknown; details?: Record<string, unknown> };
    if (value?.customType !== "pi-fabric-agent-message") return [];
    const details = value.details ?? {};
    return [{ text: typeof value.content === "string" ? value.content : JSON.stringify(value.content),
      from: details.from as MeshIdentity | undefined, delivery: details.delivery as string | undefined,
      deliverAs: (options as { deliverAs?: string } | undefined)?.deliverAs, details }];
  });
}

/** Fabric's fixture worker: an actor run answers "fake worker complete", or stays running on a HANG task until killed. */
export const FIXTURE_WORKER = path.resolve(import.meta.dirname, "../fixtures/fake-worker.mjs");

/** A keyless native model the Main's registry lists, so actor activations pass model admission (the fixture worker never calls it). */
async function fixtureModelContext(agentDir: string): Promise<Record<string, unknown>> {
  const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(agentDir, "auth.json") });
  const faux = fauxProvider({ provider: "bridge-fixture", models: [{ id: "one-turn" }] });
  modelRuntime.registerNativeProvider(faux.provider);
  await modelRuntime.getAvailable("bridge-fixture");
  return { model: faux.getModel(), modelRegistry: new ModelRegistry(modelRuntime) };
}

function makeMain(label: string, sessionId: string, cwd: string, config: FabricConfig, worker = path.join(cwd, "unused-worker.ts"),
  contextExtra: Record<string, unknown> = {}): MixedMain {
  const messages: CapturedMessage[] = [];
  const build = () => {
    const pi = { on: () => () => {}, events: { emit() {}, on: () => () => {} }, getThinkingLevel: () => "off",
      getSessionName: () => label, appendEntry() {}, sendMessage(message: unknown, options: unknown) { messages.push({ message, options }); },
    } as unknown as ExtensionAPI;
    const context = { cwd, hasUI: false, mode: "rpc", isProjectTrusted: () => true,
      isIdle: () => true, hasPendingMessages: () => false,
      modelRegistry: { getAvailable: () => [], find: () => undefined, getApiKeyAndHeaders: async () => undefined },
      sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => null, getEntries: () => [] },
      ui: { setStatus() {}, notify() {} },
      ...contextExtra,
    } as unknown as ExtensionContext;
    const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
      extension: path.join(cwd, "unused-extension.ts"), worker,
      residentHost: path.join(cwd, "unused-resident.ts"), skills: cwd,
    } });
    return { runtime, context };
  };
  let current = build();
  let running = false;
  const main: MixedMain = {
    label, sessionId, id: rootId(sessionId), cwd, transport: config.mesh.transport === "nats" ? "nats" : "file", config,
    get runtime() { return current.runtime; },
    get context() { return current.context; },
    messages,
    invoke: (ref, args, signal) => current.runtime.registry.invoke(ref, args, {
      cwd, signal: signal ?? AbortSignal.timeout(60_000), parentToolCallId: `mixed-${label}`, nestedToolCallId: ref,
      extensionContext: current.context, update() {}, approve: async () => {}, audits: [], maxResultChars: 50_000,
    }),
    delivered: () => delivered(messages),
    async start() {
      if (running) return;
      if (current.runtime.initialized) current = build();
      await current.runtime.initialize(current.context, config);
      running = true;
      // Readiness barrier: this Main's own live root is in its own directory (file state or KV watch).
      await waitFor(() => selfRecord(main), `${label} own live root`, 15_000);
    },
    async stop() {
      if (!running) return;
      running = false;
      await teardownWithin(`${label} shutdown`, () => current.runtime.shutdown("exit"), 10_000);
    },
  } as MixedMain;
  return main;
}

function baseConfig(mesh: Record<string, unknown>, agents: Record<string, unknown> = { enabled: false }): FabricConfig {
  return normalizeFabricConfig({ fullCodeMode: false,
    mesh: { enabled: true, actorPollMs: 20, ...mesh },
    agents, residency: { enabled: false }, mcp: { enabled: false }, memory: { enabled: false },
    jev: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
  });
}

// ---------------------------------------------------------------------------------------------
// NATS wire observation (in-process product transports only; the relay is the wire view)
// ---------------------------------------------------------------------------------------------

export interface WireEvent {
  readonly phase: "send" | "deliver" | "broker-ack";
  readonly machine: string;
  readonly hostId: string;
  readonly envelope: TransportEnvelope;
  readonly sequence?: number;
  readonly attempt?: number;
  readonly sourceMachine?: string;
  readonly duplicate?: boolean;
}

/**
 * Records every in-process NatsTransport send/deliver/broker-ACK for the harness lifetime. It patches the
 * shared prototype BEFORE any Main starts (an inbox iterator binds once at control-plane start), and is not
 * a vi.spyOn, so vitest restoreMocks cannot drop it between tests. restore() puts the originals back.
 * Record-only: every send, delivery and ACK is the product's own call.
 */
export class NatsWire {
  readonly events: WireEvent[] = [];
  private readonly changed = new EventEmitter();
  private readonly originals: Pick<FabricTransport, "send" | "inbox">;
  constructor(private readonly prototype: FabricTransport) {
    const send = prototype.send, inbox = prototype.inbox;
    this.originals = { send, inbox };
    const record = (event: WireEvent) => { this.events.push(event); this.changed.emit("change"); };
    prototype.send = async function<T>(this: FabricTransport, envelope: TransportEnvelope<T>, signal?: AbortSignal) {
      const result = await send.call(this, envelope, signal) as TransportSent<T>;
      record({ phase: "send", machine: this.self.machine, hostId: this.self.hostId, envelope: result.envelope as TransportEnvelope,
        sequence: result.sequence, duplicate: result.duplicate });
      return result;
    };
    prototype.inbox = async function*(this: FabricTransport, signal: AbortSignal) {
      const self = this.self;
      for await (const delivery of inbox.call(this, signal)) {
        record({ phase: "deliver", machine: self.machine, hostId: self.hostId, envelope: delivery.payload,
          sequence: delivery.sequence, attempt: delivery.attempt, sourceMachine: delivery.sourceMachine });
        yield { ...delivery, ack: async () => {
          await delivery.ack(); // real JsMsg.ackAck(); never a manufactured receipt
          record({ phase: "broker-ack", machine: self.machine, hostId: self.hostId, envelope: delivery.payload,
            sequence: delivery.sequence, attempt: delivery.attempt, sourceMachine: delivery.sourceMachine });
        } };
      }
    };
  }

  restore(): void {
    this.prototype.send = this.originals.send;
    this.prototype.inbox = this.originals.inbox;
  }

  /** Index for a per-test slice: `since(mark)`. */
  mark(): number { return this.events.length; }
  since(mark: number): WireEvent[] { return this.events.slice(mark); }

  filter(predicate: (event: WireEvent) => boolean): WireEvent[] { return this.events.filter(predicate); }

  /** Event-driven barrier with a deadline; the timer is only a safety net. */
  until(check: () => boolean, label: string, ms = 15_000): Promise<void> {
    return new Promise((resolve, reject) => {
      const test = () => {
        try { if (check()) { clearTimeout(timer); this.changed.off("change", test); resolve(); } }
        catch (error) { clearTimeout(timer); this.changed.off("change", test); reject(error); }
      };
      const timer = setTimeout(() => { this.changed.off("change", test); reject(new Error(`Timed out: ${label}`)); }, ms);
      this.changed.on("change", test);
      test();
    });
  }
}

// ---------------------------------------------------------------------------------------------
// The harness
// ---------------------------------------------------------------------------------------------

export interface MixedHarnessOptions {
  readonly binary: string;
  /** Link starter; default `loadNatsLink()`. `null` boots the Mains with no link (harness self-test). */
  readonly startLink?: StartMixedLink | null;
  /** Start the link and wait for both mirrors before returning (default: true when a link is available). */
  readonly autoStart?: boolean;
  readonly linkName?: string;
  /** Also boot a live file root that the link does NOT allow-list. */
  readonly outsider?: boolean;
  /** Also boot a second allow-listed NATS root on machine-b. */
  readonly natsPeer?: boolean;
  /** Create a persistent actor with this name, owned by the file Main (a Ryzen 1 actor, e.g. review-astra), and allow-list it. */
  readonly fileActor?: string;
  /** Make natsPeer a principal seat: org.json binds SEAT_PRINCIPAL to it and its allow entry declares that principal. */
  readonly seat?: boolean;
  /** Last-word edit of the allow-list before validation (e.g. mark a seat with `principal`). */
  readonly allow?: (allow: { file: BridgeNatsAllowEntryInput[]; nats: BridgeNatsAllowEntryInput[] }, ids: MixedIds) => void;
}

export interface MixedIds { readonly fileMain: string; readonly natsMain: string; readonly outsider: string; readonly natsPeer: string }

export interface MixedHarness {
  readonly root: string;
  readonly broker: PathBHarness;
  /** The link's only path to the broker. Faults here touch the link only. */
  readonly faults: NatsWireFaults;
  /** Every in-process NatsTransport send/deliver/broker-ACK (Mains, peers and an in-process link). */
  readonly wire: NatsWire;
  readonly fileMain: MixedMain;
  readonly natsMain: MixedMain;
  readonly outsider?: MixedMain;
  readonly natsPeer?: MixedMain;
  /** The file Main's actor (options.fileActor), as agents.create returned it. */
  readonly actor?: { readonly id: string; readonly name: string } & Record<string, unknown>;
  readonly linkName: string;
  /** The config JSON the link runs with (validated by L5's parser when present). */
  readonly linkConfig: MixedLinkConfig;
  /** The fixture org.json (0600) the link reads seat pins from; never the host's real org.json. */
  readonly orgJson: string;
  /** Link log lines (all starts). */
  readonly linkLog: string[];
  /** The file mesh the link bridges (Ryzen 1's): same root as fileMain's runtime mesh. */
  readonly fileMeshRoot: string;
  /** A raw product transport with a test-only host id, for broker reads (KV presence/leases). It uses machine-b:
   *  the bridge machine's inbox belongs to the link alone (L1's durable filters `fabric.in.<bridge>.>`). */
  readonly inspector: FabricTransport;
  link: MixedLinkHandle | undefined;
  readonly startLink: StartMixedLink | undefined;
  startLinkNow(): Promise<MixedLinkHandle>;
  stopLink(): Promise<void>;
  /** Open a product NATS transport for a credential with an arbitrary host id (negative tests). */
  openPeer(machine: typeof BRIDGE_MACHINE | typeof NATS_MAIN_MACHINE, hostId: string, viaRelay?: boolean): Promise<FabricTransport>;
  fileEvents(filter?: { topic?: string; to?: string }): MeshEvent[];
  /** NATS KV presence/lease for a root under a machine, or undefined. */
  kvRoot(machine: string, id: string): Promise<{ presence?: FabricParticipantRecord; presenceRevision?: number; lease?: FabricKvHostLease } | undefined>;
  /** Wait until each side routes the other's allow-listed root through the link. */
  waitMirrored(timeoutMs?: number): Promise<{ fileView: FabricParticipantInfo; natsView: FabricParticipantInfo }>;
  stop(): Promise<void>;
}

const kvKey = (machine: string, kind: "p" | "h", id: string): string => `${machine}.${kind}.${hashToken(id)}`;

async function waitFor<T>(read: () => T | undefined | Promise<T | undefined>, label: string, timeoutMs: number, everyMs = 50): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let last: unknown;
  for (;;) {
    try {
      const value = await read();
      if (value !== undefined) return value;
    } catch (error) { last = error; }
    if (Date.now() > deadline) throw new Error(`Timed out after ${timeoutMs}ms: ${label}${last ? ` (last error: ${String(last)})` : ""}`);
    await new Promise((resolve) => setTimeout(resolve, everyMs));
  }
}
export { waitFor as waitForValue };

/** The root a Main's runtime itself publishes, read from its own directory. */
export function selfRecord(main: MixedMain): FabricParticipantInfo | undefined {
  return main.runtime.participantInfos({ fresh: true }).find((row) => row.id === main.id && row.local && !row.stale);
}

/** A live row for `id` as `viewer` routes it, or undefined. */
export function liveRow(viewer: MixedMain, id: string): FabricParticipantInfo | undefined {
  return viewer.runtime.participantInfos({ fresh: true }).find((row) => row.id === id && !row.stale);
}

export async function createMixedHarness(options: MixedHarnessOptions): Promise<MixedHarness> {
  for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
  vi.stubEnv("SMARTY_AGENT", undefined);
  const parent = fs.realpathSync.native(process.env.TMPDIR?.trim() || os.tmpdir());
  const root = fs.realpathSync.native(fs.mkdtempSync(path.join(parent, "pi-fabric-nats-file-mixed-")));
  fs.chmodSync(root, 0o700);
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
  const cleanup: Array<() => Promise<void>> = [];
  const failures: unknown[] = [];
  const unwind = async () => {
    for (const step of cleanup.splice(0).reverse()) { try { await step(); } catch (error) { failures.push(error); } }
  };
  try {
    const broker = await createPathBHarness({ binary: options.binary, tls: {} });
    cleanup.push(() => broker.stop());
    for (const [name, value] of Object.entries(broker.adminEnv(broker.users.bootstrap))) vi.stubEnv(name, value);
    try {
      const code = await runTransportAdmin(["provision", "--ca-file", broker.tls!.caFile, "--machines", PATH_B_MACHINES.join(",")]);
      if (code !== 0) throw new Error(`nats-auth provision exited ${code}`);
    } finally {
      for (const name of Object.keys(broker.adminEnv(broker.users.bootstrap))) vi.stubEnv(name, undefined);
    }
    const faults = await NatsWireFaults.open(broker.serverUrl, broker.tls!);
    cleanup.push(() => faults.close());

    const dirs = { file: path.join(root, "ryzen1"), nats: path.join(root, "v2"), outsider: path.join(root, "ryzen1-outsider"),
      peer: path.join(root, "v2-peer"), agent: path.join(root, "agent") };
    for (const dir of Object.values(dirs)) fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    const fileMeshRoot = path.join(root, "ryzen1-mesh");
    const fileConfig = baseConfig({ transport: "file", hostTransport: "file", root: fileMeshRoot, announce: true },
      options.fileActor ? { enabled: true, transport: "process", model: "", maxConcurrent: 2, timeoutMs: 60_000, nice: 0 } : undefined);
    const natsConfig = (cwd: string) => baseConfig({ transport: "nats", hostTransport: "nats", root: path.join(cwd, "mesh"), announce: true,
      nats: { ...broker.runtimeOptions(broker.users.machineB), operatorMachine: "fabric-ops" } });
    const fileMain = makeMain("ryzen1-lead", FILE_MAIN_SESSION, dirs.file, fileConfig, options.fileActor ? FIXTURE_WORKER : undefined,
      options.fileActor ? await fixtureModelContext(dirs.agent) : {});
    const natsMain = makeMain("v2-lead", NATS_MAIN_SESSION, dirs.nats, natsConfig(dirs.nats));
    const outsider = options.outsider ? makeMain("ryzen1-outsider", FILE_OUTSIDER_SESSION, dirs.outsider, fileConfig) : undefined;
    const natsPeer = options.natsPeer ? makeMain("v2-peer", NATS_PEER_SESSION, dirs.peer, natsConfig(dirs.peer)) : undefined;

    const inspector = await openFabricTransport({ config: baseConfig({ transport: "nats", hostTransport: "nats", root: path.join(root, "inspector-mesh"),
      nats: { ...broker.runtimeOptions(broker.users.machineB), operatorMachine: "fabric-ops" } }).mesh,
      hostId: "mixed-harness-inspector", mesh: new MeshStore(path.join(root, "inspector-mesh"), 65_536, 1_000) });
    cleanup.push(() => teardownWithin("inspector close", () => inspector.close(), 5_000));
    // Before any Main binds its inbox iterator: record the whole run, and restore last.
    const wire = new NatsWire(Object.getPrototypeOf(inspector) as FabricTransport);
    cleanup.push(async () => wire.restore());

    for (const main of [fileMain, natsMain, outsider, natsPeer]) {
      if (!main) continue;
      await main.start();
      cleanup.push(() => main.stop());
    }
    const actor = options.fileActor ? await fileMain.invoke("agents.create", { name: options.fileActor, scope: "session",
      instructions: "Answer each ask once.", responseMode: "text", coalesce: false }) as MixedHarness["actor"] : undefined;

    const linkName = options.linkName ?? DEFAULT_LINK_NAME;
    const ids: MixedIds = { fileMain: fileMain.id, natsMain: natsMain.id, outsider: rootId(FILE_OUTSIDER_SESSION), natsPeer: rootId(NATS_PEER_SESSION) };
    if (options.seat && !natsPeer) throw new Error("seat needs natsPeer");
    const allow = {
      file: [allowRoot(fileMain.id), ...(actor ? [allowActor(fileMain.id, actor.name)] : [])],
      nats: [allowRoot(natsMain.id), ...(natsPeer ? [options.seat ? allowRoot(natsPeer.id, SEAT_OPERATIONS, SEAT_PRINCIPAL) : allowRoot(natsPeer.id)] : [])],
    };
    // Seat pins come only from org.json (L3). The fixture binds the product owner to a session nobody runs,
    // and SEAT_PRINCIPAL to natsPeer only when the test asks for a seat.
    const seat = (name: string, session: string) => ({ name, orgInstance: { herdrAgent: `org-${name.toLowerCase()}`,
      herdrWorkspaceLabel: `Org ${name}`, fabricSession: session } });
    const orgJson = path.join(root, "org.json");
    fs.writeFileSync(orgJson, JSON.stringify({ productOwner: { name: "Paul" },
      principals: [seat("Paul", OWNER_SEAT_SESSION), ...(options.seat ? [seat(SEAT_PRINCIPAL, NATS_PEER_SESSION)] : [])] }), { mode: 0o600 });
    // Integrator note 6: a test never reads a real org.json.
    const home = fs.realpathSync.native(os.homedir());
    if (orgJson === home || orgJson.startsWith(`${home}${path.sep}`)) throw new Error(`fixture org.json must not be under $HOME: ${orgJson}`);
    options.allow?.(allow, ids);
    // L3 (audit): every allow.nats entry pins the machine its root must be live on (after any test edit).
    allow.nats = allow.nats.map(entry => entry.machine === undefined ? { ...entry, machine: NATS_MAIN_MACHINE } : entry);
    const linkConfig: MixedLinkConfig = {
      version: 1, name: "ryzen1", link: linkName,
      file: { mesh: fileMeshRoot },
      stateDir: path.join(root, "link-state"),
      orgJson,
      nats: { servers: [faults.url], machine: BRIDGE_MACHINE, credentials: broker.users.machineA.credsPath, caFile: broker.tls!.caFile,
        custodyStream: PATH_B_PERMISSION_POLICY.custodyStream, livenessStream: PATH_B_PERMISSION_POLICY.livenessStream,
        pathBMachines: [...PATH_B_MACHINES], operatorMachine: "fabric-ops" },
      allow,
    };
    const validatedConfig = await validateLinkConfig(linkConfig);
    const startLink = options.startLink === null ? undefined : options.startLink ?? await loadNatsLink();
    const peers: FabricTransport[] = [];
    cleanup.push(async () => { await Promise.allSettled(peers.splice(0).map((peer) => teardownWithin("peer close", () => peer.close(), 5_000))); });
    const linkLog: string[] = [];
    let linkAbort: AbortController | undefined;

    const harness: MixedHarness = {
      root, broker, faults, wire, fileMain, natsMain, ...(outsider ? { outsider } : {}), ...(natsPeer ? { natsPeer } : {}), ...(actor ? { actor } : {}),
      linkName, linkConfig, orgJson, linkLog, fileMeshRoot, inspector, link: undefined, startLink,
      async startLinkNow() {
        if (!startLink) throw new Error("A' link entry (src/mesh/bridge-nats-link.ts startNatsLink) is not available");
        if (harness.link) throw new Error("link already running");
        linkAbort = new AbortController();
        linkLog.push(`--- harness: link start ${new Date().toISOString()}`);
        harness.link = await startLink(validatedConfig, { signal: linkAbort.signal, log: (line) => { linkLog.push(line); } });
        return harness.link;
      },
      async stopLink() {
        const link = harness.link, abort = linkAbort;
        harness.link = undefined; linkAbort = undefined;
        if (!link) return;
        linkLog.push(`--- harness: link stop ${new Date().toISOString()}`);
        try { await teardownWithin("link stop", () => link.stop(), 15_000); }
        finally { abort?.abort(); }
      },
      async openPeer(machine, hostId, viaRelay = false) {
        const user = machine === BRIDGE_MACHINE ? broker.users.machineA : broker.users.machineB;
        const dir = path.join(root, `peer-${peers.length}`);
        const config = baseConfig({ transport: "nats", hostTransport: "nats", root: dir,
          nats: { ...broker.runtimeOptions(user), ...(viaRelay ? { servers: faults.url } : {}), operatorMachine: "fabric-ops" } });
        const peer = await openFabricTransport({ config: config.mesh, hostId, mesh: new MeshStore(dir, 65_536, 1_000) });
        peers.push(peer);
        return peer;
      },
      fileEvents(filter = {}) {
        return fileMain.runtime.mesh.read({ ...filter, limit: 10_000 });
      },
      async kvRoot(machine, id) {
        const presence = await inspector.kv("presence").get<FabricParticipantRecord>(kvKey(machine, "p", id));
        const lease = await inspector.kv("leases").get<FabricKvHostLease>(kvKey(machine, "h", id));
        if (!presence && !lease) return undefined;
        return { ...(presence ? { presence: presence.value, presenceRevision: presence.revision } : {}), ...(lease ? { lease: lease.value } : {}) };
      },
      async waitMirrored(timeoutMs = 20_000) {
        const fileView = await waitFor(() => {
          const row = liveRow(fileMain, natsMain.id);
          return row?.remoteHost === linkName ? row : undefined;
        }, "NATS root mirrored live into the file mesh", timeoutMs);
        const natsView = await waitFor(() => {
          const row = liveRow(natsMain, fileMain.id);
          return row?.machine === BRIDGE_MACHINE ? row : undefined;
        }, "file root mirrored live into NATS KV", timeoutMs);
        if (actor) {
          await waitFor(() => {
            const row = liveRow(natsMain, actor.id);
            return row?.machine === BRIDGE_MACHINE ? row : undefined;
          }, "file actor mirrored live into NATS KV", timeoutMs);
        }
        return { fileView, natsView };
      },
      async stop() {
        await unwind();
        vi.unstubAllEnvs();
        try { fs.rmSync(root, { recursive: true, force: true }); } catch (error) { failures.push(error); }
        if (failures.length) throw new AggregateError(failures, failures.map(String).join("; "));
      },
    };
    cleanup.push(() => harness.stopLink()); // first to unwind: the link stops before the Mains and broker
    if (startLink && options.autoStart !== false) {
      try {
        await harness.startLinkNow();
        await harness.waitMirrored();
      } catch (error) {
        const rows = (viewer: MixedMain, id: string) => JSON.stringify(viewer.runtime.participantInfos({ fresh: true, includeStale: true })
          .filter((row) => row.id === id));
        throw new Error(`A' link did not come up: ${String(error)}\nfile side sees ${natsMain.id}: ${rows(fileMain, natsMain.id)}\n` +
          `nats side sees ${fileMain.id}: ${rows(natsMain, fileMain.id)}\nKV ${BRIDGE_MACHINE}: ${JSON.stringify(await harness.kvRoot(BRIDGE_MACHINE, fileMain.id))}\n` +
          (actor ? `file side actor: ${rows(fileMain, actor.id)}\nnats side actor: ${rows(natsMain, actor.id)}\n` : "") +
          `${linkLog.slice(-40).join("\n")}`, { cause: error });
      }
    }
    return harness;
  } catch (error) {
    await unwind();
    vi.unstubAllEnvs();
    fs.rmSync(root, { recursive: true, force: true });
    throw error;
  }
}

/** The link's log tail, for a failing assertion's message. */
export const linkTail = (h: MixedHarness, lines = 40): string => h.linkLog.slice(-lines).join("\n");
