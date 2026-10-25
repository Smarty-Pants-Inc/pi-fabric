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
// The link contract (APRIME2 "Interface contracts"): `startNatsLink(config)` in src/mesh/bridge-nats-link.ts
// (L2), config schema owned by L5, shared types in src/mesh/bridge-nats-types.ts (L3). Until those land,
// this file codes against the local MixedLinkConfig/MixedLinkHandle stubs below and adapts them in
// `adaptLinkConfig`, the one place to change when the real schema arrives.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import { pathToFileURL } from "node:url";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { vi } from "vitest";
import { CapturedToolCatalog } from "../../src/capture/catalog.js";
import { normalizeFabricConfig, type FabricConfig } from "../../src/config.js";
import { FabricRuntimeState } from "../../src/fabric-runtime-state.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../../src/mesh/store.js";
import { openFabricTransport } from "../../src/transport/index.js";
import { runTransportAdmin } from "../../src/transport/nats/admin.js";
import { hashToken } from "../../src/transport/nats/subjects.js";
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
export const rootId = (session: string): string => `session:${session}`;

// ---------------------------------------------------------------------------------------------
// Link contract stubs (replace with src/mesh/bridge-nats-types.ts once L3 publishes it).
// ---------------------------------------------------------------------------------------------

/** What the harness knows about the link; `adaptLinkConfig` maps it to the real L5 schema. */
export interface MixedLinkConfig {
  /** Link name: the `remoteHost` stamped on NATS roots mirrored into the hub file mesh. */
  readonly name: string;
  /** The hub's FILE mesh root (Ryzen 1). */
  readonly meshRoot: string;
  /** Link-owned durable state (cursor, ACK/dedupe ledger); kept across restarts. */
  readonly stateDir: string;
  /** The bridge machine's own NATS runtime options; `servers` is the fault relay. */
  readonly nats: {
    readonly servers: string;
    readonly credentials: string;
    readonly machine: string;
    readonly custodyStream: string;
    readonly livenessStream: string;
    readonly pathBMachines: readonly string[];
    readonly tls?: { readonly caFile: string };
  };
  /** Static allow-list (L3): canonical ids admitted per side; nothing else crosses. */
  readonly allow: {
    readonly fileRoots: readonly string[];
    readonly natsRoots: readonly string[];
    readonly fileActors?: readonly string[];
  };
}

export interface MixedLinkHandle {
  /** Graceful stop: fences mirrors, joins in-flight work, closes NATS. */
  stop(): Promise<void>;
}

export type StartMixedLink = (config: MixedLinkConfig) => Promise<MixedLinkHandle>;

/** The product entry, absent until L2 lands. A missing module returns undefined; any other import error throws. */
export async function loadNatsLink(): Promise<StartMixedLink | undefined> {
  const file = path.resolve(import.meta.dirname, "../../src/mesh/bridge-nats-link.ts");
  if (!fs.existsSync(file)) return undefined;
  const module = await import(pathToFileURL(file).href) as Record<string, unknown>;
  const start = module.startNatsLink;
  if (typeof start !== "function") throw new Error("src/mesh/bridge-nats-link.ts does not export startNatsLink");
  return async (config) => {
    const handle = await (start as (input: unknown) => Promise<unknown>)(adaptLinkConfig(config)) as Partial<MixedLinkHandle> & { close?: () => Promise<void> };
    const stop = handle.stop ?? handle.close;
    if (typeof stop !== "function") throw new Error("startNatsLink handle has no stop()");
    return { stop: () => stop.call(handle) };
  };
}

/** The single mapping point from the harness view to the L5 config schema. */
export function adaptLinkConfig(config: MixedLinkConfig): unknown {
  return config;
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

function makeMain(label: string, sessionId: string, cwd: string, config: FabricConfig): MixedMain {
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
    } as unknown as ExtensionContext;
    const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
      extension: path.join(cwd, "unused-extension.ts"), worker: path.join(cwd, "unused-worker.ts"),
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

function baseConfig(mesh: Record<string, unknown>): FabricConfig {
  return normalizeFabricConfig({ fullCodeMode: false,
    mesh: { enabled: true, actorPollMs: 20, ...mesh },
    agents: { enabled: false }, residency: { enabled: false }, mcp: { enabled: false }, memory: { enabled: false },
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

/** Records every in-process NatsTransport send/deliver/broker-ACK. Install per test (restoreMocks). */
export class NatsWire {
  readonly events: WireEvent[] = [];
  private readonly changed = new EventEmitter();
  constructor(prototype: FabricTransport) {
    const send = prototype.send, inbox = prototype.inbox;
    const record = (event: WireEvent) => { this.events.push(event); this.changed.emit("change"); };
    vi.spyOn(prototype, "send").mockImplementation(async function<T>(this: FabricTransport, envelope: TransportEnvelope<T>, signal?: AbortSignal) {
      const result = await send.call(this, envelope, signal) as TransportSent<T>;
      record({ phase: "send", machine: this.self.machine, hostId: this.self.hostId, envelope: result.envelope as TransportEnvelope,
        sequence: result.sequence, duplicate: result.duplicate });
      return result;
    });
    vi.spyOn(prototype, "inbox").mockImplementation(async function*(this: FabricTransport, signal: AbortSignal) {
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
    });
  }

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
  /** Link starter; default `loadNatsLink()`. `null` boots both Mains with no link (harness self-test). */
  readonly startLink?: StartMixedLink | null;
  readonly linkName?: string;
  /** Also boot a live file root that the link does not allow-list. */
  readonly outsider?: boolean;
}

export interface MixedHarness {
  readonly root: string;
  readonly broker: PathBHarness;
  /** The link's path to the broker. Faults here touch the link only. */
  readonly faults: NatsWireFaults;
  readonly fileMain: MixedMain;
  readonly natsMain: MixedMain;
  readonly outsider?: MixedMain;
  readonly linkName: string;
  readonly linkConfig: MixedLinkConfig;
  /** The file mesh the link bridges (Ryzen 1's): same root as fileMain's runtime mesh. */
  readonly fileMeshRoot: string;
  /** A raw product transport on machine-a with a test-only host id, for broker reads (KV presence/leases). */
  readonly inspector: FabricTransport;
  /** Prototype shared by every in-process NatsTransport (Mains, inspector and an in-process link). */
  readonly natsPrototype: FabricTransport;
  link: MixedLinkHandle | undefined;
  readonly startLink: StartMixedLink | undefined;
  startLinkNow(): Promise<MixedLinkHandle>;
  stopLink(): Promise<void>;
  /** Open a product NATS transport for a credential with an arbitrary host id (negative tests). */
  openPeer(machine: typeof BRIDGE_MACHINE | typeof NATS_MAIN_MACHINE, hostId: string, viaRelay?: boolean): Promise<FabricTransport>;
  /** A fresh MeshStore on the hub file mesh (a separate reader/writer, like another local process). */
  fileStore(): MeshStore;
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

    const fileCwd = path.join(root, "ryzen1"), natsCwd = path.join(root, "v2"), outsiderCwd = path.join(root, "ryzen1-outsider");
    for (const dir of [fileCwd, natsCwd, outsiderCwd, path.join(root, "agent")]) fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    const fileMeshRoot = path.join(root, "ryzen1-mesh");
    const fileConfig = baseConfig({ transport: "file", hostTransport: "file", root: fileMeshRoot, announce: true });
    const natsConfig = baseConfig({ transport: "nats", hostTransport: "nats", root: path.join(natsCwd, "mesh"), announce: true,
      nats: { ...broker.runtimeOptions(broker.users.machineB), operatorMachine: "fabric-ops" } });
    const fileMain = makeMain("ryzen1-lead", FILE_MAIN_SESSION, fileCwd, fileConfig);
    const natsMain = makeMain("v2-lead", NATS_MAIN_SESSION, natsCwd, natsConfig);
    const outsider = options.outsider ? makeMain("ryzen1-outsider", FILE_OUTSIDER_SESSION, outsiderCwd, fileConfig) : undefined;

    const inspector = await openFabricTransport({ config: baseConfig({ transport: "nats", hostTransport: "nats", root: path.join(root, "inspector-mesh"),
      nats: { ...broker.runtimeOptions(broker.users.machineA), operatorMachine: "fabric-ops" } }).mesh,
      hostId: "mixed-harness-inspector", mesh: new MeshStore(path.join(root, "inspector-mesh"), 65_536, 1_000) });
    cleanup.push(() => teardownWithin("inspector close", () => inspector.close(), 5_000));
    const natsPrototype = Object.getPrototypeOf(inspector) as FabricTransport;

    await fileMain.start();
    cleanup.push(() => fileMain.stop());
    await natsMain.start();
    cleanup.push(() => natsMain.stop());
    if (outsider) { await outsider.start(); cleanup.push(() => outsider.stop()); }

    const linkName = options.linkName ?? DEFAULT_LINK_NAME;
    const linkConfig: MixedLinkConfig = {
      name: linkName, meshRoot: fileMeshRoot, stateDir: path.join(root, "link-state"),
      nats: { servers: faults.url, credentials: broker.users.machineA.credsPath, machine: BRIDGE_MACHINE,
        custodyStream: PATH_B_PERMISSION_POLICY.custodyStream, livenessStream: PATH_B_PERMISSION_POLICY.livenessStream,
        pathBMachines: [...PATH_B_MACHINES], tls: { caFile: broker.tls!.caFile } },
      allow: { fileRoots: [fileMain.id], natsRoots: [natsMain.id] },
    };
    const startLink = options.startLink === null ? undefined : options.startLink ?? await loadNatsLink();
    const peers: FabricTransport[] = [];
    cleanup.push(async () => { await Promise.allSettled(peers.splice(0).map((peer) => teardownWithin("peer close", () => peer.close(), 5_000))); });

    const harness: MixedHarness = {
      root, broker, faults, fileMain, natsMain, ...(outsider ? { outsider } : {}), linkName, linkConfig, fileMeshRoot, inspector, natsPrototype,
      link: undefined, startLink,
      async startLinkNow() {
        if (!startLink) throw new Error("A' link entry (src/mesh/bridge-nats-link.ts startNatsLink) is not available");
        if (harness.link) throw new Error("link already running");
        harness.link = await startLink(linkConfig);
        return harness.link;
      },
      async stopLink() {
        const link = harness.link;
        harness.link = undefined;
        if (link) await teardownWithin("link stop", () => link.stop(), 15_000);
      },
      async openPeer(machine, hostId, viaRelay = false) {
        const user = machine === BRIDGE_MACHINE ? broker.users.machineA : broker.users.machineB;
        const runtime = broker.runtimeOptions(user);
        const config = baseConfig({ transport: "nats", hostTransport: "nats", root: path.join(root, `peer-${peers.length}`),
          nats: { ...runtime, ...(viaRelay ? { servers: faults.url } : {}), operatorMachine: "fabric-ops" } });
        const peer = await openFabricTransport({ config: config.mesh, hostId, mesh: new MeshStore(path.join(root, `peer-${peers.length}`), 65_536, 1_000) });
        peers.push(peer);
        return peer;
      },
      fileStore: () => new MeshStore(fileMeshRoot, 65_536, 1_000),
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
        const fileView = await waitFor(() => fileMain.runtime.participantInfos({ fresh: true })
          .find((row) => row.id === natsMain.id && !row.stale && row.remoteHost === linkName), "NATS root mirrored live into the file mesh", timeoutMs);
        const natsView = await waitFor(() => natsMain.runtime.participantInfos({ fresh: true })
          .find((row) => row.id === fileMain.id && !row.stale && row.machine === BRIDGE_MACHINE), "file root mirrored live into NATS KV", timeoutMs);
        return { fileView, natsView };
      },
      async stop() {
        try { await harness.stopLink(); } catch (error) { failures.push(error); }
        await unwind();
        vi.unstubAllEnvs();
        try { fs.rmSync(root, { recursive: true, force: true }); } catch (error) { failures.push(error); }
        if (failures.length) throw new AggregateError(failures, failures.map(String).join("; "));
      },
    };
    return harness;
  } catch (error) {
    await unwind();
    vi.unstubAllEnvs();
    fs.rmSync(root, { recursive: true, force: true });
    throw error;
  }
}

/** sha256 of the logical command key, as both carriers derive it (control-plane.ts request paths). */
export function expectedCommandId(senderHostId: string, ownerHostId: string, ownerIdentityId: string, targetId: string,
  operation: string, idempotencyKey: string, notRunAttempt = 0): string {
  return createHash("sha256").update(JSON.stringify([senderHostId, ownerHostId, ownerIdentityId, targetId, operation, idempotencyKey, notRunAttempt])).digest("hex");
}
