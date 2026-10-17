// expected-skip: no NATS_BIN/PATH executable: a private authenticated loopback broker is required.
// Keep reason: trust boundary + sender data safety across two native runtime owners.
// Official assumptions (not logical actor dedupe): every AckExplicit delivery needs an ACK;
// ackAck waits for server confirmation, and AckWait is a redelivery deadline, not completion.
// https://docs.nats.io/nats-concepts/jetstream/consumers
// https://docs.nats.io/learn/jetstream/acknowledgment
// https://docs.nats.io/learn/jetstream/delivery-and-acknowledgment
// Nats-Msg-Id dedupe is bounded by the broker window; it does NOT prove actor admission dedupe.
// https://docs.nats.io/nats-concepts/jetstream/headers
// https://docs.nats.io/reference/2.14/jetstream/api/headers
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { createInterface } from "node:readline";
import { EventEmitter } from "node:events";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig, type FabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { MeshStore } from "../src/mesh/store.js";
import { openFabricTransport } from "../src/transport/index.js";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import type { FabricTransport, TransportEnvelope, TransportSent } from "../src/transport/types.js";
import type { FabricActorInfo, FabricActorMessage } from "../src/actors/types.js";
import type { AgentRunRecord } from "../src/agents/types.js";
import { transportDurableName, transportInboxStream } from "../src/transport/nats/subjects.js";
import { createPathBHarness, openJetStream, PATH_B_MACHINES, resolveNatsBinary, teardownWithin, type PathBHarness } from "./helpers/nats-cluster.js";
import { launchLog, stopOwned, type LaunchLog, type Owned } from "./helpers/owned-processes.js";
import { captureDescendants, observeProcessTree, type ObservedProcessTree } from "../src/residency/launcher-owner.js";

const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1: cross-owner actor ask requires NATS_BIN");
const repository = path.resolve(import.meta.dirname, "..");
const ownerSession = "14115aaa-bbbb-4000-8000-000000000011";
const senderSession = "14115bbb-bbbb-4000-8000-000000000012";
const ownerId = `session:${ownerSession}`;
const senderId = `session:${senderSession}`;
const reply = "PATH_B_ACTOR_REPLY_" + randomUUID();

type WireEvent = { phase: "send" | "broker-ack"; hostId: string; envelope: TransportEnvelope; sequence?: number; duplicate?: boolean };
type OwnerReady = { pid: number; root: string; hostId: string };
type OwnerEvidence = { actor: FabricActorInfo; messages: FabricActorMessage[]; stages: Array<Partial<FabricActorInfo>>;
  events: WireEvent[]; queueFiles: string[]; runs: AgentRunRecord[]; session: string; modelEvents: string };
function observation(prototype: FabricTransport, events: WireEvent[], changed: EventEmitter) {
  const send = prototype.send, inbox = prototype.inbox;
  vi.spyOn(prototype, "send").mockImplementation(async function<T>(this: FabricTransport, envelope: TransportEnvelope<T>, signal?: AbortSignal) {
    const result = await send.call(this, envelope, signal) as TransportSent<T>;
    events.push({ phase: "send", hostId: this.self.hostId, envelope: result.envelope, sequence: result.sequence, duplicate: result.duplicate });
    changed.emit("change");
    return result;
  });
  vi.spyOn(prototype, "inbox").mockImplementation(async function*(this: FabricTransport, signal: AbortSignal) {
    for await (const delivery of inbox.call(this, signal)) {
      yield { ...delivery, ack: async () => {
        await delivery.ack(); // real NatsTransport -> JsMsg.ackAck(); never manufacture a receipt
        events.push({ phase: "broker-ack", hostId: this.self.hostId, envelope: delivery.payload, sequence: delivery.sequence });
        changed.emit("change");
      } };
    }
  });
}
function until(check: () => boolean, subscribe: (listener: () => void) => () => void, label: string, ms = 15_000) {
  return new Promise<void>((resolve, reject) => {
    let unsubscribe = () => {};
    const timer = setTimeout(() => { unsubscribe(); reject(new Error(`Timed out: ${label}`)); }, ms);
    const test = () => { try { if (check()) { clearTimeout(timer); unsubscribe(); resolve(); } } catch (error) { clearTimeout(timer); unsubscribe(); reject(error); } };
    unsubscribe = subscribe(test);
    test();
  });
}
function onChange(emitter: EventEmitter) {
  return (listener: () => void) => { emitter.on("change", listener); return () => { emitter.off("change", listener); }; };
}
function configFor(broker: PathBHarness, root: string, machine: "machine-a" | "machine-b"): FabricConfig {
  return normalizeFabricConfig({ fullCodeMode: false,
    mesh: { enabled: true, transport: "nats", hostTransport: "nats", root: path.join(root, "mesh"), actorPollMs: 20,
      nats: { ...broker.runtimeOptions(machine === "machine-a" ? broker.users.machineA : broker.users.machineB), operatorMachine: "fabric-ops" } },
    agents: { enabled: true, transport: "process", model: "", maxConcurrent: 1, timeoutMs: 20_000, nice: 0 },
    residency: { enabled: false }, mcp: { enabled: false }, memory: { enabled: false },
    jev: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
  });
}
function runtimeFor(root: string, sessionId: string) {
  const pi = { on: () => () => {}, events: { emit() {}, on: () => () => {} }, getThinkingLevel: () => "off",
    getSessionName: () => "path-b-sender", appendEntry() {}, sendMessage() {},
  } as unknown as ExtensionAPI;
  const context = { cwd: root, hasUI: false, mode: "rpc", isProjectTrusted: () => true,
    isIdle: () => true, hasPendingMessages: () => false,
    modelRegistry: { getAvailable: () => [], find: () => undefined, getApiKeyAndHeaders: async () => undefined },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => null, getEntries: () => [] },
    ui: { setStatus() {}, notify() {} },
  } as unknown as ExtensionContext;
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
    extension: path.join(root, "unused-extension.ts"), worker: path.join(root, "unused-worker.ts"), residentHost: path.join(root, "unused-resident.ts"), skills: root,
  } });
  const invoke = (ref: string, args: Record<string, unknown>) => runtime.registry.invoke(ref, args, {
    cwd: root, signal: AbortSignal.timeout(30_000), parentToolCallId: "path-b-actor-ask", nestedToolCallId: ref, extensionContext: context,
    update() {}, approve: async () => {}, audits: [], maxResultChars: 50_000,
  });
  return { runtime, context, invoke, nativeShutdown: runtime.shutdown.bind(runtime) };
}

// Generated only in this test's private temp tree. Both the control dispatcher and actor
// queue are created by FabricRuntimeState.initialize; no hand-built receiver/managers.
// Supported seams: runtime paths -> AgentManager (src/fabric-runtime-state.ts:1293-1297),
// source/custom worker selection (src/agents/transports/process-transport.ts:25-37),
// native provider registration before startup model selection (Pi docs/custom-provider.md).
// ActorManager permits built-in runners only (src/actors/manager.ts:1342-1345), so retain
// real Pi worker + source Fabric resource pin; discover faux via explicit profile extension.
function ownerSource() {
  const source = (name: string) => JSON.stringify(path.join(repository, "src", name));
  const sdk = JSON.stringify(path.join(repository, "node_modules/@earendil-works/pi-coding-agent/dist/index.js"));
  const ai = JSON.stringify(path.join(repository, "node_modules/@earendil-works/pi-ai/dist/index.js"));
  return `
import fs from "node:fs";
import path from "node:path";
import { createInterface } from "node:readline";
import { EventEmitter } from "node:events";
import { ModelRuntime, ModelRegistry } from ${sdk};
import { fauxProvider } from ${ai};
import { FabricRuntimeState } from ${source("fabric-runtime-state.ts")};
import { CapturedToolCatalog } from ${source("capture/catalog.ts")};
import { MeshStore } from ${source("mesh/store.ts")};
import { openFabricTransport } from ${source("transport/index.ts")};
const input = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const { root, config, sessionId } = input;
await import(input.ownershipPreload);
// Supported native model registration and the production worker/launcher. The only
// deterministic boundary is the keyless scripted model, never control/admission/ACK.
const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "agent/auth.json") });
const faux = fauxProvider({ provider: "nats-ask-faux", models: [{ id: "one-turn" }] });
modelRuntime.registerNativeProvider(faux.provider);
await modelRuntime.getAvailable("nats-ask-faux");
const providerFile = path.join(root, "faux-provider.ts"), workerFile = path.join(root, "worker.ts");
fs.writeFileSync(providerFile, "import " + JSON.stringify(input.ownershipPreload) + ";\\n" + ${JSON.stringify(`
import fs from "node:fs";
import { fauxProvider, fauxAssistantMessage } from ${ai};
export default function(pi) {
 const input = JSON.parse(fs.readFileSync(${JSON.stringify("__INPUT_FILE__")}, "utf8"));
 const faux = fauxProvider({ provider: "nats-ask-faux", models: [{ id: "one-turn" }] });
 const birth = pid => fs.readFileSync("/proc/" + pid + "/stat", "utf8").split(")").at(-1).trim().split(/\\s+/)[19];
 faux.setResponses([fauxAssistantMessage(input.reply)]);
 pi.registerProvider(faux.provider);
 pi.on("agent_start", async () => {
  fs.appendFileSync(input.root + "/model-events.jsonl", JSON.stringify({ type: "agent_start", pid: process.pid, birth: birth(process.pid), workerPid: process.ppid, workerBirth: birth(process.ppid) }) + "\\n");
  if (input.stallShutdown) await new Promise(() => { setInterval(() => {}, 1000); });
 });
 pi.on("turn_end", () => fs.appendFileSync(input.root + "/model-events.jsonl", JSON.stringify({ type: "turn_end", pid: process.pid, calls: faux.state.callCount }) + "\\n"));
}
`)}.replace('"__INPUT_FILE__"', JSON.stringify(process.argv[2])));
fs.writeFileSync(workerFile, "import " + JSON.stringify(input.ownershipPreload) + ";\\nawait import(" + JSON.stringify(${source("worker.ts")}) + ");\\n");
fs.mkdirSync(path.join(root, "agent"), { recursive: true });
fs.writeFileSync(path.join(root, "agent/settings.json"), JSON.stringify({ extensions: [providerFile], compaction: { enabled: false }, retry: { enabled: false } }));
fs.writeFileSync(path.join(root, "agent/fabric.json"), JSON.stringify(config));
const events = [], stages = [], changes = new EventEmitter();
const probe = await openFabricTransport({ config: config.mesh, hostId: "owner-inspector", mesh: new MeshStore(path.join(root, "inspector"), 65536, 1000) });
const prototype = Object.getPrototypeOf(probe), rawSend = prototype.send, rawInbox = prototype.inbox;
prototype.send = async function(envelope, signal) {
  const sent = await rawSend.call(this, envelope, signal);
  events.push({ phase: "send", hostId: this.self.hostId, envelope: sent.envelope, sequence: sent.sequence, duplicate: sent.duplicate }); changes.emit("change");
  return sent;
};
prototype.inbox = async function*(signal) {
  for await (const delivery of rawInbox.call(this, signal)) yield { ...delivery, ack: async () => {
    await delivery.ack();
    events.push({ phase: "broker-ack", hostId: this.self.hostId, envelope: delivery.payload, sequence: delivery.sequence }); changes.emit("change");
  } };
};
await probe.close();
const pi = { on: () => () => {}, events: { emit() {}, on: () => () => {} }, getThinkingLevel: () => "off", getSessionName: () => "path-b-owner", appendEntry() {}, sendMessage() {} };
const context = { cwd: root, hasUI: false, mode: "rpc", isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
 model: faux.getModel(), modelRegistry: new ModelRegistry(modelRuntime),
 sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => null, getEntries: () => [] }, ui: { setStatus() {}, notify() {} } };
const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
 extension: ${source("index.ts")}, worker: workerFile, residentHost: path.join(root, "unused-resident.ts"), skills: root } });
const invoke = (ref, args) => runtime.registry.invoke(ref, args, { cwd: root, signal: AbortSignal.timeout(30000), parentToolCallId: "path-b-owner", nestedToolCallId: ref, extensionContext: context,
 update() {}, approve: async () => {}, audits: [], maxResultChars: 50000 });
await runtime.initialize(context, config);
if (input.stallShutdown) runtime.shutdown = async () => {
 fs.writeFileSync(path.join(root, "shutdown-entered"), String(process.pid));
 await new Promise(() => {});
};
let actor;
const unsubscribe = runtime.actors.subscribe(() => { if (actor) {
 const { status, queued, messages, preparing, inFlightRun, lastRunId } = runtime.actors.status(actor.id);
 stages.push({ status, queued, messages, preparing, inFlightRun, lastRunId }); changes.emit("change");
} });
const respond = (id, value, error) => process.stdout.write("NATS_ASK_RPC " + JSON.stringify({ id, value, error }) + "\\n");
const wait = (check) => new Promise((resolve, reject) => {
 const test = () => { if (check()) { clearTimeout(timer); changes.off("change", test); resolve(); } };
 const timer = setTimeout(() => { changes.off("change", test); reject(new Error("owner evidence barrier timed out")); }, 15000);
 changes.on("change", test); test();
});
respond(0, { pid: process.pid, root, hostId: "session:" + sessionId });
const commands = createInterface({ input: process.stdin });
for await (const line of commands) {
 const request = JSON.parse(line);
 try {
  if (request.method === "create") { actor = await invoke("agents.create", request.args); respond(request.id, actor); }
  else if (request.method === "evidence") {
   await wait(() => runtime.actors.inFlightCount() === 0 && events.some(event => event.phase === "broker-ack" && event.envelope.kind === "command" &&
     (!request.args?.eventId || event.envelope.id === request.args.eventId)));
   respond(request.id, { actor: runtime.actors.status(actor.id), messages: runtime.actors.messages(actor.id), stages, events,
    queueFiles: fs.readdirSync(path.dirname(actor.sessionFile)).filter(name => name.startsWith("queue-")),
    runs: fs.existsSync(actor.logDir) ? fs.readdirSync(actor.logDir).filter(name => fs.existsSync(path.join(actor.logDir, name, "status.json")))
      .map(name => JSON.parse(fs.readFileSync(path.join(actor.logDir, name, "status.json"), "utf8"))) : [],
    session: fs.existsSync(actor.sessionFile) ? fs.readFileSync(actor.sessionFile, "utf8") : "",
    modelEvents: fs.existsSync(path.join(root, "model-events.jsonl")) ? fs.readFileSync(path.join(root, "model-events.jsonl"), "utf8") : "" });
  } else if (request.method === "shutdown") {
   unsubscribe(); await runtime.shutdown("exit");
   // Close the fixture RPC stream only after native runtime shutdown has joined
   // its queues/transports. Let Bun exit naturally; SIGKILL is a test failure.
   commands.close();
   await new Promise(resolve => process.stdout.write("NATS_ASK_RPC " + JSON.stringify({ id: request.id, value: { shutdown: true } }) + "\\n", resolve));
   process.stdin.destroy();
   break;
  }
  else throw new Error("Unknown owner RPC");
 } catch (error) { respond(request.id, undefined, String(error?.stack ?? error)); }
}
`;
}
class OwnerProcess {
  readonly child: ChildProcess;
  readonly ready: Promise<OwnerReady>;
  private serial = 0;
  private pending = new Map<number, { resolve(value: unknown): void; reject(error: Error): void; timer: NodeJS.Timeout }>();
  private stderr = "";
  private readonly tree: ObservedProcessTree;
  private readonly closed: Promise<void>;
  private readonly lines: ReturnType<typeof createInterface>;
  readonly forced: Array<{ pid: number; started: string }> = [];
  readonly liveAtForce: Array<{ pid: number; started: string }> = [];
  constructor(file: string, inputFile: string, environment: NodeJS.ProcessEnv, private readonly launches: LaunchLog) {
    this.child = spawn("bun", [file, inputFile], { env: environment, stdio: ["pipe", "pipe", "pipe"] });
    this.tree = this.child.pid ? observeProcessTree(this.child.pid) : { processes: new Map() };
    this.closed = new Promise(resolve => this.child.once("close", () => resolve()));
    this.ready = this.response<OwnerReady>(0);
    void this.ready.catch(() => undefined);
    this.child.stderr!.on("data", chunk => { this.stderr = (this.stderr + String(chunk)).slice(-12_000); });
    this.lines = createInterface({ input: this.child.stdout! }).on("line", line => {
      if (!line.startsWith("NATS_ASK_RPC ")) return;
      const message = JSON.parse(line.slice("NATS_ASK_RPC ".length));
      const waiter = this.pending.get(message.id);
      if (waiter) { clearTimeout(waiter.timer); this.pending.delete(message.id); message.error ? waiter.reject(new Error(message.error)) : waiter.resolve(message.value); }
    });
    const failed = (error: Error) => { for (const pending of this.pending.values()) { clearTimeout(pending.timer); pending.reject(error); } this.pending.clear(); };
    this.child.stdin!.on("error", failed);
    this.child.once("error", failed);
    this.child.once("exit", (code, signal) => failed(new Error(`Owner exited ${code}/${signal}: ${this.stderr}`)));
  }
  private response<T>(id: number) {
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`Owner RPC ${id} timed out: ${this.stderr}`)); }, 35_000);
      this.pending.set(id, { resolve: value => resolve(value as T), reject, timer });
    });
  }
  call<T>(method: string, args?: unknown) {
    const id = ++this.serial, result = this.response<T>(id);
    void result.catch(() => undefined); // Also covers a synchronous pipe-write failure.
    this.child.stdin!.write(JSON.stringify({ id, method, args }) + "\n");
    return result;
  }
  private ownedProcesses(): Owned[] {
    // Sampling supplements launch receipts for early startup failures, but is NOT
    // the membership proof: explicit Bun wrapper + native profile self-recording
    // retains the known worker/model births before the owner can orphan them.
    captureDescendants(this.tree);
    const sampled = [...this.tree.processes.values()].map(row => ({ pid: row.pid, started: row.processStartTime, at: 0, argv: [] }));
    return [...new Map([...sampled, ...this.launches.owned()].map(row => [`${row.pid}:${row.started}`, row])).values()];
  }
  async stop() {
    const failures: unknown[] = [];
    let owned = this.ownedProcesses(); // Retain detached levels BEFORE requesting shutdown.
    try {
      if (this.child.exitCode === null && this.child.signalCode === null) {
        await teardownWithin("owner shutdown", () => this.call("shutdown"), 3_000);
        this.child.stdin!.end();
      }
      await teardownWithin("owner native close", () => this.closed);
      owned = this.ownedProcesses();
      if (owned.some(row => processBirth(row.pid) === row.started)) throw new Error("Owner shutdown left owned descendants alive");
      if (this.child.exitCode !== 0) throw new Error(`Owner shutdown was not clean: ${this.child.exitCode}/${this.child.signalCode}`);
    } catch (error) { failures.push(error); }
    finally {
      // Even RPC/native-exit failure must reach every retained detached birth.
      try {
        owned = this.ownedProcesses();
        this.liveAtForce.push(...owned.filter(row => processBirth(row.pid) === row.started).map(({ pid, started }) => ({ pid, started })));
        // Leaf first so the live production worker/owner can reap their children.
        for (const row of [...owned].reverse()) {
          try {
            if (processBirth(row.pid) !== row.started) continue;
            this.forced.push({ pid: row.pid, started: row.started });
            const remaining = await stopOwned(row, 300, 1_000);
            if (remaining !== undefined || processBirth(row.pid) === row.started) failures.push(new Error(`Owned birth did not exit: ${row.pid}:${row.started}`));
          } catch (error) { failures.push(error); }
        }
      } catch (error) { failures.push(error); }
      try { await teardownWithin("owner forced native close", () => this.closed); } catch (error) { failures.push(error); }
      for (const waiter of this.pending.values()) { clearTimeout(waiter.timer); waiter.reject(new Error("Owner teardown finished")); }
      this.pending.clear();
      this.lines.close(); this.child.stdin?.destroy(); this.child.stdout?.destroy(); this.child.stderr?.destroy();
    }
    if (failures.length) throw new AggregateError(failures, failures.map(String).join("; "));
  }
}
function processBirth(pid: number): string | undefined {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    return stat.slice(stat.lastIndexOf(")") + 1).trim().split(/\s+/)[19];
  }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw error; }
}
async function cleanupPathB(root: string, sender: ReturnType<typeof runtimeFor>, owner?: OwnerProcess, inspector?: FabricTransport, broker?: PathBHarness) {
  const failures: unknown[] = [];
  const attempt = async (operation: () => Promise<unknown>) => { try { await operation(); } catch (error) { failures.push(error); } };
  const brokerChild = broker?.serverProcess;
  let senderFailed = false;
  try {
    // Each participant owns a bounded join; neither failure suppresses the other.
    await Promise.all([
      attempt(async () => {
        try { await teardownWithin("sender shutdown", () => sender.runtime.shutdown("exit")); }
        catch (error) { senderFailed = true; throw error; }
      }),
      attempt(() => owner?.stop() ?? Promise.resolve()),
    ]);
  } finally {
    // The fixture retains the real close capability even when its public shutdown
    // completion seam is stalled. Close native sender resources BEFORE root removal;
    // do not merely race away from a live RPC/transport in the Vitest process.
    if (senderFailed) await attempt(() => teardownWithin("sender native fallback", () => sender.nativeShutdown("exit")));
    try { await attempt(() => teardownWithin("inspector close", () => inspector?.close() ?? Promise.resolve())); }
    finally {
      try { await attempt(() => broker?.stop() ?? Promise.resolve()); }
      finally {
        vi.restoreAllMocks(); vi.unstubAllEnvs();
        try { fs.rmSync(root, { recursive: true, force: true }); } catch (error) { failures.push(error); }
      }
    }
  }
  console.log("PATH_B_ASK_CLEANUP", JSON.stringify({ ownerPid: owner?.child.pid, ownerExitCode: owner?.child.exitCode,
    ownerSignal: owner?.child.signalCode, forced: owner?.forced, liveAtForce: owner?.liveAtForce, brokerPid: brokerChild?.pid, brokerExitCode: brokerChild?.exitCode,
    brokerSignal: brokerChild?.signalCode, privateRootExists: fs.existsSync(root), failures: failures.map(String) }));
  if (failures.length) throw new AggregateError(failures, failures.map(String).join("; "));
}
function watchFile(root: string, check: () => boolean, label: string) {
  return until(check, listener => {
    const watcher = fs.watch(root, listener);
    return () => watcher.close();
  }, label);
}
async function guardedWait<T>(operation: Promise<T>, ms: number, label: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try { return await Promise.race([operation, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error(label)), ms); })]); }
  finally { clearTimeout(timer); }
}
function snapshot(root: string) {
  const bytes: Record<string, string> = {};
  const visit = (file: string) => {
    if (!fs.existsSync(file)) { bytes[path.relative(root, file)] = "<absent>"; return; }
    if (fs.statSync(file).isDirectory()) {
      bytes[path.relative(root, file) + "/"] = "<directory>";
      for (const name of fs.readdirSync(file).sort()) visit(path.join(file, name));
    } else bytes[path.relative(root, file)] = fs.readFileSync(file).toString("base64");
  };
  visit(path.join(root, "events.jsonl")); visit(path.join(root, "archive"));
  return bytes;
}

describe.skipIf(!binary || process.platform !== "linux")("Path B cross-process actor ask", () => {
  it("runs one genuine actor activation, returns its correlated reply, and confirms both inbox ACKs without sender event-log writes", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "nats-actor-ask-"));
    fs.chmodSync(root, 0o700);
    const launches = launchLog(root);
    const senderRoot = path.join(root, "sender"), ownerRoot = path.join(root, "owner");
    for (const dir of [senderRoot, ownerRoot]) fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    vi.stubEnv("SMARTY_AGENT", undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(senderRoot, "agent"));
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", senderRoot);
    let broker: PathBHarness | undefined, owner: OwnerProcess | undefined, inspector: FabricTransport | undefined;
    const sender = runtimeFor(senderRoot, senderSession);
    const events: WireEvent[] = [], changed = new EventEmitter();
    try {
      vi.stubEnv("TMPDIR", root);
      broker = await createPathBHarness({ ...(binary ? { binary } : {}), tls: {} });
      for (const [name, value] of Object.entries(broker.adminEnv(broker.users.bootstrap))) vi.stubEnv(name, value);
      expect(await runTransportAdmin(["provision", "--ca-file", broker.tls!.caFile, "--machines", PATH_B_MACHINES.join(",")])).toBe(0);
      for (const name of Object.keys(broker.adminEnv(broker.users.bootstrap))) vi.stubEnv(name, undefined);
      const ownerConfig = configFor(broker, ownerRoot, "machine-a"), senderConfig = configFor(broker, senderRoot, "machine-b");
      const senderMesh = senderConfig.mesh.root!;
      fs.mkdirSync(path.join(senderMesh, "archive"), { recursive: true });
      fs.writeFileSync(path.join(senderMesh, "event-archive.json"), JSON.stringify({ version: 1, dir: path.join(senderMesh, "archive") }));
      const ownerFile = path.join(root, "owner.ts"), inputFile = path.join(root, "owner-input.json");
      fs.writeFileSync(ownerFile, ownerSource());
      fs.writeFileSync(inputFile, JSON.stringify({ root: ownerRoot, config: ownerConfig, sessionId: ownerSession, reply, ownershipPreload: path.join(root, "launch-preload.mjs") }));
      owner = new OwnerProcess(ownerFile, inputFile, { ...process.env, ...launches.env, PI_CODING_AGENT_DIR: path.join(ownerRoot, "agent"), PI_FABRIC_PROJECT_ROOT: ownerRoot,
        PI_FABRIC_PI_BINARY: path.join(repository, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js"), TMPDIR: ownerRoot }, launches);
      const ready = await owner.ready;
      expect(ready).toMatchObject({ root: ownerRoot, hostId: ownerId });
      expect(ready.pid).not.toBe(process.pid); expect(ownerRoot).not.toBe(senderRoot);
      inspector = await openFabricTransport({ config: senderConfig.mesh, hostId: "sender-inspector", mesh: new MeshStore(path.join(senderRoot, "inspector"), 65_536, 1_000) });
      observation(Object.getPrototypeOf(inspector), events, changed);
      await sender.runtime.initialize(sender.context, senderConfig);
      const actor = await owner.call<FabricActorInfo>("create", { name: "nats-ask-actor", scope: "session", instructions: "Reply exactly once.", responseMode: "text", coalesce: false });
      await sender.runtime.mesh.publish({ topic: "test.sender-baseline", kind: "archive-canary", from: { id: senderId, name: "path-b-sender", kind: "main" }, text: "preexisting sender log and archive bytes" });
      const before = snapshot(sender.runtime.mesh.root);
      expect(Buffer.from(before["events.jsonl"]!, "base64").toString("utf8")).toContain("preexisting sender log and archive bytes");
      expect(Object.entries(before).some(([name, bytes]) => name.startsWith("archive/") && name.endsWith(".jsonl") &&
        Buffer.from(bytes, "base64").toString("utf8").includes("preexisting sender log and archive bytes"))).toBe(true);
      let result: FabricActorMessage | undefined, failure: unknown;
      try { result = await sender.invoke("agents.ask", { id: actor.id, message: "Return the unique deterministic answer.", data: { reply } }) as FabricActorMessage; }
      catch (error) { failure = error; }
      const evidence = await owner.call<OwnerEvidence>("evidence");
      if (failure) console.log("PATH_B_ASK_FAILURE", JSON.stringify({ senderPid: process.pid, ownerPid: ready.pid, senderRoot, ownerRoot,
        failure: failure instanceof Error ? failure.message : failure, evidence: { ...evidence, session: `<native transcript ${evidence.session.length} chars>` }, senderEvents: events }));
      expect(failure).toBeUndefined();
      expect(result).toMatchObject({ actorId: actor.id, direction: "out", text: reply });
      expect(evidence.actor).toMatchObject({ id: actor.id, rootId: ownerId, ownerSessionId: ownerSession, status: "idle", queued: 0 });
      expect(evidence.actor.inFlightRun).toBeUndefined();
      expect(sender.runtime.actors.owns(actor.id)).toBe(false);
      expect(sender.runtime.participantInfos().find(row => row.id === actor.id)).toMatchObject({ ownerHostId: ownerId, rootId: ownerId });
      const inbound = evidence.messages.filter(message => message.direction === "in");
      const outbound = evidence.messages.filter(message => message.direction === "out");
      expect(inbound).toHaveLength(1); expect(outbound).toEqual([result]);
      expect(evidence.stages.some(stage => stage.status === "queued" && stage.queued === 1 && stage.messages === 1)).toBe(true);
      expect(evidence.stages.every(stage => (stage.queued ?? 0) <= 1)).toBe(true);
      const activated = [...new Set(evidence.stages.flatMap(stage => stage.inFlightRun ? [stage.inFlightRun.id] : []))];
      expect(activated).toEqual([result!.runId]);
      expect(evidence.queueFiles).toEqual([]); // foreground ask queue is ephemeral, drained at the reply boundary
      expect(evidence.runs).toHaveLength(1);
      expect(evidence.runs[0]).toMatchObject({ id: result!.runId, runner: "pi", transport: "process", status: "completed", turns: 1 });
      const session = evidence.session.trim().split("\n").map(line => JSON.parse(line));
      const assistants = session.filter(entry => entry.type === "message" && entry.message?.role === "assistant");
      expect(assistants).toHaveLength(1);
      expect(assistants[0].message.content).toEqual([{ type: "text", text: reply }]);
      const modelEvents = evidence.modelEvents.trim().split("\n").map(line => JSON.parse(line));
      expect(modelEvents.map(event => event.type)).toEqual(["agent_start", "turn_end"]);
      expect(modelEvents[1]).toMatchObject({ calls: 1 });
      expect(modelEvents[0].pid).not.toBe(ready.pid); expect(modelEvents[0].pid).not.toBe(process.pid);

      const command = events.find(event => event.phase === "send" && event.hostId === senderId && event.envelope.kind === "command")!;
      expect(command).toBeDefined();
      expect(command.envelope).toMatchObject({ operation: "ask", targetId: actor.id, to: { machine: "machine-a", hostId: ownerId } });
      const commandId = (command.envelope.body as { commandId: string }).commandId;
      expect(commandId).toMatch(/^[a-f0-9]{64}$/);
      const protocolAck = evidence.events.find(event => event.phase === "send" && event.envelope.kind === "ack")!;
      expect(protocolAck.envelope).toMatchObject({ kind: "ack", to: { machine: "machine-b", hostId: senderId },
        body: { accepted: true, commandId, messageId: result!.id, targetId: actor.id, result } });
      expect(evidence.events.filter(event => event.phase === "broker-ack" && event.envelope.id === command.envelope.id)).toHaveLength(1);
      await until(() => events.some(event => event.phase === "broker-ack" && event.hostId === senderId && event.envelope.id === protocolAck.envelope.id),
        onChange(changed), "sender confirmed result ACK");
      expect(snapshot(sender.runtime.mesh.root)).toEqual(before);

      // Public agents.ask intentionally generates its own logical key. Re-publish
      // the captured real envelope, first with the same broker id (window dedupe),
      // then a NEW broker id but the SAME commandId (real durable control-seen dedupe).
      // Neither test fabricates a receiver, protocol ACK, or broker ACK.
      const brokerDuplicate = await inspector.send(command.envelope);
      expect(brokerDuplicate).toMatchObject({ duplicate: true, sequence: command.sequence });
      const duplicateEnvelope = { ...command.envelope, id: randomUUID() };
      const logicalDuplicate = await inspector.send(duplicateEnvelope);
      expect(logicalDuplicate.duplicate).toBe(false);
      const replayed = await owner.call<OwnerEvidence>("evidence", { eventId: duplicateEnvelope.id });
      const replayAck = replayed.events.find(event => event.phase === "send" && event.envelope.id === `ack:${duplicateEnvelope.id}`)!;
      expect(replayAck.envelope).toMatchObject({ body: { accepted: true, commandId, messageId: result!.id, result } });
      await until(() => events.some(event => event.phase === "broker-ack" && event.hostId === senderId && event.envelope.id === replayAck.envelope.id),
        onChange(changed), "sender confirmed replay ACK");
      expect(replayed.messages).toEqual(evidence.messages);
      expect(replayed.runs).toEqual(evidence.runs);
      expect(replayed.modelEvents).toBe(evidence.modelEvents);
      expect(replayed.session).toBe(evidence.session);
      expect(replayed.actor).toMatchObject({ status: "idle", queued: 0, lastRunId: result!.runId });
      expect(replayed.queueFiles).toEqual([]);
      expect(replayed.events.filter(event => event.phase === "broker-ack" && event.envelope.kind === "command")).toHaveLength(2);
      expect(snapshot(sender.runtime.mesh.root)).toEqual(before);
      // Birth-qualified absence proves the actual native model process AND its
      // production worker have ended; logical actor-idle alone is not custody.
      expect(modelEvents[0].birth).toMatch(/^\d+$/);
      expect(modelEvents[0].workerBirth).toMatch(/^\d+$/);
      expect(processBirth(modelEvents[0].pid)).not.toBe(modelEvents[0].birth);
      expect(processBirth(modelEvents[0].workerPid)).not.toBe(modelEvents[0].workerBirth);
      expect(evidence.runs[0]).toMatchObject({ sessionId: String(modelEvents[0].workerPid), processStartTime: modelEvents[0].workerBirth });

      // Query only AFTER real ackAck barriers; zero pending is independent broker
      // state, not an assumption based on protocol success or a timed sleep.
      // https://docs.nats.io/reference/2.14/jetstream/api/consumer/create
      const { manager } = await openJetStream(await broker.connect(broker.users.bootstrap));
      const ownerConsumer = await manager.consumers.info(transportInboxStream("machine-a"), transportDurableName(ownerId));
      const senderConsumer = await manager.consumers.info(transportInboxStream("machine-b"), transportDurableName(senderId));
      for (const consumer of [ownerConsumer, senderConsumer]) {
        expect({ pending: consumer.num_pending, ackPending: consumer.num_ack_pending }).toEqual({ pending: 0, ackPending: 0 });
        expect(consumer.ack_floor.consumer_seq).toBe(2);
      }
      console.log("PATH_B_ASK_GREEN", JSON.stringify({ senderPid: process.pid, ownerPid: ready.pid, senderRoot, ownerRoot, modelPid: modelEvents[0].pid,
        modelBirth: modelEvents[0].birth, workerPid: modelEvents[0].workerPid, workerBirth: modelEvents[0].workerBirth, nativeWritersGone: true,
        commandId, commandEventId: command.envelope.id, messageId: result!.id, runId: result!.runId, text: result!.text,
        logicalReplayEventId: duplicateEnvelope.id, brokerDuplicate: brokerDuplicate.duplicate, admitted: inbound.length, activations: activated.length,
        ownerAckFloor: ownerConsumer.ack_floor, senderAckFloor: senderConsumer.ack_floor,
        ownerAckPending: ownerConsumer.num_ack_pending, senderAckPending: senderConsumer.num_ack_pending,
        senderSnapshotPaths: Object.keys(before).sort(), senderSnapshotUnchanged: true }));
    } finally {
      await cleanupPathB(root, sender, owner, inspector, broker);
      expect(owner?.forced ?? []).toEqual([]); // Normal shutdown is still natural code 0, never a force-pass.
    }
  }, 90_000);

  // Keep: #14568 process-ownership/data safety. The native agent_start hook holds
  // a real model while its production worker is alive; cleanup completions are faulted.
  it("bounds a stalled runtime shutdown and removes the live detached worker/model tree, broker and private root", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "nats-actor-stall-"));
    fs.chmodSync(root, 0o700);
    const launches = launchLog(root);
    const senderRoot = path.join(root, "sender"), ownerRoot = path.join(root, "owner");
    for (const dir of [senderRoot, ownerRoot]) fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
    vi.stubEnv("SMARTY_AGENT", undefined);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(senderRoot, "agent"));
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", senderRoot);
    vi.stubEnv("TMPDIR", root);
    const sender = runtimeFor(senderRoot, senderSession);
    let broker: PathBHarness | undefined, owner: OwnerProcess | undefined, inspector: FabricTransport | undefined;
    const realSenderShutdown = sender.runtime.shutdown.bind(sender.runtime);
    let owned: Owned[] = [], brokerChild: ChildProcess | undefined, brokerBirth: string | undefined;
    let ask: Promise<unknown> | undefined, sibling: ChildProcess | undefined, siblingOwned: Owned | undefined;
    let nativeInspectorClose: (() => Promise<void>) | undefined;
    try {
      const siblingFile = path.join(root, "unrelated-sibling.cjs");
      fs.writeFileSync(siblingFile, 'process.stdout.write("ready\\n"); setInterval(() => {}, 1000);');
      sibling = spawn(process.execPath, [siblingFile], { stdio: ["ignore", "pipe", "ignore"] });
      siblingOwned = { pid: sibling.pid!, started: processBirth(sibling.pid!)!, at: Date.now(), argv: [] };
      await teardownWithin("unrelated sibling ready", () => new Promise<void>(resolve => sibling!.stdout!.once("data", () => resolve())));
      expect(siblingOwned.started).toMatch(/^\d+$/);
      // A deliberately stale birth must not authorize even an otherwise owned PID.
      await stopOwned({ ...siblingOwned, started: `stale-${siblingOwned.started}` }, 10, 10);
      expect(processBirth(sibling.pid!)).toBe(siblingOwned.started);
      broker = await createPathBHarness({ ...(binary ? { binary } : {}), tls: {} });
      brokerChild = broker.serverProcess; brokerBirth = processBirth(brokerChild!.pid!);
      for (const [name, value] of Object.entries(broker.adminEnv(broker.users.bootstrap))) vi.stubEnv(name, value);
      expect(await runTransportAdmin(["provision", "--ca-file", broker.tls!.caFile, "--machines", PATH_B_MACHINES.join(",")])).toBe(0);
      for (const name of Object.keys(broker.adminEnv(broker.users.bootstrap))) vi.stubEnv(name, undefined);
      const ownerFile = path.join(root, "owner.ts"), inputFile = path.join(root, "owner-input.json");
      fs.writeFileSync(ownerFile, ownerSource());
      fs.writeFileSync(inputFile, JSON.stringify({ root: ownerRoot, config: configFor(broker, ownerRoot, "machine-a"), sessionId: ownerSession,
        reply, stallShutdown: true, ownershipPreload: path.join(root, "launch-preload.mjs") }));
      owner = new OwnerProcess(ownerFile, inputFile, { ...process.env, ...launches.env, PI_CODING_AGENT_DIR: path.join(ownerRoot, "agent"),
        PI_FABRIC_PROJECT_ROOT: ownerRoot, PI_FABRIC_PI_BINARY: path.join(repository, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js"), TMPDIR: ownerRoot }, launches);
      await owner.ready;
      await sender.runtime.initialize(sender.context, configFor(broker, senderRoot, "machine-b"));
      const actor = await owner.call<FabricActorInfo>("create", { name: "stalled-actor", scope: "session", instructions: "Reply once.", responseMode: "text", coalesce: false });
      // Observe rejection immediately: forced owner exit must not become an unhandled rejection.
      ask = sender.invoke("agents.ask", { id: actor.id, message: "Hold at the native agent_start barrier." }).then(value => ({ value }), error => ({ error: String(error) }));
      const eventFile = path.join(ownerRoot, "model-events.jsonl");
      await watchFile(ownerRoot, () => fs.existsSync(eventFile) && fs.readFileSync(eventFile, "utf8").includes('"agent_start"'), "live native model agent_start");
      const model = JSON.parse(fs.readFileSync(eventFile, "utf8").trim().split("\n")[0]!);
      owned = launches.owned();
      for (const [pid, started] of [[model.pid, model.birth], [model.workerPid, model.workerBirth], [owner.child.pid, processBirth(owner.child.pid!)]]) {
        expect(started).toMatch(/^\d+$/);
        expect(owned.some(entry => entry.pid === pid && entry.started === started)).toBe(true);
        expect(processBirth(pid)).toBe(started);
      }
      console.log("PATH_B_STALL_READY", JSON.stringify({ root, ownerPid: owner.child.pid, model, brokerPid: brokerChild!.pid, brokerBirth }));
      // Keep the genuine ask in flight until the owner force branch. The retained
      // native sender close capability is joined afterward, before root removal.
      vi.spyOn(sender.runtime, "shutdown").mockImplementation(() => new Promise(() => {}));
      inspector = await openFabricTransport({ config: configFor(broker, senderRoot, "machine-b").mesh, hostId: "stall-inspector",
        mesh: new MeshStore(path.join(senderRoot, "inspector"), 65_536, 1_000) });
      nativeInspectorClose = inspector.close.bind(inspector);
      vi.spyOn(inspector, "close").mockImplementation(async () => { await nativeInspectorClose!(); await new Promise(() => {}); });
      const nc = await broker.connect(broker.users.bootstrap), nativeConnectionClosed = nc.closed();
      vi.spyOn(nc, "close").mockImplementation(() => new Promise(() => {}));
      const entered = watchFile(ownerRoot, () => fs.existsSync(path.join(ownerRoot, "shutdown-entered")), "runtime.shutdown entered");
      void entered.catch(() => undefined);
      const start = performance.now();
      let failure: unknown;
      try { await guardedWait(cleanupPathB(root, sender, owner, inspector, broker), 12_000, "cleanup exceeded outer guard"); }
      catch (error) { failure = error; }
      await entered;
      expect(failure).toBeInstanceOf(Error);
      for (const phase of ["owner shutdown", "sender shutdown", "inspector close", "NATS connection close"]) expect(String(failure)).toContain(`${phase} timed out`);
      expect(owner.liveAtForce).toEqual(expect.arrayContaining([{ pid: model.pid, started: model.birth }, { pid: model.workerPid, started: model.workerBirth }]));
      expect(owner.forced).toContainEqual({ pid: model.pid, started: model.birth });
      await teardownWithin("broker socket native close", () => nativeConnectionClosed);
      expect(nc.isClosed()).toBe(true);
      const askOutcome = await teardownWithin("in-flight ask settled", () => ask!);
      expect(askOutcome).toHaveProperty("error");
      expect(performance.now() - start).toBeLessThan(10_000);
      for (const entry of owned) expect(processBirth(entry.pid), `owned birth survived: ${entry.pid}`).not.toBe(entry.started);
      expect(processBirth(brokerChild!.pid!)).not.toBe(brokerBirth);
      expect(brokerChild!.exitCode !== null || brokerChild!.signalCode !== null).toBe(true);
      expect(fs.existsSync(root)).toBe(false);
      expect(owned.some(entry => entry.pid === sibling!.pid)).toBe(false);
      expect(processBirth(sibling.pid!)).toBe(siblingOwned.started);
      console.log("PATH_B_STALL_CLEANUP", JSON.stringify({ root, elapsedMs: performance.now() - start, failure: String(failure),
        owned: owned.map(({ pid, started }) => ({ pid, started })), siblingPid: sibling.pid, siblingBirth: siblingOwned.started, siblingPreserved: true, brokerPid: brokerChild!.pid,
        brokerExitCode: brokerChild!.exitCode, brokerSignal: brokerChild!.signalCode, privateRootExists: fs.existsSync(root) }));
    } finally {
      // Independent emergency guard also makes the intentional pre-fix RED safe.
      // Leaf first: keep the actual worker/owner alive to reap their native children.
      owned = owned.length ? owned : launches.owned();
      for (const entry of [...owned].reverse()) await stopOwned(entry, 300, 1_000);
      if (siblingOwned) await stopOwned(siblingOwned, 300, 1_000);
      if (brokerChild?.pid && brokerBirth) await stopOwned({ pid: brokerChild.pid, started: brokerBirth, at: Date.now(), argv: [] }, 300, 1_000);
      await teardownWithin("emergency inspector close", () => nativeInspectorClose?.() ?? Promise.resolve()).catch(() => undefined);
      await guardedWait(realSenderShutdown("exit"), 1_000, "emergency sender shutdown did not settle").catch(() => undefined);
      await guardedWait(ask ?? Promise.resolve(), 1_000, "ask did not settle after owned exit").catch(() => undefined);
      vi.restoreAllMocks(); vi.unstubAllEnvs(); fs.rmSync(root, { recursive: true, force: true });
      const remaining = [...owned, ...(siblingOwned ? [siblingOwned] : []), ...(brokerChild?.pid && brokerBirth ? [{ pid: brokerChild.pid, started: brokerBirth }] : [])]
        .filter(entry => processBirth(entry.pid) === entry.started).map(({ pid, started }) => ({ pid, started }));
      console.log("PATH_B_STALL_GUARD", JSON.stringify({ root, remaining, privateRootExists: fs.existsSync(root) }));
      expect(remaining).toEqual([]);
    }
  }, 45_000);

  it("reaps the private broker with bounded SIGKILL escalation even when connection close rejects", async () => {
    const broker = await createPathBHarness({ binary: binary! });
    const child = broker.serverProcess!, pid = child.pid!, birth = processBirth(pid);
    try {
      const nc = await broker.connect(broker.users.bootstrap), nativeClosed = nc.closed();
      vi.spyOn(nc, "close").mockRejectedValue(new Error("seeded connection close rejection"));
      const nativeKill = child.kill.bind(child);
      // Owned fault seam: withhold TERM only, requiring the real broker to exit
      // through the actual SIGKILL branch. No synthetic exit event/receipt.
      vi.spyOn(child, "kill").mockImplementation(signal => signal === "SIGTERM" ? true : nativeKill(signal));
      const start = performance.now();
      const failure = await broker.stop().then(() => undefined, error => error);
      expect(String(failure)).toContain("seeded connection close rejection");
      expect(performance.now() - start).toBeLessThan(4_000);
      expect(child.signalCode).toBe("SIGKILL");
      expect(processBirth(pid)).not.toBe(birth);
      await teardownWithin("rejected-connection native close", () => nativeClosed);
      expect(nc.isClosed()).toBe(true);
      expect(fs.existsSync(broker.root)).toBe(false);
      console.log("PATH_B_BROKER_ESCALATION", JSON.stringify({ pid, birth, signal: child.signalCode, elapsedMs: performance.now() - start,
        nativeConnectionClosed: nc.isClosed(), privateRoot: broker.root, privateRootExists: fs.existsSync(broker.root), failure: String(failure) }));
    } finally {
      vi.restoreAllMocks();
      await teardownWithin("emergency broker stop", () => broker.stop(), 5_000);
    }
  }, 15_000);
});
