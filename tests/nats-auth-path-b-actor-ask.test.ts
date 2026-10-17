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
import { createPathBHarness, openJetStream, PATH_B_MACHINES, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";

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
  return { runtime, context, invoke };
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
// Supported native model registration and the production worker/launcher. The only
// deterministic boundary is the keyless scripted model, never control/admission/ACK.
const modelRuntime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(root, "agent/auth.json") });
const faux = fauxProvider({ provider: "nats-ask-faux", models: [{ id: "one-turn" }] });
modelRuntime.registerNativeProvider(faux.provider);
await modelRuntime.getAvailable("nats-ask-faux");
const providerFile = path.join(root, "faux-provider.ts"), workerFile = path.join(root, "worker.ts");
fs.writeFileSync(providerFile, ${JSON.stringify(`
import fs from "node:fs";
import { fauxProvider, fauxAssistantMessage } from ${ai};
export default function(pi) {
 const input = JSON.parse(fs.readFileSync(${JSON.stringify("__INPUT_FILE__")}, "utf8"));
 const faux = fauxProvider({ provider: "nats-ask-faux", models: [{ id: "one-turn" }] });
 const birth = pid => fs.readFileSync("/proc/" + pid + "/stat", "utf8").split(")").at(-1).trim().split(/\\s+/)[19];
 faux.setResponses([fauxAssistantMessage(input.reply)]);
 pi.registerProvider(faux.provider);
 pi.on("agent_start", () => fs.appendFileSync(input.root + "/model-events.jsonl", JSON.stringify({ type: "agent_start", pid: process.pid, birth: birth(process.pid), workerPid: process.ppid, workerBirth: birth(process.ppid) }) + "\\n"));
 pi.on("turn_end", () => fs.appendFileSync(input.root + "/model-events.jsonl", JSON.stringify({ type: "turn_end", pid: process.pid, calls: faux.state.callCount }) + "\\n"));
}
`)}.replace('"__INPUT_FILE__"', JSON.stringify(process.argv[2])));
fs.writeFileSync(workerFile, "import " + JSON.stringify(${source("worker.ts")}) + ";\\n");
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
  constructor(file: string, inputFile: string, environment: NodeJS.ProcessEnv) {
    this.child = spawn("bun", [file, inputFile], { env: environment, stdio: ["pipe", "pipe", "pipe"] });
    this.ready = this.response<OwnerReady>(0);
    this.child.stderr!.on("data", chunk => { this.stderr = (this.stderr + String(chunk)).slice(-12_000); });
    createInterface({ input: this.child.stdout! }).on("line", line => {
      if (!line.startsWith("NATS_ASK_RPC ")) return;
      const message = JSON.parse(line.slice("NATS_ASK_RPC ".length));
      const waiter = this.pending.get(message.id);
      if (waiter) { clearTimeout(waiter.timer); this.pending.delete(message.id); message.error ? waiter.reject(new Error(message.error)) : waiter.resolve(message.value); }
    });
    const failed = (error: Error) => { for (const pending of this.pending.values()) { clearTimeout(pending.timer); pending.reject(error); } this.pending.clear(); };
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
    this.child.stdin!.write(JSON.stringify({ id, method, args }) + "\n");
    return result;
  }
  async stop() {
    if (this.child.exitCode !== null || this.child.signalCode !== null) return;
    const exited = new Promise<void>(resolve => this.child.once("exit", () => resolve()));
    const kill = setTimeout(() => this.child.kill("SIGKILL"), 5_000);
    try { await this.call("shutdown"); this.child.stdin!.end(); }
    finally { await exited; clearTimeout(kill); }
    if (this.child.exitCode !== 0) throw new Error(`Owner shutdown was not clean: ${this.child.exitCode}/${this.child.signalCode}`);
  }
}
function processBirth(pid: number): string | undefined {
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    return stat.slice(stat.lastIndexOf(")") + 1).trim().split(/\s+/)[19];
  }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw error; }
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
      fs.writeFileSync(inputFile, JSON.stringify({ root: ownerRoot, config: ownerConfig, sessionId: ownerSession, reply }));
      owner = new OwnerProcess(ownerFile, inputFile, { ...process.env, PI_CODING_AGENT_DIR: path.join(ownerRoot, "agent"), PI_FABRIC_PROJECT_ROOT: ownerRoot,
        PI_FABRIC_PI_BINARY: path.join(repository, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js"), TMPDIR: ownerRoot });
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
      const stopped = await Promise.allSettled([sender.runtime.shutdown("exit"), owner?.stop()]);
      try { await inspector?.close(); }
      finally {
        try { await broker?.stop(); }
        finally { vi.restoreAllMocks(); vi.unstubAllEnvs(); fs.rmSync(root, { recursive: true, force: true }); }
      }
      const failure = stopped.find(result => result.status === "rejected");
      if (failure?.status === "rejected") throw failure.reason;
      console.log("PATH_B_ASK_CLEANUP", JSON.stringify({ ownerPid: owner?.child.pid, ownerExitCode: owner?.child.exitCode,
        ownerSignal: owner?.child.signalCode, privateRootExists: fs.existsSync(root) }));
    }
  }, 90_000);
});
