// Keep: trust boundary + native-promise/identity custody; N7 needs real native sessions/processes.
// Expected platform gap: Linux-only process/fence cases; no portable kernel OWNER bootstrap proof.
// Built candidate + explicit FABRIC_NATIVE_TEST_PI_SDK_DIST and FABRIC_NATIVE_TEST_NODE are required,
// not a silent missing-input skip. N7 substitutes NATS delivery/KV only; N8 broker/frozen-provider
// proof remains the parent's separate gate. No provider credentials/model/dummy provider are used.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { NativeActorBinding } from "../src/actors/native-handler.js";
import { ActorDirectory } from "../src/actors/directory.js";
import { AgentManager } from "../src/agents/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentHostId, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { copyResidentNativeActorBindings, residentNativeActorBindingsMatch, loadResidentNativeActorHandlers,
  assertResidentNativeModulePath, NativeResidentCustodyBusyError } from "../src/residency/native-actors.js";
import { residentProcessAlive } from "../src/residency/process-identity.js";
import { StoreBridgeSide } from "../src/mesh/bridge.js";
import { FileCarrierPresence } from "../src/mesh/bridge-nats-file.js";
import { NatsBridgeCarrier, type NatsCarrierPresence } from "../src/mesh/bridge-nats-carrier.js";
import { openBridgeNatsLedger } from "../src/mesh/bridge-nats-ledger.js";
import { natsCarrierLedger, natsLinkAuthorizers } from "../src/mesh/bridge-nats-link.js";
import { createBridgeNatsIdentityGate, parseBridgeNatsAllowSection } from "../src/mesh/bridge-nats-identity.js";
import { BRIDGE_NATS_ACK_TOPIC, type BridgeNatsPresenceView } from "../src/mesh/bridge-nats-types.js";
import type { MeshIdentity } from "../src/mesh/store.js";
import { hostLeasePath, readHostLease } from "../src/topology/host-leases.js";
import { FileLockBusy, lockFile } from "../src/residency/file-lock.js";
import type { FabricControlAcceptance } from "../src/topology/control-plane.js";
import { createNativeActorOwner, type NativeActorOwner } from "./helpers/native-actor-owner.js";
import { route } from "./helpers/bridge-nats-l2-fakes.js";

// Synthetic N7 identity only; never reuse the production actor ID, even in a private mesh.
const ACTOR = "77777777777747778777777777777777";
const ROOT = "session:11111111-1111-4111-8111-111111111111";
const repo = fileURLToPath(new URL("../", import.meta.url));
const temporaries: string[] = [];
const hosts: ResidentHost[] = [];
const nativeOwners: NativeActorOwner[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const owner of nativeOwners.splice(0)) await owner.close();
  for (const host of hosts.splice(0)) await host.close();
  for (const root of temporaries.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const temp = (): string => {
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "native-residency-"));
  temporaries.push(root); return root;
};
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
const configFor = (root: string, module: string): ResidentHostConfig => ({
  format: 1, rootId: ROOT, sessionId: ROOT.slice(8), cwd: root, projectRoot: root,
  meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "actors", ROOT.slice(8)),
  residencyRoot: residentRoot(path.join(root, "mesh"), ROOT), fullCodeMode: true,
  agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
  mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, enabled: true, transport: "file", hostTransport: "file", actorScope: "session" },
  retention: { ...DEFAULT_FABRIC_CONFIG.retention },
  workerPath: path.join(root, "must-not-launch.js"), fabricExtensionPath: path.join(root, "not-imported.js"),
  piBinary: "must-not-run-pi", claudeBinary: "must-not-run-claude", vedaBinary: "must-not-run-veda",
  nativeActors: [{ actorId: ACTOR, rootId: ROOT, scope: "session", name: "native-residency", handlerKey: "fixture.v1",
    residentModule: module, ingress: { link: "native-residency", ledgerDir: path.join(root, "ledger") } }],
});

// No native Main is claimed by these narrow config/loader tests. The trusted loader
// seam and real resident startup gate are tested independently of the N7 subprocess.
describe("native resident declarations and module boundary", () => {
  it("serializes immutable own copies and treats every host pin as equality-bearing", () => {
    const root = temp(), config = configFor(root, path.join(root, "handler.mjs"));
    const copied = copyResidentNativeActorBindings(config);
    const roundtrip = JSON.parse(JSON.stringify({ ...config, nativeActors: copied })) as ResidentHostConfig;
    expect(residentNativeActorBindingsMatch(config, roundtrip)).toBe(true);
    expect(Object.isFrozen(copied)).toBe(true);
    expect(Object.isFrozen(copied[0])).toBe(true);
    expect(Object.isFrozen(copied[0]!.ingress)).toBe(true);
    const original = config.nativeActors![0] as { name: string; ingress: { ledgerDir: string } };
    original.name = "mutated"; original.ingress.ledgerDir = path.join(root, "other");
    expect(copied[0]!.name).toBe("native-residency");
    expect(copied[0]!.ingress.ledgerDir).toBe(path.join(root, "ledger"));
    expect(residentNativeActorBindingsMatch(config, roundtrip)).toBe(false);
    for (const change of [{ residentModule: path.join(root, "other.mjs") }, { handlerKey: "other.v1" },
      { actorId: "6027cfd59b1247b6bb4666970c481cc4" }, { ingress: { link: "other", ledgerDir: path.join(root, "ledger") } }]) {
      expect(residentNativeActorBindingsMatch(roundtrip, { ...roundtrip, nativeActors: [{ ...roundtrip.nativeActors![0]!, ...change }] })).toBe(false);
    }
  });

  it.each(["foreign-root", "project-scope", "nats", "relative-module", "relative-ledger", "duplicate", "extra-grant"])("refuses %s without guessing or repair", mode => {
    const root = temp(), config = configFor(root, path.join(root, "handler.mjs"));
    const value = JSON.parse(JSON.stringify(config)) as ResidentHostConfig;
    const binding = value.nativeActors![0]! as unknown as Record<string, unknown>;
    if (mode === "foreign-root") binding.rootId = "session:22222222-2222-4222-8222-222222222222";
    if (mode === "project-scope") value.mesh.actorScope = "project";
    if (mode === "nats") value.mesh.transport = "nats";
    if (mode === "relative-module") binding.residentModule = "./handler.mjs";
    if (mode === "relative-ledger") (binding.ingress as Record<string, unknown>).ledgerDir = "./ledger";
    if (mode === "duplicate") value.nativeActors = [value.nativeActors![0]!, value.nativeActors![0]!];
    if (mode === "extra-grant") binding.grants = ["*"];
    expect(() => copyResidentNativeActorBindings(value)).toThrow(/refused/);
  });

  it.skipIf(process.platform === "win32")("loads code only under custody and ignores module-selected bindings/grants", async () => {
    const root = temp(), module = path.join(root, "handler.mjs");
    fs.writeFileSync(module, 'export const handlerKey="fixture.v1"; export const binding={actorId:"wrong"}; export const grants=["*"]; export const handle=async()=>({data:"ok"});', { mode: 0o600 });
    const bindings = copyResidentNativeActorBindings(configFor(root, module));
    await expect(loadResidentNativeActorHandlers(bindings, () => { throw new Error("OWNER not held"); })).rejects.toThrow("OWNER not held");
    const loaded = await loadResidentNativeActorHandlers(bindings, () => {});
    expect(loaded[0]!.binding).toBe(bindings[0]);
    expect(loaded[0]!.binding.actorId).toBe(ACTOR);
    expect(Object.isFrozen(loaded)).toBe(true);
    const wrong = path.join(root, "wrong.mjs"); fs.writeFileSync(wrong, 'export const handlerKey="wrong"; export const handle=async()=>({data:null});', { mode: 0o600 });
    await expect(loadResidentNativeActorHandlers([{ ...bindings[0]!, residentModule: wrong }], () => {})).rejects.toThrow(/mismatch/);
    const link = path.join(root, "symlink.mjs"); fs.symlinkSync(module, link);
    expect(() => assertResidentNativeModulePath(link)).toThrow(/trusted/);
    fs.chmodSync(module, 0o666);
    expect(() => assertResidentNativeModulePath(module)).toThrow(/trusted/);
  });
});

describe.skipIf(process.platform !== "linux")("native materialization is NOT shared admission", () => {
  it("keeps source/ask/control/READY closed across asynchronous ensure suspension and failure", async () => {
    const root = temp(), module = path.join(root, "handler.mjs"), callback = path.join(root, "callback-entered");
    fs.writeFileSync(module, `import fs from 'node:fs'; export const handlerKey='fixture.v1'; export const handle=async()=>{fs.writeFileSync(${JSON.stringify(callback)},'bad');return {data:null}};`, { mode: 0o600 });
    const entered = deferred(), release = deferred();
    vi.spyOn(AgentManager.prototype, "run").mockImplementation(async () => { throw new Error("FORBIDDEN model run"); });
    const controlStart = vi.spyOn((await import("../src/topology/control-plane.js")).FabricControlPlane.prototype, "start");
    const original = ActorDirectory.prototype.ensureNativeActor;
    vi.spyOn(ActorDirectory.prototype, "ensureNativeActor").mockImplementation(async function(this: ActorDirectory, binding: NativeActorBinding) {
      entered.resolve(); await release.promise;
      // The real ensure rechecks the narrow precondition under its actual registry lock.
      await original.call(this, binding);
      throw new Error("forced materialization failure");
    });
    const host = new ResidentHost(configFor(root, module)); hosts.push(host);
    const starting = host.start();
    const outcome = starting.then(() => undefined, error => error as Error);
    await Promise.race([entered.promise, outcome.then(error => {
      throw error ?? new Error("Resident startup bypassed the native-birth suspension");
    })]);
    try {
      expect(host.participants.canConsumeMesh()).toBe(true); // actual confirmed FILE lease
      expect(host.control.options.canConsumeMesh?.()).toBe(false); // #ready is still false
      expect(controlStart).not.toHaveBeenCalled();
      expect(fs.existsSync(path.join(host.config.residencyRoot, "owner.json"))).toBe(false);
      expect(fs.existsSync(path.join(host.config.residencyRoot, "maintenance-ready.json"))).toBe(false);
      await expect(Promise.resolve().then(() => host.actors.ask(ACTOR, "fixture.v1", { beforeReady: true }))).rejects.toThrow();
      expect(fs.existsSync(callback)).toBe(false);
      expect(AgentManager.prototype.run).not.toHaveBeenCalled();
    } finally { release.resolve(); }
    expect((await outcome)?.message).toContain("forced materialization failure");
    expect(controlStart).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(host.config.residencyRoot, "owner.json"))).toBe(false);
    expect(fs.existsSync(path.join(host.config.residencyRoot, "maintenance-ready.json"))).toBe(false);
    expect(fs.existsSync(callback)).toBe(false);
    expect(AgentManager.prototype.run).not.toHaveBeenCalled();
  }, 60_000);

  it("retains the REAL OWNER fence/lease on actor-manager unsettled-promise refusal, then releases only after settlement", async () => {
    // Explicit actor-manager teardown adapter, not a claim that this unit ran a callback.
    // The real abort-ignoring callback fixture exists for the parent's separate process proof.
    const root = temp(), module = path.join(root, "handler.mjs");
    fs.writeFileSync(module, 'export const handlerKey="fixture.v1";export const handle=async()=>({data:null});', { mode: 0o600 });
    const host = new ResidentHost(configFor(root, module)); hosts.push(host); await host.start();
    const token = JSON.parse(fs.readFileSync(path.join(host.config.residencyRoot, "owner.json"), "utf8")).token;
    const actualPromise = deferred();
    let unsettled = true;
    void actualPromise.promise.then(() => { unsettled = false; });
    vi.spyOn(host.actors, "inFlightActorIds").mockImplementation(() => unsettled ? [ACTOR] : []);
    const close = vi.spyOn(host.actors, "close").mockRejectedValueOnce(new Error("native callback remains unsettled"));
    await expect(host.close()).rejects.toBeInstanceOf(NativeResidentCustodyBusyError);
    expect(host.participants.canConsumeMesh()).toBe(true);
    expect(JSON.parse(fs.readFileSync(path.join(host.config.residencyRoot, "owner.json"), "utf8")).token).toBe(token);
    let accidentalFd: number | undefined;
    try {
      await expect(lockFile(path.join(host.config.residencyRoot, "host.lock"), 0, true).then(fd => {
        accidentalFd = fd;
      })).rejects.toBeInstanceOf(FileLockBusy);
    } finally { if (accidentalFd !== undefined) fs.closeSync(accidentalFd); }
    await expect(host.close()).rejects.toBeInstanceOf(NativeResidentCustodyBusyError);
    expect(close).toHaveBeenCalledTimes(1);
    actualPromise.resolve(); await actualPromise.promise;
    await host.close();
    const successorFence = await lockFile(path.join(host.config.residencyRoot, "host.lock"), 0, true);
    fs.closeSync(successorFence);
  }, 60_000);
});

/** N7 component delivery: REAL file presence, allow gate, carrier, receipt/ledger,
 * StoreBridgeSide -> resident control -> serial native callback. Only source NATS
 * delivery/KV is substituted here. This is NOT N8's broker/frozen-provider proof. */
const residentIngress = async (owner: NativeActorOwner) => {
  const file = new StoreBridgeSide(owner.mesh, owner.binding.ingress.link);
  const presence = new FileCarrierPresence(file);
  const sender: MeshIdentity = { id: "session:22222222-2222-4222-8222-222222222222", name: "N7-delivery-boundary", kind: "main" };
  const sourceMachine = "native-fixture-source", bridgeMachine = "native-fixture-hub";
  const nats: BridgeNatsPresenceView = { side: "nats", records: id => id === sender.id ? [{ id, name: sender.name, kind: "main",
    rootId: id, ownerHostId: id, ownerIdentityId: id, machine: sourceMachine, live: true, observedAt: Date.now() }] : [] };
  const allow = parseBridgeNatsAllowSection({ file: [{ kind: "root", id: owner.pin.rootId, send: [], receive: [] },
    { kind: "actor", id: ACTOR, rootId: owner.pin.rootId, name: owner.binding.name, send: [], receive: ["ask", "cancel"] }],
    nats: [{ kind: "root", id: sender.id, machine: sourceMachine, send: ["ask", "cancel"], receive: [] }] },
  { link: owner.binding.ingress.link, natsMachine: bridgeMachine });
  const gate = createBridgeNatsIdentityGate(allow, { pins: { seats: new Map(), bound: new Map() } });
  const auth = natsLinkAuthorizers(gate, { file: () => presence.view(), nats: () => nats });
  const ledger = await openBridgeNatsLedger({ dir: owner.ledgerDir });
  const source: NatsCarrierPresence = { file: id => presence.file(id), nats: id => {
    if (id === sender.id) return route(sender, sourceMachine, "native-fixture-source-inc");
    const actor = presence.view().records(id).find(record => record.kind === "actor");
    return actor ? route({ id: actor.id, name: actor.name, kind: "actor" }, bridgeMachine, actor.ownerIncarnation!,
      { ownerHostId: actor.ownerHostId, ownerIdentityId: actor.ownerIdentityId, capabilities: ["ask"] }) : undefined;
  } };
  const carrier = new NatsBridgeCarrier({ link: owner.binding.ingress.link, machine: bridgeMachine,
    presence: source, ledger: natsCarrierLedger(ledger, owner.binding.ingress.link), authorize: auth.authorize });
  const deliver = async (data: unknown) => {
    await presence.refresh(true);
    const target = presence.view().records(ACTOR).find(record => record.kind === "actor");
    if (!target?.ownerIncarnation) throw new Error("Actual native actor owner incarnation unavailable");
    const now = Date.now(), id = randomUUID();
    return carrier.inbound({ sourceMachine, sequence: 1, envelope: { id, kind: "command", from: sender,
      to: { machine: bridgeMachine, hostId: target.ownerHostId }, targetId: ACTOR, operation: "ask",
      body: { version: 1, commandId: id, targetId: ACTOR, operation: "ask", replyTo: sender.id,
        ownerIncarnation: target.ownerIncarnation, message: owner.binding.handlerKey, data,
        requestCreatedAt: now, requestedAt: now, deadlineAt: now + 60_000 } } });
  };
  const ask = async (data: unknown): Promise<FabricControlAcceptance> => {
    const plan = await deliver(data);
    if (plan.kind !== "event") throw new Error(`N7 bridge component refused: ${plan.kind}`);
    const raw = plan.event, body = raw.data as Record<string, unknown>;
    await file.publish({ topic: raw.topic, kind: raw.kind, from: raw.from, to: raw.to!,
      ...(raw.principal ? { principal: raw.principal } : {}), data: { ...body, bridge: { from: owner.binding.ingress.link, id: raw.id } } });
    let acceptance: FabricControlAcceptance | undefined;
    await owner.until(() => {
      const ack = owner.mesh.read({ after: 0, limit: 1_000 }).find(event => event.topic === BRIDGE_NATS_ACK_TOPIC &&
        (event.data as Record<string, unknown>)?.commandId === body.commandId);
      if (ack) acceptance = ack.data as FabricControlAcceptance;
      return acceptance !== undefined;
    }, "resident native control ACK");
    return acceptance!;
  };
  return { ask, deliver, presence, close: () => ledger.close() };
};

const realOwner = async (): Promise<NativeActorOwner> => {
  const sdkDirectory = process.env.FABRIC_NATIVE_TEST_PI_SDK_DIST;
  const nodeBinary = process.env.FABRIC_NATIVE_TEST_NODE;
  if (!sdkDirectory || !nodeBinary) throw new Error("N7 BLOCKED: explicit FABRIC_NATIVE_TEST_PI_SDK_DIST + FABRIC_NATIVE_TEST_NODE required; no SDK/model/runtime fallback");
  const owner = await createNativeActorOwner({ candidateRoot: repo, sdkDirectory, nodeBinary, actorId: ACTOR });
  nativeOwners.push(owner); return owner;
};

const expectTrustedMain = (owner: NativeActorOwner): void => {
  // The real SDK context is the evidence, not saved config or missing project files.
  expect(owner.observations()).toContainEqual(expect.objectContaining({
    event: "session-start", role: "main", pid: owner.mainPid(), projectTrusted: true,
  }));
};

describe.skipIf(process.platform !== "linux")("N7 actual credential-free Pi SDK Main and detached resident", () => {
  it("refuses root input, keeps resident idle/alive, refuses Main-down ingress, restarts the SAME native file/actor", async () => {
    const owner = await realOwner();
    await owner.start();
    expectTrustedMain(owner);
    const initialOwner = owner.owner();
    expect(initialOwner.hostId).toBe(residentHostId(owner.pin.rootId));
    expect(initialOwner.hostId).not.toBe(owner.pin.rootId);
    expect(initialOwner.nativeActors).toEqual([owner.binding]);
    const registry = new ActorRegistryStore(path.join(owner.meshRoot, "actors", owner.pin.sessionId));
    const row = () => registry.snapshot().actors.find(actor => actor.id === ACTOR);
    expect(row()).toMatchObject({ id: ACTOR, rootId: owner.pin.rootId, runner: "native", instructions: null,
      execution: { kind: "native", version: 1, handlerKey: owner.binding.handlerKey } });
    for (const type of ["prompt", "steer", "follow_up"] as const) {
      const reply = await owner.request(type, "MUST_BE_HANDLED_WITHOUT_INFERENCE");
      expect(reply.success).toBe(true); expect(reply.data?.disposition).toBe("handled");
    }
    expect((await owner.request("get_messages")).data?.messages).toEqual([]);
    const crossing = await residentIngress(owner);
    try {
      const first = await crossing.ask({ request: "after-READY" });
      expect(first.accepted).toBe(true);
      expect((first.result as { data: unknown } | undefined)?.data).toEqual({ echo: { request: "after-READY" }, sender: "session:22222222-2222-4222-8222-222222222222", actorId: ACTOR });
      expect(owner.callbacks()).toHaveLength(1);
      // Real elapsed time exceeds the CONFIGURED idle-exit interval; no clock mock,
      // interval increase, local wake, or another application ask is used in this gap.
      await new Promise(resolve => setTimeout(resolve, owner.idleExitMs + 1_100));
      expect(residentProcessAlive(initialOwner.pid, initialOwner.processStartTime)).toBe(true);
      expect(row()).toMatchObject({ id: ACTOR, status: "idle" });
      expect((await crossing.ask({ request: "past-idle-exit" })).accepted).toBe(true);
      await owner.stopMain(); // Test restart barrier: no automatic new Main hides this negative.
      await owner.until(async () => {
        await crossing.presence.refresh(true);
        return !crossing.presence.view().records(owner.pin.rootId).some(record => record.live);
      }, "actual Main non-live under unchanged gate");
      expect(residentProcessAlive(initialOwner.pid, initialOwner.processStartTime)).toBe(true);
      const count = owner.callbacks().length;
      const refusal = await crossing.deliver({ request: "MUST_NOT_RUN_MAIN_DOWN" });
      expect(refusal.kind).not.toBe("event");
      expect(owner.callbacks()).toHaveLength(count);
      await owner.start();
      expectTrustedMain(owner);
      expect(owner.pin.rootId).toBe(owner.binding.rootId);
      expect(owner.owner().hostId).toBe(initialOwner.hostId);
      expect(row()?.id).toBe(ACTOR);
      expect((await crossing.ask({ request: "same-session-restart" })).accepted).toBe(true);
      expect(owner.callbacks()).toHaveLength(count + 1);
      const observed = owner.observations();
      expect(observed.filter(event => event.event === "module-loaded")).toEqual([
        expect.objectContaining({ resident: true, pid: initialOwner.pid }),
      ]);
      const starts = observed.filter(event => event.event === "session-start");
      expect(starts.length).toBeGreaterThanOrEqual(3); // first Main, resident, reconstructed Main
      expect(starts.every(event => event.selectedModel === false && event.unconfigured === true)).toBe(true);
      expect(starts.some(event => event.role === "resident")).toBe(true);
      expect(observed.filter(event => event.event === "input-handled" && event.role === "main")).toHaveLength(3);
      // These explicit zeros are NOT replaced by the supplemental deny-all probe.
      for (const event of ["FORBIDDEN-agent-preflight", "FORBIDDEN-agent-start", "FORBIDDEN-provider-request",
        "FORBIDDEN-provider-stream", "FORBIDDEN-provider-response", "FORBIDDEN-tool-call"]) {
        expect(observed.filter(value => value.event === event)).toHaveLength(0);
      }
      expect(observed.some(event => String(event.event).startsWith("FORBIDDEN-"))).toBe(false);
      for (const type of ["agent_start", "turn_start", "tool_execution_start", "tool_execution_update", "tool_execution_end"]) {
        expect(owner.rpcEvents.filter(event => event.type === type)).toHaveLength(0);
      }
      expect(fs.readFileSync(owner.sessionFile, "utf8")).not.toContain("MUST_BE_HANDLED_WITHOUT_INFERENCE");
      await owner.stopMain();
      const nativeBytes = fs.readFileSync(owner.sessionFile);
      // Retain the genuine raw native trace BEFORE destructive negative injection,
      // including if this test fails or is killed before restoration.
      fs.writeFileSync(path.join(owner.root, "session-before-empty.jsonl"), nativeBytes, { mode: 0o600, flag: "wx" });
      fs.writeFileSync(owner.sessionFile, "", { mode: 0o600 });
      await expect(owner.start()).rejects.toThrow(/missing or empty/);
      // Restore only this test's own native file bytes for teardown; no synthetic header.
      fs.writeFileSync(owner.sessionFile, nativeBytes, { mode: 0o600 });
    } finally { await crossing.close(); }
  }, 180_000);

  it("fences a second actual Main and provisioning/verify before session access; SIGKILL releases only Main custody and preserves its resident", async () => {
    const owner = await realOwner(); await owner.start();
    expectTrustedMain(owner);
    const resident = owner.owner(), firstMain = owner.mainPid();
    expect(firstMain).toBeGreaterThan(1);
    expect(firstMain).not.toBe(resident.pid);
    const bytes = fs.readFileSync(owner.sessionFile), sessionPin = fs.readFileSync(owner.sessionPin);
    for (const attempt of [() => owner.attemptSecondMain(),
      () => owner.attemptSessionAccess("verify"), () => owner.attemptSessionAccess("provision")]) {
      const refused = await attempt();
      expect(refused.code).not.toBeNull(); expect(refused.code).not.toBe(0);
      expect(refused.signal).toBeNull();
      expect(refused.stderr).toContain("NATIVE_OWNER_SESSION_BUSY");
      expect(fs.readFileSync(owner.sessionFile)).toEqual(bytes);
      expect(fs.readFileSync(owner.sessionPin)).toEqual(sessionPin);
      expect(owner.mainPid()).toBe(firstMain);
      expect((await owner.request("get_state")).success).toBe(true);
      expect(owner.observations().filter(event => event.pid === refused.pid)).toHaveLength(0);
      expect(residentProcessAlive(resident.pid, resident.processStartTime)).toBe(true);
      expect(owner.owner().token).toBe(resident.token);
    }
    // Public crash seam for N8: an exact ChildProcess SIGKILL + terminal CLOSE,
    // not EOF, a signal acknowledgement, name/group kill or a fabricated receipt.
    await owner.crashMain();
    expect(owner.mainPid()).toBeUndefined();
    expect(residentProcessAlive(resident.pid, resident.processStartTime)).toBe(true);
    expect(owner.owner().token).toBe(resident.token);
    expect(fs.readFileSync(owner.sessionFile)).toEqual(bytes);
    // If the detached resident inherited the Main lock this actual fenced verify
    // (and the following same-build reconstruction) would still be refused.
    const afterCrash = await owner.attemptSessionAccess("verify");
    expect(afterCrash.code).toBe(0); expect(afterCrash.signal).toBeNull();
    expect(JSON.parse(afterCrash.stdout)).toEqual(owner.pin);
    await owner.start();
    expectTrustedMain(owner);
    expect(owner.mainPid()).not.toBe(firstMain);
    expect(owner.owner().pid).toBe(resident.pid);
    expect(owner.owner().token).toBe(resident.token);
    expect(owner.observations().filter(event => event.event === "module-loaded")).toHaveLength(1);
    expect(fs.readFileSync(owner.sessionFile)).toEqual(bytes);
    expect(owner.callbacks()).toHaveLength(0);
    expect(owner.observations().filter(event => String(event.event).startsWith("FORBIDDEN-"))).toHaveLength(0);
  }, 180_000);

  it("an actual 11-second worst-phase stall loses freshness/presence, NOT kernel OWNER custody", async () => {
    const owner = await realOwner(); await owner.start();
    expectTrustedMain(owner);
    const initial = owner.owner(), crossing = await residentIngress(owner);
    try {
      const leaseFile = hostLeasePath(owner.meshRoot, initial.hostId);
      const answer = crossing.ask({ mode: "stall", phaseLeaseFile: leaseFile });
      // Observe rejection immediately too: it cannot leak an unhandled rejection while stalled.
      const completed = answer.then(value => ({ value }), error => ({ error }));
      await owner.until(() => owner.observations().some(event => event.event === "stall-enter"), "actual synchronous callback entry");
      const stalled = owner.observations().find(event => event.event === "stall-enter")!;
      expect(stalled.leaseAgeMs).toBeGreaterThanOrEqual(4_700);
      const before = readHostLease(owner.meshRoot, initial.hostId)!;
      // This is the REAL recorded lease boundary, not a polling/retry delay and not
      // an increased lease. The 11s stall starts just before the unchanged 5s renewal.
      await new Promise(resolve => setTimeout(resolve, Math.max(0, before.expiresAt - Date.now() + 30)));
      expect(Date.now() - before.updatedAt).toBeGreaterThan(10_000); // confirmed freshness was already lost
      expect(readHostLease(owner.meshRoot, initial.hostId)?.updatedAt).toBe(before.updatedAt);
      await crossing.presence.refresh(true);
      expect(crossing.presence.view().records(ACTOR).some(record => record.live)).toBe(false);
      expect(owner.observations().some(event => event.event === "stall-settled")).toBe(false);
      expect(residentProcessAlive(initial.pid, initial.processStartTime)).toBe(true);
      // A real successor fence attempt cannot infer OWNER loss from stale presence.
      let accidentalFd: number | undefined;
      try {
        await expect(lockFile(path.join(owner.residencyRoot, "host.lock"), 0, true).then(fd => {
          accidentalFd = fd;
        })).rejects.toBeInstanceOf(FileLockBusy);
      } finally { if (accidentalFd !== undefined) fs.closeSync(accidentalFd); }
      expect(owner.owner().token).toBe(initial.token);
      await completed; // Outcome may be refused after freshness loss; never assert false success.
      await owner.until(() => owner.observations().some(event => event.event === "stall-settled"), "actual synchronous callback settlement");
      const settled = owner.observations().find(event => event.event === "stall-settled")!;
      expect(Number(settled.at) - Number(stalled.at)).toBeGreaterThanOrEqual(11_000);
      expect(owner.owner().token).toBe(initial.token);
      await owner.until(async () => {
        await crossing.presence.refresh(true);
        return crossing.presence.view().records(ACTOR).some(record => record.live);
      }, "normal unchanged-heartbeat recovery after the stall");
      expect((await crossing.ask({ request: "after-stall-recovery" })).accepted).toBe(true);
    } finally { await crossing.close(); }
  }, 180_000);
});
