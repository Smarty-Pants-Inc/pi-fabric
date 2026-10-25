// expected-skip: non-Linux: actual SDK Main/resident + kernel session custody required.
// Keep: #342 cross-version presence/control trust boundary and recorded N7 discovery failure.
// Shape/component experiments ONLY: not N7/N8 acceptance, callback delivery, or a product fix.
// Parent runs this file ONLY inside its existing live launcher/archive-before-cleanup driver.
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import { createJiti } from "jiti";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshStateEntry } from "../src/mesh/store.js";
import { StoreBridgeSide } from "../src/mesh/bridge.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { presenceValue } from "../src/topology/participant-view.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { NatsBridgePresence } from "../src/mesh/bridge-nats-presence.js";
import { openNatsBridgeSide, type NatsBridgePort } from "../src/mesh/bridge-nats-port.js";
import { bridgeNatsLinkIdentity } from "../src/mesh/bridge-nats-types.js";
import { openNatsTransport } from "../src/transport/nats/index.js";
import type { TransportEnvelope } from "../src/transport/types.js";
import { watchResidentChild } from "../src/residency/child-lifetime.js";
import { createNativeActorOwner, type NativeActorOwner } from "./helpers/native-actor-owner.js";
import { FROZEN_F3393715_COMMIT, FROZEN_F3393715_SHA256, type FrozenF3393715 } from "./helpers/frozen-f3393715.js";
import { launchLog } from "./helpers/owned-processes.js";
import { kvWorld, pKey, hKey } from "./helpers/bridge-nats-kv.js";
import { createPathBHarness, resolveNatsBinary, teardownWithin, type PathBHarness } from "./helpers/nats-cluster.js";

const repo = fileURLToPath(new URL("../", import.meta.url));
const producer = fileURLToPath(new URL("./fixtures/native-old-provider-pod.mjs", import.meta.url));
const ACTOR = "34200000000000000000000000000061"; // Synthetic; never the production actor.
const ORDINARY = "34200000000000000000000000000062";
const MACHINE = "machine-a";
const zeros = { beforeAgentStart: 0, agentStart: 0, providerRequest: 0, providerStream: 0,
  providerResponse: 0, toolCall: 0, toolExecution: 0 };
const digest = (file: string): string => createHash("sha256").update(fs.readFileSync(file)).digest("hex");
const localKey = (id: string): string => `topology/participants/${createHash("sha256").update(id).digest("hex")}`;
const copy = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;
const save = (file: string, value: unknown): void => fs.writeFileSync(file, JSON.stringify(value, null, 2) + "\n", { mode: 0o600, flag: "wx" });
const outcome = async <T>(work: () => T | Promise<T>): Promise<{ ok: true; result: T } | { ok: false; error: string }> => {
  try { return { ok: true, result: await work() }; }
  catch (error) { return { ok: false, error: error instanceof Error ? error.message : String(error) }; }
};
type PublicOutcome = { ok: boolean; result?: unknown; error?: string };
type Ready = { pin: { rootId: string; sessionId: string; sessionFile: string }; timeoutMs: number;
  counts: typeof zeros; sdkUnconfigured: boolean; provenance: FrozenF3393715["provenance"] };

// Local launch plumbing for the EXISTING producer's state/discover/ask/stop actions.
// Not a new producer, RPC protocol, fabricated SDK context, or imported test suite.
function oldPod(owner: NativeActorOwner, broker: PathBHarness, actorId: string, label: string,
  sdkDirectory: string, nodeBinary: string) {
  const root = path.join(owner.root, `shape-old-pod-${label}`);
  fs.mkdirSync(root, { mode: 0o700 });
  const cwd = path.join(root, "service"), agentDir = path.join(root, "profile"), sessions = path.join(root, "sessions");
  const sourceDirectory = path.join(root, "frozen-source");
  for (const dir of [cwd, agentDir, sessions, sourceDirectory]) fs.mkdirSync(dir, { mode: 0o700 });
  save(path.join(agentDir, "settings.json"), { modelPolicy: { allow: [] }, packages: [], extensions: [],
    enableInstallTelemetry: false, compaction: { enabled: false }, retry: { enabled: false } });
  // Same supported saved-trust format as NativeOwner, scoped to this exact private cwd.
  save(path.join(agentDir, "trust.json"), { [fs.realpathSync(cwd)]: true });
  const pins = path.join(root, "pod-pins.json");
  save(pins, { cwd, agentDir, sourceDirectory, sdkDirectory, nodeBinary, repoRoot: repo, actorId,
    sessionFile: path.join(sessions, "pod.jsonl"), sessionPin: path.join(root, "session-pin.json"),
    policyEnv: broker.policyEnv, nats: { ...broker.runtimeOptions(broker.users.machineB), operatorMachine: "fabric-ops" } });
  const owned = launchLog(root);
  const env: NodeJS.ProcessEnv = Object.fromEntries(["PATH", "LANG", "LC_ALL", "CI", "GITHUB_ACTIONS", "RUNNER_ENVIRONMENT", "RUNNER_TEMP", "PI_TEST_FLEET_HOST"]
    .flatMap(key => process.env[key] === undefined ? [] : [[key, process.env[key]]]));
  Object.assign(env, owned.env, broker.policyEnv, { PI_OFFLINE: "1", HOME: path.resolve(owner.root, "../home"),
    USERPROFILE: path.resolve(owner.root, "../home"), TMPDIR: root, TMP: root, TEMP: root,
    PI_CODING_AGENT_DIR: agentDir, PI_CODING_AGENT_SESSION_DIR: sessions,
    XDG_CONFIG_HOME: path.join(root, "config"), XDG_CACHE_HOME: path.join(root, "cache"),
    XDG_DATA_HOME: path.join(root, "data"), XDG_STATE_HOME: path.join(root, "state"),
    PI_FABRIC_MESH_ROOT: path.join(root, "mesh"), PI_FABRIC_PROJECT_ROOT: cwd,
    PI_FABRIC_RUN_ROOT: path.join(root, "runs"), PI_FABRIC_AGENT_DIR: path.join(root, "exports"),
    PI_FABRIC_LOCK_STATS: "0", FABRIC_NATIVE_OLD_POD_PINS: pins });
  const child = spawn(nodeBinary, [producer], { cwd, env, stdio: ["ignore", "pipe", "pipe", "ipc"] });
  const lifetime = watchResidentChild(child);
  let resolveReady!: (value: Ready) => void, rejectReady!: (error: unknown) => void;
  let readySeen = false, sequence = 0;
  const pending = new Map<string, (value: PublicOutcome) => void>();
  const ready = new Promise<Ready>((resolve, reject) => { resolveReady = resolve; rejectReady = reject; });
  void ready.catch(() => undefined);
  const timer = setTimeout(() => rejectReady(new Error(`BLOCKED: actual old SDK/broker startup deadline; ${root}`)), 90_000);
  const closed = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(resolve => {
    child.once("close", (code, signal) => {
      clearTimeout(timer);
      save(path.join(root, "parent-native-close.json"), { pid: child.pid, code, signal, owned: owned.owned() });
      if (!readySeen) rejectReady(new Error(`BLOCKED: old SDK/config/auth startup exited ${code}/${signal}; ${root}`));
      for (const settle of pending.values()) settle({ ok: false, error: `BLOCKED: producer exited ${code}/${signal}` });
      pending.clear(); resolve({ code, signal });
    });
  });
  child.once("error", error => { clearTimeout(timer); rejectReady(error); });
  child.stdout!.on("data", bytes => fs.appendFileSync(path.join(root, "stdout.log"), bytes, { mode: 0o600 }));
  child.stderr!.on("data", bytes => fs.appendFileSync(path.join(root, "stderr.log"), bytes, { mode: 0o600 }));
  child.on("message", message => {
    fs.appendFileSync(path.join(root, "parent-ipc.jsonl"), JSON.stringify(message) + "\n", { mode: 0o600 });
    const value = message as { event?: string; id?: string } & PublicOutcome;
    if (value.event === "ready") { readySeen = true; clearTimeout(timer); resolveReady(message as Ready); }
    else if (value.id) { pending.get(value.id)?.(value); pending.delete(value.id); }
  });
  const call = (action: "state" | "discover" | "ask" | "stop", payload: Record<string, unknown> = {}): Promise<PublicOutcome> => {
    if (!child.connected || lifetime.exited) return Promise.reject(new Error("BLOCKED: actual old producer is not running"));
    const id = `${label}-${++sequence}`;
    return new Promise((resolve, reject) => {
      // Existing producer: ask abort observation at 40s, discover failure at 60s.
      // Neither changes the frozen 24h provider default or substitutes a business client.
      const deadline = setTimeout(() => { pending.delete(id); reject(new Error(`BLOCKED: old ${action} IPC observation deadline`)); }, 70_000);
      pending.set(id, value => { clearTimeout(deadline); resolve(value); });
      child.send({ ...payload, id, action }, error => {
        if (error) { clearTimeout(deadline); pending.delete(id); reject(error); }
      });
    });
  };
  return { root, child, ready, call, async stop(): Promise<void> {
    try {
      if (!lifetime.exited) {
        if (readySeen) {
          const result = await call("stop");
          if (!result.ok) throw new Error(`Old producer stop refused: ${result.error}`);
        } else await lifetime.stop();
      }
      const terminal = await teardownWithin("shape old producer native CLOSE", () => closed, 10_000);
      if (readySeen && (terminal.code !== 0 || terminal.signal !== null)) throw new Error("Old producer did not stop cleanly");
    } finally {
      if (!lifetime.exited) await lifetime.stop();
      await teardownWithin("shape old producer final CLOSE", () => closed, 10_000);
    }
  } };
}

let owner: NativeActorOwner;
let native: FabricParticipantRecord, rootRecord: FabricParticipantRecord, ordinary: FabricParticipantRecord;
let frozen: FrozenF3393715;
let OldDirectory: typeof ParticipantDirectory;
let OldMesh: typeof MeshStore;
let oldPresenceValue: typeof presenceValue;
let OldProjection: typeof import("../src/topology/nats-local-projection.js").NatsLocalProjection;
let OldHostLease: typeof import("../src/topology/host-leases.js").TransportHostLease;
let oldRunTransportAdmin: typeof import("../src/transport/nats/admin.js").runTransportAdmin;
let oldReadFile: typeof import("../src/topology/participant-files.js").readParticipantFile;
let sdkDirectory: string, nodeBinary: string;
const nativeForms = (): Array<[string, unknown, boolean]> => [
  ["omitted", undefined, false], ["literal-native", "native", true],
  ["null", null, false], ["number", 17, false], ["malformed-token", "bad runner", false],
];
function form(runner: unknown): FabricParticipantRecord {
  const record = copy(native) as unknown as Record<string, unknown>;
  delete record.runner;
  if (runner !== undefined) record.runner = runner;
  return record as unknown as FabricParticipantRecord; // Deliberately malformed input, NOT runtime identity.
}
function kvEntry(record: FabricParticipantRecord) {
  return { key: pKey(MACHINE, record.id), machine: MACHINE, owner: `${record.ownerHostId}#${record.ownerIncarnation}`,
    revision: 1, value: record };
}
const evidence = (name: string, value: unknown): void => save(path.join(owner.root, name), value);

async function retainBroker(broker: PathBHarness, child: ChildProcess): Promise<void> {
  await broker.closeConnections();
  if (child.exitCode === null && child.signalCode === null) {
    const life = watchResidentChild(child);
    await life.stop(); await teardownWithin("shape broker actual terminal", () => life.exit, 10_000);
  }
  evidence("shape-broker-terminal.json", { pid: child.pid, code: child.exitCode, signal: child.signalCode, version: broker.serverVersion });
  // Existing harness.stop deletes its root. Retain the WHOLE private synthetic broker first.
  // If copying fails, do NOT invoke destructive harness cleanup: retain its root for Parent.
  fs.cpSync(broker.root, path.join(owner.root, "shape-broker-retained"), { recursive: true, dereference: false, force: false, errorOnExist: true });
  await broker.stop();
}

describe.skipIf(process.platform !== "linux")("f339 native participant SHAPE experiments (not callback acceptance)", () => {
  beforeAll(async () => {
    sdkDirectory = process.env.FABRIC_NATIVE_TEST_PI_SDK_DIST ?? "";
    const explicitNode = process.env.FABRIC_NATIVE_TEST_NODE;
    if (!sdkDirectory || !explicitNode) throw new Error("BLOCKED: approved SDK + explicit Node + live external retention driver required");
    nodeBinary = fs.realpathSync(explicitNode);
    owner = await createNativeActorOwner({ candidateRoot: repo, sdkDirectory, nodeBinary, actorId: ACTOR });
    await owner.start();
    // Observe real committed producer bytes, bypassing NO parser or control gate.
    // N7 fails downstream of these bytes; absence here is a prerequisite failure, not shape evidence.
    await owner.until(() => {
      const entries = owner.mesh.listAll("topology/participants/", { fresh: true });
      const actors = entries.filter(entry => (entry.value as FabricParticipantRecord)?.id === ACTOR);
      const roots = entries.filter(entry => (entry.value as FabricParticipantRecord)?.id === owner.pin.rootId);
      if (actors.length !== 1 || roots.length !== 1) return false;
      const actor = actors[0]!.value as FabricParticipantRecord;
      if (!actor.ownerIncarnation || actor.status !== "idle") return false;
      native = copy(actor); rootRecord = copy(roots[0]!.value as FabricParticipantRecord);
      evidence("shape-native-source.json", { actorEntry: actors[0], rootEntry: roots[0], actualOwner: owner.owner(),
        actualSession: owner.pin, warning: "Subsequent runner variants are controlled component experiments, NOT live receiver proof" });
      return true;
    }, "unique real idle native participant + real epoch/root");
    expect(native).not.toHaveProperty("runner");
    expect(native.rootId).toBe(owner.pin.rootId);
    expect(native.ownerHostId).toBe(owner.owner().hostId);
    expect(native.ownerIdentityId).toBe(owner.owner().hostId);
    expect(native.capabilities).toEqual(["stop", "ask"]);
    // ORDINARY model-actor metadata control only. NEVER a candidate encoding of native.
    ordinary = { ...copy(native), id: ORDINARY, name: "ordinary-shape-control", runner: "pi" };
    const source = path.join(owner.root, "shape-frozen-source"); fs.mkdirSync(source, { mode: 0o700 });
    const jiti = createJiti(import.meta.url, { tryNative: false, fsCache: path.join(owner.root, "shape-jiti-cache") });
    const helper = await jiti.import<typeof import("./helpers/frozen-f3393715.js")>(path.join(repo, "tests/helpers/frozen-f3393715.ts"));
    frozen = await helper.loadFrozenF3393715(source, repo);
    expect(frozen.provenance.commit).toBe(FROZEN_F3393715_COMMIT);
    expect(frozen.provenance.sourceTree).toBe("98c0b004c04662de15b3d4e1440125d67892fcce");
    expect(frozen.provenance.sha256).toEqual(FROZEN_F3393715_SHA256);
    const [directory, store, view, projection, files, leases, admin] = await Promise.all([
      jiti.import<typeof import("../src/topology/participant-directory.js")>(path.join(source, "src/topology/participant-directory.ts")),
      jiti.import<typeof import("../src/mesh/store.js")>(path.join(source, "src/mesh/store.ts")),
      jiti.import<typeof import("../src/topology/participant-view.js")>(path.join(source, "src/topology/participant-view.ts")),
      jiti.import<typeof import("../src/topology/nats-local-projection.js")>(path.join(source, "src/topology/nats-local-projection.ts")),
      jiti.import<typeof import("../src/topology/participant-files.js")>(path.join(source, "src/topology/participant-files.ts")),
      jiti.import<typeof import("../src/topology/host-leases.js")>(path.join(source, "src/topology/host-leases.ts")),
      jiti.import<typeof import("../src/transport/nats/admin.js")>(path.join(source, "src/transport/nats/admin.ts")),
    ]);
    OldDirectory = directory.ParticipantDirectory; OldMesh = store.MeshStore; oldPresenceValue = view.presenceValue;
    OldProjection = projection.NatsLocalProjection; oldReadFile = files.readParticipantFile;
    OldHostLease = leases.TransportHostLease;
    oldRunTransportAdmin = admin.runTransportAdmin;
    const pins = {
      "src/transport/nats/admin.ts": "a8db9be5d95ef0d6618195f80f9dbc1f142046ea25d9f343cc9f53850e6f4b3d",
      "src/transport/nats/inbox.ts": "910434d4fb7c321ab79088564764e8b4a461f04c28a8a1bc9c3b647d59f487bc",
      "src/transport/nats/provision.ts": "9de0455682c1f862c878eeb84e243c70d207eee02bde9f85fd61d52d66234561",
      "src/topology/host-leases.ts": "3acd9923ff5ec1be1183e615d8d17739464d54a48fd1ab1ed26ab3f9d45d92bc",
      "src/topology/participant-directory.ts": "90023c73296cdaab894b78a0697c0365619443be2d2bac8a6750b70a9a57b01c",
      "src/topology/participant-view.ts": "4b9c7830799f9e9d8c6ef35e93f69563814e064e8f791a359ea96e60660ce1f8",
      "src/topology/nats-local-projection.ts": "f61475456ba7572b227a26691b8479e8dd7d70a19a2e55b5714d99240270a212",
    };
    for (const [file, hash] of Object.entries(pins)) expect(digest(path.join(source, file))).toBe(hash);
    evidence("shape-source-provenance.json", { featureSourceHead: null, reason: "Parent-held docs HEAD over dirty feature source",
      frozen: frozen.provenance, additionalFrozenPins: pins, producerSha256: digest(producer),
      current: Object.fromEntries(["src/topology/participant-directory.ts", "src/topology/participant-view.ts", "src/topology/records.ts",
        "src/mesh/bridge-nats-presence.ts", "src/mesh/bridge-nats-port.ts"].map(file => [file, digest(path.join(repo, file))])) });
  }, 120_000);

  afterAll(async () => {
    if (!owner) return;
    try {
      evidence("shape-final-native-observation.json", { callbacks: owner.callbacks(), observations: owner.observations(),
        rpcEvents: owner.rpcEvents, actualOwner: owner.owner() });
      expect(owner.callbacks()).toHaveLength(0);
      expect(owner.observations().some(row => String(row.event).startsWith("FORBIDDEN-"))).toBe(false);
      for (const event of ["agent_start", "turn_start", "tool_execution_start", "tool_execution_update", "tool_execution_end"]) {
        expect(owner.rpcEvents.filter(row => row.type === event)).toHaveLength(0);
      }
    } finally {
      // LAST: native Main/resident terminal join + manifest including ALL raw pods/source/broker traces.
      // External custodian MUST archive/verify the entire tree before cleanup, success OR failure.
      await owner.close();
    }
  }, 90_000);

  it("runs actual OLD FILE parser (through public list) + OLD/current KV validators on omitted/native/malformed, with valid controls", async () => {
    const observations: unknown[] = [];
    for (const [label, runner, accepted] of nativeForms()) {
      const record = form(runner);
      const mesh = new OldMesh(path.join(owner.root, `shape-old-parser-${label}`), 256 * 1024, 1_000);
      const directory = new OldDirectory(mesh, { enabled: true, hostId: "shape-read-only-observer", rootId: owner.pin.rootId,
        identity: { id: "shape-read-only-observer", name: "component parser observer", kind: "agent" }, reapDeadHosts: false });
      try {
        // No start(), lease, fabricated Main/ctx or live receiver claim. Real MeshStore entries,
        // metadata-only read. Public OLD list necessarily invokes its unexported participantFromEntry.
        for (const value of [rootRecord, ordinary, record]) await mesh.put({ key: localKey(value.id), value,
          identity: { id: value.ownerIdentityId, name: "controlled parser metadata", kind: "agent" } });
        const listed = directory.list({ scope: "project", includeStale: true, fresh: true });
        const row = { label, input: record, oldFile: listed.find(value => value.id === ACTOR) ?? null,
          oldKv: oldPresenceValue(kvEntry(record)) ?? null, currentKv: presenceValue(kvEntry(record)) ?? null,
          controls: listed.filter(value => value.id === ORDINARY || value.id === rootRecord.id) };
        observations.push(row); evidence(`shape-parser-${label}.json`, row);
        expect(row.controls.map(value => value.id).sort()).toEqual([ORDINARY, rootRecord.id].sort());
        expect(row.oldFile !== null).toBe(accepted);
        expect(row.oldKv !== null).toBe(accepted);
        expect(row.currentKv !== null).toBe(accepted);
        if (accepted) expect(row.oldFile).toMatchObject({ runner: "native", capabilities: ["stop", "ask"] });
      } finally { await directory.close(); mesh.closeState(); }
    }
    // Missing runner on an ordinary root/agent remains malformed; no generic permissive repair.
    for (const kind of ["root", "agent"] as const) {
      const bad = { ...copy(rootRecord), kind }; delete bad.runner;
      expect(presenceValue(kvEntry(bad))).toBeUndefined(); expect(oldPresenceValue(kvEntry(bad))).toBeUndefined();
    }
    evidence("shape-parser-summary.json", observations);
  }, 30_000);

  it("measures CURRENT full directory publish-batch prefix abort versus bridge partial publication, exact PUT count/order (unit KV only)", async () => {
    const rows = { root: rootRecord, ordinary, native };
    const results: unknown[] = [];
    // Actual ParticipantDirectory start -> refreshNats -> KvParticipantDirectory.publish -> ownRecord.
    // Existing unit KV, NOT a broker/native-callback acceptance substitute. Ordering is source order.
    const orders: ReadonlyArray<ReadonlyArray<keyof typeof rows>> = [["native", "root", "ordinary"],
      ["root", "native", "ordinary"], ["root", "ordinary", "native"], ["root", "ordinary"]];
    for (const order of orders) {
      const label = order.join("-");
      const w = kvWorld(), mesh = new MeshStore(path.join(owner.root, `shape-batch-${label}`, "mesh"), 256 * 1024, 1_000);
      const state = path.join(owner.root, `shape-batch-${label}`, "transport-state"); fs.mkdirSync(state, { recursive: true, mode: 0o700 });
      const directory = new ParticipantDirectory(mesh, { enabled: true, transport: w.transport(MACHINE, rootRecord.id),
        transportStateDir: state, hostId: rootRecord.id, rootId: rootRecord.id,
        identity: { id: rootRecord.id, name: "unit metadata publisher, not Main", kind: "main" }, reapDeadHosts: false });
      directory.registerSource(() => order.map(kind => copy(rows[kind])));
      try {
        const result = await outcome(() => directory.start());
        const puts = [...w.writes];
        const expectedIds = order.slice(0, order.indexOf("native") < 0 ? order.length : order.indexOf("native"))
          .map(kind => rows[kind].id);
        const observation = { scope: "unit KV actual directory lifecycle", order, result, puts,
          presencePutIds: puts.filter(key => key.includes(".p.")).map(key => Object.values(rows).find(row => pKey(MACHINE, row.id) === key)?.id) };
        evidence(`shape-publish-batch-${label}.json`, observation); results.push(observation);
        expect(result.ok).toBe(!order.includes("native"));
        if (!result.ok) expect(result.error).toContain("Invalid owned KV participant record");
        expect(puts).toEqual([hKey(MACHINE, rootRecord.id), ...expectedIds.map(id => pKey(MACHINE, id))]);
      } finally { await directory.close(); mesh.closeState(); }
    }
    // Actual bridge writer isolates a refused actor instead of aborting the later ordinary actor.
    for (const actors of [[native, ordinary], [ordinary, native]]) {
      const w = kvWorld(), now = Date.now();
      const side = new NatsBridgePresence({ transport: w.transport(MACHINE, "shape-unit-bridge"), link: owner.binding.ingress.link,
        admit: (record, direction) => direction === "toNats" && [rootRecord.id, ACTOR, ORDINARY].includes(record.id) });
      try {
        side.start(); await side.whenReady(AbortSignal.timeout(5_000));
        const host = { record: { format: 1 as const, id: rootRecord.id, rootId: rootRecord.id,
          identity: { id: rootRecord.id, name: rootRecord.name, kind: "main" as const },
          startedAt: rootRecord.startedAt, updatedAt: now, expiresAt: now + 60_000 }, observedAt: now, expiresAt: now + 60_000 };
        const roots = await side.mirror({ hosts: [host], participants: [rootRecord] }, now);
        const actorResult = await side.mirrorActors(actors, { sampledAt: now, complete: true });
        const observation = { scope: "unit KV actual bridge writer", order: actors.map(row => row.id), roots, actorResult, puts: [...w.writes] };
        evidence(`shape-bridge-partial-${actors[0]!.id}.json`, observation); results.push(observation);
        expect(roots.held).toEqual([rootRecord.id]); expect(actorResult.held).toEqual([ORDINARY]);
        expect(actorResult.refused).toEqual([{ id: ACTOR, code: "FABRIC_BRIDGE_NATS_RECORD_INVALID" }]);
        expect(w.writes).toEqual([hKey(MACHINE, rootRecord.id), pKey(MACHINE, rootRecord.id),
          hKey(MACHINE, ordinary.ownerHostId), pKey(MACHINE, ORDINARY)]);
        expect(w.presence.values.has(pKey(MACHINE, ACTOR))).toBe(false);
        expect(w.presence.values.has(pKey(MACHINE, ORDINARY))).toBe(true);
      } finally { await side.close(); }
    }
    evidence("shape-current-batch-summary.json", results);
  }, 60_000);

  it("uses real broker committed rows in the ACTUAL OLD projector and actual SDK public ask; ordinary routing prerequisite before native classification", async () => {
    const binary = resolveNatsBinary(); if (!binary) throw new Error("BLOCKED: explicit already-installed NATS_BIN required");
    let broker: PathBHarness | undefined, brokerChild: ChildProcess | undefined, port: NatsBridgePort | undefined;
    let inspector: Awaited<ReturnType<typeof openNatsTransport>> | undefined;
    const pods: ReturnType<typeof oldPod>[] = [], frames: Array<{ sourceMachine: string; envelope: TransportEnvelope }> = [];
    const inboxAbort = new AbortController(); let inboxTask: Promise<void> | undefined;
    const cleanupErrors: unknown[] = [], results: unknown[] = [];
    try {
      broker = await createPathBHarness({ binary, tls: {} }); brokerChild = broker.serverProcess;
      if (!brokerChild) throw new Error("BLOCKED: actual broker child custody missing");
      expect(broker.serverVersion).toBe("2.14.7");
      const admin = broker.adminEnv(broker.users.bootstrap);
      const previous = Object.fromEntries(Object.keys(admin).map(key => [key, process.env[key]]));
      for (const [key, value] of Object.entries(admin)) vi.stubEnv(key, value);
      // Actual frozen deployments have max_age:0. The current admin creates bounded
      // streams, which the unchanged OLD verifier correctly rejects before any ask.
      // Provision this private TEST broker with the exact OLD admin, never relax that
      // verifier or change production retention. Current transport accepts legacy 0.
      try { expect(await oldRunTransportAdmin(["provision", "--ca-file", broker.tls!.caFile, "--machines", "machine-a,machine-b"])).toBe(0); }
      finally { for (const [key, value] of Object.entries(previous)) vi.stubEnv(key, value); }
      const runtime = broker.runtimeOptions(broker.users.machineA), link = owner.binding.ingress.link;
      // openNatsConnection retains its caller signal for the connection lifetime.
      // Bound STARTUP independently; the real bridge must live until owned teardown,
      // not die at 20s while the unchanged producer's 40s observation is pending.
      port = await teardownWithin("shape real bridge startup", () => openNatsBridgeSide({ link,
        nats: { servers: [runtime.servers], machine: MACHINE,
          credentials: runtime.credentials, caFile: broker!.tls!.caFile, ...broker!.permissionPolicy },
        allow: { file: [{ kind: "root", id: rootRecord.id },
          { kind: "actor", rootId: native.rootId, name: native.name, id: ACTOR },
          { kind: "actor", rootId: ordinary.rootId, name: ordinary.name, id: ORDINARY }], nats: [] } },
      { signal: inboxAbort.signal, log: line => fs.appendFileSync(path.join(owner.root, "shape-real-port.log"), line + "\n", { mode: 0o600 }) }), 20_000);
      const fileSide = new StoreBridgeSide(owner.mesh, link);
      const refreshMirror = async (label: string): Promise<void> => {
        // Sample the ACTUAL live FILE root/host deadlines. No invented source lease/epoch,
        // periodic sampler or widened freshness bound. Each new shape starts from a fresh sample.
        const snapshot = await fileSide.presence(), sampledAt = Date.now();
        const actualRoot = snapshot.participants.find(row => row.id === rootRecord.id);
        const actualNative = owner.mesh.listAll("topology/participants/", { fresh: true })
          .filter(entry => (entry.value as FabricParticipantRecord)?.id === ACTOR);
        if (!actualRoot || actualNative.length !== 1 || !snapshot.hosts.some(host => host.record.id === native.ownerHostId)) {
          throw new Error("BLOCKED: actual FILE owner/root sample unavailable; not native-form incompatibility");
        }
        expect((actualNative[0]!.value as FabricParticipantRecord).ownerIncarnation).toBe(native.ownerIncarnation);
        expect((actualNative[0]!.value as FabricParticipantRecord).status).toBe("idle");
        evidence(`shape-real-source-sample-${label}.json`, { snapshot, actualNativeEntry: actualNative[0],
          actualOwner: owner.owner(), controlledOrdinary: ordinary, sampledAt });
        await port!.mirror({ hosts: snapshot.hosts, participants: [actualRoot, native, ordinary],
          linkIdentity: bridgeNatsLinkIdentity(link) }, sampledAt);
      };
      await refreshMirror("ordinary-control");
      inspector = await openNatsTransport({ ...runtime, hostId: "shape-committed-record-inspector", events: false, provision: false });
      const presence = inspector.kv("presence");
      expect(await presence.get(pKey(MACHINE, ACTOR))).toBeUndefined(); // Actual mirror refused just the native row.
      expect(await presence.get(pKey(MACHINE, ORDINARY))).toBeDefined();
      inboxTask = (async () => {
        for await (const delivery of port!.inbox(inboxAbort.signal)) {
          const frame = { sourceMachine: delivery.sourceMachine, envelope: copy(delivery.envelope) };
          frames.push(frame);
          fs.appendFileSync(path.join(owner.root, "shape-actual-old-control-frames.jsonl"), JSON.stringify(frame) + "\n", { mode: 0o600 });
          // Broker delivery ACK ONLY. No Fabric control ACK, response, FILE append, or callback.
          await delivery.ack();
        }
      })();
      void inboxTask.catch(() => undefined);
      const checkReady = (ready: Ready): void => {
        expect(ready.provenance.commit).toBe(FROZEN_F3393715_COMMIT);
        expect(ready.provenance.sourceTree).toBe(frozen.provenance.sourceTree);
        expect(ready.provenance.sha256).toEqual(FROZEN_F3393715_SHA256);
        expect(ready.timeoutMs).toBe(86_400_000); expect(ready.sdkUnconfigured).toBe(true); expect(ready.counts).toEqual(zeros);
      };
      const ask = async (pod: ReturnType<typeof oldPod>, id: string, label: string) => {
        const before = frames.length;
        const resolutionBefore = await outcome(() => port!.resolve(id) ?? null);
        const data = { experiment: label, opaque: "  exact whitespace  " };
        const result = await pod.call("ask", { actorId: id, message: "native-residency.test.v1", data });
        const asks = frames.slice(before).filter(row => row.envelope.kind === "command" && row.envelope.operation === "ask" && row.envelope.targetId === id);
        const observation = { label, result, actualAskFrames: asks, allFrames: frames.slice(before),
          resolutionBefore, actualPortResolution: await outcome(() => port!.resolve(id) ?? null),
          producerState: await pod.call("state") };
        evidence(`shape-old-public-${label}.json`, observation); results.push(observation);
        if (!result.ok && /Unknown Fabric (?:action|provider)|disabled|BLOCKED:|not initialized|not registered/i.test(result.error ?? "")) {
          throw new Error(`BLOCKED: old action/config/runtime prerequisite, NOT native-form incompatibility: ${result.error}`);
        }
        expect(result.ok, "No Fabric reply is manufactured; actual public cancellation/refusal must settle").toBe(false);
        if (observation.producerState.ok) expect((observation.producerState.result as Ready).counts).toEqual(zeros);
        else throw new Error(`BLOCKED: actual producer state failed: ${observation.producerState.error}`);
        for (const row of asks) {
          expect(row.sourceMachine).toBe("machine-b");
          expect(row.envelope.body).toMatchObject({ targetId: id, message: "native-residency.test.v1", data });
          expect(row.envelope.body).not.toHaveProperty("binding"); expect(row.envelope.body).not.toHaveProperty("overrides");
        }
        return { result, asks };
      };
      // This ordinary ACTOR metadata is a separate valid pi control, NOT native relabelled pi.
      // Its actual SDK/context/config/action/authenticated route MUST work before classifying native.
      const control = oldPod(owner, broker, ORDINARY, "ordinary-control", sdkDirectory, nodeBinary); pods.push(control);
      checkReady(await control.ready);
      await refreshMirror("ordinary-control-ready");
      const controlDiscovery = await control.call("discover", { actorId: ORDINARY });
      evidence("shape-old-ordinary-discovery.json", controlDiscovery);
      if (!controlDiscovery.ok) throw new Error(`BLOCKED: ordinary visibility prerequisite: ${controlDiscovery.error}`);
      expect(controlDiscovery.result).toMatchObject({ id: ORDINARY, runner: "pi", machine: MACHINE });
      const ordinaryAsk = await ask(control, ORDINARY, "ordinary-control");
      if (ordinaryAsk.asks.length !== 1) throw new Error(`BLOCKED: ordinary public agents.ask did not reach broker; ${JSON.stringify(ordinaryAsk.result)}`);
      await control.stop();
      for (const [label, runner, accepted] of nativeForms().filter(([label]) => ["omitted", "literal-native", "malformed-token"].includes(label))) {
        await refreshMirror(label);
        const record = form(runner), key = pKey(MACHINE, ACTOR);
        const previousEntry = await presence.get(key);
        // EXPLICIT controlled-shape input via real own-machine authenticated KV, NOT a product
        // mirror fix/auth bypass. Required because current mirror refuses the omitted form.
        // The actual port holds the same real native owner epoch/lease via the ordinary control.
        const revision = await presence.put(key, `${record.ownerHostId}#${record.ownerIncarnation}`, record, previousEntry?.revision ?? 0);
        const committed = await presence.get(key);
        expect(committed).toMatchObject({ key, revision, value: record });
        const mesh = new OldMesh(path.join(owner.root, `shape-old-real-projection-${label}`), 256 * 1024, 1_000);
        const errors: string[] = [];
        const leaseState = path.join(owner.root, "shape-projection-lease-state"); fs.mkdirSync(leaseState, { recursive: true, mode: 0o700 });
        // Real old transport lease + broker CAS/process/receipt custody; never leaseLive: () => true.
        const projectionLease = new OldHostLease({ kv: inspector.kv("leases"), self: inspector.self,
          rootId: owner.pin.rootId, identityId: inspector.self.hostId, incarnation: `shape-${label}`, localStateDir: leaseState });
        const projection = new OldProjection({ mesh, transport: inspector, leaseLive: () => projectionLease.live(),
          onError: error => { errors.push(String(error)); } });
        try {
          const acquired = await projectionLease.acquire(AbortSignal.timeout(10_000));
          evidence(`shape-old-projector-actual-lease-${label}.json`, { acquired, self: inspector.self,
            committed: await inspector.kv("leases").get(projectionLease.key) });
          expect(projectionLease.live()).toBe(true);
          projection.start(); await teardownWithin("actual frozen projector ready", () => projection.whenReady(), 15_000);
          const projected = oldReadFile(mesh.root, localKey(ACTOR));
          const goodRoot = oldReadFile(mesh.root, localKey(rootRecord.id));
          const goodOrdinary = oldReadFile(mesh.root, localKey(ORDINARY));
          const observation = { label, scope: "actual frozen projector over real committed broker records, controlled shape",
            committed, ownerLease: await inspector.kv("leases").get(hKey(MACHINE, native.ownerHostId)),
            projected: projected ?? null, goodRoot, goodOrdinary, metrics: projection.metrics(), errors };
          evidence(`shape-old-real-projection-${label}.json`, observation); results.push(observation);
          expect(errors).toEqual([]); expect(goodRoot).toBeDefined(); expect(goodOrdinary).toBeDefined();
          expect(projected !== undefined).toBe(accepted);
          if (projected) expect((projected as MeshStateEntry & { natsProjection?: unknown }).natsProjection).toMatchObject({ kvKey: key, revision, machine: MACHINE });
        } finally {
          try { await projection.close(); }
          finally { try { await projectionLease.release(AbortSignal.timeout(10_000)); } finally { mesh.closeState(); } }
        }
        if (label === "malformed-token") continue; // Parser/projection negative, not another costly ask timeout.
        const pod = oldPod(owner, broker, ACTOR, label, sdkDirectory, nodeBinary); pods.push(pod);
        checkReady(await pod.ready); // Any SDK/broker/config/action failure is BLOCKED, never "unsupported".
        await refreshMirror(`${label}-public-ready`);
        if (accepted) {
          const discovered = await pod.call("discover", { actorId: ACTOR }); evidence(`shape-old-discovery-${label}.json`, discovered);
          expect(discovered.ok).toBe(true);
          expect(discovered.result).toMatchObject({ id: ACTOR, runner: "native", rootId: native.rootId,
            ownerHostId: native.ownerHostId, ownerIncarnation: native.ownerIncarnation, machine: MACHINE });
        }
        const actual = await ask(pod, ACTOR, label);
        expect(actual.asks).toHaveLength(accepted ? 1 : 0);
        if (!accepted) expect(actual.result.error).toContain("Unknown Fabric actor");
        for (const row of actual.asks) expect(row.envelope).toMatchObject({ to: { machine: MACHINE, hostId: native.ownerHostId },
          from: { id: (await pod.ready).pin.rootId }, body: { ownerIncarnation: native.ownerIncarnation } });
        await pod.stop();
      }
      expect(owner.callbacks()).toHaveLength(0);
      evidence("shape-old-broker-public-summary.json", { results, frames, limitation: "No control response or native receiver invoked; actual public error is retained, not business notRun" });
    } finally {
      const cleanup = async (work: () => Promise<unknown>): Promise<void> => { try { await work(); } catch (error) { cleanupErrors.push(error); } };
      for (const pod of pods.reverse()) await cleanup(() => pod.stop());
      inboxAbort.abort();
      if (port) await cleanup(() => port!.close());
      if (inboxTask) await cleanup(() => teardownWithin("actual port capture ended", () => inboxTask!, 10_000));
      if (inspector) await cleanup(() => inspector!.close());
      if (broker && brokerChild) await cleanup(() => retainBroker(broker!, brokerChild!));
      else if (broker) cleanupErrors.push(new Error(`BLOCKED: broker has no captured child; retain ${broker.root}`));
      evidence("shape-broker-finally.json", { results, frames, cleanupErrors: cleanupErrors.map(String),
        brokerRoot: broker?.root, note: "NativeOwner.close manifest follows in afterAll; Parent must archive whole custody root before cleanup" });
      if (cleanupErrors.length) throw new AggregateError(cleanupErrors, "Shape experiment custody incomplete; retain external launcher root");
    }
  }, 300_000);
});
