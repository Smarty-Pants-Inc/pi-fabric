import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { MainAgentController, type HeldAgentMessage } from "../src/main-agent.js";
import type { NativeRecoveryCustodyReader, NativeRecoveryCustodyResult, NativeRecoverySessionRef,
  NativeRecoveryTransfer } from "../src/core/session-receipts.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { MainInboxMaintenance, mainInboxActive, mainInboxOwns, recordMainSuccessor,
  registerMainInbox, stageMainSuccessor, confirmMainSuccessor, reconcileNativeMainInboxCustody,
  resolveNativeMainSuccessor, registeredMainLauncherIdentity } from "../src/topology/stall-alarms.js";
import { processStartTime } from "../src/residency/process-identity.js";
import * as atomicWrite from "../src/core/atomic-write.js";
import * as processIdentity from "../src/residency/process-identity.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

// Keep: data safety + native/Fabric custody trust boundary. These use the real
// mesh lock, Main journals and route files; only N's canonical validator is injected.
// Positive receiver birth evidence is Linux /proc evidence, unavailable on other hosts.
const nativeIt = (name: string, run: () => Promise<void>) =>
  it.skipIf(process.platform !== "linux")(name, run, 60_000); // real file + namespace barriers across repeated custody passes
const nativeFixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-custody-"));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  const controllers: MainAgentController[] = [];
  const transfers: NativeRecoveryTransfer[] = [];
  let unavailable = false;
  const reader: NativeRecoveryCustodyReader = scope => {
    if (unavailable) throw new Error("native file confirmation unavailable");
    const outgoing = transfers.findIndex(edge => edge.source.sessionId === scope.sessionId && edge.source.sessionFile === scope.sessionFile);
    const last = outgoing >= 0 ? outgoing : transfers.findIndex(edge => edge.target.sessionId === scope.sessionId && edge.target.sessionFile === scope.sessionFile);
    return last < 0 ? { status: "none" } : { status: "validated", transfers: transfers.slice(0, last + 1) };
  };
  const identity = (n: number): MeshIdentity => {
    const id = `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
    return { id: `session:${id}`, sessionId: id, name: `native-${n}`, kind: "main" };
  };
  const S = identity(1), T = identity(2), U = identity(3), peer = identity(4);
  const ref = (who: MeshIdentity): NativeRecoverySessionRef => ({ sessionId: who.sessionId!, sessionFile: path.join(root, `${who.sessionId}.jsonl`) });
  const mailbox = path.join(mesh.root, "main-followups");
  const journal = (who: MeshIdentity) => path.join(mailbox, `${who.sessionId}.json`);
  const owner = (who: MeshIdentity) => path.join(mailbox, `${who.sessionId}.owner.json`);
  const hashed = (directory: string, id: string) => path.join(mailbox, directory, `${createHash("sha256").update(id).digest("hex")}.json`);
  const route = (id: string) => hashed("routes", id);
  const successor = (who: MeshIdentity) => hashed("successors", who.id);
  const load = (file: string) => JSON.parse(fs.readFileSync(file, "utf8"));
  const save = (file: string, value: unknown) => { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, JSON.stringify(value)); };
  const items = (who: MeshIdentity): HeldAgentMessage[] => fs.existsSync(journal(who)) ? load(journal(who)).items : [];
  const inbox = (who: MeshIdentity, idle = false) => {
    const sendMessage = vi.fn();
    const pi = { on: () => () => {}, getThinkingLevel: () => "off", sendMessage } as unknown as ExtensionAPI;
    const scope = ref(who);
    if (!fs.existsSync(scope.sessionFile)) fs.writeFileSync(scope.sessionFile, `${JSON.stringify({ type: "session", version: 3, id: scope.sessionId })}\n`);
    const ctx = { isIdle: () => idle, hasPendingMessages: () => false, sessionManager: {
      getSessionId: () => scope.sessionId, getSessionFile: () => scope.sessionFile, getEntries: () => [], isPersisted: () => true,
    } } as unknown as ExtensionContext;
    const main = new MainAgentController(pi, who.id, true, root, who.sessionId);
    controllers.push(main);
    const activation = registerMainInbox(mesh.root, who, scope.sessionId, scope.sessionFile);
    main.attachFollowUpDrain(ctx, 60_000, journal(who), 600, {
      active: () => mainInboxActive(mesh.root, who.id, activation), owns: id => mainInboxOwns(mesh.root, who.id, id),
    }, reader);
    return { main, sendMessage, activation, scope };
  };
  const options = { rootPresenceAlarmMs: 60_000, undeliveredAlarmMs: 60_000, rootGoneTtlMs: 100 };
  const maintenance = (who: MeshIdentity, main: MainAgentController, live: MeshIdentity[] = []) => new MainInboxMaintenance(
    new MeshStore(mesh.root, 64 * 1024, 500), who,
    { list: () => live.map(who => ({ id: who.id, kind: "root", stale: false })) } as unknown as FabricParticipantSource,
    main, options, reader);
  const edge = (source: MeshIdentity, target: MeshIdentity, messages: Array<{ customType: string; details: unknown }>, phase: "prepared" | "received" = "prepared"): NativeRecoveryTransfer => {
    const common = { version: 1 as const, operationId: `operation-${source.sessionId}`, preparedDigest: createHash("sha256").update(source.id).digest("hex"),
      preparedEntryId: `prepared-${source.sessionId}`, source: ref(source), target: ref(target),
      occurrences: messages.map((customMessage, ordinal) => ({ ordinal, state: "pending" as const, customMessage })),
    };
    return phase === "received" ? { ...common, phase, receivedEntryId: `received-${target.sessionId}` } : { ...common, phase };
  };
  const receive = (edge: NativeRecoveryTransfer) => { Object.assign(edge, { phase: "received", receivedEntryId: `received-${edge.target.sessionId}` }); };
  const reconcile = (who: MeshIdentity = S) => reconcileNativeMainInboxCustody(mesh, ref(who), reader);
  const close = () => { for (const main of controllers) main.closeFollowUpDrain(); vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); };
  return { root, mesh, S, T, U, peer, transfers, reader, ref, inbox, maintenance, edge, receive, reconcile,
    journal, owner, successor, route, load, save, items, unavailable: (value: boolean) => { unavailable = value; }, close };
};

it("resume installs C before successor publication: competing drainers never inherit historical live D", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-resume-custody-"));
  for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
  vi.stubEnv("PI_FABRIC_RUN_ROOT", path.join(root, "runs"));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  const config = normalizeFabricConfig({ fullCodeMode: false,
    mesh: { enabled: true, root: mesh.root, followUpFlushMs: 60_000 },
    residency: { enabled: false }, agents: { enabled: false }, records: { enabled: false },
    mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
    prewalk: { enabled: false, alwaysRearm: false }, speculation: { enabled: false },
  });
  const identity = (id: string): MeshIdentity => ({ id: `session:${id}`, sessionId: id, name: id, kind: "main" });
  const A = identity("A"), B = identity("B"), C = identity("C"), D = identity("D");
  const controllers: MainAgentController[] = [];
  const inbox = (who: MeshIdentity, idle = false) => {
    const sendMessage = vi.fn();
    const pi = { on: () => () => {}, getThinkingLevel: () => "off", sendMessage } as unknown as ExtensionAPI;
    const file = path.join(root, `${who.sessionId}.jsonl`);
    const ctx = { isIdle: () => idle, hasPendingMessages: () => false,
      sessionManager: { getSessionId: () => who.sessionId, getSessionFile: () => file, getEntries: () => [] },
    } as unknown as ExtensionContext;
    const main = new MainAgentController(pi, who.id, true, root, who.sessionId);
    controllers.push(main);
    const activation = registerMainInbox(mesh.root, who, who.sessionId!, file);
    main.attachFollowUpDrain(ctx, 60_000, path.join(mesh.root, "main-followups", `${who.sessionId}.json`), 600,
      { active: () => mainInboxActive(mesh.root, who.id, activation), owns: id => mainInboxOwns(mesh.root, who.id, id) });
    return { main, sendMessage, file, activation };
  };
  const sendMessage = vi.fn();
  const pi = { on: () => () => {}, events: { emit: vi.fn() }, getThinkingLevel: () => "off",
    getSessionName: () => "C", sendMessage } as unknown as ExtensionAPI;
  const context = { cwd: root, hasUI: false, mode: "rpc", isProjectTrusted: () => true,
    isIdle: () => true, hasPendingMessages: () => false,
    modelRegistry: { getAvailable: () => [], find: () => undefined, getApiKeyAndHeaders: async () => ({ ok: true }) },
    sessionManager: { getSessionId: () => "C", getSessionFile: () => path.join(root, "C.jsonl"),
      getEntries: () => [], getBranch: () => [], getLeafId: () => null },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  } as unknown as ExtensionContext;
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
    extension: path.join(root, "unused-extension.mjs"), worker: path.join(root, "unused-worker.mjs"),
    residentHost: path.join(root, "unused-resident.mjs"), skills: root,
  } });
  let release!: () => void, reachConfirmation!: () => void;
  const hold = new Promise<void>(resolve => { release = resolve; });
  const confirmed = new Promise<void>(resolve => { reachConfirmation = resolve; });
  let initializing: Promise<void> | undefined;
  try {
    const historicalC = inbox(C);
    historicalC.main.closeFollowUpDrain();
    await recordMainSuccessor(mesh, C.id, "C", D.id);
    const liveD = inbox(D, true), competingA = inbox(A, true), oldB = inbox(B);
    const carrier = oldB.main.deliverAgent({ from: A, message: "intended for resumed C", delivery: "followUp", deliveryId: "resume-carrier" });
    const original = JSON.parse(fs.readFileSync(path.join(mesh.root, "main-followups", "B.json"), "utf8")).items[0];
    oldB.main.closeFollowUpDrain();
    await stageMainSuccessor(mesh, B.id, "B", historicalC.file);
    const successorFile = path.join(mesh.root, "main-followups", "successors", `${createHash("sha256").update(B.id).digest("hex")}.json`);
    const routeFile = path.join(mesh.root, "main-followups", "routes", `${createHash("sha256").update(carrier.messageId).digest("hex")}.json`);
    // C has no new presence yet; historical D is genuinely alive. Each competing
    // Main uses its own mesh store, like another process on the shared mesh.
    const participants = { list: () => [A, D].map(who => ({ id: who.id, kind: "root", stale: false })) } as unknown as FabricParticipantSource;
    const aDrain = new MainInboxMaintenance(new MeshStore(mesh.root, 64 * 1024, 500), A, participants, competingA.main, config.mesh);
    const dDrain = new MainInboxMaintenance(new MeshStore(mesh.root, 64 * 1024, 500), D, participants, liveD.main, config.mesh);
    // Inbox succession takes the file custody lock (smarty-dev#6477 L5); pause after
    // MeshStore.custody returns, i.e. after every lock it took has been released.
    const custody = MeshStore.prototype.custody;
    let paused = false;
    vi.spyOn(MeshStore.prototype, "custody").mockImplementation(async function<T>(this: MeshStore, operation: () => T, timeout?: number): Promise<T> {
      const result = await custody.call(this, operation, timeout) as T;
      if (this === runtime.mesh && !paused && fs.existsSync(successorFile) &&
        JSON.parse(fs.readFileSync(successorFile, "utf8")).newRoot === C.id) {
        paused = true;
        // Inject exactly the old confirmation/registration gap, AFTER release of
        // the custody lock but BEFORE initialize can attach C's receiving drain.
        reachConfirmation();
        await hold;
      }
      return result;
    });
    initializing = runtime.initialize(context, config);
    await Promise.race([confirmed, initializing.then(() => { throw new Error("Resume missed confirmation boundary"); })]);
    await Promise.all([aDrain.run(), dDrain.run()]);
    expect(liveD.sendMessage).not.toHaveBeenCalled();
    expect(competingA.sendMessage).not.toHaveBeenCalled();
    expect(mainInboxActive(mesh.root, C.id, historicalC.activation)).toBe(false);
    expect(JSON.parse(fs.readFileSync(routeFile, "utf8"))).toMatchObject({ newRoot: C.id, item: { id: carrier.messageId } });
    expect(JSON.parse(fs.readFileSync(routeFile, "utf8")).done).toBeUndefined();
    release();
    await initializing;
    await Promise.all([aDrain.run(), dDrain.run()]);
    expect(sendMessage).toHaveBeenCalledOnce();
    expect(sendMessage.mock.calls[0]![0].details.id).toBe(carrier.messageId);
    expect(liveD.sendMessage).not.toHaveBeenCalled();
    expect(mainInboxOwns(mesh.root, C.id, carrier.messageId)).toBe(true);
    expect(mainInboxOwns(mesh.root, B.id, carrier.messageId)).toBe(false);
    expect(mainInboxOwns(mesh.root, D.id, carrier.messageId)).toBe(false);
    const resumedB = inbox(B, true);
    expect(() => resumedB.main.receiveInboxItem(original)).toThrow("claim belongs elsewhere");
    expect(resumedB.sendMessage).not.toHaveBeenCalled();
    expect(mesh.read({ topic: "fleet.work.inbox-receipts" })).toMatchObject([
      { to: A.id, text: `rerouted: ${B.id} -> ${C.id}`, data: { messageId: carrier.messageId } },
    ]);
    expect(mesh.read({ topic: "fleet.work.inbox-receipts" })).toHaveLength(1);
  } finally {
    release();
    await initializing?.catch(() => undefined);
    await runtime.shutdown();
    for (const main of controllers) main.closeFollowUpDrain();
    vi.restoreAllMocks(); vi.unstubAllEnvs();
    fs.rmSync(root, { recursive: true, force: true });
  }
});

nativeIt("PREPARED reserves the outer carrier and every batch child despite live source/TTL, including passive context", async () => {
  const f = nativeFixture();
  try {
    const source = f.inbox(f.S), survivor = f.inbox(f.peer, true);
    const a = source.main.deliverAgent({ from: f.peer, message: "identical text", delivery: "steer", deliveryId: "batch-a" });
    const b = source.main.deliverAgent({ from: f.peer, message: "identical text", delivery: "nextTurn", triggerTurn: false, deliveryId: "batch-b" });
    source.main.deliverAgent({ from: f.peer, message: "Fabric-only neighbor", delivery: "followUp", deliveryId: "ordinary" });
    const children = source.sendMessage.mock.calls.map(call => call[0].details);
    const batch = { customType: "pi-fabric-agent-message", details: { ...children[0], id: "outer-native-batch", items: children } };
    f.transfers.push(f.edge(f.S, f.T, [batch]));
    const maintenance = f.maintenance(f.peer, survivor.main, [f.S, f.peer]);
    await maintenance.run(1_000); await maintenance.run(100_000);
    for (const id of ["outer-native-batch", a.messageId, b.messageId]) {
      expect(f.load(f.route(id))).toMatchObject({ oldRoot: f.S.id, newRoot: f.T.id, messageId: id,
        nativeRecovery: { operationId: f.transfers[0]!.operationId, target: f.ref(f.T), ordinals: [0] } });
      expect(mainInboxOwns(f.mesh.root, f.S.id, id)).toBe(false);
      expect(mainInboxOwns(f.mesh.root, f.T.id, id)).toBe(true);
    }
    expect(mainInboxActive(f.mesh.root, f.S.id, source.activation)).toBe(false);
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toThrow("no committed successor locator");
    expect(() => source.main.deliverAgent({ from: f.peer, message: "must not reopen S", delivery: "steer" })).toThrow("rotated");
    expect(() => registerMainInbox(f.mesh.root, f.S, f.S.sessionId!, source.scope.sessionFile)).toThrow("native recovery source");
    expect(f.items(f.S).map(item => item.id)).toEqual(expect.arrayContaining([a.messageId, b.messageId]));
    expect(survivor.sendMessage).not.toHaveBeenCalled();
    expect(f.mesh.read({ topic: "fleet.work.inbox-receipts" })).toEqual([]);
    source.main.closeFollowUpDrain();
    const target = f.inbox(f.T, true); f.receive(f.transfers[0]!); await f.reconcile(f.T);
    const targetMaintenance = f.maintenance(f.T, target.main, [f.T]);
    await targetMaintenance.run(); await targetMaintenance.run();
    // Equal text does not merge distinct children, and native nextTurn stays passive.
    expect(f.items(f.T).filter(item => item.message === "identical text").map(item => item.id).sort()).toEqual([a.messageId, b.messageId].sort());
    expect(f.items(f.T).find(item => item.id === b.messageId)).toMatchObject({ deliverAs: "nextTurn", triggerTurn: false });
    expect(target.sendMessage).toHaveBeenCalledOnce(); // the unrelated Fabric-only neighbor
    expect(target.sendMessage.mock.calls[0]![0].details.deliveryId).toBe("ordinary");
  } finally { f.close(); }
});

nativeIt("RECEIVED preserves exact receiver evidence and original bookkeeping, but only consumption retires payload", async () => {
  const f = nativeFixture();
  try {
    const source = f.inbox(f.S);
    const native = source.main.deliverAgent({ from: f.peer, message: "native queued", delivery: "steer", deliveryId: "native-delivery" });
    const ordinary = source.main.deliverAgent({ from: f.peer, message: "Fabric-only", delivery: "followUp", deliveryId: "ordinary-delivery" });
    const edge = f.edge(f.S, f.T, [source.sendMessage.mock.calls[0]![0]]);
    f.transfers.push(edge);
    await f.reconcile();
    source.main.closeFollowUpDrain();
    await stageMainSuccessor(f.mesh, f.S.id, f.S.sessionId!, f.ref(f.T).sessionFile);
    const target = f.inbox(f.T, true);
    f.receive(edge); await f.reconcile(f.T);
    const expected = { oldRoot: f.S.id, newRoot: f.T.id, activationId: source.activation,
      nativeRecovery: { version: 1, operationId: edge.operationId, preparedDigest: edge.preparedDigest,
        preparedEntryId: edge.preparedEntryId, receivedEntryId: edge.receivedEntryId,
        source: f.ref(f.S), target: f.ref(f.T), phase: "received", receiver: {
          rootId: f.T.id, activationId: target.activation, pid: process.pid, host: os.hostname(), processStartedAt: processStartTime(process.pid),
          processBootId: fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim(),
        } } };
    expect(f.load(f.successor(f.S))).toEqual(expected);
    expect(await confirmMainSuccessor(f.mesh, f.T.id, f.ref(f.T).sessionFile)).toBe(true);
    expect(await confirmMainSuccessor(f.mesh, f.T.id, f.ref(f.T).sessionFile)).toBe(true);
    await stageMainSuccessor(f.mesh, f.S.id, f.S.sessionId!, f.ref(f.T).sessionFile);
    expect(await confirmMainSuccessor(f.mesh, f.T.id, f.ref(f.T).sessionFile)).toBe(true);
    expect(f.load(f.successor(f.S))).toEqual(expected);
    const maintenance = f.maintenance(f.T, target.main, [f.T]);
    await maintenance.run(); await maintenance.run();
    expect(target.sendMessage.mock.calls.map(call => call[0].details.id)).toEqual([ordinary.messageId]);
    expect(f.items(f.T).map(item => item.id)).toContain(native.messageId);
    expect(f.load(f.route(native.messageId))).toHaveProperty("item.id", native.messageId);
    expect(f.items(f.S).map(item => item.id)).toContain(native.messageId);
    expect(resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toMatchObject({ rootId: f.T.id,
      bindings: [{ operationId: edge.operationId, phase: "received" }] });
    // A mere native custom-message line, without N's validated occurrence linkage,
    // cannot override pending custody or fabricate a consumed transfer.
    fs.appendFileSync(f.ref(f.T).sessionFile, `${JSON.stringify({ type: "custom_message", customType: "pi-fabric-agent-message", details: { id: native.messageId } })}\n`);
    await maintenance.run();
    expect(f.items(f.T).map(item => item.id)).toContain(native.messageId);
    edge.occurrences = edge.occurrences.map(occurrence => ({ ...occurrence, state: "appended" as const }));
    await maintenance.run();
    expect(f.items(f.S).map(item => item.id)).not.toContain(native.messageId);
    expect(f.items(f.T).map(item => item.id)).not.toContain(native.messageId);
    expect(f.load(f.route(native.messageId))).toMatchObject({ done: true, newRoot: f.T.id });
    expect(f.load(f.route(native.messageId)).item).toBeUndefined();
    expect(target.sendMessage).toHaveBeenCalledOnce();
  } finally { f.close(); }
});

nativeIt("done native routes retain pending/ambiguous payload across source journal removal and repeated maintenance", async () => {
  const f = nativeFixture();
  try {
    const source = f.inbox(f.S);
    const carrier = source.main.deliverAgent({ from: f.peer, message: "native pending", delivery: "steer", deliveryId: "pending" });
    const edge = f.edge(f.S, f.T, [source.sendMessage.mock.calls[0]![0]]);
    f.transfers.push(edge); await f.reconcile();
    source.main.closeFollowUpDrain();
    const reserved = f.load(f.route(carrier.messageId));
    f.save(f.route(carrier.messageId), { ...reserved, done: true });
    // Crash after the permanent claim owns the original but S's payload was removed.
    f.save(f.journal(f.S), { version: 1, items: [] });
    const target = f.inbox(f.T, true);
    f.receive(edge); await f.reconcile(f.T);
    const maintenance = f.maintenance(f.T, target.main, [f.T]);
    await maintenance.run(); await maintenance.run();
    expect(target.sendMessage).not.toHaveBeenCalled();
    expect(f.items(f.T).map(item => item.id)).toEqual([carrier.messageId]);
    expect(f.load(f.route(carrier.messageId))).toMatchObject({ done: true, item: { id: carrier.messageId } });
    edge.occurrences = edge.occurrences.map(occurrence => ({ ...occurrence, state: "ambiguous" as const }));
    await maintenance.run(100_000); await maintenance.run(1_000_000);
    expect(f.load(f.route(carrier.messageId))).toHaveProperty("item.id", carrier.messageId);
    expect(target.sendMessage).not.toHaveBeenCalled();
    edge.occurrences = edge.occurrences.map(occurrence => ({ ...occurrence, state: "handled" as const }));
    await maintenance.run();
    expect(f.items(f.T)).toEqual([]);
    expect(f.load(f.route(carrier.messageId)).item).toBeUndefined();
    expect(target.sendMessage).not.toHaveBeenCalled();
  } finally { f.close(); }
});

nativeIt("native read failures hold journal and route custody; validated nonmembers use ordinary transfer", async () => {
  const f = nativeFixture();
  try {
    const source = f.inbox(f.S);
    const carrier = source.main.deliverAgent({ from: f.peer, message: "ordinary original", delivery: "followUp", deliveryId: "ordinary" });
    source.main.closeFollowUpDrain(); await recordMainSuccessor(f.mesh, f.S.id, f.S.sessionId!, f.T.id);
    const target = f.inbox(f.T, true), maintenance = f.maintenance(f.T, target.main, [f.T]);
    f.unavailable(true);
    await maintenance.run(1_000); await maintenance.run(1_000_000);
    expect(f.items(f.S).map(item => item.id)).toEqual([carrier.messageId]);
    expect(target.sendMessage).not.toHaveBeenCalled();
    f.unavailable(false);
    await maintenance.run(); await maintenance.run();
    expect(target.sendMessage.mock.calls.map(call => call[0].details.id)).toEqual([carrier.messageId]);
    expect(f.items(f.S)).toEqual([]);
    expect(resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toBeUndefined();
  } finally { f.close(); }
});

nativeIt("reserved claims cannot follow an ordinary alternate successor or a conflicting native operation/digest/target", async () => {
  const f = nativeFixture();
  try {
    const source = f.inbox(f.S), survivor = f.inbox(f.peer, true);
    const carrier = source.main.deliverAgent({ from: f.peer, message: "reserved", delivery: "steer", deliveryId: "reserved" });
    const edge = f.edge(f.S, f.T, [source.sendMessage.mock.calls[0]![0]]);
    f.transfers.push(edge); await f.reconcile();
    const permanent = f.load(f.route(carrier.messageId));
    source.main.closeFollowUpDrain();
    await expect(recordMainSuccessor(f.mesh, f.S.id, f.S.sessionId!, f.U.id)).rejects.toThrow("native");
    await expect(stageMainSuccessor(f.mesh, f.S.id, f.S.sessionId!, f.ref(f.U).sessionFile)).rejects.toThrow("native");
    // Even a preexisting/corrupt ordinary redirect cannot steal native release ownership.
    f.save(f.successor(f.S), { oldRoot: f.S.id, newRoot: f.U.id, activationId: source.activation });
    const maintenance = f.maintenance(f.peer, survivor.main, [f.peer]);
    await maintenance.run(1_000); await maintenance.run(1_000_000);
    expect(f.load(f.route(carrier.messageId))).toEqual(permanent);
    expect(survivor.sendMessage).not.toHaveBeenCalled();
    f.save(f.successor(f.S), { oldRoot: f.S.id, activationId: source.activation });
    for (const replacement of [
      { ...edge, operationId: "different-operation" },
      { ...edge, preparedDigest: "f".repeat(64) },
      { ...edge, target: f.ref(f.U) },
    ]) {
      f.transfers[0] = replacement;
      await expect(f.reconcile()).rejects.toThrow("Conflicting native");
      expect(f.load(f.route(carrier.messageId))).toEqual(permanent);
    }
    f.transfers[0] = edge;
    await f.reconcile();
    expect(f.load(f.route(carrier.messageId))).toEqual(permanent);
  } finally { f.close(); }
});

nativeIt("RECEIVED cannot commit from file existence, a copied local receiver label, or caller receiver claims", async () => {
  const f = nativeFixture();
  try {
    const source = f.inbox(f.S);
    const carrier = source.main.deliverAgent({ from: f.peer, message: "native", delivery: "steer", deliveryId: "native" });
    const edge = f.edge(f.S, f.T, [source.sendMessage.mock.calls[0]![0]], "received");
    f.transfers.push(edge);
    fs.writeFileSync(f.ref(f.T).sessionFile, `${JSON.stringify({ type: "session", version: 3, id: f.T.sessionId })}\n`);
    await expect(f.reconcile()).rejects.toThrow("registration");
    expect(f.load(f.route(carrier.messageId))).toHaveProperty("newRoot", f.T.id);
    expect(f.load(f.successor(f.S)).nativeRecovery).toBeUndefined();
    const copied = f.load(f.owner(f.S));
    f.save(f.owner(f.T), { ...copied, rootId: f.T.id, ownerIdentityId: f.T.id, sessionId: f.T.sessionId,
      sessionFile: f.ref(f.T).sessionFile, retired: undefined, nativeRecovery: undefined,
      nativeLauncherIdentity: { ...copied.nativeLauncherIdentity, rootId: f.T.id } });
    f.save(f.successor(f.T), { oldRoot: f.T.id, activationId: copied.activationId });
    await expect(f.reconcile()).rejects.toThrow("Unregistered local");
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toThrow("no committed successor locator");
    const target = f.inbox(f.T, true);
    // Extra serialized event/body fields are not copied into the registration receipt.
    Object.assign(edge, { receiver: { rootId: f.T.id, activationId: "caller-forged", pid: 1, host: "elsewhere", processStartedAt: "1" } });
    await f.reconcile(f.T);
    expect(f.load(f.successor(f.S)).nativeRecovery.receiver).toEqual(registeredMainLauncherIdentity(f.mesh.root, f.T.id));
    expect(f.load(f.successor(f.S)).activationId).toBe(source.activation);
    expect(f.load(f.successor(f.S)).nativeRecovery.receiver.activationId).toBe(target.activation);
    const actual = f.load(f.owner(f.T));
    f.save(f.owner(f.T), { ...actual, sessionFile: path.join(f.root, "wrong-native-file.jsonl") });
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toThrow("scope mismatch");
    await expect(f.reconcile()).rejects.toThrow("scope mismatch");
    f.save(f.owner(f.T), actual);
    expect(resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)?.rootId).toBe(f.T.id);
    const missingTarget: NativeRecoveryCustodyReader = scope => scope.sessionId === f.T.sessionId ? { status: "none" } : f.reader(scope);
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, missingTarget)).toThrow("pair/locator is missing");
    await expect(reconcileNativeMainInboxCustody(f.mesh, f.ref(f.S), missingTarget)).rejects.toThrow("target pair unavailable");
    // A stale receipt cannot route. Exact canonical reconciliation may refresh
    // an explicitly re-registered SAME T, never an operation/digest/target change.
    const previous = f.load(f.successor(f.S));
    const birth = processStartTime(process.ppid)!;
    f.save(f.successor(f.S), { ...previous, nativeRecovery: { ...previous.nativeRecovery, receiver: {
      ...previous.nativeRecovery.receiver, pid: process.ppid, processStartedAt: birth, activationId: "live-competitor",
    } } });
    await expect(f.reconcile()).rejects.toThrow("receiver receipt PID/full birth/host");
    f.save(f.successor(f.S), previous);
    const renewed = registerMainInbox(f.mesh.root, f.T, f.T.sessionId!, f.ref(f.T).sessionFile);
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toThrow("receiver receipt mismatch");
    await f.reconcile();
    expect(f.load(f.successor(f.S))).toMatchObject({ activationId: source.activation,
      nativeRecovery: { receiver: { activationId: renewed } } });
    expect(resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)?.rootId).toBe(f.T.id);
  } finally { f.close(); }
});

nativeIt("derived successor equalities bind S activation separately from T receiver and exact native roots", async () => {
  const f = nativeFixture();
  try {
    const source = f.inbox(f.S);
    const edge = f.edge(f.S, f.T, [], "received"); f.transfers.push(edge);
    const target = f.inbox(f.T, true); await f.reconcile(f.T);
    const actual = f.load(f.successor(f.S));
    expect(actual.activationId).toBe(source.activation);
    expect(actual.nativeRecovery.receiver.activationId).toBe(target.activation);
    expect(actual.activationId).not.toBe(actual.nativeRecovery.receiver.activationId);
    for (const bad of [
      { ...actual, activationId: target.activation },
      { ...actual, oldRoot: f.peer.id },
      { ...actual, newRoot: f.U.id },
      { ...actual, nativeRecovery: { ...actual.nativeRecovery, receiver: { ...actual.nativeRecovery.receiver, rootId: f.U.id } } },
      { ...actual, nativeRecovery: { ...actual.nativeRecovery, receiver: { ...actual.nativeRecovery.receiver, activationId: source.activation } } },
      { ...actual, nativeRecovery: { ...actual.nativeRecovery, target: { ...actual.nativeRecovery.target, sessionFile: path.join(f.root, "wrong.jsonl") } } },
    ]) {
      f.save(f.successor(f.S), bad);
      expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toThrow();
    }
    f.save(f.successor(f.S), actual);
    const owner = f.load(f.owner(f.T));
    for (const bad of [
      { ...owner, processStartedAt: "0", nativeLauncherIdentity: { ...owner.nativeLauncherIdentity, processStartedAt: "0" } },
      { ...owner, host: "other-host", nativeLauncherIdentity: { ...owner.nativeLauncherIdentity, host: "other-host" } },
      { ...owner, pid: 999999999, nativeLauncherIdentity: { ...owner.nativeLauncherIdentity, pid: 999999999 } },
      { ...owner, processBootId: undefined, nativeLauncherIdentity: { ...owner.nativeLauncherIdentity, processBootId: undefined } },
      { ...owner, processBootId: "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA", nativeLauncherIdentity: { ...owner.nativeLauncherIdentity, processBootId: "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA" } },
      { ...owner, processBootId: f.peer.sessionId, nativeLauncherIdentity: { ...owner.nativeLauncherIdentity, processBootId: f.peer.sessionId } },
    ]) {
      f.save(f.owner(f.T), bad);
      f.save(f.successor(f.S), { ...actual, nativeRecovery: { ...actual.nativeRecovery, receiver: bad.nativeLauncherIdentity } });
      expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toThrow();
    }
    f.save(f.successor(f.S), actual);
    f.save(f.owner(f.T), owner);
    expect(resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)?.rootId).toBe(f.T.id);
    const registration = f.load(f.successor(f.T));
    fs.unlinkSync(f.successor(f.T));
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toThrow("active native Main registration");
    f.save(f.successor(f.T), { ...registration, activationId: "stale-target-registration" });
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toThrow("active native Main registration");
    f.save(f.successor(f.T), registration);
    expect(resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)?.rootId).toBe(f.T.id);
  } finally { f.close(); }
});

nativeIt("validated received chains advance exact claims and reject ordinary-only, missing, looping and sealed endpoints", async () => {
  const f = nativeFixture();
  try {
    const source = f.inbox(f.S);
    const carrier = source.main.deliverAgent({ from: f.peer, message: "across two recoveries", delivery: "steer", deliveryId: "chain" });
    const message = source.sendMessage.mock.calls[0]![0];
    const first = f.edge(f.S, f.T, [message], "received");
    f.transfers.push(first); source.main.closeFollowUpDrain();
    const middle = f.inbox(f.T, true); await f.reconcile(f.T);
    await f.maintenance(f.T, middle.main, [f.T]).run();
    expect(middle.sendMessage).not.toHaveBeenCalled();
    expect(resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)?.rootId).toBe(f.T.id);
    const second = f.edge(f.T, f.U, [message]);
    f.transfers.push(second);
    // The accessor sees PREPARED before C has written a source seal/successor row.
    // Missing T's locator cannot make a received S->T chain runnable again.
    const activeMiddle = f.load(f.successor(f.T)); fs.unlinkSync(f.successor(f.T));
    const inspectTerminal = vi.fn(f.reader);
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, inspectTerminal)).toThrow("terminal is sealed");
    expect(inspectTerminal).toHaveBeenCalledWith(f.ref(f.T));
    f.save(f.successor(f.T), activeMiddle);
    await f.reconcile(f.T);
    // PREPARED disables T immediately, despite a received S->T and live same PID.
    expect(mainInboxActive(f.mesh.root, f.T.id, middle.activation)).toBe(false);
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toThrow("terminal is sealed");
    middle.main.closeFollowUpDrain();
    const target = f.inbox(f.U, true); f.receive(second); await f.reconcile(f.U);
    await f.reconcile(f.U); await f.reconcile(f.S);
    expect(f.load(f.route(carrier.messageId))).toMatchObject({ oldRoot: f.S.id, newRoot: f.U.id,
      nativeRecovery: { operationId: second.operationId, source: f.ref(f.T), target: f.ref(f.U) } });
    const resolved = resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader);
    expect(resolved?.rootId).toBe(f.U.id);
    expect(resolved?.bindings.map(binding => binding.operationId)).toEqual([first.operationId, second.operationId]);
    const maintenance = f.maintenance(f.U, target.main, [f.U]);
    await maintenance.run(); await maintenance.run();
    expect(target.sendMessage).not.toHaveBeenCalled();
    expect(f.items(f.U).map(item => item.id)).toEqual([carrier.messageId]);
    const middleRow = f.load(f.successor(f.T));
    fs.unlinkSync(f.successor(f.T));
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toThrow("terminal is sealed");
    f.save(f.successor(f.T), { oldRoot: f.T.id, newRoot: f.U.id, activationId: middle.activation });
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toThrow("terminal is sealed");
    f.save(f.successor(f.T), { ...middleRow, newRoot: f.S.id });
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toThrow("successor locator");
    f.save(f.successor(f.T), middleRow);
    const targetOwner = f.load(f.owner(f.U)); fs.unlinkSync(f.owner(f.U));
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toThrow("registration");
    f.save(f.owner(f.U), targetOwner);
    expect(resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)?.rootId).toBe(f.U.id);
    const unavailable: NativeRecoveryCustodyReader = () => ({ status: "hold", reason: "native pair unreadable" });
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, unavailable)).toThrow("HOLD");
  } finally { f.close(); }
});

// C routing's independently supplied proposedTornNativeLocatorMustHold counterexample.
// Keep: a pre-accessor parse error must cross the real directory boundary as HOLD,
// not undefined/no recovery permitting ordinary old-address fallback.
nativeIt("proposedTornNativeLocatorMustHold: directory propagates pre-reader locator errors", async () => {
  const f = nativeFixture();
  const directory = new ParticipantDirectory(f.mesh, { enabled: true, hostId: f.T.id, rootId: f.T.id, identity: f.T,
    reapDeadHosts: false, heartbeatMs: 60_000, leaseMs: 120_000 });
  try {
    f.inbox(f.S); f.inbox(f.T);
    f.transfers.push(f.edge(f.S, f.T, [], "received")); await f.reconcile(f.T);
    const reader = vi.fn(f.reader); directory.setNativeRecoveryReader(reader);
    const locator = f.successor(f.S), prior = fs.readFileSync(locator);
    try {
      fs.writeFileSync(locator, "{torn native locator");
      expect(() => directory.resolveNativeMessageTarget(f.S.id, "followUp")).toThrow(/HOLD|JSON|Unexpected/i);
      expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, reader)).toThrow(/HOLD|JSON|Unexpected/i);
      expect(reader).not.toHaveBeenCalled();
    } finally { fs.writeFileSync(locator, prior); }
    for (const invalid of [null, [], {}, 1, { oldRoot: f.S.id, nativeRecovery: null },
      { ...JSON.parse(prior.toString()), nativeRecovery: { ...JSON.parse(prior.toString()).nativeRecovery, phase: "prepared" } }]) {
      f.save(locator, invalid);
      expect(() => directory.resolveNativeMessageTarget(f.S.id, "followUp")).toThrow();
    }
    fs.writeFileSync(locator, prior);
    expect(resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)?.rootId).toBe(f.T.id);
  } finally { await directory.close(); f.close(); }
});

nativeIt("incoming-only final RECEIVED is positive no-outgoing recovery for direct T, never a sealed-source fallback", async () => {
  const f = nativeFixture();
  try {
    f.inbox(f.S); f.inbox(f.T);
    const incoming = f.edge(f.S, f.T, [], "received"); f.transfers.push(incoming);
    await f.reconcile();
    expect(resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)?.rootId).toBe(f.T.id);
    expect(resolveNativeMainSuccessor(f.mesh.root, f.T.id, f.reader)).toBeUndefined();
    const received = f.load(f.owner(f.S));
    f.save(f.owner(f.S), { ...received, processBootId: undefined,
      nativeLauncherIdentity: { ...received.nativeLauncherIdentity, processBootId: undefined } });
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.T.id, f.reader)).toThrow(/registration/);
    f.save(f.owner(f.S), received);
    const mutating: NativeRecoveryCustodyReader = scope => {
      const result = f.reader(scope); f.save(f.successor(f.T), { oldRoot: f.T.id, activationId: "changed" }); return result;
    };
    const active = fs.readFileSync(f.successor(f.T));
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.T.id, mutating)).toThrow(/registration|changed/);
    fs.writeFileSync(f.successor(f.T), active);
    const unknown: NativeRecoveryCustodyReader = () => ({ status: "validated", transfers: [{ ...incoming,
      target: { ...f.ref(f.T), sessionFile: path.join(f.root, "unknown.jsonl") } }] });
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.T.id, unknown)).toThrow(/HOLD/);
    const { receivedEntryId: _received, ...prepared } = incoming;
    const incomingPrepared: NativeRecoveryCustodyReader = () => ({ status: "validated", transfers: [{ ...prepared, phase: "prepared" }] });
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.T.id, incomingPrepared)).toThrow(/successor locator|PREPARED/);
    const outgoing = f.edge(f.T, f.U, []); f.transfers.push(outgoing);
    // Neither a source seal nor a successor locator has been written for T->U.
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.T.id, f.reader)).toThrow(/successor locator/);
    f.receive(outgoing);
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.T.id, f.reader)).toThrow(/successor locator/);
    f.transfers.pop();
    const torn = fs.readFileSync(f.successor(f.T)); fs.writeFileSync(f.successor(f.T), "{torn");
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.T.id, f.reader)).toThrow();
    fs.writeFileSync(f.successor(f.T), torn);
    f.unavailable(true);
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.T.id, f.reader)).toThrow(/HOLD/);
    f.unavailable(false);
    expect(resolveNativeMainSuccessor(f.mesh.root, f.T.id, f.reader)).toBeUndefined();
  } finally { f.close(); }
});

nativeIt("only genuine absence returns undefined; bare PREPARED and malformed source seals hold", async () => {
  const f = nativeFixture();
  try {
    const source = f.inbox(f.S);
    expect(resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toBeUndefined();
    f.transfers.push(f.edge(f.S, f.T, [])); // canonical PREPARED before C reconciliation
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toThrow("no committed successor locator");
    f.transfers.length = 0;
    const owner = f.load(f.owner(f.S)); f.save(f.owner(f.S), { ...owner, nativeRecovery: null });
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toThrow("no committed successor locator");
    expect(() => registerMainInbox(f.mesh.root, f.S, f.S.sessionId!, source.scope.sessionFile)).toThrow("native recovery source");
    f.save(f.owner(f.S), owner);
    const mutating: NativeRecoveryCustodyReader = () => {
      f.save(f.successor(f.S), { oldRoot: f.S.id, activationId: source.activation, nativeRecovery: null });
      return { status: "none" };
    };
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, mutating)).toThrow("changed during resolution");
    f.save(f.successor(f.S), { oldRoot: f.S.id, activationId: source.activation });
    expect(resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toBeUndefined();
  } finally { f.close(); }
});

nativeIt("unknown boot at original registration is omitted, never invented, and native commit holds", async () => {
  const f = nativeFixture();
  try {
    const unknown = vi.spyOn(atomicWrite, "readPhysicalHostIdentity").mockReturnValue(undefined);
    f.inbox(f.S);
    const original = f.load(f.owner(f.S));
    expect(original).not.toHaveProperty("processBootId");
    expect(original.nativeLauncherIdentity).not.toHaveProperty("processBootId");
    unknown.mockRestore();
    f.inbox(f.T); f.transfers.push(f.edge(f.S, f.T, [], "received"));
    await expect(f.reconcile()).rejects.toThrow("registration");
    expect(f.load(f.successor(f.S))).not.toHaveProperty("nativeRecovery");
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toThrow("no committed successor locator");
    expect(f.load(f.owner(f.S))).not.toHaveProperty("processBootId");
    expect(registeredMainLauncherIdentity(f.mesh.root, f.S.id)?.processBootId).toBeUndefined();
  } finally { f.close(); }
});

nativeIt("original registration and receiver require native full birth; unknown observations hold without fabricated facts", async () => {
  const f = nativeFixture();
  try {
    const source = f.inbox(f.S), target = f.inbox(f.T);
    const boot = fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim();
    const original = f.load(f.owner(f.S));
    expect(original.processBootId).toBe(boot);
    expect(original.nativeLauncherIdentity.processBootId).toBe(boot);
    f.transfers.push(f.edge(f.S, f.T, [], "received")); await f.reconcile();
    const committed = f.load(f.successor(f.S));
    expect(committed).toMatchObject({ activationId: source.activation, nativeRecovery: { receiver: {
      activationId: target.activation, pid: original.pid, host: original.host, processStartedAt: original.processStartedAt, processBootId: boot,
    } } });
    const launchBirth = registeredMainLauncherIdentity(f.mesh.root, f.S.id);
    for (const changed of [
      { ...original, processBootId: undefined, nativeLauncherIdentity: { ...original.nativeLauncherIdentity, processBootId: undefined } },
      { ...original, processBootId: f.peer.sessionId, nativeLauncherIdentity: { ...original.nativeLauncherIdentity, processBootId: f.peer.sessionId } },
      { ...original, processStartedAt: "0", nativeLauncherIdentity: { ...original.nativeLauncherIdentity, processStartedAt: "0" } },
    ]) {
      f.save(f.owner(f.S), { ...changed, retired: true, nativeRecovery: committed.nativeRecovery });
      expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toThrow();
      await expect(f.reconcile()).rejects.toThrow();
      expect(f.load(f.successor(f.S))).toEqual(committed);
      expect(registeredMainLauncherIdentity(f.mesh.root, f.S.id)).toEqual(launchBirth);
    }
    f.save(f.owner(f.S), { ...original, retired: true, nativeRecovery: committed.nativeRecovery });
    const unknownBoot = vi.spyOn(atomicWrite, "readPhysicalHostIdentity").mockReturnValue(undefined);
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toThrow("full birth");
    await expect(f.reconcile()).rejects.toThrow("full birth");
    unknownBoot.mockRestore();
    const unknownTicks = vi.spyOn(processIdentity, "processStartTime").mockReturnValue(undefined);
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toThrow("full birth");
    await expect(f.reconcile()).rejects.toThrow("full birth");
    unknownTicks.mockRestore();
    expect(resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)?.rootId).toBe(f.T.id);
  } finally { f.close(); }
});

nativeIt("a positive live different receiver PID cannot commit or resolve against the original source anchor", async () => {
  const f = nativeFixture();
  try {
    f.inbox(f.S); f.inbox(f.T);
    f.transfers.push(f.edge(f.S, f.T, [], "received")); await f.reconcile();
    const committed = f.load(f.successor(f.S)), owner = f.load(f.owner(f.T));
    // Pure reader fixtures: no spawned process, host PID signals or process cleanup.
    const otherPid = process.pid + 1_000_000, otherTicks = "12345";
    const nativeTicks = processIdentity.processStartTime;
    const ticks = vi.spyOn(processIdentity, "processStartTime").mockImplementation(pid => pid === otherPid ? otherTicks : nativeTicks(pid));
    const live = vi.spyOn(processIdentity, "residentProcessAlive").mockReturnValue(true);
    const other = { ...owner.nativeLauncherIdentity, pid: otherPid, processStartedAt: otherTicks };
    f.save(f.owner(f.T), { ...owner, pid: otherPid, processStartedAt: otherTicks, nativeLauncherIdentity: other });
    f.save(f.successor(f.S), { ...committed, nativeRecovery: { ...committed.nativeRecovery, receiver: other } });
    const foreignReceipt = f.load(f.successor(f.S));
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)).toThrow("original live source PID/full birth/host");
    await expect(f.reconcile()).rejects.toThrow("original live source PID/full birth/host");
    expect(live).toHaveBeenCalledWith(otherPid, otherTicks);
    expect(f.load(f.successor(f.S))).toEqual(foreignReceipt);
    ticks.mockRestore(); live.mockRestore();
    f.save(f.owner(f.T), owner); f.save(f.successor(f.S), committed);
    expect(resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)?.rootId).toBe(f.T.id);
  } finally { f.close(); }
});

nativeIt("native registration/locator changes during canonical readback hold commit and resolution", async () => {
  const f = nativeFixture();
  try {
    f.inbox(f.S); f.inbox(f.T);
    f.transfers.push(f.edge(f.S, f.T, [], "received")); await f.reconcile();
    const sourceOwner = f.load(f.owner(f.S)), committed = f.load(f.successor(f.S));
    let changed = false;
    const mutating: NativeRecoveryCustodyReader = scope => {
      const result = f.reader(scope);
      if (scope.sessionId === f.T.sessionId && !changed) {
        changed = true; f.save(f.owner(f.S), { ...sourceOwner, name: "changed during readback" });
      }
      return result;
    };
    expect(() => resolveNativeMainSuccessor(f.mesh.root, f.S.id, mutating)).toThrow("changed during resolution");
    f.save(f.owner(f.S), sourceOwner); changed = false;
    await expect(reconcileNativeMainInboxCustody(f.mesh, f.ref(f.S), mutating)).rejects.toThrow("changed during read");
    expect(f.load(f.successor(f.S))).toEqual(committed);
    f.save(f.owner(f.S), sourceOwner);
    expect(resolveNativeMainSuccessor(f.mesh.root, f.S.id, f.reader)?.rootId).toBe(f.T.id);
  } finally { f.close(); }
});

nativeIt("reconciliation reads native facts only after custody acquisition and lifecycle fence", async () => {
  const f = nativeFixture();
  try {
    const source = f.inbox(f.S);
    source.main.deliverAgent({ from: f.peer, message: "fenced", delivery: "steer", deliveryId: "fenced" });
    f.transfers.push(f.edge(f.S, f.T, [source.sendMessage.mock.calls[0]![0]]));
    let inside = false;
    const custody = f.mesh.custody.bind(f.mesh);
    vi.spyOn(f.mesh, "custody").mockImplementation((operation, timeout) => custody(() => {
      inside = true; try { return operation(); } finally { inside = false; }
    }, timeout));
    const reader = vi.fn((scope: NativeRecoverySessionRef): NativeRecoveryCustodyResult => {
      expect(inside).toBe(true); return f.reader(scope);
    });
    await expect(reconcileNativeMainInboxCustody(f.mesh, f.ref(f.S), reader, () => false)).rejects.toThrow("retired");
    expect(reader).not.toHaveBeenCalled();
    expect(mainInboxActive(f.mesh.root, f.S.id, source.activation)).toBe(true);
    await reconcileNativeMainInboxCustody(f.mesh, f.ref(f.S), reader, () => true);
    expect(reader).toHaveBeenCalledOnce();
    expect(mainInboxActive(f.mesh.root, f.S.id, source.activation)).toBe(false);
  } finally { f.close(); }
});

it("the optional native reader leaves ordinary legacy fileless transfers unchanged", async () => {
  const f = nativeFixture();
  try {
    const source = f.inbox(f.S);
    const carrier = source.main.deliverAgent({ from: f.peer, message: "legacy ordinary", delivery: "followUp", deliveryId: "legacy" });
    source.main.closeFollowUpDrain();
    const owner = f.load(f.owner(f.S)); delete owner.sessionFile; f.save(f.owner(f.S), owner);
    await recordMainSuccessor(f.mesh, f.S.id, f.S.sessionId!, f.T.id);
    const target = f.inbox(f.T, true), maintenance = f.maintenance(f.T, target.main, [f.T]);
    await maintenance.run(); await maintenance.run();
    expect(target.sendMessage.mock.calls.map(call => call[0].details.id)).toEqual([carrier.messageId]);
    expect(f.items(f.S)).toEqual([]);
  } finally { f.close(); }
});

nativeIt("source-durable history is not native pending and never transfers from a stale Main journal", async () => {
  const f = nativeFixture();
  try {
    const source = f.inbox(f.S);
    const carrier = source.main.deliverAgent({ from: f.peer, message: "already appended", delivery: "steer", deliveryId: "durable" });
    const original = f.items(f.S)[0]!;
    fs.appendFileSync(source.scope.sessionFile, `${JSON.stringify({ type: "custom_message", customType: "pi-fabric-agent-message", details: { id: carrier.messageId } })}\n`);
    source.main.closeFollowUpDrain();
    // Deliberately retain the original stale C row after native history is durable.
    f.save(f.journal(f.S), { version: 1, items: [original] });
    await recordMainSuccessor(f.mesh, f.S.id, f.S.sessionId!, f.T.id);
    const target = f.inbox(f.T, true);
    await f.maintenance(f.T, target.main, [f.T]).run();
    expect(target.sendMessage).not.toHaveBeenCalled();
    expect(fs.existsSync(f.route(carrier.messageId))).toBe(false);
    expect(f.mesh.read({ topic: "fleet.work.inbox-receipts" })).toEqual([]);
  } finally { f.close(); }
});
