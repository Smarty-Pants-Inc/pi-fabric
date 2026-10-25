// Keep: data safety/trust boundary (#15310). Exercise real admission paths: a drain
// refusal must neither journal nor hand off new work, and retries must not lose it.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MainAgentController, type HeldAgentMessage } from "../src/main-agent.js";
import { MAIN_DRAIN_INGRESS, probeIngress } from "../src/main-drain/control.js";
import {
  closeMainInbox, FABRIC_MAIN_DRAINING, FabricMainDrainingError,
  mainInboxDrainGate, reopenMainInbox,
} from "../src/main-drain/gate.js";
import { fenceNativeInput, observeNativeInput, releaseNativeInput } from "../src/main-drain/native-port.js";
import { MeshStore } from "../src/mesh/store.js";

type Handler = (event: unknown, ctx: ExtensionContext) => unknown;
const sender = { id: "actor:drain-sender", name: "sender", kind: "actor" as const };
const roots: string[] = [];
const ids = new Set<string>();
const mains: MainAgentController[] = [];
const agentManagers: AgentManager[] = [];
const actorManagers: ActorManager[] = [];
let sequence = 0;

const busy = (idle = false) => ({
  isIdle: () => idle,
  isPromptPending: () => false,
  hasPendingMessages: () => false,
  signal: new AbortController().signal,
  sessionManager: { getEntries: () => [] },
}) as unknown as ExtensionContext;

const fakePi = (nativeFenceApi = false) => {
  let fenced = false;
  const handlers = new Map<string, Handler[]>();
  const sendMessage = vi.fn((_message: unknown, _options?: unknown) => {
    if (fenced) throw Object.assign(new Error("native input fenced"), { code: "INPUT_ADMISSION_FENCED" });
  });
  const sendUserMessage = vi.fn((_message: unknown, _options?: unknown) => {
    if (fenced) throw Object.assign(new Error("native input fenced"), { code: "INPUT_ADMISSION_FENCED" });
  });
  const pi = {
    on: (name: string, handler: Handler) => {
      handlers.set(name, [...(handlers.get(name) ?? []), handler]);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter(candidate => candidate !== handler));
    },
    sendMessage, sendUserMessage,
    getThinkingLevel: () => "off",
    ...(nativeFenceApi ? { hostCapabilities: { inputAdmission: 1, extensionInputFence: 1 } } : {}),
  } as unknown as ExtensionAPI;
  const release = vi.fn(() => { fenced = false; });
  const ctx = {
    ...busy(true),
    fenceInputs: vi.fn(async () => { fenced = true; return { release }; }),
    getInputState: () => ({ inputsFenced: fenced, inputAdmissionCount: 0, queued: 0, drained: true }),
  };
  return {
    pi, sendMessage, sendUserMessage, ctx, release,
    emit: (name: string, event: unknown, context: ExtensionContext) => {
      for (const handler of handlers.get(name) ?? []) handler(event, context);
    },
  };
};

const fixture = (idle = false, nativeFenceApi = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-drain-gate-"));
  roots.push(root);
  const sessionId = `drain-test-${++sequence}`;
  const id = `session:${sessionId}`;
  ids.add(id);
  const journal = path.join(root, "main-followups", `${sessionId}.json`);
  const h = fakePi(nativeFenceApi);
  const context = nativeFenceApi ? h.ctx : busy(idle);
  const main = new MainAgentController(h.pi, id, true, root, sessionId);
  mains.push(main);
  main.attachFollowUpDrain(context, 60_000, journal);
  return { root, sessionId, id, journal, h, context, main };
};

const journalBytes = (journal: string): Buffer | null => fs.existsSync(journal) ? fs.readFileSync(journal) : null;
const carrier = (id = "carrier-1"): HeldAgentMessage => ({
  id, deliveryId: `rotation:${id}`, from: sender, message: "retained carrier", sentAt: 1_000,
});
const refusal = async (attempt: () => unknown): Promise<unknown> => {
  try { await attempt(); return undefined; } catch (error) { return error; }
};
const expectDraining = (error: unknown, id: string) => {
  expect(error).toBeInstanceOf(FabricMainDrainingError);
  expect(error).toMatchObject({ code: FABRIC_MAIN_DRAINING, retryable: true, notRun: true, rootId: id });
};

beforeEach(() => {
  for (const name of ["PI_FABRIC_DEPTH", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET", "PI_FABRIC_WRITE_POLICY"])
    vi.stubEnv(name, undefined);
});
afterEach(async () => {
  try {
    for (const main of mains.splice(0)) main.closeFollowUpDrain();
    for (const actors of actorManagers.splice(0)) await actors.close();
    for (const agents of agentManagers.splice(0)) await agents.close();
  } finally {
    for (const id of ids) { reopenMainInbox(id); releaseNativeInput(id); }
    ids.clear();
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  }
});

describe("Main drain gate admission", () => {
  for (const delivery of ["followUp", "steer"] as const) {
    for (const deliveryId of [undefined, "delivery-refused"] as const) {
      it.each([false, true])(`refuses ${delivery} ${deliveryId ? "with" : "without"} a deliveryId (existing journal: %s)`, async existing => {
        const f = fixture();
        if (existing) f.main.deliverAgent({ from: sender, message: "already held", delivery: "followUp", deliveryId: "seed" });
        const before = journalBytes(f.journal);
        expect(before === null).toBe(!existing);
        closeMainInbox(f.id);
        const request = { from: sender, message: "refuse before admission", delivery, ...(deliveryId ? { deliveryId } : {}) };
        expectDraining(await refusal(() => f.main.deliverAgent(request)), f.id);
        expect(journalBytes(f.journal)).toEqual(before);
        expect(f.main.queueDepth().pendingFollowUps).toBe(existing ? 1 : 0);
        expect(f.h.sendMessage).not.toHaveBeenCalled();
      });
    }
  }

  it.each(["followUp", "steer"] as const)("acknowledges an admitted %s deliveryId as duplicate even while closed", delivery => {
    const f = fixture(true);
    const request = { from: sender, message: "admitted before drain", delivery, deliveryId: "admitted-1" };
    const first = f.main.deliverAgent(request);
    expect(first.duplicate).not.toBe(true);
    expect(f.h.sendMessage).toHaveBeenCalledTimes(1);
    const before = journalBytes(f.journal);
    expect(before).not.toBeNull();
    closeMainInbox(f.id);
    expect(f.main.deliverAgent(request)).toMatchObject({ duplicate: true, messageId: first.messageId, triggered: false });
    expect(journalBytes(f.journal)).toEqual(before);
    expect(f.h.sendMessage).toHaveBeenCalledTimes(1);
  });

  it.each(["followUp", "steer"] as const)("refuses dashboard deliverUser (%s) before Pi sees it", async delivery => {
    const f = fixture();
    const before = journalBytes(f.journal);
    closeMainInbox(f.id);
    expectDraining(await refusal(() => f.main.deliverUser("dashboard input", delivery)), f.id);
    expect(f.h.sendUserMessage).not.toHaveBeenCalled();
    expect(f.h.sendMessage).not.toHaveBeenCalled();
    expect(journalBytes(f.journal)).toEqual(before);
  });

  it.each([false, true])("refuses a new rotation carrier without changing the journal (existing: %s)", async existing => {
    const f = fixture();
    if (existing) f.main.deliverAgent({ from: sender, message: "already held", delivery: "followUp", deliveryId: "seed" });
    const before = journalBytes(f.journal);
    closeMainInbox(f.id);
    expectDraining(await refusal(() => f.main.receiveInboxItem(carrier())), f.id);
    expect(journalBytes(f.journal)).toEqual(before);
    expect(f.main.queueDepth().pendingFollowUps).toBe(existing ? 1 : 0);
    expect(f.h.sendMessage).not.toHaveBeenCalled();
  });

  it.each(["followUp", "steer"] as const)("loses nothing: a refused %s deliveryId is admitted exactly once after reopening", async delivery => {
    const f = fixture(true);
    const request = { from: sender, message: "retry after drain", delivery, deliveryId: "retry-1" };
    const before = journalBytes(f.journal);
    closeMainInbox(f.id);
    for (let retry = 0; retry < 2; retry++) expectDraining(await refusal(() => f.main.deliverAgent(request)), f.id);
    expect(journalBytes(f.journal)).toEqual(before);
    expect(f.h.sendMessage).not.toHaveBeenCalled();
    expect(reopenMainInbox(f.id)).toBe(true);
    const first = f.main.deliverAgent(request);
    expect(first).toMatchObject({ queued: true });
    expect(first.duplicate).not.toBe(true);
    const admitted = journalBytes(f.journal);
    expect(JSON.parse(admitted!.toString()).items).toEqual([expect.objectContaining({ deliveryId: request.deliveryId, message: request.message })]);
    expect(f.main.deliverAgent(request)).toMatchObject({ duplicate: true, messageId: first.messageId });
    expect(journalBytes(f.journal)).toEqual(admitted);
    expect(f.h.sendMessage).toHaveBeenCalledTimes(1);
    expect(f.h.sendMessage.mock.calls[0]![0]).toMatchObject({ details: { deliveryId: request.deliveryId } });
  });

  it("a reloaded controller sees the same closed gate, and either controller's id can reopen it", async () => {
    const f = fixture(true);
    closeMainInbox(f.main.id);
    f.main.closeFollowUpDrain();
    const h = fakePi();
    const reloaded = new MainAgentController(h.pi, f.id, true, f.root, f.sessionId);
    mains.push(reloaded);
    reloaded.attachFollowUpDrain(busy(true), 60_000, f.journal);
    const request = { from: sender, message: "after reload", delivery: "steer" as const };
    expectDraining(await refusal(() => reloaded.deliverAgent(request)), f.id);
    expect(h.sendMessage).not.toHaveBeenCalled();
    expect(reopenMainInbox(reloaded.id)).toBe(true);
    expect(mainInboxDrainGate(f.main.id)).toBeUndefined();
    expect(reloaded.deliverAgent(request)).toMatchObject({ queued: true });
    closeMainInbox(reloaded.id);
    expectDraining(await refusal(() => f.main.deliverAgent(request)), f.id);
    expectDraining(await refusal(() => reloaded.deliverAgent(request)), f.id);
    expect(reopenMainInbox(f.main.id)).toBe(true);
    expect(mainInboxDrainGate(reloaded.id)).toBeUndefined();
    expect(reloaded.deliverAgent(request)).toMatchObject({ queued: true });
    expect(h.sendMessage).toHaveBeenCalledTimes(2);
    expect(f.h.sendMessage).not.toHaveBeenCalled();
  });
});

// These are real managers with only their later validation deliberately invalid:
// an open drain gate may fail later, but must not report a drain refusal or launch work.
const managers = (f: ReturnType<typeof fixture>) => {
  const agents = new AgentManager(f.root, { ...DEFAULT_FABRIC_CONFIG.agents, enabled: true, maxDepth: 10, budgetUsd: 0 }, {
    mainAgentId: f.id, runRoot: path.join(f.root, "runs"),
  });
  agentManagers.push(agents);
  const mesh = new MeshStore(path.join(f.root, "mesh"), 64 * 1024, 100);
  const actors = new ActorManager(f.sessionId, { id: f.id, name: "Main", kind: "main", sessionId: f.sessionId },
    mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, enabled: true }, agents, () => {}, {
      rootId: f.id, actorRoot: path.join(f.root, "actors"), persistent: true,
      canConsumeMesh: () => false, reapDeadSessionPresence: false, meshRetentionSweepPath: false, closeGraceMs: 0,
    });
  actorManagers.push(actors);
  return { agents, actors };
};

type ProbeFixture = ReturnType<typeof fixture>;
type WorkManagers = ReturnType<typeof managers>;
type LiveIngress = {
  mechanism: "fabric-gate" | "native-fence" | "both" | "hold";
  run(f: ProbeFixture, work: WorkManagers | undefined): unknown;
};
const agentIngress = (f: ProbeFixture) => f.main.deliverAgent({
  from: sender, message: "new ingress delivery", delivery: "steer",
});
const nativeIngress = (f: ProbeFixture) => f.h.pi.sendMessage({
  customType: "drain-test-native", content: "native ingress", display: false,
}, { triggerTurn: true });
// DRAIN-DESIGN.r1 §1.9. One path lookup; an unknown/old row fails instead of
// silently exercising a guessed substitute. HOLD is intentionally never run.
const LIVE_INGRESS: Record<string, LiveIngress> = {
  "control.followUp-steer": { mechanism: "fabric-gate", run: agentIngress },
  "in-process.producer": { mechanism: "fabric-gate", run: agentIngress },
  "resident-host.delivery": { mechanism: "fabric-gate", run: agentIngress },
  "completion.journal": { mechanism: "fabric-gate", run: agentIngress },
  "completion.inbox": { mechanism: "native-fence", run: nativeIngress },
  "follow-up-alarm": { mechanism: "native-fence", run: nativeIngress },
  "dashboard.composer": { mechanism: "both", run: f => f.main.deliverUser("dashboard ingress", "followUp") },
  "tui.keyboard": { mechanism: "native-fence", run: nativeIngress },
  "feed.herdr-prompt": { mechanism: "native-fence", run: nativeIngress },
  "peer-steer-wake": { mechanism: "fabric-gate", run: agentIngress },
  "actor-halt-lift": { mechanism: "native-fence", run: nativeIngress },
  "legacy-fabric-steer": { mechanism: "fabric-gate", run: agentIngress },
  "extension.sendMessage": { mechanism: "native-fence", run: nativeIngress },
  // Exercise the input arriving at the current Main before a replacement/reload.
  "session-replacement-reload": { mechanism: "both", run: agentIngress },
  "code-route": { mechanism: "hold", run: () => { throw new Error("HOLD must not be probed live"); } },
  "rotation-carrier": { mechanism: "fabric-gate", run: f => f.main.receiveInboxItem(carrier()) },
  "agents.spawn": { mechanism: "fabric-gate", run: (_f, work) => work!.agents.spawn({ task: "" }) },
  "actors.create": { mechanism: "fabric-gate", run: (_f, work) => work!.actors.create({ name: "invalid actor name!", instructions: "Never run." }) },
};

describe("Main drain probe/live parity (ruling 10-25 (3))", () => {
  for (const row of MAIN_DRAIN_INGRESS) {
    it.skipIf(String(row.mechanism) === "hold")(`${row.path} has the same dry-run and live verdict while closed and open`, async () => {
      const entry = LIVE_INGRESS[row.path];
      expect(entry, `Unmapped live ingress: ${row.path}`).toBeDefined();
      expect(row.mechanism).toBe(entry!.mechanism);
      const f = fixture(true, true);
      const work = row.path === "agents.spawn" || row.path === "actors.create" ? managers(f) : undefined;
      // BOTH rows also test each half independently: an AND canary must not
      // accidentally become OR (or inspect only one gate).
      const states = entry!.mechanism === "both"
        ? [[true, true], [true, false], [false, true], [false, false]] as const
        : [[true, true], [false, false]] as const;
      for (const [fabricClosed, nativeClosed] of states) {
        if (fabricClosed) closeMainInbox(f.id); else reopenMainInbox(f.id);
        if (nativeClosed) expect(await fenceNativeInput(f.h.pi, f.h.ctx, f.id)).toBe(true);
        else releaseNativeInput(f.id);
        const observation = observeNativeInput(f.h.pi, f.h.ctx, f.id);
        expect(observation).toMatchObject({ fenceApi: true, piInputsFenced: nativeClosed, inputsFenced: nativeClosed });
        const probe = probeIngress(f.id, observation, f.main.probeAdmission()).find(candidate => candidate.path === row.path)!;
        const error = await refusal(() => entry!.run(f, work));
        const fabricRefused = entry!.mechanism !== "native-fence" && fabricClosed;
        const nativeRefused = entry!.mechanism !== "fabric-gate" && nativeClosed;
        if (fabricRefused) expectDraining(error, f.id);
        else if (nativeRefused) expect(error).toMatchObject({ code: "INPUT_ADMISSION_FENCED" });
        else if (row.path === "agents.spawn") expect(error).toMatchObject({ message: "Agent task must not be empty" });
        else if (row.path === "actors.create") expect(error).toMatchObject({ message: "Invalid Fabric actor name: invalid actor name!" });
        else expect(error).toBeUndefined();
        const liveAdmits = !(error instanceof FabricMainDrainingError) &&
          (error as { code?: string } | undefined)?.code !== "INPUT_ADMISSION_FENCED";
        expect(probe.admits, `${row.path}, fabricClosed=${fabricClosed}, nativeClosed=${nativeClosed}`).toBe(liveAdmits);
        expect(probe.admits).toBe(!fabricRefused && !nativeRefused);
      }
    });
  }

  it("reports every native ingress as unknown without an extension fence API, whether the Fabric gate is open or closed", () => {
    const f = fixture();
    for (const closed of [true, false]) {
      if (closed) closeMainInbox(f.id); else reopenMainInbox(f.id);
      const observation = observeNativeInput(f.h.pi, f.context, f.id);
      expect(observation).toMatchObject({ fenceApi: false, piInputsFenced: null, fenceError: "FENCE_API_UNAVAILABLE" });
      const probes = probeIngress(f.id, observation, f.main.probeAdmission());
      for (const probe of probes.filter(row => String(row.mechanism) === "hold")) expect(probe.admits, probe.path).toBeNull();
      const native = probes.filter(row => row.mechanism === "native-fence");
      expect(native.map(row => row.path)).toEqual(MAIN_DRAIN_INGRESS.filter(row => row.mechanism === "native-fence").map(row => row.path));
      expect(native.length).toBeGreaterThan(0);
      for (const probe of native) expect(probe.admits, probe.path).toBeNull();
    }
  });
});
