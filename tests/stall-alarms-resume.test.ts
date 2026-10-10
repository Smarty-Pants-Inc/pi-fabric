import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { MainInboxMaintenance, mainInboxActive, mainInboxOwns, recordMainSuccessor,
  registerMainInbox, stageMainSuccessor } from "../src/topology/stall-alarms.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

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
