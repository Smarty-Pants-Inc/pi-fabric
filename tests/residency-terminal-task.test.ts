import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentDeliveryPrefix, type ResidentHostConfig } from "../src/residency/protocol.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { saveFinalAnswerReceipt } from "../src/worker/terminal-answer.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";

beforeEach(() => installInProcessResidentFence());
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-terminal-"));
  const config: ResidentHostConfig = { format: RESIDENT_HOST_FORMAT, rootId: "session:terminal-root", sessionId: "terminal-root",
    cwd: process.cwd(), projectRoot: process.cwd(), meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: true }, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("src/index.ts"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda", piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" } };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  writeJsonAtomic(path.join(config.residencyRoot, "config.json"), config);
  // The resident broker/bootstrap is real; capture transport close explicitly
  // so final-answer routing cannot accidentally stand in for execution exit.
  const closures = new Map<string, () => void>();
  const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
    const args = new Map<string, string>();
    for (let i = 0; i < request.workerArguments.length; i += 2) args.set(request.workerArguments[i]!, request.workerArguments[i + 1]!);
    writeJsonAtomic(args.get("--status-file")!, { id: request.id, name: request.name, task: "task", status: "running", runner: "pi", transport: "process",
      cwd: request.cwd, startedAt: Date.now(), updatedAt: Date.now(), turns: 1, toolCalls: 0, text: "progress",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } });
    let alive = true;
    const closed = new Promise<void>(resolve => { closures.set(request.id, () => { alive = false; resolve(); }); });
    return { kind: "process", closed, isAlive: async () => alive, waitForClose: () => closed, stop: async () => { await closed; } };
  });
  return { root, config, host: new ResidentHost(config, () => {}), close: (id?: string) => {
    if (id) closures.get(id)!(); else for (const close of closures.values()) close();
  }, launch };
};
const freeze = async (host: ResidentHost, id: string) => {
  const run = host.agents.runDirectory(id)!;
  await vi.waitFor(() => expect(fs.existsSync(path.join(run, "status.json"))).toBe(true));
  const snapshot = JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8"));
  const receipt = saveFinalAnswerReceipt(run, id, "durable final answer");
  writeJsonAtomic(path.join(run, "status.json"), { ...snapshot, status: "completed", text: receipt.text,
    finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt }, finishedAt: receipt.recordedAt });
  return { receipt, run };
};

describe("resident ordinary-task terminal notices", () => {
  it("bootstraps exact root-sender durable notices and typed late owner rejections", async () => {
    const f = fixture();
    const who: MeshIdentity = { id: f.config.rootId, name: "Root sender", kind: "main" };
    const sender = new FabricControlPlane(new MeshStore(f.config.meshRoot, 65536, 1000), who,
      { enabled: true, hostId: who.id, pollMs: 20, acknowledgementTimeoutMs: 2000 });
    try {
      await f.host.start(); sender.start(() => ({ accepted: false }));
      const task = await f.host.agents.spawn({ task: "HANG", transport: "process" });
      const queued = await sender.request(f.host.hostId, task.id, "followUp", { message: "stale queued assignment" }, f.host.identity.id);
      const { receipt } = await freeze(f.host, task.id);
      expect(f.host.agents.status(task.id).status).toBe("finishing");
      await expect(sender.request(f.host.hostId, task.id, "steer", { message: "late while finishing" }, f.host.identity.id))
        .rejects.toMatchObject({ code: "FABRIC_TARGET_TERMINAL", targetId: task.id, finalAnswerReceiptId: receipt.id });
      f.close(task.id);
      expect(await f.host.agents.wait(task.id, { timeoutMs: 3000 })).toMatchObject({ status: "completed", finalAnswerReceipt: { id: receipt.id } });
      let delivery: any;
      await vi.waitFor(() => {
        delivery = f.host.mesh.listAll(residentDeliveryPrefix(who.id)).map(row => row.value as any).find(row => row.data?.code === "FABRIC_TARGET_TERMINAL");
        expect(delivery).toBeDefined();
      });
      expect(delivery).toMatchObject({ rootId: who.id, from: { id: task.id, kind: "agent" }, delivery: "steer", triggerTurn: false,
        data: { code: "FABRIC_TARGET_TERMINAL", targetId: task.id, messageId: queued.messageId, delivery: "followUp", finalAnswerReceiptId: receipt.id,
          sender: { id: who.id, name: who.name, verified: "mesh" } } });
      await expect(sender.request(f.host.hostId, task.id, "steer", { message: "late" }, f.host.identity.id))
        .rejects.toMatchObject({ code: "FABRIC_TARGET_TERMINAL", targetId: task.id, finalAnswerReceiptId: receipt.id });
    } finally { f.close(); await sender.close(); await f.host.close(); f.launch.mockRestore(); fs.rmSync(f.root, { recursive: true, force: true }); }
  });

  it("E-steer-final resident owner refuses an append-race ACK and still admits pre-final input", async () => {
    const f = fixture();
    const who: MeshIdentity = { id: f.config.rootId, name: "Root sender", kind: "main" };
    const sender = new FabricControlPlane(new MeshStore(f.config.meshRoot, 65536, 1000), who,
      { enabled: true, hostId: who.id, pollMs: 20, acknowledgementTimeoutMs: 2000 });
    let appendSpy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      await f.host.start(); sender.start(() => ({ accepted: false }));
      const task = await f.host.agents.spawn({ task: "HANG", transport: "process" });
      const run = f.host.agents.runDirectory(task.id)!;
      const journal = path.join(run, "steer.jsonl");
      const allowed = await sender.request(f.host.hostId, task.id, "steer", { message: "before final" }, f.host.identity.id);
      expect(allowed).toMatchObject({ acknowledged: true });
      // The fake child does not consume controls. Mark the exact original
      // admission consumed, as native context delivery does in E-steer-final.
      const admission = JSON.parse(fs.readFileSync(journal, "utf8").trim());
      writeJsonAtomic(path.join(run, "terminal-controls", `${allowed.messageId}.json`), {
        id: allowed.messageId, delivery: "steer", state: "delivered", provenance: admission.provenance,
      });
      const append = fs.appendFileSync;
      let receipt: ReturnType<typeof saveFinalAnswerReceipt> | undefined;
      let racingId = "";
      appendSpy = vi.spyOn(fs, "appendFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, data: string | Uint8Array, options?: fs.WriteFileOptions) => {
        if (file === journal && !receipt) {
          racingId = JSON.parse(String(data)).id;
          const snapshot = JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8"));
          receipt = saveFinalAnswerReceipt(run, task.id, "durable final answer");
          writeJsonAtomic(path.join(run, "status.json"), { ...snapshot, status: "completed", text: receipt.text,
            finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt }, finishedAt: receipt.recordedAt });
        }
        return append(file, data, options);
      }) as typeof fs.appendFileSync);
      await expect(sender.request(f.host.hostId, task.id, "steer", { message: "racing final" }, f.host.identity.id))
        .rejects.toMatchObject({ code: "FABRIC_TARGET_TERMINAL", deliveryStatus: "undelivered", targetId: task.id,
          reason: "run-ended", runState: "completed", message: expect.stringContaining("undelivered: task already finished") });
      expect(receipt).toBeDefined();
      expect(await f.host.agents.wait(task.id, { timeoutMs: 3000 })).toMatchObject({ status: "completed", text: receipt!.text });
      await vi.waitFor(() => {
        const deliveries = f.host.mesh.listAll(residentDeliveryPrefix(who.id)).map(row => row.value as any);
        expect(deliveries.filter(row => row.data?.code === "FABRIC_TARGET_TERMINAL")).toEqual([expect.objectContaining({
          rootId: who.id, triggerTurn: false, message: expect.stringContaining("undelivered: task already finished"),
          data: expect.objectContaining({ messageId: racingId, finalAnswerReceiptId: receipt!.id, sender: expect.objectContaining({ id: who.id }) }),
        })]);
      });
      const bytes = fs.readFileSync(journal, "utf8");
      await expect(sender.request(f.host.hostId, task.id, "steer", { message: "after final" }, f.host.identity.id))
        .rejects.toMatchObject({ code: "FABRIC_TARGET_TERMINAL", deliveryStatus: "undelivered", runState: "completed" });
      expect(fs.readFileSync(journal, "utf8")).toBe(bytes);
    } finally {
      appendSpy?.mockRestore(); await sender.close(); await f.host.close();
      // Retain this race proof; no real fixture deletion in the local path.
    }
  });

  // Decision C: only the manager's append-race receipt is mocked to pin a resolved
  // run state; the resident host, control planes and sender error are real.
  it.each(["completed", "failed"] as const)("resident owner carries the real %s run state of an append-race receipt", async runState => {
    const f = fixture();
    const who: MeshIdentity = { id: f.config.rootId, name: "Root sender", kind: "main" };
    const sender = new FabricControlPlane(new MeshStore(f.config.meshRoot, 65536, 1000), who,
      { enabled: true, hostId: who.id, pollMs: 20, acknowledgementTimeoutMs: 2000 });
    try {
      await f.host.start(); sender.start(() => ({ accepted: false }));
      const task = await f.host.agents.spawn({ task: "HANG", transport: "process" });
      vi.spyOn(f.host.agents, "steer").mockReturnValueOnce({ queued: true, messageId: "m-race", undelivered: { code: "FABRIC_TARGET_TERMINAL",
        reason: "run-ended", deliveryStatus: "undelivered", messageId: "m-race", delivery: "steer", finalAnswerReceiptId: "race-receipt", runState } });
      await expect(sender.request(f.host.hostId, task.id, "steer", { message: "racing" }, f.host.identity.id))
        .rejects.toMatchObject({ code: "FABRIC_TARGET_TERMINAL", targetId: task.id, finalAnswerReceiptId: "race-receipt", runState });
    } finally { vi.restoreAllMocks(); await sender.close(); await f.host.close(); }
  });

  it("retains an unavailable peer sender and retries through its exact restored owner on status", async () => {
    const f = fixture();
    const who: MeshIdentity = { id: "session:peer-sender", name: "Peer sender", kind: "main" };
    const peerMesh = new MeshStore(f.config.meshRoot, 65536, 1000);
    const sender = new FabricControlPlane(peerMesh, who, { enabled: true, hostId: who.id, pollMs: 20, acknowledgementTimeoutMs: 2000 });
    const peerDirectory = new ParticipantDirectory(peerMesh, { enabled: true, rootId: who.id, hostId: who.id, identity: who, heartbeatMs: 60000, leaseMs: 120000 });
    const accepted: any[] = [];
    try {
      await f.host.start(); sender.start(command => { accepted.push(command); return { accepted: true, messageId: command.commandId }; });
      const task = await f.host.agents.spawn({ task: "HANG", transport: "process" });
      const queued = await sender.request(f.host.hostId, task.id, "steer", { message: "stale queued assignment" }, f.host.identity.id);
      const { receipt, run } = await freeze(f.host, task.id);
      expect(f.host.agents.status(task.id).status).toBe("finishing");
      f.close(task.id);
      await f.host.agents.wait(task.id, { timeoutMs: 3000 });
      const noticeFile = path.join(run, "terminal-notices", `${queued.messageId}.json`);
      await vi.waitFor(() => expect(fs.existsSync(noticeFile)).toBe(true));
      expect(accepted).toHaveLength(0);
      peerDirectory.registerSource(() => [{ format: 1, id: who.id, kind: "root", rootId: who.id, ownerHostId: who.id, ownerIdentityId: who.id,
        name: who.name, status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp"], startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" }]);
      await peerDirectory.start();
      f.host.agents.status(task.id);
      await vi.waitFor(() => expect(fs.existsSync(`${noticeFile}.delivered`)).toBe(true));
      expect(accepted).toHaveLength(1);
      expect(accepted[0]).toMatchObject({ targetId: who.id, operation: "steer", triggerTurn: false,
        data: { code: "FABRIC_TARGET_TERMINAL", finalAnswerReceiptId: receipt.id, sender: { id: who.id }, messageId: queued.messageId } });
      f.host.agents.status(task.id);
      await new Promise(resolve => setTimeout(resolve, 30));
      expect(accepted).toHaveLength(1);
    } finally { f.close(); await peerDirectory.close(); await sender.close(); await f.host.close(); f.launch.mockRestore(); fs.rmSync(f.root, { recursive: true, force: true }); }
  });
});
