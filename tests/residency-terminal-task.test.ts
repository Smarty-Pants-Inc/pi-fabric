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
  return { root, config, host: new ResidentHost(config, () => {}) };
};
// A transport handle is not worker readiness. Read and validate in the retry,
// then carry the snapshot into synchronous append spies (which cannot await).
const runningSnapshot = (run: string) => vi.waitFor(() => {
  const snapshot = JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8"));
  expect(snapshot).toMatchObject({ status: "running", id: path.basename(run) });
  return snapshot;
}, { timeout: 3000 });
const gatedHang = async (f: ReturnType<typeof fixture>) => {
  const gate = path.join(f.root, "worker-boot-release");
  const task = await f.host.agents.spawn({ task: JSON.stringify({ task: "HANG", fakeWorkerBootGate: gate }), transport: "process" });
  const run = f.host.agents.runDirectory(task.id)!;
  await vi.waitFor(() => expect(fs.existsSync(`${gate}.entered`)).toBe(true), { timeout: 3000 });
  expect(fs.existsSync(path.join(run, "status.json"))).toBe(false);
  expect(fs.existsSync(path.join(run, "final-answer.json"))).toBe(false);
  return { task, run, release: () => fs.writeFileSync(gate, "release") };
};
const freeze = async (host: ResidentHost, id: string) => {
  const run = host.agents.runDirectory(id)!;
  const snapshot = await runningSnapshot(run);
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
    } finally { await sender.close(); await f.host.close(); fs.rmSync(f.root, { recursive: true, force: true }); }
  });

  it("E-steer-final resident owner refuses an append-race ACK and still admits pre-final input", async () => {
    const f = fixture();
    const who: MeshIdentity = { id: f.config.rootId, name: "Root sender", kind: "main" };
    const sender = new FabricControlPlane(new MeshStore(f.config.meshRoot, 65536, 1000), who,
      { enabled: true, hostId: who.id, pollMs: 20, acknowledgementTimeoutMs: 2000 });
    let appendSpy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      await f.host.start(); sender.start(() => ({ accepted: false }));
      const { task, run, release } = await gatedHang(f);
      release();
      const snapshot = await runningSnapshot(run);
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
  it.each(["steer", "followUp"] as const)("returns a typed %s refusal when the append target disappears after final publication", async delivery => {
    const f = fixture();
    const who: MeshIdentity = { id: f.config.rootId, name: "Root sender", kind: "main" };
    const sender = new FabricControlPlane(new MeshStore(f.config.meshRoot, 65536, 1000), who,
      { enabled: true, hostId: who.id, pollMs: 20, acknowledgementTimeoutMs: 2000 });
    let appendSpy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      await f.host.start(); sender.start(() => ({ accepted: false }));
      const { task, run, release } = await gatedHang(f);
      release();
      const snapshot = await runningSnapshot(run);
      const journal = path.join(run, "steer.jsonl");
      const append = fs.appendFileSync;
      let receipt: ReturnType<typeof saveFinalAnswerReceipt> | undefined;
      appendSpy = vi.spyOn(fs, "appendFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, data: string | Uint8Array, options?: fs.WriteFileOptions) => {
        if (file === journal && !receipt) {
          receipt = saveFinalAnswerReceipt(run, task.id, "durable final answer");
          writeJsonAtomic(path.join(run, "status.json"), { ...snapshot, status: "completed", text: receipt.text,
            finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt }, finishedAt: receipt.recordedAt });
          const error = new Error("append target disappeared during terminal transition") as NodeJS.ErrnoException;
          error.code = "ENOENT";
          throw error;
        }
        return append(file, data, options);
      }) as typeof fs.appendFileSync);
      await expect(sender.request(f.host.hostId, task.id, delivery, { message: "racing final" }, f.host.identity.id))
        .rejects.toMatchObject({ code: "FABRIC_TARGET_TERMINAL", deliveryStatus: "undelivered", targetId: task.id,
          reason: "run-ended", runState: "completed", message: expect.stringContaining("undelivered: task already finished") });
      expect(receipt).toBeDefined();
    } finally {
      appendSpy?.mockRestore(); await sender.close(); await f.host.close();
    }
  });

  // Real relay counterexamples: neither a pre-receipt filesystem failure nor
  // an unrelated append error after publication is evidence of terminal refusal.
  it.each((["steer", "followUp"] as const).flatMap(delivery =>
    (["boot-read", "append-ENOENT", "append-EACCES-after-final"] as const).map(fault => [delivery, fault] as const)
  ))("%s preserves %s as a nonterminal relay rejection", async (delivery, fault) => {
    const f = fixture();
    const who: MeshIdentity = { id: f.config.rootId, name: "Root sender", kind: "main" };
    const sender = new FabricControlPlane(new MeshStore(f.config.meshRoot, 65536, 1000), who,
      { enabled: true, hostId: who.id, pollMs: 20, acknowledgementTimeoutMs: 2000 });
    let appendSpy: ReturnType<typeof vi.spyOn> | undefined;
    try {
      await f.host.start(); sender.start(() => ({ accepted: false }));
      const { task, run, release } = await gatedHang(f);
      const snapshot = fault === "boot-read" ? undefined : (release(), await runningSnapshot(run));
      const journal = path.join(run, "steer.jsonl");
      const append = fs.appendFileSync;
      let injected: unknown;
      let receipt: ReturnType<typeof saveFinalAnswerReceipt> | undefined;
      appendSpy = vi.spyOn(fs, "appendFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, data: string | Uint8Array, options?: fs.WriteFileOptions) => {
        if (file !== journal) return append(file, data, options);
        try {
          if (fault === "boot-read") {
            // Exact merged-base ordering: synchronous read BEFORE receipt creation.
            // The gate makes its original boot race deterministic, without CPU load.
            JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8"));
            receipt = saveFinalAnswerReceipt(run, task.id, "must not be published before readiness");
          }
          if (fault === "append-EACCES-after-final") {
            receipt = saveFinalAnswerReceipt(run, task.id, "durable final answer");
            writeJsonAtomic(path.join(run, "status.json"), { ...snapshot, status: "completed", text: receipt.text,
              finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt }, finishedAt: receipt.recordedAt });
          }
          const code = fault === "append-EACCES-after-final" ? "EACCES" : "ENOENT";
          throw Object.assign(new Error(`${code}: injected genuine append failure`), { code });
        } catch (error) { injected = error; throw error; }
      }) as typeof fs.appendFileSync);
      const rejection = await sender.request(f.host.hostId, task.id, delivery, { message: "filesystem counterexample" }, f.host.identity.id)
        .then(() => { throw new Error("Unexpected ACK"); }, error => error);
      expect(injected).toMatchObject({ code: fault === "append-EACCES-after-final" ? "EACCES" : "ENOENT" });
      expect(rejection.message).toBe((injected as Error).message);
      expect(rejection.code).not.toBe("FABRIC_TARGET_TERMINAL");
      expect(rejection.finalAnswerReceiptId).toBeUndefined();
      expect(fs.existsSync(journal)).toBe(false);
      expect(fs.existsSync(path.join(run, "final-answer.json"))).toBe(fault === "append-EACCES-after-final");
      if (fault === "boot-read") {
        expect(injected).toMatchObject({ syscall: "open", path: path.join(run, "status.json") });
        expect(receipt).toBeUndefined();
        expect(fs.existsSync(path.join(run, "status.json"))).toBe(false);
        appendSpy.mockRestore();
        release();
        await runningSnapshot(run);
        expect(await sender.request(f.host.hostId, task.id, delivery, { message: "before final" }, f.host.identity.id))
          .toMatchObject({ acknowledged: true });
      }
    } finally { appendSpy?.mockRestore(); await sender.close(); await f.host.close(); }
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
    } finally { await peerDirectory.close(); await sender.close(); await f.host.close(); fs.rmSync(f.root, { recursive: true, force: true }); }
  });
});
