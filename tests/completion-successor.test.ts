import fs from "node:fs";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentCompletionInbox } from "../src/agents/completion-inbox.js";
import * as atomicWrite from "../src/core/atomic-write.js";
import { CompletionJournal, completionConsumed, consumeCompletion, pendingCompletions, saveCompletion, saveWorkerCompletion, type CompletionRecipient } from "../src/agents/completion-journal.js";
import type { AgentRunResult, AgentHandleInfo } from "../src/agents/types.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import * as processIdentity from "../src/residency/process-identity.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentHost } from "../src/residency/host.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { residentDeliveryPrefix, residentHostId, residentResultPath, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../src/topology/types.js";

// Most legacy tests below exercise crash-left recovery, not the normal consume
// path. Restore the just-archived inode to represent a stop after durable receipt
// and before rename. The receipt-time archive suite tests the normal path itself.
const receiptBeforeArchive = (meshRoot: string, id: string, sessionId: string): void => {
  consumeCompletion(meshRoot, id, sessionId);
  const name = `${createHash("sha256").update(id).digest("hex")}.json`;
  const directory = path.join(meshRoot, "agent-completions");
  const archive = path.join(directory, "archive", name);
  if (fs.existsSync(archive)) fs.renameSync(archive, path.join(directory, name));
};
const roots: string[] = [];
const clients: ResidencyClient[] = [];
const inboxes: AgentCompletionInbox[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  for (const manager of managers.splice(0)) await manager.close();
  vi.restoreAllMocks();
  for (const client of clients.splice(0)) await client.close();
  for (const inbox of inboxes.splice(0)) inbox.close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});
const waitFor = async (test: () => boolean) => {
  const end = Date.now() + 3000;
  while (!test()) { if (Date.now() > end) throw new Error("completion probe timed out"); await new Promise(resolve => setTimeout(resolve, 10)); }
};
const harness = (small = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "completion-successor-")); roots.push(root);
  const meshRoot = path.join(root, "mesh");
  const mesh = new MeshStore(meshRoot, small ? 1024 : DEFAULT_FABRIC_CONFIG.mesh.maxEventBytes, 100, small ? { maxStateBytes: 4096, maxStateTombstones: 2 } : {});
  let live: FabricParticipantInfo[] = [];
  const participant = (session: string, startedAt: number, extra = {}): FabricParticipantInfo => ({
    format: 1, id: `session:${session}`, rootId: `session:${session}`, ownerHostId: `session:${session}`,
    ownerIdentityId: `session:${session}`, sessionId: session, name: "main", role: "lane-main", project: root,
    cwd: root, kind: "root", status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"],
    interactive: true, startedAt, updatedAt: startedAt, controlProtocol: "v1", local: false, stale: false, ...extra,
  });
  const participants = { list: () => live, lastKnown: () => undefined, scheduleRefresh() {} } as unknown as FabricParticipantSource;
  const config = (session: string, startedAt: number): ResidentHostConfig => ({
    format: 1, rootId: `session:${session}`, sessionId: session, cwd: root, projectRoot: root,
    mainName: "main", mainStartedAt: startedAt, role: "lane-main", project: root, meshRoot,
    residencyRoot: residentRoot(meshRoot, `session:${session}`), actorRoot: path.join(meshRoot, "actors"), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents }, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: "unused", fabricExtensionPath: "unused", piBinary: "unused", claudeBinary: "unused", vedaBinary: "unused",
  });
  const result: AgentRunResult = { id: "a".repeat(32), name: "finished child", task: "work", status: "completed",
    runner: "pi", transport: "process", cwd: root, startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0,
    text: "authoritative task result", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } };
  const recipient: CompletionRecipient = { rootId: "session:A", sessionId: "A", cwd: root, projectRoot: root,
    name: "main", role: "lane-main", startedAt: 100 };
  const client = (session: string, startedAt: number, extra = {}) => {
    const handlers = new Map<string, (event: unknown, ctx: ExtensionContext) => unknown>();
    const sessionFile = path.join(root, `${session}-${clients.length}.jsonl`);
    fs.writeFileSync(sessionFile, JSON.stringify({ type: "session", id: session }) + "\n");
    const sendMessage = vi.fn(message => fs.appendFileSync(sessionFile, JSON.stringify({ type: "custom_message", ...message }) + "\n"));
    const context = { hasUI: false, isIdle: () => false, hasPendingMessages: () => false,
      sessionManager: { getSessionId: () => session, getSessionFile: () => sessionFile } } as unknown as ExtensionContext;
    const inbox = new AgentCompletionInbox({ on: (name: string, handler: any) => handlers.set(name, handler), sendMessage } as any, context); inboxes.push(inbox);
    const completed = vi.fn((value: AgentRunResult, delivered: () => void) => inbox.enqueue(value, delivered));
    const cfg = { ...config(session, startedAt), ...extra };
    const client = new ResidencyClient({ config: cfg, mesh, participants,
      mainAgent: { local: true, deliverAgent: vi.fn() } as any, onBackgroundComplete: completed,
      onResultConsumed: id => inbox.acknowledge(id) }); clients.push(client);
    return { client, inbox, completed, sendMessage, turn: () => handlers.get("turn_end")?.({ message: { role: "assistant", stopReason: "stop" } }, context) };
  };
  const seedResident = async () => {
    const cfg = config("A", 100);
    fs.mkdirSync(path.join(cfg.residencyRoot, "results"), { recursive: true });
    fs.writeFileSync(path.join(cfg.residencyRoot, "config.json"), JSON.stringify(cfg));
    fs.writeFileSync(residentResultPath(cfg.residencyRoot, result.id), JSON.stringify(result));
    const sourceKey = `${residentDeliveryPrefix(cfg.rootId)}legacy-result`;
    await mesh.put({ key: sourceKey, identity: { id: residentHostId(cfg.rootId), name: "resident", kind: "main" }, ifVersion: 0,
      value: { format: 1, id: "legacy-result", rootId: cfg.rootId, from: { id: result.id, name: result.name, kind: "agent" },
        agentCompletionId: result.id, delivery: "followUp", triggerTurn: true, message: "legacy clipped summary", data: { fabricTruncated: true }, createdAt: 2 } });
    return sourceKey;
  };
  return { root, meshRoot, mesh, participant, participants, recipient, result, client, seedResident, setLive: (value: FabricParticipantInfo[]) => { live = value; } };
};

const invocation = { cwd: process.cwd(), extensionContext: { modelRegistry: { getAvailable: () => [] } }, update() {}, activity() {} } as unknown as FabricInvocationContext;
const managerFor = (h: ReturnType<typeof harness>, owner = h.client("A", 100)) => {
  const manager = new AgentManager(h.root, { ...DEFAULT_FABRIC_CONFIG.agents, nice: 19, sessionExport: false }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(h.root, "runs"),
    meshRoot: h.meshRoot, completionRecipient: h.recipient,
    onSettled: result => owner.client.enqueueCompletion(result),
  });
  managers.push(manager); return manager;
};
const providerFor = (h: ReturnType<typeof harness>, client: ResidencyClient, manager = managerFor(h)) => new AgentsProvider(
  manager, { identity: { id: client.options.config.rootId, kind: "main", name: "main" },
    status: () => { throw new Error("Unknown Fabric actor"); } } as any, {} as any,
  { local: true, matches: () => false } as any, h.participants, undefined, {} as any, () => false, client, false,
);

const legacyFenceFaults = ["malformed", "root mismatch", "run mismatch", "invalid consumption", "unreadable", "dangling"] as const;
const damageLegacyFence = (h: ReturnType<typeof harness>, fault: typeof legacyFenceFaults[number]) => {
  const file = path.join(residentRoot(h.meshRoot, h.recipient.rootId), "agents", `${h.result.id}.json`);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const consumed = { rootId: h.recipient.rootId, id: h.result.id, completionConsumedAt: 3 };
  fs.writeFileSync(file, JSON.stringify(consumed));
  if (fault === "malformed") fs.writeFileSync(file, "{torn");
  if (fault === "root mismatch") fs.writeFileSync(file, JSON.stringify({ ...consumed, rootId: "session:other" }));
  if (fault === "run mismatch") fs.writeFileSync(file, JSON.stringify({ ...consumed, id: "c".repeat(32) }));
  if (fault === "invalid consumption") fs.writeFileSync(file, JSON.stringify({ ...consumed, completionConsumedAt: "unknown" }));
  if (fault === "dangling") { fs.unlinkSync(file); fs.symlinkSync(path.join(h.root, "missing-metadata"), file); }
  let readFault: ReturnType<typeof vi.spyOn> | undefined;
  let asyncReadFault: ReturnType<typeof vi.spyOn> | undefined;
  if (fault === "unreadable") {
    const read = fs.readFileSync;
    readFault = vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...args: any[]) => {
      if (String(target) === file) throw Object.assign(new Error("legacy metadata inaccessible"), { code: "EACCES" });
      return (read as any)(target, ...args);
    }) as typeof fs.readFileSync);
    const asyncRead = fs.promises.readFile.bind(fs.promises);
    asyncReadFault = vi.spyOn(fs.promises, "readFile").mockImplementation((async (target: any, ...args: any[]) => {
      if (String(target) === file) throw Object.assign(new Error("legacy metadata inaccessible"), { code: "EACCES" });
      return asyncRead(target, ...args);
    }) as typeof fs.promises.readFile);
  }
  return { file, repair: () => {
    readFault?.mockRestore(); asyncReadFault?.mockRestore();
    if (fault === "dangling") fs.unlinkSync(file);
    fs.writeFileSync(file, JSON.stringify(consumed));
  } };
};

// Fail only AFTER this target's rename, not its temporary-file sync or mkdir barriers.
const postRenameFault = (target: string) => {
  const rename = fs.renameSync; const sync = fs.fsyncSync; const open = fs.promises.open;
  const state = { renamed: false, barrier: "directory" as "directory" | "file" | "none", fileSyncs: 0, directorySyncs: 0 };
  vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
    rename(source, destination);
    if (String(destination) === target) state.renamed = true;
  });
  vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
    const directory = fs.fstatSync(fd).isDirectory();
    if (state.renamed) {
      if (directory) state.directorySyncs++; else state.fileSyncs++;
      if ((directory && state.barrier === "directory") || (!directory && state.barrier === "file")) {
        throw new Error(`post-rename ${state.barrier} barrier failed`);
      }
    }
    sync(fd);
  });
  vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
    const handle = await open(...args);
    const syncHandle = handle.sync.bind(handle);
    vi.spyOn(handle, "sync").mockImplementation(async () => {
      const directory = (await handle.stat()).isDirectory();
      if (state.renamed && ((directory && state.barrier === "directory") || (!directory && state.barrier === "file"))) {
        throw new Error(`post-rename ${state.barrier} barrier failed`);
      }
      await syncHandle();
    });
    return handle;
  });
  return state;
};

describe("round 5 Windows completion file confirmation", () => {
  it("F6: reopens existing envelopes and receipts with writable Windows handles without changing their bytes or inode", () => {
    const h = harness(); saveCompletion(h.meshRoot, h.recipient, h.result);
    const dir = path.join(h.meshRoot, "agent-completions");
    const envelope = path.join(dir, fs.readdirSync(dir).find(file => file.endsWith(".json"))!);
    const receipt = path.join(dir, "receipts", path.basename(envelope));
    const originalEnvelope = fs.readFileSync(envelope, "utf8");
    const inode = fs.statSync(envelope);
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    const open = fs.openSync; const sync = fs.fsyncSync;
    const flags = new Map<number, string | number>();
    const namespace = vi.spyOn(atomicWrite, "syncPathNamespace");
    const opened = vi.spyOn(fs, "openSync").mockImplementation((file, mode, permissions) => {
      const fd = open(file, mode, permissions); flags.set(fd, mode); return fd;
    });
    let fileSyncs = 0;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      // Model Windows FlushFileBuffers, not a directory barrier failure.
      if (fs.fstatSync(fd).isFile()) {
        fileSyncs++;
        if (flags.get(fd) === "r" || flags.get(fd) === fs.constants.O_RDONLY) {
          throw Object.assign(new Error("Windows file fsync requires a writable handle"), { code: "EPERM" });
        }
      }
      sync(fd);
    });
    try {
      Object.defineProperty(process, "platform", { ...platform, value: "win32" });
      saveCompletion(h.meshRoot, h.recipient, h.result); // Existing-envelope retry.
      expect(opened.mock.calls.filter(([file]) => file === envelope).map(([, mode]) => mode)).toEqual(["r+"]);
      expect(fs.readFileSync(envelope, "utf8")).toBe(originalEnvelope);
      expect(fs.statSync(envelope)).toMatchObject({ dev: inode.dev, ino: inode.ino });
      receiptBeforeArchive(h.meshRoot, h.result.id, "B");
      const originalReceipt = fs.readFileSync(receipt, "utf8");
      const receiptInode = fs.statSync(receipt);
      expect(completionConsumed(h.meshRoot, h.result.id)).toBe(true);
      fs.utimesSync(receipt, new Date(0), new Date(0)); // Model an uncached receipt incarnation.
      receiptBeforeArchive(h.meshRoot, h.result.id, "C"); // Confirm once, never replace B's receipt.
      expect(pendingCompletions(h.meshRoot, h.root)).toEqual([]);
      const receiptOpens = opened.mock.calls.filter(([file]) => file === receipt);
      expect(receiptOpens.length).toBeGreaterThan(0);
      expect(receiptOpens.every(([, mode]) => mode === "r+")).toBe(true);
      expect(fs.readFileSync(receipt, "utf8")).toBe(originalReceipt);
      expect(JSON.parse(originalReceipt)).toMatchObject({ sessionId: "B" });
      expect(fs.statSync(receipt)).toMatchObject({ dev: receiptInode.dev, ino: receiptInode.ino });
      expect(namespace.mock.calls.some(([file, stat]) => file === envelope && stat?.dev === inode.dev && stat?.ino === inode.ino)).toBe(true);
      expect(namespace.mock.calls.some(([file, stat]) => file === receipt && stat?.dev === receiptInode.dev && stat?.ino === receiptInode.ino)).toBe(true);
      expect(fileSyncs).toBeGreaterThanOrEqual(3);
    } finally { Object.defineProperty(process, "platform", platform); }
  });

  it.each(["win32", "linux", "darwin"])("F6: %s explicit and recovery consumption retries require receipt confirmation", async platformName => {
    const h = harness(); h.setLive([h.participant("B", 200)]);
    const journal = new CompletionJournal(h.meshRoot, { ...h.recipient, rootId: "session:B", sessionId: "B", startedAt: 200 }, h.participants, h.mesh, vi.fn());
    journal.save(h.result); await journal.drain(false);
    const [claim] = h.mesh.listAll("residency/completion-claims/"); expect(claim).toBeDefined();
    receiptBeforeArchive(h.meshRoot, h.result.id, "B"); // Visible receipt with a crash-left claim.
    const dir = path.join(h.meshRoot, "agent-completions");
    const envelope = path.join(dir, fs.readdirSync(dir).find(file => file.endsWith(".json"))!);
    const receipt = path.join(dir, "receipts", path.basename(envelope));
    const originalReceipt = fs.readFileSync(receipt, "utf8");
    fs.utimesSync(receipt, new Date(0), new Date(0)); // Uncached, like a fresh process.
    const originalEnvelope = fs.readFileSync(envelope, "utf8");
    const sync = fs.fsyncSync;
    const denied = Object.assign(new Error("file fsync denied"), { code: "EPERM" });
    const failed = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (fs.fstatSync(fd).isFile()) throw denied;
      sync(fd);
    });
    const open = fs.promises.open;
    const asyncFailed = vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      const handle = await open(...args);
      const syncHandle = handle.sync.bind(handle);
      vi.spyOn(handle, "sync").mockImplementation(async () => {
        if ((await handle.stat()).isFile()) throw denied;
        await syncHandle();
      });
      return handle;
    });
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    try {
      Object.defineProperty(process, "platform", { ...platform, value: platformName });
      expect(completionConsumed(h.meshRoot, h.result.id)).toBe(true); // Plain scan read.
      expect(() => receiptBeforeArchive(h.meshRoot, h.result.id, "C")).toThrow(denied);
      expect(() => journal.acknowledge(h.result.id)).toThrow(denied);
      expect(() => journal.forget(h.result.id)).toThrow(denied);
      await expect(journal.drain(false)).rejects.toThrow(denied);
      expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(1);
      expect(asyncFailed.mock.calls.some(([file, mode]) => String(file) === receipt && mode === (platformName === "win32" ? "r+" : "r"))).toBe(true);
      expect(fs.readFileSync(receipt, "utf8")).toBe(originalReceipt);
      expect(fs.readFileSync(envelope, "utf8")).toBe(originalEnvelope);
      expect(fs.existsSync(path.join(dir, "archive", path.basename(envelope)))).toBe(false);
      expect(journal.enqueue).not.toHaveBeenCalled();
    } finally { Object.defineProperty(process, "platform", platform); failed.mockRestore(); asyncFailed.mockRestore(); }
    await journal.drain(false);
    expect(fs.readFileSync(path.join(dir, "archive", path.basename(envelope)), "utf8")).toBe(originalEnvelope);
    expect(fs.readFileSync(receipt, "utf8")).toBe(originalReceipt);
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(0);
    expect(journal.enqueue).not.toHaveBeenCalled();
  });
});

describe("round 5 spawn-time completion authority", () => {
  it("snapshots queued spawns and stopped settlements before later host renames", async () => {
    const h = harness();
    const owner = h.client("A", 100);
    let name = "main";
    const manager = new AgentManager(h.root, { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, budgetUsd: 0, nice: 19, sessionExport: false }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(h.root, "runs"),
      meshRoot: h.meshRoot, completionRecipient: () => ({ ...h.recipient, name }),
      onSettled: result => owner.client.enqueueCompletion(result),
    });
    managers.push(manager);
    const first = await manager.spawn({ task: "HANG", transport: "process" });
    name = "probe-lane";
    const queued = await manager.spawn({ task: "private queued work", transport: "process" });
    expect(queued.status).toBe("queued");
    name = "renamed-again";
    await manager.stop(first.id);
    await manager.wait(queued.id, { timeoutMs: 5_000, deferConsumption() {} });
    const envelopes = pendingCompletions(h.meshRoot, h.root);
    expect(envelopes.find(value => value.result.id === first.id)?.recipient.name).toBe("main");
    expect(envelopes.find(value => value.result.id === queued.id)?.recipient.name).toBe("probe-lane");
    const manifest = JSON.parse(fs.readFileSync(path.join(manager.runDirectory(queued.id)!, "completion-recipient.json"), "utf8"));
    expect(manifest.recipient.name).toBe("probe-lane");
  });

  it("refuses missing, corrupt or foreign run manifests instead of relabeling a body to the current name", () => {
    const h = harness();
    const journal = new CompletionJournal(h.meshRoot, () => ({ ...h.recipient, name: "renamed-again" }), h.participants, h.mesh, vi.fn());
    const run = path.join(h.root, "run"); fs.mkdirSync(run);
    const result = { ...h.result, logFile: path.join(run, "events.jsonl") };
    const file = path.join(run, "completion-recipient.json");
    expect(() => journal.save(result)).toThrow("Missing admitted completion recipient");
    fs.writeFileSync(file, "broken");
    expect(() => journal.save(result)).toThrow();
    fs.writeFileSync(file, JSON.stringify({ meshRoot: path.join(h.root, "foreign"), recipient: h.recipient }));
    expect(() => journal.save(result)).toThrow("Invalid admitted completion recipient");
    expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(0);
    fs.writeFileSync(file, JSON.stringify({ meshRoot: h.meshRoot, recipient: { ...h.recipient, name: "probe-lane" } }));
    journal.save(result);
    expect(pendingCompletions(h.meshRoot, h.root)[0]?.recipient.name).toBe("probe-lane");
  });
});
describe("round 4 completion fences", () => {
  // #3178: quiet outcomes survive absence; only the exact root/session can recover them.
  it.each([true, false])("Astra 3: quiet resident settlement survives a live supervisor; returning owner notices=%s", async notifyOnComplete => {
    const h = harness();
    const a = h.client("A", 100, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, notifyOnComplete: false, maxConcurrent: 2, budgetUsd: 0, sessionExport: false, nice: 19 },
      piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
    });
    const cfg = a.client.options.config;
    fs.mkdirSync(cfg.residencyRoot, { recursive: true });
    fs.writeFileSync(path.join(cfg.residencyRoot, "config.json"), JSON.stringify(cfg));
    const host = new ResidentHost(cfg);
    // Public durable spawns belong to Main, never the hidden resident executor.
    const callerMesh = new MeshStore(cfg.meshRoot, cfg.mesh.maxEventBytes, cfg.mesh.maxReadEvents);
    const callerParticipants = new ParticipantDirectory(callerMesh, {
      enabled: true, hostId: cfg.rootId, rootId: cfg.rootId,
      identity: { id: cfg.rootId, name: "main", kind: "main", sessionId: cfg.sessionId },
    });
    callerParticipants.registerSource(() => [h.participant("A", 100, { local: true })]);
    try {
      await callerParticipants.start();
      await host.start(); h.setLive([h.participant("A", 100)]);
      const launchClient = new ResidencyClient({ config: cfg, mesh: callerMesh, participants: callerParticipants,
        mainAgent: { local: false } as any }); clients.push(launchClient);
      const long = await launchClient.spawnAgent({ task: "HANG", transport: "process", residency: "durable" }, AbortSignal.timeout(5_000));
      const child = await launchClient.spawnAgent({ task: "LARGE_RESULT", transport: "process", residency: "durable" }, AbortSignal.timeout(5_000));
      const settled = await host.agents.wait(child.id, { timeoutMs: 5_000, deferConsumption() {} });
      expect(settled).toMatchObject({ status: "completed", text: "x".repeat(100_000) });
      expect(fs.existsSync(residentResultPath(cfg.residencyRoot, child.id))).toBe(true);
      // Model the worker's attempt publication too: this is NOT sufficient while its supervisor lives.
      saveWorkerCompletion(path.join(host.agents.runDirectory(child.id)!, "status.json"), settled);
      const provisionalRun = path.join(h.root, "provisional"); fs.mkdirSync(provisionalRun);
      fs.writeFileSync(path.join(provisionalRun, "completion-recipient.json"), JSON.stringify({
        meshRoot: h.meshRoot, recipient: h.recipient, supervisor: { pid: process.pid },
      }));
      saveWorkerCompletion(path.join(provisionalRun, "status.json"), { ...h.result, status: "failed", text: "PROVISIONAL_ONLY" });
      expect(host.agents.status(long.id).status).toBe("running");
      expect(h.mesh.listAll(residentDeliveryPrefix(cfg.rootId))).toHaveLength(0);
      expect(a.completed).not.toHaveBeenCalled(); expect(a.sendMessage).not.toHaveBeenCalled();
      // A disappears, but its resident host remains alive and owns the unrelated long run.
      await callerParticipants.close();
      h.setLive([h.participant("A", 100), h.participant("other-role", 300, { role: "other-lane" }),
        h.participant("other-cwd", 300, { cwd: path.join(h.root, "other") })]);
      expect(pendingCompletions(h.meshRoot, h.root).map(value => value.result.id)).toEqual([child.id]);
      const b = h.client("A", 100, { agents: { ...DEFAULT_FABRIC_CONFIG.agents, notifyOnComplete } }); b.client.start();
      await waitFor(() => b.client.listAgents().some(value => value.id === child.id && "text" in value));
      expect(b.client.listAgents().find(value => value.id === child.id)).toMatchObject({
        text: settled.text, completionDelivery: { status: "undelivered", addressedTo: "A" },
      });
      expect(b.client.listAgents().some(value => value.id === h.result.id)).toBe(false);
      for (const extra of [{ role: "other-lane" }, { cwd: path.join(h.root, "other") }]) {
        const observer = h.client("observer", 300, extra); observer.client.start();
        const summary = observer.client.statusAgent(child.id);
        expect(summary).toMatchObject({ id: child.id, status: "completed" });
        for (const key of ["text", "error", "value", "task", "logFile", "sessionFile"]) expect(summary).not.toHaveProperty(key);
        expect(await observer.client.waitAgent(child.id)).not.toHaveProperty("text");
        expect(completionConsumed(h.meshRoot, child.id)).toBe(false);
        expect(observer.completed).not.toHaveBeenCalled();
      }
      expect(await b.client.waitAgent(child.id)).toMatchObject({ status: "completed", text: settled.text });
      expect(completionConsumed(h.meshRoot, child.id)).toBe(true);
      b.turn(); expect(b.sendMessage).not.toHaveBeenCalled(); // explicit wait retracts any inbox notice
      if (!notifyOnComplete) expect(b.completed).not.toHaveBeenCalled();
      expect(host.agents.status(long.id).status).toBe("running");
      await b.client.close(); h.setLive([h.participant("C", 400)]);
      const c = h.client("C", 400); c.client.start();
      await waitFor(() => h.mesh.listAll("residency/completion-claims/").length === 0); c.turn();
      expect(c.client.listAgents()).toHaveLength(0);
      expect(c.completed).not.toHaveBeenCalled(); expect(c.sendMessage).not.toHaveBeenCalled();
      expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(0);
    } finally {
      try { await host.close(); } finally { await callerParticipants.close(); }
    }
  }, 15_000);

  // #3178: replay-fence faults still block the exact owner; foreign roots never reach admission.
  it.each(legacyFenceFaults)("F4/journal: unknown legacy fence (%s) blocks claims, bodies and replacement receipts repeatedly", async fault => {
    if (fault === "dangling" && process.platform === "win32") return; // symlink privilege is not portable
    const h = harness(); saveCompletion(h.meshRoot, h.recipient, h.result);
    const damaged = damageLegacyFence(h, fault);
    h.setLive([h.participant("A", 100)]); const delivered = vi.fn();
    const journal = new CompletionJournal(h.meshRoot, { ...h.recipient, rootId: "session:A", sessionId: "A", startedAt: 100 }, h.participants, h.mesh, delivered);
    for (let index = 0; index < 3; index++) {
      await expect(journal.drain()).rejects.toThrow(/legacy.*replay fence/i);
      expect(() => journal.result(h.result.id)).toThrow(/legacy.*replay fence/i);
      expect(() => journal.acknowledge(h.result.id, true)).toThrow(/legacy.*replay fence/i);
      expect(completionConsumed(h.meshRoot, h.result.id)).toBe(false);
      expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(0);
    }
    expect(delivered).not.toHaveBeenCalled();
    expect(fs.readdirSync(path.join(h.meshRoot, "agent-completions")).filter(file => file.endsWith(".json"))).toHaveLength(1);
    damaged.repair(); await journal.drain(); expect(delivered).not.toHaveBeenCalled();
  });

  it.each(legacyFenceFaults)("F4/import: unknown old-Main fence (%s) retains source and reports one diagnostic until repaired", async fault => {
    if (fault === "dangling" && process.platform === "win32") return;
    const h = harness(); const source = await h.seedResident(); const damaged = damageLegacyFence(h, fault);
    const warning = vi.spyOn(console, "warn").mockImplementation(() => {});
    h.setLive([h.participant("B", 200)]); const b = h.client("B", 200); b.client.start();
    await waitFor(() => warning.mock.calls.length > 0); await new Promise(resolve => setTimeout(resolve, 80));
    expect(warning).toHaveBeenCalledOnce(); expect(String(warning.mock.calls[0]![0])).toMatch(/remains pending.*legacy.*replay fence/i);
    expect(h.mesh.get(source)).toBeDefined(); expect(b.completed).not.toHaveBeenCalled();
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(0);
    expect(completionConsumed(h.meshRoot, h.result.id)).toBe(false);
    expect(fs.existsSync(path.join(h.meshRoot, "agent-completions"))).toBe(false);
    expect(fs.existsSync(residentResultPath(residentRoot(h.meshRoot, h.recipient.rootId), h.result.id))).toBe(true);
    damaged.repair(); await waitFor(() => completionConsumed(h.meshRoot, h.result.id));
    await waitFor(() => !h.mesh.get(source)); b.turn(); expect(b.completed).not.toHaveBeenCalled(); expect(b.sendMessage).not.toHaveBeenCalled();
  });

  it("F4: legacy consumption must be a positive finite number, or proven absent", () => {
    const h = harness(); saveCompletion(h.meshRoot, h.recipient, h.result);
    const file = path.join(residentRoot(h.meshRoot, h.recipient.rootId), "agents", `${h.result.id}.json`);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    for (const completionConsumedAt of [null, 0, -1, "3"]) {
      fs.writeFileSync(file, JSON.stringify({ rootId: h.recipient.rootId, id: h.result.id, completionConsumedAt }));
      expect(() => pendingCompletions(h.meshRoot, h.root)).toThrow(/legacy.*replay fence/i);
    }
    fs.writeFileSync(file, `{"rootId":"${h.recipient.rootId}","id":"${h.result.id}","completionConsumedAt":1e400}`);
    expect(() => pendingCompletions(h.meshRoot, h.root)).toThrow(/legacy.*replay fence/i);
    fs.writeFileSync(file, JSON.stringify({ rootId: h.recipient.rootId, id: h.result.id }));
    expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(1);
    fs.unlinkSync(file); expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(1);
  });

  it.skipIf(process.platform === "win32")("F5/receipt: explicit and recovery retries require barriers before archiving the outcome", async () => {
    const h = harness(); h.setLive([h.participant("B", 200)]);
    let delivered!: () => void;
    const journal = new CompletionJournal(h.meshRoot, { ...h.recipient, rootId: "session:B", sessionId: "B", startedAt: 200 }, h.participants, h.mesh, (_result, callback) => { delivered = callback; });
    journal.save(h.result); await journal.drain();
    const dir = path.join(h.meshRoot, "agent-completions", "receipts"); fs.mkdirSync(dir);
    const target = path.join(dir, fs.readdirSync(path.join(h.meshRoot, "agent-completions")).find(file => file.endsWith(".json"))!);
    const fault = postRenameFault(target);
    expect(() => delivered()).toThrow(/post-rename directory barrier failed/);
    expect(fault.renamed).toBe(true); const original = fs.readFileSync(target, "utf8");
    const retry = new CompletionJournal(h.meshRoot, journal.recipient, h.participants, h.mesh, () => {});
    for (const barrier of ["directory", "file", "directory"] as const) {
      fault.barrier = barrier;
      expect(() => receiptBeforeArchive(h.meshRoot, h.result.id, "C")).toThrow(/post-rename .* barrier failed/);
      expect(completionConsumed(h.meshRoot, h.result.id)).toBe(true); // Scan reads do not claim durability.
      await expect(retry.drain(false)).rejects.toThrow(/post-rename .* barrier failed/);
      expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(1);
      expect(fs.existsSync(path.join(path.dirname(dir), path.basename(target)))).toBe(true);
      expect(fs.existsSync(path.join(path.dirname(dir), "archive", path.basename(target)))).toBe(false);
      expect(fs.readFileSync(target, "utf8")).toBe(original);
    }
    fault.barrier = "none"; fault.fileSyncs = 0; fault.directorySyncs = 0;
    receiptBeforeArchive(h.meshRoot, h.result.id, "C");
    expect(fault.fileSyncs).toBeGreaterThan(0); expect(fault.directorySyncs).toBeGreaterThan(0);
    expect(JSON.parse(fs.readFileSync(target, "utf8"))).toMatchObject({ sessionId: "B" });
    expect(fs.readFileSync(target, "utf8")).toBe(original);
    await retry.drain(false); expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(0);
  });

  it.skipIf(process.platform === "win32")("F5/envelope: a failed post-rename save keeps its candidate until retry barriers pass", () => {
    const h = harness(); const run = path.join(h.root, "run"); fs.mkdirSync(run);
    fs.writeFileSync(path.join(run, "completion-recipient.json"), JSON.stringify({ meshRoot: h.meshRoot, recipient: h.recipient, supervisor: { pid: process.pid } }));
    saveWorkerCompletion(path.join(run, "status.json"), h.result);
    const dir = path.join(h.meshRoot, "agent-completions");
    const candidate = path.join(dir, "attempts", fs.readdirSync(path.join(dir, "attempts"))[0]!);
    const target = path.join(dir, path.basename(candidate)); const fault = postRenameFault(target);
    expect(() => saveCompletion(h.meshRoot, h.recipient, h.result)).toThrow(/post-rename directory barrier failed/);
    expect(fault.renamed).toBe(true); const original = fs.readFileSync(target, "utf8");
    for (const barrier of ["directory", "file", "directory"] as const) {
      fault.barrier = barrier;
      expect(() => saveCompletion(h.meshRoot, h.recipient, h.result)).toThrow(/post-rename .* barrier failed/);
      expect(fs.existsSync(candidate)).toBe(true); expect(fs.readFileSync(target, "utf8")).toBe(original);
    }
    fault.barrier = "none"; fault.fileSyncs = 0; fault.directorySyncs = 0;
    saveCompletion(h.meshRoot, h.recipient, h.result);
    expect(fault.fileSyncs).toBeGreaterThan(0); expect(fault.directorySyncs).toBeGreaterThan(0);
    expect(fs.existsSync(candidate)).toBe(false); expect(fs.readFileSync(target, "utf8")).toBe(original);
  });

  it.skipIf(process.platform === "win32")("F5/manager: settlement-save retry cannot collect a worker source after failed rename barriers", async () => {
    const h = harness(); const a = h.client("A", 100); const manager = managerFor(h, a);
    const dir = path.join(h.meshRoot, "agent-completions"); fs.mkdirSync(dir, { recursive: true });
    const rename = fs.renameSync; const sync = fs.fsyncSync; let target: string | undefined; let barrier: "directory" | "file" | "none" = "directory";
    vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
      rename(source, destination);
      if (path.dirname(String(destination)) === dir) target = String(destination);
    });
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (target && ((barrier === "directory" && fs.fstatSync(fd).isDirectory()) || (barrier === "file" && fs.fstatSync(fd).isFile()))) throw new Error("post-rename barrier failed");
      sync(fd);
    });
    const result = await manager.run({ task: "LARGE_RESULT", transport: "process" });
    const run = manager.runDirectory(result.id)!; const worker = fs.readFileSync(path.join(run, "status.json"), "utf8");
    expect(target).toBeDefined(); expect(result.warnings?.join(" ")).toMatch(/save failed.*retained/i);
    for (const required of ["directory", "file", "directory"] as const) {
      barrier = required;
      await expect(manager.cleanup(result.id)).rejects.toThrow(/Terminal result save failed.*post-rename/);
      expect(fs.readFileSync(path.join(run, "status.json"), "utf8")).toBe(worker);
    }
    barrier = "none"; await manager.cleanup(result.id);
    expect(fs.existsSync(run)).toBe(false);
    expect(JSON.parse(fs.readFileSync(target!, "utf8")).result).toMatchObject({ id: result.id, text: "x".repeat(100_000), value: { output: "x".repeat(100_000) } });
  }, 15_000);
});

describe("round 3 completion fences", () => {
  // #3178: attempt fences remain; only the exact owner receives final settlement.
  it.each([
    { mode: "credential startup", scope: "session" }, { mode: "recoverable stop", scope: "session" },
    { mode: "credential startup", scope: "durable" }, { mode: "recoverable stop", scope: "durable" },
  ] as const)("F2/provider $scope: $mode status cannot consume an attempt", async ({ mode, scope }) => {
    const h = harness(); const a = h.client("A", 100);
    const consumed = vi.fn((id: string) => a.client.acknowledgeCompletion(id, true));
    const manager = new AgentManager(h.root, { ...DEFAULT_FABRIC_CONFIG.agents, nice: 19, sessionExport: false }, {
      workerPath: "unused", runRoot: path.join(scope === "durable" ? a.client.options.config.residencyRoot : h.root, "runs"),
      meshRoot: h.meshRoot, completionRecipient: h.recipient,
      onSettled: result => {
        if (scope === "durable") {
          const file = residentResultPath(a.client.options.config.residencyRoot, result.id);
          fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, JSON.stringify(result));
        }
        a.client.enqueueCompletion(result);
      }, onResultConsumed: consumed,
    }); managers.push(manager);
    // A separate Main manager forces the real residency-first status path for durable runs.
    const provider = providerFor(h, a.client, scope === "durable" ? managerFor(h) : manager);
    let launches = 0; let statusFile = ""; let final: AgentRunResult | undefined;
    const stoppedAttempts: number[] = [];
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const attempt = ++launches;
      let exited = false;
      const args = new Map<string, string>();
      for (let i = 0; i < request.workerArguments.length; i += 2) args.set(request.workerArguments[i]!, request.workerArguments[i + 1]!);
      statusFile = args.get("--status-file")!;
      final = { ...h.result, id: request.id, startedAt: Date.now(), updatedAt: Date.now(), text: "FINAL_SUCCESS_R3" };
      fs.writeFileSync(statusFile, JSON.stringify({ ...final, status: "running", finishedAt: undefined }));
      // Each fake worker keeps its own lifetime. Relaunch must not make the
      // exited predecessor alive again; terminal publication is not exit proof.
      return { kind: "process", isAlive: async () => !exited && attempt > 1,
        stop: async () => { stoppedAttempts.push(attempt); exited = true; } };
    });
    const handle = await manager.spawn({ task: "retry me", transport: "process", residency: scope, nice: 19 });
    let metadataFile: string | undefined;
    if (scope === "durable") {
      const dir = path.join(a.client.options.config.residencyRoot, "agents"); fs.mkdirSync(dir, { recursive: true });
      metadataFile = path.join(dir, `${handle.id}.json`);
      fs.writeFileSync(metadataFile, JSON.stringify({ format: 1, rootId: h.recipient.rootId, id: handle.id,
        runDirectory: manager.runDirectory(handle.id), handle, createdAt: 1, updatedAt: 2 }));
    }
    const attempt = { ...final!, status: mode === "credential startup" ? "failed" as const : "stopped" as const,
      turns: mode === "credential startup" ? 0 : 2, text: "PROVISIONAL_R3",
      error: mode === "credential startup" ? "No API key found for openai-codex" : "Agent stopped" };
    fs.writeFileSync(statusFile, JSON.stringify(attempt)); saveWorkerCompletion(statusFile, attempt);
    await waitFor(() => manager.status(handle.id).status === attempt.status);
    expect(await provider.invoke("status", { id: handle.id }, invocation)).toMatchObject({ status: attempt.status });
    // Also enforce the owning-manager boundary, not just this provider's caller-side guard.
    manager.markForeground(handle.id);
    expect(consumed).not.toHaveBeenCalled(); expect(completionConsumed(h.meshRoot, handle.id)).toBe(false);
    if (metadataFile) expect(JSON.parse(fs.readFileSync(metadataFile, "utf8"))).not.toHaveProperty("completionConsumedAt");
    await waitFor(() => launches === 2);
    fs.writeFileSync(statusFile, JSON.stringify(final)); saveWorkerCompletion(statusFile, final!);
    await manager.join(handle.id);
    expect(stoppedAttempts).toEqual([1, 2]);
    expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(1);
    h.setLive([h.participant("B", 200)]); const b = h.client("A", 100); b.client.start();
    await waitFor(() => b.completed.mock.calls.length === 1); b.turn();
    expect(b.sendMessage.mock.calls[0]![0].content).toContain("FINAL_SUCCESS_R3");
    expect(completionConsumed(h.meshRoot, handle.id)).toBe(true);
    await b.client.close(); h.setLive([h.participant("C", 300)]); const c = h.client("C", 300); c.client.start();
    await new Promise(resolve => setTimeout(resolve, 60)); expect(c.completed).not.toHaveBeenCalled();
  }, 15_000);

  // #3178: settled durable outcomes go only to their bound owner.
  it.each(["failed", "stopped"] as const)("F2/provider durable: %s status cannot receipt a live supervisor's attempt", async status => {
    const h = harness(); const a = h.client("A", 100); const cfg = a.client.options.config;
    const runDirectory = path.join(cfg.residencyRoot, "runs", h.result.id);
    fs.mkdirSync(runDirectory, { recursive: true }); fs.mkdirSync(path.join(cfg.residencyRoot, "agents"), { recursive: true });
    const metadataFile = path.join(cfg.residencyRoot, "agents", `${h.result.id}.json`);
    fs.writeFileSync(metadataFile, JSON.stringify({ format: 1, rootId: cfg.rootId, id: h.result.id, runDirectory,
      handle: { ...h.result, status: "running", residency: "durable" }, createdAt: 1, updatedAt: 2 }));
    fs.writeFileSync(path.join(runDirectory, "completion-recipient.json"), JSON.stringify({ meshRoot: h.meshRoot, recipient: h.recipient, supervisor: { pid: process.pid } }));
    const attempt = { ...h.result, status, text: "PROVISIONAL_R3" };
    const statusFile = path.join(runDirectory, "status.json"); fs.writeFileSync(statusFile, JSON.stringify(attempt)); saveWorkerCompletion(statusFile, attempt);
    const provider = providerFor(h, a.client);
    expect(await provider.invoke("status", { id: h.result.id }, invocation)).toMatchObject({ status });
    a.client.acknowledgeCompletion(h.result.id);
    a.client.acknowledgeCompletion(h.result.id, true); // ordinary-owner fallback cannot certify a resident attempt
    expect(JSON.parse(fs.readFileSync(metadataFile, "utf8"))).not.toHaveProperty("completionConsumedAt");
    expect(completionConsumed(h.meshRoot, h.result.id)).toBe(false);
    fs.writeFileSync(statusFile, JSON.stringify(h.result)); a.client.enqueueCompletion(h.result);
    h.setLive([h.participant("B", 200)]); const b = h.client("A", 100); b.client.start();
    await waitFor(() => b.completed.mock.calls.length === 1); b.turn();
    expect(completionConsumed(h.meshRoot, h.result.id)).toBe(true);
    await b.client.close(); h.setLive([h.participant("C", 300)]); const c = h.client("C", 300); c.client.start();
    await new Promise(resolve => setTimeout(resolve, 60)); expect(c.completed).not.toHaveBeenCalled();
  });

  // #3178: a damaged fence blocks the exact owner, not another principal's drain.
  it.skipIf(process.platform === "win32")("F4: ENOENT through a dangling existing receipt is not proven absence", async () => {
    const h = harness(); saveCompletion(h.meshRoot, h.recipient, h.result); receiptBeforeArchive(h.meshRoot, h.result.id, "B");
    const dir = path.join(h.meshRoot, "agent-completions", "receipts"); const file = path.join(dir, fs.readdirSync(dir)[0]!);
    fs.unlinkSync(file); fs.symlinkSync(path.join(h.root, "missing-receipt"), file);
    h.setLive([h.participant("A", 100)]); const delivered = vi.fn();
    const journal = new CompletionJournal(h.meshRoot, { ...h.recipient, rootId: "session:A", sessionId: "A", startedAt: 100 }, h.participants, h.mesh, delivered);
    await expect(journal.drain()).rejects.toThrow(/replay fence/);
    expect(() => receiptBeforeArchive(h.meshRoot, h.result.id, "C")).toThrow(/replay fence/);
    expect(() => journal.result(h.result.id)).toThrow(/replay fence/);
    expect(fs.lstatSync(file).isSymbolicLink()).toBe(true); expect(delivered).not.toHaveBeenCalled();
  });

  // #3178: storage diagnostics apply to the exact owner, not an inferred successor.
  it("F4/client: a blocked replay stays pending and surfaces a deduplicated storage diagnostic", async () => {
    const h = harness(); saveCompletion(h.meshRoot, h.recipient, h.result); receiptBeforeArchive(h.meshRoot, h.result.id, "B");
    const dir = path.join(h.meshRoot, "agent-completions", "receipts"); const file = path.join(dir, fs.readdirSync(dir)[0]!);
    fs.writeFileSync(file, "not a receipt"); const warning = vi.spyOn(console, "warn").mockImplementation(() => {});
    h.setLive([h.participant("A", 100)]); const c = h.client("A", 100); c.client.start();
    await waitFor(() => warning.mock.calls.length > 0); await new Promise(resolve => setTimeout(resolve, 80));
    expect(warning).toHaveBeenCalledOnce(); expect(String(warning.mock.calls[0]![0])).toMatch(/remains pending.*replay fence/);
    expect(c.completed).not.toHaveBeenCalled(); expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(0);
    expect(fs.readFileSync(file, "utf8")).toBe("not a receipt");
    expect(fs.readdirSync(path.join(h.meshRoot, "agent-completions")).filter(name => name.endsWith(".json"))).toHaveLength(1);
  });

  // #3178: malformed fences still fail closed for the exact owner.
  it.each(["malformed", "identity mismatch", "unreadable"] as const)("F4: an existing %s fence fails closed repeatedly and is never overwritten", async fault => {
    const h = harness(); saveCompletion(h.meshRoot, h.recipient, h.result); receiptBeforeArchive(h.meshRoot, h.result.id, "B");
    const dir = path.join(h.meshRoot, "agent-completions", "receipts"); const file = path.join(dir, fs.readdirSync(dir)[0]!);
    if (fault === "malformed") fs.writeFileSync(file, "{torn");
    if (fault === "identity mismatch") fs.writeFileSync(file, JSON.stringify({ id: "c".repeat(32), sessionId: "B", consumedAt: 3 }));
    const original = fs.readFileSync(file, "utf8");
    if (fault === "unreadable") {
      const read = fs.readFileSync;
      vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...args: any[]) => {
        if (String(target) === file) throw Object.assign(new Error("receipt inaccessible"), { code: "EACCES" });
        return (read as any)(target, ...args);
      }) as typeof fs.readFileSync);
      const asyncRead = fs.promises.readFile.bind(fs.promises);
      vi.spyOn(fs.promises, "readFile").mockImplementation((async (target: any, ...args: any[]) => {
        if (String(target) === file) throw Object.assign(new Error("receipt inaccessible"), { code: "EACCES" });
        return asyncRead(target, ...args);
      }) as typeof fs.promises.readFile);
    }
    h.setLive([h.participant("A", 100)]); const delivered = vi.fn();
    const journal = new CompletionJournal(h.meshRoot, { ...h.recipient, rootId: "session:A", sessionId: "A", startedAt: 100 }, h.participants, h.mesh, delivered);
    for (let i = 0; i < 3; i++) {
      await expect(journal.drain()).rejects.toThrow(/replay fence/i);
      expect(() => receiptBeforeArchive(h.meshRoot, h.result.id, "C")).toThrow(/replay fence/i);
      expect(() => journal.result(h.result.id)).toThrow(/replay fence/i);
    }
    expect(delivered).not.toHaveBeenCalled(); expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(0);
    vi.restoreAllMocks(); expect(fs.readFileSync(file, "utf8")).toBe(original);
    expect(fs.readdirSync(path.join(h.meshRoot, "agent-completions")).filter(name => name.endsWith(".json"))).toHaveLength(1);
    // Repairing the consumed fence restores certainty, not replay permission.
    fs.writeFileSync(file, JSON.stringify({ id: h.result.id, sessionId: "B", consumedAt: 3 }));
    await journal.drain(); expect(delivered).not.toHaveBeenCalled();
  });
});

describe("Astra round 3 legacy retirement ordering", () => {
  const claimKey = (id: string) => `residency/completion-claims/${createHash("sha256").update(id).digest("hex")}`;
  const bodyPath = (h: ReturnType<typeof harness>, id: string) => path.join(h.meshRoot, "agent-completions", `${claimKey(id).split("/").at(-1)}.json`);
  const address = (h: ReturnType<typeof harness>, session: string, startedAt: number) => ({ ...h.recipient, rootId: `session:${session}`, sessionId: session, startedAt });
  const legacyClaim = async (h: ReturnType<typeof harness>, id: string) => {
    await h.mesh.put({ key: claimKey(id), ifVersion: 0,
      identity: { id: h.recipient.rootId, name: "main", kind: "main" },
      value: { rootId: h.recipient.rootId, sessionId: h.recipient.sessionId } });
    const claim = h.mesh.get(claimKey(id), { fresh: true })!;
    expect(claim.value).not.toHaveProperty("recipient");
    return claim;
  };

  // #3178: legacy CAS retries retain evidence; retirement retries remain exact-root/session.
  it.each([false, true])("pre-commit lock timeout retains durably archived legacy evidence; fresh journal=%s reclaims capacity", async fresh => {
    const h = harness(true); const enqueue = vi.fn();
    const b = new CompletionJournal(h.meshRoot, h.recipient, h.participants, h.mesh, enqueue);
    const retry = fresh ? new CompletionJournal(h.meshRoot, h.recipient, h.participants, h.mesh, enqueue) : b;
    for (let index = 1; index <= 12; index++) {
      const result = { ...h.result, id: index.toString(16).padStart(32, "0") };
      saveCompletion(h.meshRoot, h.recipient, result);
      const claim = await legacyClaim(h, result.id);
      receiptBeforeArchive(h.meshRoot, result.id, "B");
      h.setLive([h.participant("B", 200)]);
      const fault = vi.spyOn(h.mesh, "delete").mockRejectedValue(new Error("MeshStore lock timeout before commit"));
      await b.drain();
      expect(fault).toHaveBeenCalledExactlyOnceWith({ key: claim.key, ifVersion: claim.version });
      expect(h.mesh.get(claim.key, { fresh: true })).toEqual(claim);
      expect(fs.existsSync(bodyPath(h, result.id))).toBe(false);
      expect(fs.existsSync(path.join(path.dirname(bodyPath(h, result.id)), "archive", path.basename(bodyPath(h, result.id))))).toBe(true);
      expect(b.result(result.id)).toMatchObject({ id: result.id, text: result.text });
      expect(completionConsumed(h.meshRoot, result.id)).toBe(true);
      fault.mockRestore();
      h.setLive([h.participant("A", 100)]);
      await retry.drain();
      expect(h.mesh.get(claim.key, { fresh: true })).toBeUndefined();
      expect(fs.existsSync(bodyPath(h, result.id))).toBe(false);
      expect(enqueue).not.toHaveBeenCalled();
      // New admission must succeed, not merely leave a consumed result fenced.
      const next = { ...h.result, id: (100 + index).toString(16).padStart(32, "0") };
      retry.save(next); await retry.drain(false);
      expect(h.mesh.get(claimKey(next.id), { fresh: true })?.value).toMatchObject({ sessionId: "A" });
      receiptBeforeArchive(h.meshRoot, next.id, "A"); await retry.drain(false);
      expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(0);
      expect(fs.statSync(path.join(h.meshRoot, "state.json")).size).toBeLessThan(4096);
    }
  }, 60_000); // Twelve real-storage fsync/CAS cycles; do not impose a latency budget on this recovery proof.

  // #3178: only the returning exact owner may confirm an interrupted archive before CAS.
  it("a crash after archive rename retains the legacy claim until the same Main confirms its namespace", async () => {
    const h = harness(true); saveCompletion(h.meshRoot, h.recipient, h.result);
    const claim = await legacyClaim(h, h.result.id); receiptBeforeArchive(h.meshRoot, h.result.id, "B");
    h.setLive([h.participant("B", 200)]);
    const body = bodyPath(h, h.result.id); const rename = fs.promises.rename;
    const crash = vi.spyOn(fs.promises, "rename").mockImplementation(async (source, target) => {
      await rename(source, target);
      if (String(source) === body) {
        expect(h.mesh.get(claim.key, { fresh: true })).toEqual(claim);
        throw new Error("stop after archive rename, before directory barriers");
      }
    });
    const b = new CompletionJournal(h.meshRoot, h.recipient, h.participants, h.mesh, vi.fn());
    await expect(b.drain()).rejects.toThrow("stop after archive rename, before directory barriers");
    expect(h.mesh.get(claim.key, { fresh: true })).toEqual(claim); expect(fs.existsSync(body)).toBe(false);
    expect(fs.existsSync(path.join(path.dirname(body), "archive-pending", path.basename(body)))).toBe(true);
    crash.mockRestore();
    const receipt = path.join(path.dirname(body), "receipts", path.basename(body));
    const read = vi.spyOn(fs.promises, "readFile"); const enqueue = vi.fn(); h.setLive([h.participant("C", 300)]);
    await new CompletionJournal(h.meshRoot, h.recipient, h.participants, h.mesh, enqueue).drain();
    expect(read.mock.calls.some(([file]) => String(file) === receipt)).toBe(true);
    expect(fs.existsSync(body)).toBe(false); expect(fs.existsSync(path.join(path.dirname(body), "archive", path.basename(body)))).toBe(true);
    expect(fs.existsSync(receipt)).toBe(true); expect(enqueue).not.toHaveBeenCalled();
  });

  // #3178: process-death CAS coverage remains; cleanup recovery uses the same root/session.
  it.each(["acknowledge", "forget", "delivery"] as const)("%s: current owner process death before/after legacy CAS preserves recoverable evidence", async consumption => {
    for (const stage of ["before delete", "after delete"] as const) {
      const h = harness(true); saveCompletion(h.meshRoot, h.recipient, h.result);
      const claim = await legacyClaim(h, h.result.id); const body = bodyPath(h, h.result.id);
      const archived = path.join(path.dirname(body), "archive", path.basename(body));
      // Exit the actual consuming process at the mesh-delete boundary. Its async
      // retirement and same-session memory cannot help the successor recover.
      const script = `
        import fs from 'node:fs';
        import {CompletionJournal, completionConsumed} from ${JSON.stringify(path.resolve("src/agents/completion-journal.ts"))};
        import {MeshStore} from ${JSON.stringify(path.resolve("src/mesh/store.ts"))};
        const mesh = new MeshStore(${JSON.stringify(h.meshRoot)}, 1024, 100, {maxStateBytes:4096, maxStateTombstones:2});
        const remove = mesh.delete.bind(mesh);
        mesh.delete = async input => {
          if (input.key !== ${JSON.stringify(claim.key)} || input.ifVersion !== ${claim.version}) throw new Error('missing versioned CAS');
          if (!fs.existsSync(${JSON.stringify(archived)}) || !completionConsumed(${JSON.stringify(h.meshRoot)}, ${JSON.stringify(h.result.id)})) throw new Error('legacy evidence lost before delete');
          if (${JSON.stringify(stage)} === 'after delete') await remove(input);
          if (!fs.existsSync(${JSON.stringify(archived)})) throw new Error('envelope unlinked before process stop');
          console.log('stopped ${consumption}: ${stage}'); process.exit(0);
        };
        const journal = new CompletionJournal(${JSON.stringify(h.meshRoot)}, ${JSON.stringify(h.recipient)}, {list:()=>[]}, mesh, (_result, delivered)=>delivered());
        if (${JSON.stringify(consumption)} === 'delivery') await journal.drain();
        else journal[${JSON.stringify(consumption)}](${JSON.stringify(h.result.id)});
        setTimeout(()=>{throw new Error('retirement checkpoint not reached')}, 3000);
      `;
      expect(execFileSync("bun", ["--eval", script], { encoding: "utf8", timeout: 15_000 })).toContain(`stopped ${consumption}: ${stage}`);
      expect(fs.existsSync(body)).toBe(false); expect(fs.existsSync(archived)).toBe(true); expect(completionConsumed(h.meshRoot, h.result.id)).toBe(true);
      expect(h.mesh.get(claim.key, { fresh: true })).toEqual(stage === "before delete" ? claim : undefined);
      const enqueue = vi.fn(); h.setLive([h.participant("B", 200)]);
      const b = new CompletionJournal(h.meshRoot, h.recipient, h.participants, h.mesh, enqueue);
      await b.drain();
      expect(h.mesh.get(claim.key, { fresh: true })).toBeUndefined(); expect(fs.existsSync(body)).toBe(false);
      expect(completionConsumed(h.meshRoot, h.result.id)).toBe(true); expect(enqueue).not.toHaveBeenCalled();
      const next = { ...h.result, id: "b".repeat(32) }; b.save(next); await b.drain(false);
      expect(h.mesh.get(claimKey(next.id), { fresh: true })?.value).toMatchObject({ sessionId: "A" });
      receiptBeforeArchive(h.meshRoot, next.id, "B"); await b.drain(false);
    }
  });
});

describe("round 2 completion security", () => {
  const legacyClaimKey = (id: string) => `residency/completion-claims/${createHash("sha256").update(id).digest("hex")}`;
  it.each(["cwd", "role"] as const)("F1: unrelated %s gets bounded list/status/wait, original live or dead", async lane => {
    for (const originalLive of [true, false]) {
      const h = harness(); saveCompletion(h.meshRoot, h.recipient, { ...h.result, task: "PRIVATE_TASK", error: "PRIVATE_ERROR", value: { secret: "PRIVATE_STRUCTURED" }, stderr: "PRIVATE_STDERR" });
      const extra = lane === "cwd" ? { cwd: path.join(h.root, "other") } : { role: "unrelated" };
      h.setLive([...(originalLive ? [h.participant("A", 100)] : []), h.participant("X", 200, extra)]);
      const x = h.client("X", 200, extra);
      for (const value of [x.client.listAgents()[0], x.client.statusAgent(h.result.id), await x.client.waitAgent(h.result.id, AbortSignal.timeout(500))]) {
        expect(value).toMatchObject({ id: h.result.id, status: "completed", completionDelivery: { status: "undelivered", addressedTo: "A" } });
        for (const field of ["text", "task", "error", "value", "stderr", "usage", "nestedAgents", "logFile", "sessionId", "attachCommand"]) expect(value).not.toHaveProperty(field);
      }
      expect(completionConsumed(h.meshRoot, h.result.id)).toBe(false);
    }
  });

  it.each(["cwd", "role"] as const)("F1/provider: unrelated %s cannot read or consume original live/dead results", async lane => {
    for (const originalLive of [true, false]) {
      const h = harness(); saveCompletion(h.meshRoot, h.recipient, { ...h.result, task: "PRIVATE_TASK", error: "PRIVATE_ERROR", value: { secret: "PRIVATE_VALUE" } });
      const extra = lane === "cwd" ? { cwd: path.join(h.root, "other") } : { role: "unrelated" };
      h.setLive([...(originalLive ? [h.participant("A", 100)] : []), h.participant("X", 200, extra)]);
      const x = h.client("X", 200, extra); const provider = providerFor(h, x.client);
      const list = await provider.invoke("list", { scope: "local" }, invocation) as unknown[];
      for (const value of [list[0], await provider.invoke("status", { id: h.result.id }, invocation), await provider.invoke("wait", { id: h.result.id }, invocation)]) {
        expect(value).toMatchObject({ id: h.result.id, completionDelivery: { status: "undelivered", addressedTo: "A" } });
        for (const field of ["text", "task", "error", "value", "usage"]) expect(value).not.toHaveProperty(field);
      }
      expect(completionConsumed(h.meshRoot, h.result.id)).toBe(false);
    }
  });

  it("F1: an unrelated observer cannot forge a receipt before the owner publishes its completion", () => {
    const h = harness(); const x = h.client("X", 200, { role: "unrelated" });
    x.client.acknowledgeCompletion(h.result.id);
    expect(completionConsumed(h.meshRoot, h.result.id)).toBe(false);
    saveCompletion(h.meshRoot, h.recipient, h.result);
    expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(1);
    expect(x.client.statusAgent(h.result.id)).not.toHaveProperty("text");
  });

  // #3178: a claim/receipt from old inferred succession is not an adoption record.
  it("same-lane different root cannot read or acknowledge even a forged successor claim/receipt", async () => {
    const h = harness(); saveCompletion(h.meshRoot, h.recipient, h.result); h.setLive([h.participant("B", 200)]);
    const key = legacyClaimKey(h.result.id);
    await h.mesh.put({ key, ifVersion: 0, identity: { id: "session:B", name: "main", kind: "main" },
      value: { rootId: "session:B", sessionId: "B", recipient: { ...h.recipient, rootId: "session:B", sessionId: "B" } } });
    const b = new CompletionJournal(h.meshRoot, { ...h.recipient, rootId: "session:B", sessionId: "B", startedAt: 200 }, h.participants, h.mesh, vi.fn());
    await b.drain(); expect(b.enqueue).not.toHaveBeenCalled();
    expect(b.result(h.result.id)).not.toHaveProperty("text");
    expect(b.acknowledge(h.result.id)).toBe(false); b.forget(h.result.id);
    expect(completionConsumed(h.meshRoot, h.result.id)).toBe(false);
    // The bound owner can repair a foreign pre-fix claim, even while B is live.
    const a = new CompletionJournal(h.meshRoot, h.recipient, h.participants, h.mesh, (_value, delivered) => delivered());
    await a.drain(); expect(completionConsumed(h.meshRoot, h.result.id)).toBe(true);
    await a.drain();
    const another = { ...h.result, id: "b".repeat(32) };
    saveCompletion(h.meshRoot, h.recipient, another); receiptBeforeArchive(h.meshRoot, another.id, "B");
    // A legacy B receipt is not explicit adoption either.
    expect(b.result(another.id)).not.toHaveProperty("text"); expect(b.acknowledge(another.id)).toBe(false);
  });

  // #3178: lease expiry cannot grant cleanup authority to the same-lane B.
  it("legacy predecessor envelope survives lease expiry until the exact owner returns", async () => {
    const h = harness(); saveCompletion(h.meshRoot, h.recipient, h.result); receiptBeforeArchive(h.meshRoot, h.result.id, "A");
    const key = legacyClaimKey(h.result.id);
    await h.mesh.put({ key, ifVersion: 0, identity: { id: "session:A", name: "main", kind: "main" }, value: { rootId: "session:A", sessionId: "A" } });
    const b = new CompletionJournal(h.meshRoot, { ...h.recipient, rootId: "session:B", sessionId: "B", startedAt: 200 }, h.participants, h.mesh, vi.fn());
    const body = path.join(h.meshRoot, "agent-completions", `${createHash("sha256").update(h.result.id).digest("hex")}.json`);
    for (const live of [[h.participant("A", 100), h.participant("B", 200)], [h.participant("B", 200)]]) {
      h.setLive(live); await b.drain(false); expect(h.mesh.get(key)).toBeDefined(); expect(fs.existsSync(body)).toBe(true);
    }
    await new CompletionJournal(h.meshRoot, h.recipient, h.participants, h.mesh, vi.fn()).drain(false);
    expect(h.mesh.get(key)).toBeUndefined(); expect(fs.existsSync(body)).toBe(false);
  });

  // #3178: even a legacy B receipt is not a completion-adoption record for C.
  it("legacy A-addressed envelope with B-owned receipt is not retired by C", async () => {
    const h = harness(); saveCompletion(h.meshRoot, h.recipient, h.result); receiptBeforeArchive(h.meshRoot, h.result.id, "B");
    const key = legacyClaimKey(h.result.id);
    await h.mesh.put({ key, ifVersion: 0, identity: { id: "session:B", name: "main", kind: "main" }, value: { rootId: "session:B", sessionId: "B" } });
    h.setLive([h.participant("C", 300)]);
    const c = new CompletionJournal(h.meshRoot, { ...h.recipient, rootId: "session:C", sessionId: "C", startedAt: 300 }, h.participants, h.mesh, vi.fn());
    await c.drain(false); expect(h.mesh.get(key)).toBeDefined();
    expect(fs.existsSync(path.join(h.meshRoot, "agent-completions", `${createHash("sha256").update(h.result.id).digest("hex")}.json`))).toBe(true);
    expect(c.result(h.result.id)).not.toHaveProperty("text"); expect(c.acknowledge(h.result.id)).toBe(false);
  });

  // #3178: promotion/attempt fencing is unchanged; final delivery remains exact-owner only.
  it.each(["failed", "stopped"] as const)("F2: retryable %s worker attempt is not a settled completion", async status => {
    const h = harness(); const run = path.join(h.root, "run"); fs.mkdirSync(run);
    const manifest = { meshRoot: h.meshRoot, recipient: h.recipient, supervisor: { pid: process.pid } };
    fs.writeFileSync(path.join(run, "completion-recipient.json"), JSON.stringify(manifest));
    const attempt = { ...h.result, status, text: "attempt only", error: status === "failed" ? "No API key found for openai-codex" : "worker stopped", turns: status === "failed" ? 0 : 2 };
    saveWorkerCompletion(path.join(run, "status.json"), attempt);
    h.setLive([h.participant("B", 200)]); const b = h.client("A", 100); b.client.start();
    await new Promise(resolve => setTimeout(resolve, 60));
    expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(0);
    expect(b.completed).not.toHaveBeenCalled(); expect(b.client.hasAgent(h.result.id)).toBe(false);
    saveCompletion(h.meshRoot, h.recipient, h.result);
    await waitFor(() => b.completed.mock.calls.length === 1);
    expect(await b.client.waitAgent(h.result.id)).toMatchObject({ status: "completed", text: h.result.text });
    b.turn(); expect(b.sendMessage).not.toHaveBeenCalled(); // wait retracted the final inbox item
    saveWorkerCompletion(path.join(run, "status.json"), attempt);
    expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(0);
  });

  it("F2: a legacy manifest without supervisor identity cannot certify an orphan attempt as settled", async () => {
    const h = harness(); const run = path.join(h.root, "run"); fs.mkdirSync(run);
    fs.writeFileSync(path.join(run, "completion-recipient.json"), JSON.stringify({ meshRoot: h.meshRoot, recipient: h.recipient }));
    saveWorkerCompletion(path.join(run, "status.json"), { ...h.result, status: "failed", text: "UNCOMMITTED_LEGACY_ATTEMPT" });
    h.setLive([h.participant("B", 200)]); const b = h.client("B", 200); b.client.start();
    expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(0);
    await new Promise(resolve => setTimeout(resolve, 60)); expect(b.completed).not.toHaveBeenCalled();
  });

  // #3178: promotion/attempt fencing is unchanged; final delivery remains exact-owner only.
  it("F2: promotes an orphan attempt only after its supervisor can no longer retry", async () => {
    const h = harness(); const run = path.join(h.root, "run"); fs.mkdirSync(run);
    fs.writeFileSync(path.join(run, "completion-recipient.json"), JSON.stringify({ meshRoot: h.meshRoot, recipient: h.recipient, supervisor: { pid: 2147483647 } }));
    saveWorkerCompletion(path.join(run, "status.json"), h.result);
    h.setLive([h.participant("B", 200)]); const b = h.client("A", 100); b.client.start();
    await waitFor(() => b.completed.mock.calls.length === 1); b.turn();
    expect(b.sendMessage).toHaveBeenCalledOnce();
  });

  it.each(["failed", "stopped"] as const)("F2/residency: wait must not return a retryable %s attempt from a live supervisor", async status => {
    const h = harness(); const a = h.client("A", 100); const cfg = a.client.options.config;
    const runDirectory = path.join(cfg.residencyRoot, "runs", h.result.id);
    fs.mkdirSync(runDirectory, { recursive: true }); fs.mkdirSync(path.join(cfg.residencyRoot, "agents"), { recursive: true });
    const handle = { id: h.result.id, name: h.result.name, cwd: h.root, status: "running", runner: "pi", transport: "process", residency: "durable" };
    fs.writeFileSync(path.join(cfg.residencyRoot, "agents", `${h.result.id}.json`), JSON.stringify({ format: 1, rootId: cfg.rootId, id: h.result.id, runDirectory, handle, createdAt: 1, updatedAt: 2 }));
    fs.writeFileSync(path.join(runDirectory, "completion-recipient.json"), JSON.stringify({ meshRoot: h.meshRoot, recipient: h.recipient, supervisor: { pid: process.pid } }));
    const statusFile = path.join(runDirectory, "status.json"); const attempt = { ...h.result, status, text: "NOT_FINAL" };
    fs.writeFileSync(statusFile, JSON.stringify(attempt)); saveWorkerCompletion(statusFile, attempt);
    await expect(a.client.waitAgent(h.result.id, AbortSignal.timeout(80))).rejects.toThrow("aborted");
    expect(completionConsumed(h.meshRoot, h.result.id)).toBe(false);
    fs.writeFileSync(statusFile, JSON.stringify(h.result)); a.client.enqueueCompletion(h.result);
    expect(await a.client.waitAgent(h.result.id, AbortSignal.timeout(500))).toMatchObject({ status: "completed", text: h.result.text });
  });

  // #3178: final retry settlement goes to the exact owner, not a same-lane Main.
  it.each(["credential startup", "recoverable stop"] as const)("F2/manager: %s retries with journal enabled; owner receives final success only", async mode => {
    const h = harness(); const owner = h.client("A", 100); const manager = managerFor(h, owner);
    let launches = 0; let statusFile = ""; let final: AgentRunResult | undefined;
    const stoppedAttempts: number[] = [];
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      const attempt = ++launches; let exited = false; const args = new Map<string, string>();
      for (let i = 0; i < request.workerArguments.length; i += 2) args.set(request.workerArguments[i]!, request.workerArguments[i + 1]!);
      statusFile = args.get("--status-file")!;
      const now = Date.now();
      const result = { ...h.result, id: request.id, task: "retry me", startedAt: now, updatedAt: now, finishedAt: now,
        ...(launches === 1 ? { status: mode === "credential startup" ? "failed" as const : "stopped" as const,
          turns: mode === "credential startup" ? 0 : 2, text: "PROVISIONAL_ATTEMPT",
          error: mode === "credential startup" ? "No API key found for openai-codex" : "Agent stopped" } : { text: "FINAL_RECOVERED_SUCCESS" }) };
      if (launches === 1) {
        fs.writeFileSync(statusFile, JSON.stringify(result)); saveWorkerCompletion(statusFile, result);
      } else {
        final = result; fs.writeFileSync(statusFile, JSON.stringify({ ...result, status: "running", finishedAt: undefined }));
      }
      // Each fake worker keeps its own lifetime. Relaunch must not make the
      // exited predecessor alive again; terminal publication is not exit proof.
      return { kind: "process", isAlive: async () => !exited && attempt > 1,
        stop: async () => { stoppedAttempts.push(attempt); exited = true; } };
    });
    h.setLive([h.participant("B", 200)]); const b = h.client("A", 100); b.client.start();
    const handle = await manager.spawn({ task: "retry me", transport: "process", nice: 19 });
    expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(0);
    await waitFor(() => launches === 2); expect(b.completed).not.toHaveBeenCalled();
    // Simulate the supervisor dying while its already-launched retry is still running.
    // The superseded attempt must have been withdrawn, not promoted as the final result.
    const orphaned = vi.spyOn(processIdentity, "residentProcessAlive").mockReturnValue(false);
    expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(0);
    await new Promise(resolve => setTimeout(resolve, 60)); expect(b.completed).not.toHaveBeenCalled();
    orphaned.mockRestore();
    const manifest = JSON.parse(fs.readFileSync(path.join(manager.runDirectory(handle.id)!, "completion-recipient.json"), "utf8"));
    expect(manifest.supervisor).toMatchObject({ pid: process.pid });
    fs.writeFileSync(statusFile, JSON.stringify(final)); saveWorkerCompletion(statusFile, final!);
    const settled = await manager.wait(handle.id); expect(settled).toMatchObject({ status: "completed", text: "FINAL_RECOVERED_SUCCESS" });
    expect(stoppedAttempts).toEqual([1, 2]);
    await waitFor(() => b.completed.mock.calls.length === 1);
    b.turn();
    expect(await b.client.waitAgent(handle.id)).toMatchObject({ status: "completed", text: "FINAL_RECOVERED_SUCCESS" });
    expect(completionConsumed(h.meshRoot, handle.id)).toBe(true);
    await b.client.close(); h.setLive([h.participant("C", 300)]);
    const c = h.client("C", 300); c.client.start();
    await new Promise(resolve => setTimeout(resolve, 60)); expect(c.completed).not.toHaveBeenCalled();
    expect(b.sendMessage).toHaveBeenCalledOnce();
    expect(b.sendMessage.mock.calls[0]![0].content).toContain("FINAL_RECOVERED_SUCCESS");
    expect(b.sendMessage.mock.calls[0]![0].content).not.toContain("PROVISIONAL_ATTEMPT");
  }, 15_000);

  it("Astra 2/provider: ordinary spawn -> wait -> log -> cleanup retains local ownership with journaling", async () => {
    const h = harness(); const a = h.client("A", 100); const manager = managerFor(h, a); const provider = providerFor(h, a.client, manager);
    const handle = await provider.invoke("spawn", { task: "ordinary task", transport: "process", nice: 19 }, invocation) as AgentHandleInfo;
    await vi.waitFor(() => expect(manager.status(handle.id).status).toBe("completed"), { timeout: 5000 });
    const result = await provider.invoke("wait", { id: handle.id }, invocation);
    expect(result).toMatchObject({ status: "completed" });
    expect(a.client.hasAgent(handle.id)).toBe(true);
    expect(await provider.invoke("log", { id: handle.id }, invocation)).toMatchObject({ id: handle.id, events: expect.any(Array) });
    expect(await provider.invoke("cleanup", { id: handle.id }, invocation)).toMatchObject({ cleaned: true });
    expect(a.client.ownsAgent(handle.id)).toBe(false);
    expect(fs.existsSync(path.join(h.root, "runs", handle.id))).toBe(false);
  }, 10_000);

  it("Astra 2/provider: a journal-only ordinary wait retracts an already admitted notification", async () => {
    const h = harness(); h.setLive([h.participant("A", 100)]); const a = h.client("A", 100);
    const provider = providerFor(h, a.client); a.client.enqueueCompletion(h.result); a.client.start();
    await waitFor(() => a.completed.mock.calls.length === 1);
    expect(await provider.invoke("wait", { id: h.result.id }, invocation)).toMatchObject({ text: h.result.text });
    a.turn(); expect(a.sendMessage).not.toHaveBeenCalled(); expect(completionConsumed(h.meshRoot, h.result.id)).toBe(true);
    await waitFor(() => h.mesh.listAll("residency/completion-claims/").length === 0);
  });

  // #3178: same-lane roots cannot claim an outcome or clean its predecessor's run.
  it("Astra 2/provider: a same-lane different root cannot claim or clean up its predecessor's run", async () => {
    const h = harness(); saveCompletion(h.meshRoot, h.recipient, h.result); h.setLive([h.participant("B", 200)]);
    const b = h.client("B", 200); b.client.start(); await new Promise(resolve => setTimeout(resolve, 80));
    expect(b.completed).not.toHaveBeenCalled();
    const provider = providerFor(h, b.client);
    await expect(provider.invoke("cleanup", { id: h.result.id }, invocation)).rejects.toThrow(/Unknown .*agent/);
    expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(1);
  });

  // #3178: lease absence is not cleanup authority; only exact A can retire its claim.
  it.each([true, false])("Astra P1-1: B retains A's consumed claim until exact A returns after a crash, body present=%s", async bodyPresent => {
    const h = harness(); h.setLive([h.participant("A", 100)]);
    const a = new CompletionJournal(h.meshRoot, h.recipient, h.participants, h.mesh, vi.fn());
    a.save(h.result); await a.drain(false);
    const [claim] = h.mesh.listAll("residency/completion-claims/");
    receiptBeforeArchive(h.meshRoot, h.result.id, "A"); // Crash before retirement.
    const body = path.join(h.meshRoot, "agent-completions", `${claim!.key.split("/").at(-1)}.json`);
    if (!bodyPresent) fs.unlinkSync(body);
    h.setLive([h.participant("B", 200)]);
    const enqueue = vi.fn();
    const b = new CompletionJournal(h.meshRoot, { ...h.recipient, rootId: "session:B", sessionId: "B", startedAt: 200 }, h.participants, h.mesh, enqueue);
    await b.drain();
    expect(h.mesh.listAll("residency/completion-claims/")).toEqual([claim]);
    expect(fs.existsSync(body)).toBe(bodyPresent);
    await new CompletionJournal(h.meshRoot, h.recipient, h.participants, h.mesh, enqueue).drain();
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(0);
    expect(fs.existsSync(body)).toBe(false); expect(enqueue).not.toHaveBeenCalled();
    expect(completionConsumed(h.meshRoot, h.result.id)).toBe(true);
  });

  it.each(["live predecessor", "foreign cwd", "foreign role", "forged owner", "unregistered successor"]) (
    "Astra P1-1: %s cannot authorize a predecessor receipt read or retirement", async fault => {
      const h = harness(); h.setLive([h.participant("A", 100)]);
      const a = new CompletionJournal(h.meshRoot, h.recipient, h.participants, h.mesh, vi.fn());
      a.save(h.result); await a.drain(false); receiptBeforeArchive(h.meshRoot, h.result.id, "A");
      const [claim] = h.mesh.listAll("residency/completion-claims/");
      const file = path.join(h.meshRoot, "agent-completions", "receipts", `${claim!.key.split("/").at(-1)}.json`);
      fs.unlinkSync(path.join(path.dirname(path.dirname(file)), path.basename(file)));
      const recipient = { ...h.recipient, rootId: "session:B", sessionId: "B", startedAt: 200,
        ...(fault === "foreign cwd" ? { cwd: path.dirname(h.root) } : {}),
        ...(fault === "foreign role" ? { role: "other" } : {}) };
      h.setLive(fault === "unregistered successor" ? [] : [h.participant("B", 200), ...(fault === "live predecessor" ? [h.participant("A", 100)] : [])]);
      if (fault === "forged owner") await h.mesh.put({ key: claim!.key, ifVersion: claim!.version,
        identity: { id: "forger", name: "forger", kind: "main" }, value: claim!.value });
      const read = vi.spyOn(fs, "readFileSync");
      await new CompletionJournal(h.meshRoot, recipient, h.participants, h.mesh, vi.fn()).drain(false);
      expect(read.mock.calls.some(([target]) => String(target) === file)).toBe(false);
      expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(1);
    });

  it("Astra P1-2: one async receipt confirmation precedes archive and claim retirement", async () => {
    const h = harness(); h.setLive([h.participant("A", 100)]);
    const journal = new CompletionJournal(h.meshRoot, h.recipient, h.participants, h.mesh, vi.fn());
    journal.save(h.result); await journal.drain(false); receiptBeforeArchive(h.meshRoot, h.result.id, "A");
    const [claim] = h.mesh.listAll("residency/completion-claims/");
    const body = path.join(h.meshRoot, "agent-completions", `${claim!.key.split("/").at(-1)}.json`);
    const receipt = path.join(path.dirname(body), "receipts", path.basename(body));
    const open = fs.promises.open; let confirmations = 0;
    const opened = vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      if (String(args[0]) === receipt && ++confirmations > 1) throw new Error("second barrier unavailable");
      return open(...args);
    });
    const sync = vi.spyOn(fs, "fsyncSync"); await journal.drain(false);
    expect(confirmations).toBe(1); expect(sync).not.toHaveBeenCalled();
    expect(fs.existsSync(body)).toBe(false); expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(0);
    opened.mockRestore(); await journal.drain(false); // Bodyless idle pass owes no barrier at all.
    expect(confirmations).toBe(1); expect(sync).not.toHaveBeenCalled();
  });

  // #3178: bounded reclamation is same-root crash recovery, not new-principal succession.
  it("Astra P1-1: reduced-capacity crash rounds retain Main identity and reclaim bodyless owner claims", async () => {
    const h = harness(true);
    const recipient = (_index: number) => h.recipient;
    for (let index = 1; index <= 30; index++) {
      h.setLive([h.participant(`round-${index}`, 100 + index)]);
      const address = recipient(index);
      const journal = new CompletionJournal(h.meshRoot, address, h.participants, h.mesh, vi.fn());
      await journal.drain(false); // The same root/session cleans its preceding crash-left claim.
      expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(0);
      const result = { ...h.result, id: index.toString(16).padStart(32, "0") };
      journal.save(result); await journal.drain(false);
      receiptBeforeArchive(h.meshRoot, result.id, address.sessionId);
      const [claim] = h.mesh.listAll("residency/completion-claims/");
      fs.unlinkSync(path.join(h.meshRoot, "agent-completions", `${claim!.key.split("/").at(-1)}.json`));
      // Crash: leave the receipt and claim, not a callback or in-memory journal.
    }
    h.setLive([h.participant("round-31", 131)]);
    await new CompletionJournal(h.meshRoot, recipient(31), h.participants, h.mesh, vi.fn()).drain();
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(0);
    expect(fs.statSync(path.join(h.meshRoot, "state.json")).size).toBeLessThan(4096);
  });

  it.skipIf(process.platform === "win32").each(["namespace alias", "new child", "replaced child"])(
    "Astra P1-2: explicit and recovery retries owe namespace barriers after %s", async mutation => {
      const h = harness(); h.setLive([h.participant("A", 100)]);
      const journal = new CompletionJournal(h.meshRoot, h.recipient, h.participants, h.mesh, vi.fn());
      journal.save(h.result); await journal.drain(false);
      const [warmClaim] = h.mesh.listAll("residency/completion-claims/");
      const warmReceipt = path.join(h.meshRoot, "agent-completions", "receipts", `${warmClaim!.key.split("/").at(-1)}.json`);
      fs.mkdirSync(path.dirname(warmReceipt), { recursive: true });
      // Import, rather than consumeCompletion: the former implementation's endpoint
      // cache must miss here so the async ancestor cache is actually warmed.
      fs.writeFileSync(warmReceipt, JSON.stringify({ id: h.result.id, sessionId: "A", consumedAt: 1 }));
      await journal.drain(false);
      const result = { ...h.result, id: "b".repeat(32) }; journal.save(result); await journal.drain(false);
      const [claim] = h.mesh.listAll("residency/completion-claims/");
      const dir = path.join(h.meshRoot, "agent-completions");
      const body = path.join(dir, `${claim!.key.split("/").at(-1)}.json`);
      const receipt = path.join(dir, "receipts", path.basename(body));
      const value = { id: result.id, sessionId: "A", consumedAt: 1 };
      if (mutation === "namespace alias") {
        receiptBeforeArchive(h.meshRoot, result.id, "A"); // Cache this unchanged receipt inode.
        const before = fs.statSync(receipt);
        fs.renameSync(path.dirname(receipt), path.join(h.meshRoot, "moved-receipts"));
        fs.symlinkSync("../moved-receipts", path.dirname(receipt), "dir");
        const after = fs.statSync(receipt);
        for (const field of ["dev", "ino", "size", "mtimeMs", "ctimeMs"] as const) expect(after[field]).toBe(before[field]);
      } else {
        // The parent meshRoot is unchanged, but its child directory entry now owes a barrier.
        const moved = path.join(h.meshRoot, "old-completions"); fs.renameSync(dir, moved);
        fs.mkdirSync(path.dirname(receipt), { recursive: true });
        fs.copyFileSync(path.join(moved, path.basename(body)), body);
        if (mutation === "new child") {
          fs.mkdirSync(path.join(h.meshRoot, "new-tree", "receipts"), { recursive: true });
          fs.rmdirSync(path.dirname(receipt));
          fs.symlinkSync("../new-tree/receipts", path.dirname(receipt), "dir");
        }
        fs.writeFileSync(receipt, JSON.stringify(value)); // Imported visible receipt, not yet confirmed.
      }
      const open = fs.promises.open;
      const failed = vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
        const handle = await open(...args);
        if (String(args[0]) === h.meshRoot) vi.spyOn(handle, "sync").mockRejectedValue(new Error("owed parent barrier failed"));
        return handle;
      });
      const nativeSync = fs.fsyncSync;
      const sync = vi.spyOn(fs, "fsyncSync");
      expect(pendingCompletions(h.meshRoot, h.root)).toEqual([]);
      expect(sync).not.toHaveBeenCalled(); expect(failed).not.toHaveBeenCalled();
      expect(fs.existsSync(body)).toBe(true); expect(h.mesh.listAll("residency/completion-claims/")).toEqual([claim]);
      expect(sync).not.toHaveBeenCalled(); expect(journal.enqueue).not.toHaveBeenCalled();
      // Explicit consumption is a synchronous commit API, but may not use the old
      // endpoint-only cache either: the identical parent barrier still has to pass.
      const parent = fs.statSync(h.meshRoot);
      sync.mockImplementation(fd => {
        const opened = fs.fstatSync(fd);
        if (opened.dev === parent.dev && opened.ino === parent.ino) throw new Error("owed parent barrier failed");
        nativeSync(fd);
      });
      expect(() => receiptBeforeArchive(h.meshRoot, result.id, "A")).toThrow("owed parent barrier failed");
      expect(() => journal.acknowledge(result.id)).toThrow("owed parent barrier failed");
      expect(fs.existsSync(body)).toBe(true); expect(h.mesh.listAll("residency/completion-claims/")).toEqual([claim]);
      sync.mockRestore(); const plainSync = vi.spyOn(fs, "fsyncSync");
      await expect(journal.drain(false)).rejects.toThrow("owed parent barrier failed");
      expect(fs.existsSync(body)).toBe(true); expect(h.mesh.listAll("residency/completion-claims/")).toEqual([claim]);
      failed.mockRestore(); await journal.drain(false);
      expect(plainSync).not.toHaveBeenCalled();
      expect(fs.existsSync(body)).toBe(false); expect(fs.existsSync(path.join(dir, "archive", path.basename(body)))).toBe(true);
      expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(0); plainSync.mockRestore();
    });

  it("F3: repeated receipts retire claims within reduced mesh capacity and prevent successor replay", async () => {
    const h = harness(true); h.setLive([h.participant("A", 100)]);
    const journal = new CompletionJournal(h.meshRoot, h.recipient, h.participants, h.mesh, (_result, delivered) => delivered());
    for (let index = 1; index <= 30; index++) {
      const result = { ...h.result, id: index.toString(16).padStart(32, "0") }; journal.save(result); await journal.drain();
      await waitFor(() => h.mesh.listAll("residency/completion-claims/").length === 0);
      expect(completionConsumed(h.meshRoot, result.id)).toBe(true);
    }
    h.setLive([h.participant("B", 200)]); const b = h.client("B", 200); b.client.start();
    await new Promise(resolve => setTimeout(resolve, 60)); expect(b.completed).not.toHaveBeenCalled();
    expect(fs.statSync(path.join(h.meshRoot, "state.json")).size).toBeLessThan(4096);
  });

  it("F3: retirement CAS cannot erase a replacement claim version", async () => {
    const h = harness(); h.setLive([h.participant("A", 100)]);
    const journal = new CompletionJournal(h.meshRoot, h.recipient, h.participants, h.mesh, () => {});
    journal.save(h.result); await journal.drain(); receiptBeforeArchive(h.meshRoot, h.result.id, "A");
    const claim = h.mesh.listAll("residency/completion-claims/")[0]!;
    const remove = h.mesh.delete; let replaced = false;
    const spy = vi.spyOn(h.mesh, "delete").mockImplementation(async input => {
      if (!replaced) {
        replaced = true;
        await h.mesh.put({ key: input.key, ifVersion: claim.version,
          identity: { id: "session:A", name: "main", kind: "main" },
          value: { rootId: "session:A", sessionId: "A", replacement: true } });
      }
      return remove.call(h.mesh, input);
    });
    await journal.drain(false);
    expect(h.mesh.get(claim.key)?.value).toMatchObject({ replacement: true });
    expect(h.mesh.get(claim.key)?.version).toBeGreaterThan(claim.version);
    spy.mockRestore(); await journal.drain(false);
    expect(h.mesh.get(claim.key)).toBeUndefined(); expect(completionConsumed(h.meshRoot, h.result.id)).toBe(true);
  });

  it.each([true, false])("F3: reconciles a crash after receipt before retirement, preserving pending claims (notify=%s)", async notify => {
    const h = harness(); h.setLive([h.participant("A", 100)]);
    const journal = new CompletionJournal(h.meshRoot, h.recipient, h.participants, h.mesh, () => {});
    journal.save(h.result); await journal.drain();
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(1);
    receiptBeforeArchive(h.meshRoot, h.result.id, "A");
    const fresh = new CompletionJournal(h.meshRoot, h.recipient, h.participants, h.mesh, () => {}); await fresh.drain(notify);
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(0);
    const pending = { ...h.result, id: "b".repeat(32) }; fresh.save(pending); await fresh.drain();
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(1);
    fresh.forget(pending.id); await fresh.drain();
    expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(0);
  });
});
describe("exact Main completion recipients", () => {
  it("a live owner with a simulated 20 s stale lease receives once on return; same-lane B receives nothing", async () => {
    const h = harness();
    const directory = (session: string) => {
      const value = new ParticipantDirectory(h.mesh, { enabled: true, hostId: `session:${session}`, rootId: `session:${session}`,
        identity: { id: `session:${session}`, name: "main", kind: "main", sessionId: session } });
      value.registerSource(() => [h.participant(session, session === "A" ? 100 : 200, { local: true })]);
      return value;
    };
    const aDirectory = directory("A"), bDirectory = directory("B");
    try {
      await aDirectory.start(); await bDirectory.start();
      expect(bDirectory.list({ scope: "project", kinds: ["root"], fresh: true }).some(root => root.id === h.recipient.rootId)).toBe(true);
      let now = Date.now() + 20_000;
      expect(bDirectory.list({ scope: "project", kinds: ["root"], fresh: true }, now).some(root => root.id === h.recipient.rootId)).toBe(false);
      expect(bDirectory.list({ scope: "project", kinds: ["root"], includeStale: true, fresh: true }, now).find(root => root.id === h.recipient.rootId)?.stale).toBe(true);
      const participants = { list: (options: Parameters<ParticipantDirectory["list"]>[0]) => bDirectory.list(options, now) } as unknown as FabricParticipantSource;
      saveCompletion(h.meshRoot, h.recipient, h.result);
      const b = new CompletionJournal(h.meshRoot, { ...h.recipient, rootId: "session:B", sessionId: "B", startedAt: 200 }, participants, h.mesh, vi.fn());
      await b.drain(); await b.drain();
      expect(b.enqueue).not.toHaveBeenCalled(); expect(b.result(h.result.id)).not.toHaveProperty("text");
      expect(b.acknowledge(h.result.id)).toBe(false); b.forget(h.result.id);
      expect(completionConsumed(h.meshRoot, h.result.id)).toBe(false);
      expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(1);
      expect(h.mesh.listAll("residency/completion-claims/")).toHaveLength(0);
      now = Date.now(); await aDirectory.refresh();
      let delivered!: () => void;
      const enqueue = vi.fn((_result: AgentRunResult, callback: () => void) => { delivered = callback; });
      const a = new CompletionJournal(h.meshRoot, h.recipient, participants, h.mesh, enqueue);
      await a.drain(); await a.drain(); expect(enqueue).toHaveBeenCalledOnce();
      expect(enqueue.mock.calls[0]![0]).toMatchObject({ text: h.result.text }); delivered();
      await a.drain(); await b.drain();
      expect(enqueue).toHaveBeenCalledOnce(); expect(b.enqueue).not.toHaveBeenCalled();
      expect(completionConsumed(h.meshRoot, h.result.id)).toBe(true);
    } finally { await bDirectory.close(); await aDirectory.close(); }
  });
  // #3178: dead Main without a completion adoption record means retained, not redirected.
  it("retains an authenticated dead-owner resident result until exact A returns, then delivers once", async () => {
    const h = harness(); const key = await h.seedResident(); h.setLive([h.participant("B", 200)]);
    const b = h.client("B", 200); b.client.start();
    await waitFor(() => b.client.listAgents().length === 1); b.turn();
    expect(b.client.statusAgent(h.result.id)).not.toHaveProperty("text");
    b.client.acknowledgeCompletion(h.result.id);
    expect(b.completed).not.toHaveBeenCalled(); expect(b.sendMessage).not.toHaveBeenCalled();
    expect(completionConsumed(h.meshRoot, h.result.id)).toBe(false); expect(h.mesh.get(key)).toBeDefined();
    expect(pendingCompletions(h.meshRoot, h.root)).toMatchObject([{ recipient: { rootId: "session:A", sessionId: "A" }, result: { text: h.result.text } }]);
    expect(JSON.parse(fs.readFileSync(residentResultPath(residentRoot(h.meshRoot, h.recipient.rootId), h.result.id), "utf8"))).toMatchObject({ text: h.result.text });
    h.setLive([h.participant("A", 100), h.participant("B", 200)]);
    const a = h.client("A", 100); a.client.start(); await waitFor(() => a.completed.mock.calls.length === 1);
    a.turn(); a.turn(); await waitFor(() => completionConsumed(h.meshRoot, h.result.id));
    expect(a.sendMessage).toHaveBeenCalledOnce(); expect(a.sendMessage.mock.calls[0]![0].content).toContain(h.result.text);
    expect(a.sendMessage.mock.calls[0]![0].content).not.toContain("re-delivered from dead Main");
    await waitFor(() => !h.mesh.get(key));
    b.turn(); expect(b.completed).not.toHaveBeenCalled(); expect(b.sendMessage).not.toHaveBeenCalled();
  });

  it("keeps no-successor results pending and visible in list/status, even with notifications disabled", async () => {
    const h = harness(); const key = await h.seedResident(); h.setLive([h.participant("observer", 200, { role: "other-lane" })]);
    const b = h.client("observer", 200, { role: "other-lane", agents: { ...DEFAULT_FABRIC_CONFIG.agents, notifyOnComplete: false } }); b.client.start();
    await waitFor(() => b.client.listAgents().length === 1);
    expect(b.client.statusAgent(h.result.id)).toMatchObject({ completionDelivery: { status: "undelivered", addressedTo: "A" } });
    b.client.acknowledgeCompletion(h.result.id); b.turn();
    expect(b.sendMessage).not.toHaveBeenCalled(); expect(completionConsumed(h.meshRoot, h.result.id)).toBe(false);
    expect(h.mesh.get(key)).toBeDefined(); expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(1);
  });

  // #3178: losing an inbox host does not transfer an unconsumed exact-root outcome.
  it("retains an unconsumed inbox after owner death; only the same session can recover", async () => {
    const h = harness(); h.setLive([h.participant("A", 100)]); const a = h.client("A", 100);
    a.client.enqueueCompletion(h.result); a.client.start(); await waitFor(() => a.completed.mock.calls.length === 1);
    await a.client.close(); a.inbox.close();
    for (const [session, started] of [["B", 200], ["C", 300]] as const) {
      h.setLive([h.participant(session, started)]); const x = h.client(session, started); x.client.start();
      await new Promise(resolve => setTimeout(resolve, 80)); x.turn();
      expect(x.completed).not.toHaveBeenCalled(); expect(x.sendMessage).not.toHaveBeenCalled();
      expect(x.client.statusAgent(h.result.id)).not.toHaveProperty("text");
      expect(completionConsumed(h.meshRoot, h.result.id)).toBe(false);
    }
    expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(1);
    const resumed = h.client("A", 100); resumed.client.start();
    await waitFor(() => resumed.completed.mock.calls.length === 1); resumed.turn(); resumed.turn();
    expect(resumed.sendMessage).toHaveBeenCalledOnce(); expect(completionConsumed(h.meshRoot, h.result.id)).toBe(true);
  });

  // #3178: a newer same-lane root cannot duplicate the exact owner's admitted notice.
  it("concurrently live same-lane root cannot duplicate an exact-owner inbox result", async () => {
    const h = harness(); saveCompletion(h.meshRoot, h.recipient, h.result);
    h.setLive([h.participant("A", 100)]); const a = h.client("A", 100); a.client.start();
    await waitFor(() => a.completed.mock.calls.length === 1);
    h.setLive([h.participant("A", 100), h.participant("B", 200)]); const b = h.client("B", 200); b.client.start();
    await new Promise(resolve => setTimeout(resolve, 100)); b.turn(); expect(b.completed).not.toHaveBeenCalled();
    expect(a.client.statusAgent(h.result.id)).toMatchObject({ text: h.result.text });
    expect(b.client.statusAgent(h.result.id)).not.toHaveProperty("text"); a.turn(); a.turn();
    expect(a.sendMessage).toHaveBeenCalledOnce(); await new Promise(resolve => setTimeout(resolve, 60));
    expect(b.sendMessage).not.toHaveBeenCalled(); expect(completionConsumed(h.meshRoot, h.result.id)).toBe(true);
  });

  // #3178: BOTH identity fields are required; name, role, age and missing lease grant no authority.
  it.each([{ rootId: "session:B", sessionId: "B" }, { rootId: "session:A", sessionId: "B" }, { rootId: "session:B", sessionId: "A" }])("requires exact root/session, not $rootId / $sessionId", async identity => {
    const h = harness(); saveCompletion(h.meshRoot, h.recipient, h.result);
    h.setLive([h.participant("B", 200)]);
    const x = new CompletionJournal(h.meshRoot, { ...h.recipient, ...identity, startedAt: 200 }, h.participants, h.mesh, vi.fn());
    await x.drain(); expect(x.enqueue).not.toHaveBeenCalled(); expect(x.result(h.result.id)).not.toHaveProperty("text");
    expect(x.acknowledge(h.result.id)).toBe(false); x.forget(h.result.id);
    expect(completionConsumed(h.meshRoot, h.result.id)).toBe(false); expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(1);
  });

  // #3178: orphan promotion keeps its host-owned exact return address, never the observing lane.
  it("journals an orphan worker's terminal outcome and retains it for its exact root", async () => {
    const h = harness(); const run = path.join(h.root, "run"); fs.mkdirSync(run);
    fs.writeFileSync(path.join(run, "completion-recipient.json"), JSON.stringify({ meshRoot: h.meshRoot, recipient: h.recipient, supervisor: { pid: 2147483647 } }));
    fs.writeFileSync(path.join(run, "status.json"), JSON.stringify(h.result));
    saveWorkerCompletion(path.join(run, "status.json"), h.result);
    h.setLive([h.participant("B", 200)]); const b = h.client("B", 200); b.client.start();
    await waitFor(() => b.client.listAgents().length === 1); b.turn();
    expect(b.sendMessage).not.toHaveBeenCalled(); expect(completionConsumed(h.meshRoot, h.result.id)).toBe(false);
    expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(1);
    expect(JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8"))).toMatchObject({ text: h.result.text });
    const a = h.client("A", 100); a.client.start(); await waitFor(() => a.completed.mock.calls.length === 1); a.turn();
    expect(a.sendMessage).toHaveBeenCalledOnce(); expect(await a.client.waitAgent(h.result.id)).toMatchObject({ text: h.result.text });
  });

  it("honors a legacy metadata receipt even when its host journaled the outcome", async () => {
    const h = harness(); saveCompletion(h.meshRoot, h.recipient, h.result);
    const agents = path.join(residentRoot(h.meshRoot, h.recipient.rootId), "agents"); fs.mkdirSync(agents, { recursive: true });
    fs.writeFileSync(path.join(agents, `${h.result.id}.json`), JSON.stringify({ rootId: h.recipient.rootId, id: h.result.id, completionConsumedAt: 3 }));
    h.setLive([h.participant("B", 200)]); const b = h.client("B", 200); b.client.start();
    await new Promise(resolve => setTimeout(resolve, 60)); expect(b.completed).not.toHaveBeenCalled();
    expect((b.client.statusAgent(h.result.id) as AgentRunResult).completionDelivery).toBeUndefined();
  });

  it("persists late-wait receipt before notification publication, suppressing successor delivery", async () => {
    const h = harness(); const a = h.client("A", 100);
    // The owning manager can consume despite an onSettled journal-save fault.
    a.client.acknowledgeCompletion(h.result.id, true);
    saveCompletion(h.meshRoot, h.recipient, h.result); h.setLive([h.participant("B", 200)]);
    const b = h.client("B", 200); b.client.start(); await new Promise(resolve => setTimeout(resolve, 60));
    expect(b.completed).not.toHaveBeenCalled(); expect(pendingCompletions(h.meshRoot, h.root)).toHaveLength(0);
  });
});
