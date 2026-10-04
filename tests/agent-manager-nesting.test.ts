import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorChildCompletionStore } from "../src/actors/child-completions.js";
import type { AgentRunRecord } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentTranscriptReader } from "../src/ui/transcript-reader.js";
import { NativeConversationReader } from "../src/ui/conversation-native-reader.js";

const managers: AgentManager[] = [];
const roots: string[] = [];
beforeEach(() => {
  for (const key of Object.keys(process.env).filter((key) => key.startsWith("PI_FABRIC_"))) {
    vi.stubEnv(key, undefined);
  }
});
afterEach(async () => {
  for (const manager of managers.splice(0).reverse()) await manager.close();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const makeManager = (runRoot?: string) => {
  const manager = new AgentManager(process.cwd(), {
    ...DEFAULT_FABRIC_CONFIG.agents,
    budgetUsd: 0,
    retainRuns: false,
    sessionExport: false,
    transport: "process",
  }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    fullCodeMode: false,
    ...(runRoot ? { runRoot } : {}),
  });
  managers.push(manager);
  return manager;
};
const startParent = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-nesting-test-"));
  roots.push(root);
  const parent = makeManager(root);
  const handle = await parent.spawn({ task: "HANG parent", recursive: true });
  await vi.waitFor(() => expect(fs.existsSync(path.join(parent.runDirectory(handle.id)!, "status.json"))).toBe(true));
  return { parent, handle, root, directory: parent.runDirectory(handle.id)! };
};
const writeNested = (directory: string, record: AgentRunRecord) => {
  const childDirectory = path.join(directory, "nested", record.id);
  fs.mkdirSync(childDirectory, { recursive: true });
  fs.writeFileSync(path.join(childDirectory, "status.json"), JSON.stringify(record));
  return childDirectory;
};

describe("nested agent shutdown", () => {
  it.each([false, true])("collects an archived actor child's execution files only after archive success (failure=%s)", async (archiveFailure) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-nested-actor-archive-"));
    roots.push(root);
    const nested = path.join(root, "activation", "nested");
    const actorId = "b".repeat(32);
    const session = path.join(root, "actor", "session.jsonl");
    const store = new ActorChildCompletionStore(session);
    vi.stubEnv("PI_FABRIC_RUN_ROOT", nested);
    vi.stubEnv("PI_FABRIC_DEPTH", "1");
    vi.stubEnv("PI_FABRIC_LINEAGE", JSON.stringify({
      version: 1, rootSessionId: "root", runId: "activation", depth: 1, childIndex: 0, worker: true,
    }));
    vi.stubEnv("PI_FABRIC_ACTOR_ID", actorId);
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "activation");
    vi.stubEnv("PI_FABRIC_ACTOR_SESSION_FILE", session);
    const manager = new AgentManager(process.cwd(), {
      ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, retainRuns: false, sessionExport: false,
    }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false,
      onSettled: result => {
        if (archiveFailure) throw new Error("archive unavailable");
        store.enqueue(result, result.spawner!, false);
      },
    });
    managers.push(manager);
    const result = await manager.run({ task: "private actor child", extensions: false });
    expect(result).toMatchObject({ status: "completed", spawner: { id: actorId, kind: "actor", runId: "activation" } });
    const directory = path.dirname(result.logFile!);
    await manager.close();
    expect(fs.existsSync(directory)).toBe(archiveFailure);
    expect(fs.existsSync(nested), "the enclosing run still owns its root").toBe(true);
    expect(fs.existsSync(store.resultFile(result.id))).toBe(!archiveFailure);
    if (!archiveFailure) expect(JSON.parse(fs.readFileSync(store.resultFile(result.id), "utf8"))).toMatchObject({
      id: result.id, status: "completed", text: result.text, spawner: result.spawner,
    });
  });

  it("preserves nested terminal records and readable transcripts until the owning run is cleaned up", async () => {
    const { parent, handle, root, directory } = await startParent();
    const nestedRoot = path.join(directory, "nested");
    vi.stubEnv("PI_FABRIC_RUN_ROOT", nestedRoot);
    vi.stubEnv("PI_FABRIC_DEPTH", "1");
    vi.stubEnv("PI_FABRIC_LINEAGE", JSON.stringify({
      version: 1, rootSessionId: "root", runId: handle.id, depth: 1, childIndex: 0, worker: true,
    }));
    const child = makeManager();
    const completed = await child.run({ task: "Read nested transcript", extensions: false });
    // The lightweight fake worker uses a legacy string assistant body. Give
    // both readers the native Pi message shape emitted by a real worker.
    fs.writeFileSync(completed.logFile!, JSON.stringify({
      type: "message_end",
      message: { role: "assistant", content: [{ type: "text", text: "fake worker complete" }], timestamp: 1 },
    }) + "\n");
    const running = await child.spawn({ task: "HANG nested", extensions: false });
    await child.close();

    expect(fs.existsSync(nestedRoot)).toBe(true);
    expect(JSON.parse(fs.readFileSync(path.join(nestedRoot, running.id, "status.json"), "utf8")).status).toBe("stopped");
    await parent.stop(handle.id);
    const snapshot = parent.status(handle.id) as AgentRunRecord;
    expect(snapshot.nestedAgents?.find((record) => record.id === running.id)?.status).toBe("stopped");
    const retained = snapshot.nestedAgents!.find((record) => record.id === completed.id)!;
    expect(retained.status).toBe("completed");
    const transcript = new AgentTranscriptReader().read(retained);
    expect(transcript.entries).toContainEqual(expect.objectContaining({ kind: "assistant", text: "fake worker complete" }));
    const nativeReader = new NativeConversationReader();
    try {
      const native = nativeReader.read(retained);
      expect(native.messages).toContainEqual(expect.objectContaining({ role: "assistant", content: [{ type: "text", text: "fake worker complete" }] }));
    } finally {
      nativeReader.clear();
    }
    expect((parent.listForUi()[0] as AgentRunRecord).nestedAgents).toEqual(snapshot.nestedAgents);

    await parent.cleanup(handle.id);
    expect(fs.existsSync(directory)).toBe(false);
    await parent.close();
    expect(fs.existsSync(root)).toBe(true); // A25: the supplied allocation belongs to the caller.
    expect(fs.readdirSync(root)).toEqual([]);
  });

  it.each(["missing lineage", "different owner", "explicit root"])("does not adopt or remove a supplied root with %s", async (scenario) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-nested-ownership-"));
    roots.push(root);
    const nested = path.join(root, "owner", "nested");
    fs.mkdirSync(nested, { recursive: true });
    vi.stubEnv("PI_FABRIC_RUN_ROOT", nested);
    vi.stubEnv("PI_FABRIC_DEPTH", "1");
    if (scenario !== "missing lineage") vi.stubEnv("PI_FABRIC_LINEAGE", JSON.stringify({
      version: 1, rootSessionId: "root", runId: scenario === "different owner" ? "other" : "owner",
      depth: 1, childIndex: 0, worker: true,
    }));
    const manager = makeManager(scenario === "explicit root" ? nested : undefined);
    await manager.close();
    expect(fs.existsSync(nested)).toBe(true); // Missing lineage never grants ownership of the supplied root.
    expect(fs.readdirSync(nested)).toEqual([]);
  });

  it("does not leave cached session descendants running when their owner ends without retained terminal evidence", async () => {
    const { parent, handle, directory } = await startParent();
    const owner = parent.status(handle.id) as AgentRunRecord;
    const childDirectory = writeNested(directory, { ...owner, id: "child", name: "child", status: "running", currentTool: "bash" });
    writeNested(childDirectory, { ...owner, id: "grandchild", name: "grandchild", status: "queued" });
    writeNested(directory, { ...owner, id: "completed", name: "completed", status: "completed", text: "finished", finishedAt: 1 });
    expect((parent.status(handle.id) as AgentRunRecord).nestedAgents?.find((record) => record.id === "child")?.status).toBe("running");
    fs.rmSync(path.join(directory, "nested"), { recursive: true, force: true });

    await parent.stop(handle.id);
    const result = parent.status(handle.id) as AgentRunRecord;
    const child = result.nestedAgents!.find((record) => record.id === "child")!;
    expect(child).toMatchObject({ status: "failed", error: expect.stringMatching(/owner/i), finishedAt: expect.any(Number) });
    expect(child.currentTool).toBeUndefined();
    expect(child.nestedAgents?.[0]?.status).toBe("failed");
    expect(result.nestedAgents!.find((record) => record.id === "completed")).toMatchObject({ status: "completed", text: "finished", finishedAt: 1 });
    expect((parent.listForUi()[0] as AgentRunRecord).nestedAgents).toEqual(result.nestedAgents);
  });

  it("refreshes final child results and does not settle independently durable descendants", async () => {
    const { parent, handle, directory } = await startParent();
    const owner = parent.status(handle.id) as AgentRunRecord;
    const childDirectory = writeNested(directory, { ...owner, id: "child", name: "child" });
    const durableDirectory = writeNested(directory, { ...owner, id: "durable", name: "durable", residency: "durable" });
    writeNested(durableDirectory, { ...owner, id: "durable-child", name: "durable child" });
    const staleDirectory = writeNested(directory, { ...owner, id: "stale", name: "stale" });
    expect((parent.status(handle.id) as AgentRunRecord).nestedAgents).toHaveLength(3);
    fs.writeFileSync(path.join(childDirectory, "status.json"), JSON.stringify({ ...owner, id: "child", name: "child", status: "completed", finishedAt: Date.now() }));
    await parent.stop(handle.id);

    const snapshot = parent.status(handle.id) as AgentRunRecord;
    expect(snapshot.nestedAgents!.find((record) => record.id === "child")?.status).toBe("completed");
    const durable = snapshot.nestedAgents!.find((record) => record.id === "durable")!;
    expect(durable.status).toBe("running");
    expect(durable.nestedAgents?.[0]?.status).toBe("running");
    expect(snapshot.nestedAgents!.find((record) => record.id === "stale")).toMatchObject({ status: "failed", error: expect.stringMatching(/owner/i) });
    // Project loss of ownership without fabricating a worker-written terminal result.
    expect(JSON.parse(fs.readFileSync(path.join(staleDirectory, "status.json"), "utf8")).status).toBe("running");
  });
});
