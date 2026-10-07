import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";

it("does not delete a new image handoff when an old Windows native close arrives", async () => {
  const nativePlatform = Object.getOwnPropertyDescriptor(process, "platform")!;
  Object.defineProperty(process, "platform", { ...nativePlatform, value: "win32" });
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "manager-image-close-"));
  let release!: () => void, exited = false;
  const joined = new Promise<void>(resolve => { release = () => { exited = true; resolve(); }; });
  const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockResolvedValue({
    kind: "process", sessionId: "2147483647", isAlive: async () => !exited,
    waitForClose: () => joined, stop: async () => { release(); },
  });
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true }, {
    runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
  });
  try {
    const handle = await manager.spawn({ task: "image handoff", transport: "process", extensions: false,
      images: [{ type: "image", mimeType: "image/png", data: "aGVsbG8=" }] });
    const directory = manager.runDirectory(handle.id)!;
    const file = path.join(directory, "images.json");
    expect(fs.existsSync(file)).toBe(true);
    const now = Date.now();
    writeJsonAtomic(path.join(directory, "status.json"), {
      ...handle, task: "image handoff", status: "completed", transport: "process", sessionId: "2147483647",
      startedAt: now, updatedAt: now, finishedAt: now, text: "saved", turns: 1, toolCalls: 0,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    });
    expect(await manager.wait(handle.id)).toMatchObject({ status: "completed" });
    // Sensitive inputs must still be erased at logical settlement.
    expect(fs.existsSync(file)).toBe(false);
    // A replay owns a new file at the same path before the old native handle closes.
    fs.writeFileSync(file, "new replay handoff");
    release();
    await vi.waitFor(() => expect(manager.retentionReferences({ budgetMs: 100 }).has(handle.id)).toBe(false));
    expect(fs.readFileSync(file, "utf8")).toBe("new replay handoff");
  } finally {
    release(); await manager.close(); launch.mockRestore();
    Object.defineProperty(process, "platform", nativePlatform);
    fs.rmSync(root, { recursive: true, force: true });
  }
});

it("keeps newly reported work alive when caller abort precedes the next status poll", async () => {
  vi.useFakeTimers();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "manager-progress-abort-"));
  let exited = false;
  const stop = vi.fn(async () => { exited = true; });
  const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
    const file = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
    writeJsonAtomic(file, { id: request.id, name: request.name, task: "progress", status: "running", runner: "pi",
      transport: "process", sessionId: "2147483647", cwd: root, startedAt: Date.now(), updatedAt: Date.now(),
      turns: 0, toolCalls: 0, text: "", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } });
    return { kind: "process", sessionId: "2147483647", isAlive: async () => !exited, stop };
  });
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true }, {
    runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
  });
  const abort = new AbortController();
  try {
    const handle = await manager.spawn({ task: "progress", transport: "process", extensions: false }, abort.signal);
    await vi.advanceTimersByTimeAsync(0);
    expect(manager.status(handle.id)).toMatchObject({ status: "running", turns: 0 });
    const file = path.join(manager.runDirectory(handle.id)!, "status.json");
    const record = JSON.parse(fs.readFileSync(file, "utf8"));
    writeJsonAtomic(file, { ...record, turns: 3, toolCalls: 1 });
    abort.abort();
    await vi.advanceTimersByTimeAsync(0);
    expect(stop).not.toHaveBeenCalled();
    expect(manager.status(handle.id).status).toBe("running");
  } finally {
    vi.useRealTimers(); await manager.close(); launch.mockRestore();
    fs.rmSync(root, { recursive: true, force: true });
  }
});
