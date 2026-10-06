import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";

it("Windows native-close discharge preserves main custody; cached invalidation deferred to smarty-dev#5132", async () => {
  const nativePlatform = Object.getOwnPropertyDescriptor(process, "platform")!;
  Object.defineProperty(process, "platform", { ...nativePlatform, value: "win32" });
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "manager-retention-close-"));
  let release!: () => void, exited = false;
  const joined = new Promise<void>(resolve => { release = () => { exited = true; resolve(); }; });
  const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockResolvedValue({
    kind: "process", sessionId: "2147483647", isAlive: async () => !exited,
    waitForClose: () => joined, stop: async () => { release(); },
  });
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true }, {
    runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
  });
  const now = Date.now();
  try {
    const handle = await manager.spawn({ task: "native join custody", transport: "process", extensions: false });
    const file = path.join(manager.runDirectory(handle.id)!, "status.json");
    writeJsonAtomic(file, {
      ...handle, id: handle.id, task: "native join custody", status: "completed", transport: "process", sessionId: "2147483647",
      startedAt: now, updatedAt: now, finishedAt: now, text: "exact saved outcome", turns: 1, toolCalls: 0,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    });
    expect(await manager.wait(handle.id)).toMatchObject({ status: "completed", text: "exact saved outcome" });
    const snapshot = () => manager.retentionReferences({ now, budgetMs: 100, maxEntries: 128 });
    expect(snapshot().has(handle.id)).toBe(true);
    // Windows uses main's uncached reference scan, not the V2 targeted veto.
    manager.listForUi(); manager.status(handle.id);
    expect(snapshot().has(handle.id)).toBe(true);
    release();
    // Main has no retention cache on Windows: each call must observe the real
    // native close/debt-discharge transition without an artificial clock advance.
    await vi.waitFor(() => {
      expect(snapshot().has(handle.id)).toBe(false);
      // Join the original complete ownership outcome, not V2 bounded custody.
      expect(snapshot().has("*")).toBe(false);
    });
    expect(fs.existsSync(file)).toBe(true);
  } finally {
    release(); await manager.close(); launch.mockRestore();
    Object.defineProperty(process, "platform", nativePlatform);
    fs.rmSync(root, { recursive: true, force: true });
  }
});

it.skipIf(process.platform === "win32")("POSIX custody retries a deadline hint without a clock advance (Windows: smarty-dev#5132)", async () => {
  const nativePlatform = Object.getOwnPropertyDescriptor(process, "platform")!;
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "manager-retention-timeout-"));
  const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockResolvedValue({
    kind: "process", sessionId: "2147483647", isAlive: async () => false,
    waitForClose: async () => {}, stop: async () => {},
  });
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true }, {
    runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
  });
  const now = Date.now();
  try {
    const handle = await manager.spawn({ task: "completed custody", transport: "process", extensions: false });
    const file = path.join(manager.runDirectory(handle.id)!, "status.json");
    writeJsonAtomic(file, {
      ...handle, task: "completed custody", status: "completed", transport: "process", sessionId: "2147483647",
      startedAt: now, updatedAt: now, finishedAt: now, text: "saved", turns: 1, toolCalls: 0,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    });
    await manager.wait(handle.id);
    await vi.waitFor(() => expect(manager.retentionReferences({ now, budgetMs: 100 }).has(handle.id)).toBe(false));
    let clock = 0, slow = true;
    vi.spyOn(performance, "now").mockImplementation(() => clock);
    const read = fs.readFileSync;
    vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof read>) => {
      const value = read(...args);
      if (String(args[0]) === file && slow) clock += 3;
      return value;
    });
    expect(manager.retentionReferences({ now, budgetMs: 2, refresh: true }).has("*")).toBe(true);
    for (let slice = 0; slice < 8; slice++) manager.retentionReferences({ now, budgetMs: 2 });
    slow = false;
    let refs = new Set([handle.id]);
    for (let slice = 0; slice < 8 && (refs.has(handle.id) || refs.has("*")); slice++) refs = manager.retentionReferences({ now, budgetMs: 2 });
    expect(refs.has(handle.id)).toBe(false); expect(refs.has("*")).toBe(false);
    expect(manager.status(handle.id)).toMatchObject({ status: "completed" });
  } finally {
    vi.restoreAllMocks(); await manager.close(); launch.mockRestore();
    Object.defineProperty(process, "platform", nativePlatform);
    fs.rmSync(root, { recursive: true, force: true });
  }
});
