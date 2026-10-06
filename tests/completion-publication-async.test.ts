import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { completionPublishedAsync, consumeCompletionAsync, saveCompletionAsync, completionConsumed, type CompletionRecipient } from "../src/agents/completion-journal.js";
import type { AgentRunResult } from "../src/agents/types.js";
const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "completion-publication-async-")); roots.push(root);
  const meshRoot = path.join(root, "mesh");
  const recipient: CompletionRecipient = { rootId: "session:main", sessionId: "main", name: "main", cwd: root, projectRoot: root, startedAt: 1 };
  const result: AgentRunResult = { id: "a".repeat(32), name: "task", task: "fixture", status: "completed", runner: "pi", transport: "process", cwd: root, text: "x".repeat(4000), startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } };
  const hash = (id: string) => createHash("sha256").update(id).digest("hex");
  const envelope = path.join(meshRoot, "agent-completions", hash(result.id) + ".json");
  const receipt = path.join(path.dirname(envelope), "receipts", path.basename(envelope));
  const candidate = path.join(path.dirname(envelope), "attempts", path.basename(envelope));
  const legacy = path.join(meshRoot, "residency", hash(recipient.rootId), "agents", result.id + ".json");
  const write = (file: string, value: unknown) => { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, typeof value === "string" ? value : JSON.stringify(value)); };
  return { root, meshRoot, recipient, result, envelope, receipt, candidate, legacy, write };
};
describe("async completion publication (#4233)", () => {
  it("publishes with async barriers and does not repeatedly parse/sync unchanged envelopes", async () => {
    const f = setup(), sync = vi.spyOn(fs, "fsyncSync");
    await saveCompletionAsync(f.meshRoot, f.recipient, f.result);
    expect(sync).not.toHaveBeenCalled(); expect(await completionPublishedAsync(f.meshRoot, f.result.id)).toBe(true);
    const read = vi.spyOn(fs.promises, "readFile"), open = vi.spyOn(fs.promises, "open");
    for (let n = 0; n < 3; n++) await saveCompletionAsync(f.meshRoot, f.recipient, f.result);
    expect(open).not.toHaveBeenCalled();
    expect(read.mock.calls.some(([file]) => String(file) === f.envelope)).toBe(false);
    expect(sync).not.toHaveBeenCalled();
  });
  it("coalesces overlapping saves while a barrier is held and yields to the event loop", async () => {
    const f = setup(), open = fs.promises.open.bind(fs.promises);
    let entered!: () => void, release!: () => void;
    const reading = new Promise<void>(resolve => { entered = resolve; });
    const held = new Promise<void>(resolve => { release = resolve; });
    const opened = vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      const handle = await open(...args);
      if (String(args[0]) === f.envelope) { const sync = handle.sync.bind(handle); vi.spyOn(handle, "sync").mockImplementation(async () => { entered(); await held; await sync(); }); }
      return handle;
    });
    const first = saveCompletionAsync(f.meshRoot, f.recipient, f.result);
    await reading;
    try {
      expect(saveCompletionAsync(f.meshRoot, f.recipient, f.result)).toBe(first);
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(await completionPublishedAsync(f.meshRoot, f.result.id)).toBe(false);
    } finally { release(); await first; }
    expect(opened.mock.calls.filter(([file]) => String(file) === f.envelope)).toHaveLength(1);
  });
  it("does not cache or delete a candidate after a failed visible publication barrier", async () => {
    const f = setup(); f.write(f.candidate, { retained: true });
    const open = fs.promises.open.bind(fs.promises); let fail = true;
    vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      const handle = await open(...args);
      if (String(args[0]) === f.envelope && fail) { fail = false; vi.spyOn(handle, "sync").mockRejectedValueOnce(new Error("fsync fault")); }
      return handle;
    });
    await expect(saveCompletionAsync(f.meshRoot, f.recipient, f.result)).rejects.toThrow("fsync fault");
    expect(await completionPublishedAsync(f.meshRoot, f.result.id)).toBe(false); expect(fs.existsSync(f.candidate)).toBe(true);
    await saveCompletionAsync(f.meshRoot, f.recipient, f.result);
    expect(await completionPublishedAsync(f.meshRoot, f.result.id)).toBe(true); expect(fs.existsSync(f.candidate)).toBe(false);
  });
  it.each(["replace", "in-place"])("invalidates a successfully published %s with same size/restored mtime", async change => {
    const f = setup(); await saveCompletionAsync(f.meshRoot, f.recipient, f.result);
    const before = fs.statSync(f.envelope), value = JSON.parse(fs.readFileSync(f.envelope, "utf8")); value.result.text = "y".repeat(4000);
    if (change === "replace") { f.write(f.envelope + ".new", value); fs.renameSync(f.envelope + ".new", f.envelope); }
    else f.write(f.envelope, value);
    fs.utimesSync(f.envelope, before.atime, before.mtime);
    expect(await completionPublishedAsync(f.meshRoot, f.result.id)).toBe(false);
    await saveCompletionAsync(f.meshRoot, f.recipient, f.result);
    expect(JSON.parse(fs.readFileSync(f.envelope, "utf8")).result.text).toBe("y".repeat(4000)); // Preserve immutable existing outcome.
  });
  it.each(["global", "legacy"])("does not suppress an unreadable %s replay fence on a publication cache hit", async fence => {
    const f = setup(); await saveCompletionAsync(f.meshRoot, f.recipient, f.result);
    f.write(fence === "global" ? f.receipt : f.legacy, "{");
    await expect(saveCompletionAsync(f.meshRoot, f.recipient, f.result)).rejects.toThrow(/replay fence/);
    expect(fs.existsSync(f.envelope)).toBe(true);
  });
  it("confirms consumption asynchronously and never republishes a consumed outcome", async () => {
    const f = setup(); await saveCompletionAsync(f.meshRoot, f.recipient, f.result);
    const sync = vi.spyOn(fs, "fsyncSync");
    await consumeCompletionAsync(f.meshRoot, f.result.id, f.recipient.sessionId);
    expect(completionConsumed(f.meshRoot, f.result.id)).toBe(true);
    f.write(f.candidate, { retained: true });
    await saveCompletionAsync(f.meshRoot, f.recipient, f.result);
    expect(sync).not.toHaveBeenCalled(); expect(fs.existsSync(f.candidate)).toBe(false);
  });
  it("a cached publication hint cannot delete a new candidate without fresh barriers", async () => {
    const f = setup(); await saveCompletionAsync(f.meshRoot, f.recipient, f.result);
    f.write(f.candidate, { retained: true });
    await saveCompletionAsync(f.meshRoot, f.recipient, f.result);
    expect(fs.existsSync(f.candidate)).toBe(true);
  });
});
