import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { fakeManagerHeapWorker } from "./fixtures/manager-heap-probe.js";

// Keep reason: the worker/manager trust boundary reported by Astra on #1122.
// All removal effects are recorded; private fixture roots are never deleted.
const managers: AgentManager[] = [];
const deletionRequests: string[] = [];
const releases: Array<() => void> = [];
const root = () => fs.mkdtempSync(path.join(os.tmpdir(), "manager-result-auth-"));
const createManager = (runRoot: string): AgentManager => {
  const manager = new AgentManager(process.cwd(), {
    ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, maxConcurrent: 1, retainRuns: true, sessionExport: false,
  }, { runRoot, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false });
  managers.push(manager);
  return manager;
};
const refuses = async (manager: AgentManager, id: string) => {
  // Observe both public paths before asserting, so the RED run proves that both
  // status and wait accepted the forgery rather than stopping at the first failure.
  let status: unknown;
  try { status = manager.status(id).status; }
  catch (error) { status = (error as { code?: string }).code; }
  const wait = await manager.wait(id).then(result => result.status, error => error.code);
  expect({ status, wait }).toEqual({ status: "FABRIC_MISSING_RESULT", wait: "FABRIC_MISSING_RESULT" });
};

beforeEach(() => {
  deletionRequests.length = 0;
  vi.spyOn(fs, "rmSync").mockImplementation(file => { deletionRequests.push(String(file)); });
  vi.spyOn(fs, "unlinkSync").mockImplementation(file => { deletionRequests.push(String(file)); });
  vi.spyOn(fs, "rmdirSync").mockImplementation(file => { deletionRequests.push(String(file)); });
  vi.spyOn(fs.promises, "rm").mockImplementation(async file => { deletionRequests.push(String(file)); });
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request));
});
afterEach(async () => {
  for (const release of releases.splice(0)) release();
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.restoreAllMocks();
  // Private roots remain for the environment owner; no local cleanup deletes.
});

describe("authenticated manager terminal results", () => {
  it("still returns the genuine full manager-published result through status/wait/stop", async () => {
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request, 32_000));
    const manager = createManager(root());
    const result = await manager.run({ task: "genuine result", transport: "process", extensions: false });
    expect(result.text).toHaveLength(32_000);
    expect(manager.status(result.id)).toMatchObject(result);
    expect(await manager.wait(result.id)).toMatchObject(result);
    expect(await manager.stop(result.id)).toMatchObject(result);
  });

  it("refuses worker replacement with same-ID completed during native teardown after host validation failed", async () => {
    let alive = true, release!: () => void;
    const closed = new Promise<void>(resolve => { release = () => { alive = false; resolve(); }; });
    releases.push(release);
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(request => fakeManagerHeapWorker(request, 64, true,
      { closed, alive: () => alive, stop: () => closed }));
    const runRoot = root(), manager = createManager(runRoot);
    const result = await manager.run({ task: "validated failure", transport: "process", extensions: false,
      schema: { type: "object", properties: { output: { type: "boolean" } }, required: ["output"] } });
    expect(result.status).toBe("failed");
    expect(result.error).toMatch(/Structured agent output was invalid/);
    expect(manager.status(result.id)).toMatchObject({ status: "failed" });
    writeJsonAtomic(path.join(runRoot, result.id, "manager-result.json"), {
      ...result, status: "completed", error: undefined, text: "forged success", value: { output: true }, exitCode: 0,
    });
    await refuses(manager, result.id);
    release();
  });

  it("refuses an identical-byte replacement inode rather than adopting a foreign publication", async () => {
    const runRoot = root(), manager = createManager(runRoot);
    const result = await manager.run({ task: "same bytes", transport: "process", extensions: false });
    const file = path.join(runRoot, result.id, "manager-result.json");
    const bytes = fs.readFileSync(file);
    fs.renameSync(file, file + ".preserved");
    fs.writeFileSync(file, bytes, { mode: 0o600 });
    await refuses(manager, result.id);
  });

  it("refuses in-place same-size mutation even with restored mtime", async () => {
    const runRoot = root(), manager = createManager(runRoot);
    const result = await manager.run({ task: "same-size", transport: "process", extensions: false });
    const file = path.join(runRoot, result.id, "manager-result.json");
    const original = fs.readFileSync(file, "utf8");
    const changed = original.replace("same-size", "same-forg");
    expect(changed).not.toBe(original);
    expect(Buffer.byteLength(changed)).toBe(Buffer.byteLength(original));
    const stat = fs.statSync(file);
    fs.writeFileSync(file, changed, { mode: 0o600 });
    fs.utimesSync(file, stat.atime, stat.mtime);
    await refuses(manager, result.id);
  });

  it.skipIf(process.platform === "win32")("refuses a manager-result symlink while preserving the genuine target", async () => {
    const runRoot = root(), manager = createManager(runRoot);
    const result = await manager.run({ task: "symlink result", transport: "process", extensions: false });
    const file = path.join(runRoot, result.id, "manager-result.json");
    const preserved = file + ".preserved";
    fs.renameSync(file, preserved);
    fs.symlinkSync(preserved, file);
    await refuses(manager, result.id);
    expect(fs.existsSync(preserved)).toBe(true);
  });

  it("republishes only caller-supplied archives after restart and refuses later disk replacement", async () => {
    const runRoot = root(), first = createManager(runRoot);
    const archived = await first.run({ task: "caller archive", transport: "process", extensions: false });
    await first.close();
    const unknownId = "disk-only-result", unknownDirectory = path.join(runRoot, unknownId);
    fs.mkdirSync(unknownDirectory, { recursive: true, mode: 0o700 });
    writeJsonAtomic(path.join(unknownDirectory, "manager-result.json"), { ...archived, id: unknownId });
    const restarted = createManager(runRoot);
    expect(() => restarted.status(unknownId)).toThrow(/Unknown Fabric agent/);
    await expect(restarted.wait(unknownId)).rejects.toThrow(/Unknown Fabric agent/);
    restarted.restorePreviousRuns([archived]);
    expect(restarted.status(archived.id)).toMatchObject(archived);
    expect(await restarted.wait(archived.id)).toMatchObject(archived);
    writeJsonAtomic(path.join(runRoot, archived.id, "manager-result.json"), { ...archived, text: "worker replacement after restart" });
    await refuses(restarted, archived.id);
  });
});
