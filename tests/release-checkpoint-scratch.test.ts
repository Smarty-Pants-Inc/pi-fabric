import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import * as scopes from "../src/storage/process-scratch-scope.js";
import { allocateRunTmpDirectory, UNRESOLVED_SCRATCH_FILE } from "../src/storage/run-scratch.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const manager of managers.splice(0)) await manager.close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const DEAD_PID = "2147483647";
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-release-scratch-")); roots.push(root);
  const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    runRoot: root, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
  }); managers.push(manager);
  return { root, manager };
};
/** An offline terminal process run; with `scratch`, its unscoped scratch is
 * eligible for immediate disposal (dead worker, recorded native close). */
const offlineRun = (root: string, scratch: boolean) => {
  const run = path.join(root, "offline-run");
  fs.mkdirSync(run, { mode: 0o700 });
  const terminal = () => fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ status: "completed", transport: "process", sessionId: DEAD_PID, finishedAt: Date.now() }));
  if (!scratch) { terminal(); return run; }
  // Fresh allocation (before status.json) yields the v3 unscoped fence.
  vi.spyOn(scopes, "createProcessScratchScope").mockReturnValue(undefined);
  const allocation = allocateRunTmpDirectory(run);
  fs.writeFileSync(path.join(allocation.directory, "data"), "handover must keep this");
  terminal(); allocation.workerClosed(Number(DEAD_PID));
  const fence = JSON.parse(fs.readFileSync(path.join(run, UNRESOLVED_SCRATCH_FILE), "utf8"));
  expect(fence).toMatchObject({ version: 3, closedPid: Number(DEAD_PID) });
  return run;
};
const snapshot = (run: string) => {
  const files = ["status.json", UNRESOLVED_SCRATCH_FILE, path.join("tmp", "data")];
  const stat = (name: string) => { try { const s = fs.lstatSync(path.join(run, name)); return { ino: s.ino, mtimeMs: s.mtimeMs, ctimeMs: s.ctimeMs, size: s.size }; } catch { return undefined; } };
  return {
    names: fs.readdirSync(run).sort(),
    root: stat("."), tmp: stat("tmp"),
    files: Object.fromEntries(files.map(name => [name, { stat: stat(name), text: fs.existsSync(path.join(run, name)) ? fs.readFileSync(path.join(run, name), "utf8") : undefined }])),
  };
};

describe.skipIf(process.platform !== "linux")("release checkpoint is non-destructive for run scratch", () => {
  it("accepted: a clean terminal tree passes without creating custody locks or touching metadata", async () => {
    const { root, manager } = setup(); const run = offlineRun(root, false);
    const before = snapshot(run);
    await expect(manager.checkpointForRelease()).resolves.toBeUndefined();
    expect(snapshot(run)).toEqual(before);
    expect(fs.existsSync(path.join(run, ".scratch-custody-lock"))).toBe(false);
  });

  it("refused: eligible scratch vetoes the handover and is preserved byte-for-byte", async () => {
    const { root, manager } = setup(); const run = offlineRun(root, true);
    const before = snapshot(run);
    expect(before.files[path.join("tmp", "data")]?.text).toBe("handover must keep this");
    await expect(manager.checkpointForRelease()).rejects.toThrow(/unresolved run tree/);
    expect(snapshot(run)).toEqual(before);
    expect(fs.existsSync(path.join(run, ".scratch-custody-lock"))).toBe(false);
    // A repeated release check stays read-only too.
    await expect(manager.checkpointForRelease()).rejects.toThrow(/unresolved run tree/);
    expect(snapshot(run)).toEqual(before);
  });

  it("cancelled: a closing manager refuses before inspection and preserves scratch", async () => {
    const { root, manager } = setup(); const run = offlineRun(root, true);
    const before = snapshot(run);
    manager.beginClose();
    await expect(manager.checkpointForRelease()).rejects.toThrow(/pending/);
    expect(snapshot(run)).toEqual(before);
  });

  it("cancelled: an elapsed inspection deadline preserves scratch", async () => {
    const { root, manager } = setup(); const run = offlineRun(root, true);
    const before = snapshot(run);
    let now = 0;
    vi.spyOn(performance, "now").mockImplementation(() => (now += 1_000));
    await expect(manager.checkpointForRelease()).rejects.toThrow(/unresolved run tree/);
    vi.restoreAllMocks();
    expect(snapshot(run)).toEqual(before);
  });
});
