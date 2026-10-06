import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createJevFabricBashOperations } from "../src/jev-fabric/operations.js";
import type { JevFabricCli } from "../src/jev-fabric/client.js";

const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-launch-cancel-")); roots.push(root);
  const abort = new AbortController();
  const calls: string[] = [];
  const cli = {
    start: vi.fn(async () => { calls.push("start"); return "job"; }),
    stop: vi.fn(async () => { calls.push("stop"); return { state: "stopped" }; }),
    follow: vi.fn(async () => ({ reason: "finished", next: 0, receipt: { state: "stopped" } })),
  };
  const ops = createJevFabricBashOperations({ cli: cli as unknown as JevFabricCli, taskId: "task", scriptDirectory: root,
    defaultTimeoutMs: 1000, maxTimeoutMs: 1000, detached: new AbortController().signal,
    onStarted: async () => { calls.push("onStarted"); }, onFinished() {} });
  return { root, abort, calls, cli, run: () => ops.exec("touch effect", root, { onData() {}, signal: abort.signal }) };
};
describe("durable launch cancellation", () => {
  it("A8 refuses launch when stop arrives during awaited script preparation", async () => {
    const f = fixture();
    const write = fs.promises.writeFile.bind(fs.promises);
    vi.spyOn(fs.promises, "writeFile").mockImplementation(async (...args) => {
      await write(...args); f.abort.abort(new Error("task stopped during preparation"));
    });
    await expect(f.run()).rejects.toThrow(/stop|abort/);
    expect(f.cli.start).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(f.root, "task.sh"))).toBe(false);
  });
  it("A8 preserves stop during submission and stops before awaited launch bookkeeping", async () => {
    const f = fixture();
    f.cli.start.mockImplementation(async () => { f.calls.push("start"); f.abort.abort(); return "job"; });
    await expect(f.run()).rejects.toThrow(/abort/);
    expect(f.calls.indexOf("stop")).toBeGreaterThan(-1);
    expect(f.calls.indexOf("stop")).toBeLessThan(f.calls.indexOf("onStarted"));
  });
});
