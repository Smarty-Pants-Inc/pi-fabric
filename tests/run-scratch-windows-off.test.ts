import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import * as scratch from "../src/storage/run-scratch.js";
import * as windowsRoots from "../src/storage/windows-temp-root.js";
import { spawnDetached, WorkerNotStartedError } from "../src/agents/transports/process-utils.js";

vi.mock("../src/agents/transports/process-utils.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/agents/transports/process-utils.js")>();
  return { ...actual, spawnDetached: vi.fn() };
});
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); });

describe("Windows per-run scratch scope cut (#4800)", () => {
  it.each([false, true])("inherits normal user temp without scratch or per-run ACL work (actor=%s)", async actor => {
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    vi.stubEnv("TEMP", "C:\\Users\\runner\\AppData\\Local\\Temp");
    vi.stubEnv("TMP", "C:\\Users\\runner\\AppData\\Local\\Temp");
    vi.stubEnv("TMPDIR", undefined);
    const allocate = vi.spyOn(scratch, "allocateRunTmpDirectory");
    const acl = vi.spyOn(windowsRoots, "windowsDataRoot");
    const closed = Promise.resolve();
    vi.mocked(spawnDetached).mockResolvedValue({ pid: 123, isAlive: async () => false,
      lostContact: () => undefined, waitForClose: async () => {}, stop: async () => {}, closed });
    const workerArguments = ["--status-file", path.resolve("run/status.json"), ...(actor ? ["--actor-id", "test-actor"] : [])];
    const handle = await new ProcessTransport().launch({ id: "test", name: "test", cwd: process.cwd(),
      workerPath: path.resolve("custom-worker.mjs"), workerArguments });
    const env = vi.mocked(spawnDetached).mock.calls.at(-1)![4]!;
    expect(env.TEMP).toBe(process.env.TEMP);
    expect(env.TMP).toBe(process.env.TMP);
    expect(env.TMPDIR).toBeUndefined();
    expect(vi.mocked(spawnDetached).mock.calls.at(-1)![8]).toBeUndefined();
    await handle.waitForClose!();
    await handle.stop();
    expect(allocate).not.toHaveBeenCalled();
    expect(acl).not.toHaveBeenCalled();
  });

  it("preserves inherited TMPDIR and propagates pre-spawn refusal without scratch cleanup", async () => {
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    vi.stubEnv("TMPDIR", "caller-selected-temp");
    const allocate = vi.spyOn(scratch, "allocateRunTmpDirectory");
    vi.mocked(spawnDetached).mockRejectedValue(new WorkerNotStartedError("refused"));
    await expect(new ProcessTransport().launch({ id: "test", name: "test", cwd: process.cwd(),
      workerPath: path.resolve("custom-worker.mjs"), workerArguments: ["--status-file", path.resolve("run/status.json")] })).rejects.toThrow("refused");
    expect(vi.mocked(spawnDetached).mock.calls.at(-1)![4]!.TMPDIR).toBe("caller-selected-temp");
    expect(allocate).not.toHaveBeenCalled();
  });
});
