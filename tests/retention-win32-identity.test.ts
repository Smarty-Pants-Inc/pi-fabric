import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createProcessIncarnationReader, readPhysicalHostIdentity, readPhysicalMachineId, type IncarnationCommandRunner } from "../src/core/atomic-write.js";
import { runTreeExitVeto } from "../src/storage/retention.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const tree = (sessionId?: string, processStartTime?: string) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "retention-win32-identity-")); roots.push(root);
  const child = path.join(root, "nested", "child"); fs.mkdirSync(child, { recursive: true });
  fs.writeFileSync(path.join(root, "status.json"), JSON.stringify({ status: "completed", transport: "process", sessionId: "2147483647" }));
  const status = path.join(child, "status.json");
  fs.writeFileSync(status, JSON.stringify({ status: "completed", transport: "process", sessionId, processStartTime }));
  const task = path.join(child, "task.txt"); fs.writeFileSync(task, "retained descendant input");
  return { root, status, task };
};

const withoutPhysicalEvidence = () => {
  const read = fs.readFileSync.bind(fs);
  vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof fs.readFileSync>) => {
    const file = String(args[0]);
    if (file === "/etc/machine-id" || file.startsWith("/proc/")) {
      throw Object.assign(new Error("no Windows machine/boot/procfs evidence"), { code: "ENOENT" });
    }
    return read(...args);
  });
  expect(readPhysicalMachineId()).toBeUndefined();
  expect(readPhysicalHostIdentity()).toBeUndefined();
};

const gone = () => { throw Object.assign(new Error("confirmed absent PID"), { code: "ESRCH" }); };

describe("Windows-simulated retained process identity", () => {
  it.each([undefined, "not-a-pid"])("keeps the nested cleanup veto for saved PID %j even after native exit", async sessionId => {
    withoutPhysicalEvidence();
    const run = vi.fn<IncarnationCommandRunner>().mockRejectedValue(Object.assign(new Error("Windows start identity denied"), { code: "EACCES" }));
    const reader = createProcessIncarnationReader({ platform: "win32", systemRoot: "C:\\Windows", run });
    const start = await reader.read(123);
    expect(start).toBeUndefined(); expect(run).toHaveBeenCalledOnce();
    const fixture = tree(sessionId, start);
    // Both real processes have exited, but an invalid saved descendant PID cannot be
    // repaired by probing some other PID or by a missing Windows birth/boot identity.
    const kill = vi.spyOn(process, "kill").mockImplementation(gone);
    expect(runTreeExitVeto(fixture.root, 0, undefined, true)).toMatch(/exit.*unconfirmed/);
    expect([...new Set(kill.mock.calls.map(([pid]) => pid))]).toEqual([2147483647]);
    expect(fs.existsSync(fixture.task)).toBe(true);
    fs.writeFileSync(fixture.status, JSON.stringify({ status: "completed", transport: "process", sessionId: "123" }));
    expect(runTreeExitVeto(fixture.root, 0, undefined, true)).toBeUndefined();
  });

  it.each(["EPERM", "EACCES", "EIO"])("does not mistake Windows PID probe %s for exit", code => {
    withoutPhysicalEvidence();
    const fixture = tree("123", "win32:639264528000000000");
    vi.spyOn(process, "kill").mockImplementation((pid) => {
      if (pid === 2147483647) return gone();
      throw Object.assign(new Error("unconfirmable Windows process"), { code });
    });
    expect(runTreeExitVeto(fixture.root, 0, undefined, true)).toMatch(/exit.*unconfirmed/);
    expect(fs.existsSync(fixture.task)).toBe(true);
    vi.mocked(process.kill).mockImplementation(gone);
    expect(runTreeExitVeto(fixture.root, 0, undefined, true)).toBeUndefined();
  });

  it("never substitutes a different Windows birth token for strict descendant exit", async () => {
    withoutPhysicalEvidence();
    const run = vi.fn<IncarnationCommandRunner>().mockResolvedValue("639264528000000001");
    const reader = createProcessIncarnationReader({ platform: "win32", systemRoot: "C:\\Windows", run });
    expect(await reader.read(123)).toBe("win32:639264528000000001");
    const fixture = tree("123", "win32:639264528000000000");
    vi.spyOn(process, "kill").mockImplementation((pid) => pid === 2147483647 ? gone() : true);
    expect(runTreeExitVeto(fixture.root, 0, undefined, true)).toMatch(/exit.*unconfirmed/);
    expect(fs.existsSync(fixture.task)).toBe(true);
  });
});
