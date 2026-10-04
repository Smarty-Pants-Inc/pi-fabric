import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { spawnDetached } from "../src/agents/transports/process-utils.js";

const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const fixture = (script = 'while [ "$1" != "--" ]; do shift; done; shift; exec "$@"') => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-process-slice-")); roots.push(root);
  const worker = path.join(root, "worker.mjs");
  fs.writeFileSync(worker, 'import fs from "node:fs"; fs.appendFileSync("started", String(process.pid)+"\\n"); setInterval(() => {}, 1000);');
  fs.writeFileSync(path.join(root, "systemd-run"), `#!/bin/sh\nprintf '%s\\n' "$@" > "${root}/scope-args"\n${script}\n`, { mode: 0o700 });
  vi.stubEnv("PATH", root); // runtime is process.execPath, never PATH
  return { root, worker, request: { id: "test", name: "test", cwd: root, workerPath: worker, workerArguments: [] } };
};
const workerStarted = async (root: string) => { await vi.waitFor(() => expect(fs.existsSync(path.join(root, "started"))).toBe(true)); return Number(fs.readFileSync(path.join(root, "started"), "utf8").trim()); };

describe.skipIf(process.platform !== "linux")("ProcessTransport processSlice (#4383)", () => {
  it("propagates the host slice from AgentManager through a real process worker", async () => {
    const f = fixture();
    const manager = new AgentManager(f.root, { ...DEFAULT_FABRIC_CONFIG.agents, processSlice: "batch.slice", budgetUsd: 0, sessionExport: false, timeoutMs: 5_000 }, {
      runRoot: path.join(f.root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    });
    try {
      const result = await manager.run({ task: "slice propagation", transport: "process" });
      expect(result.status).toBe("completed");
      expect(fs.readFileSync(path.join(f.root, "scope-args"), "utf8")).toContain("--slice=batch.slice\n");
    } finally { await manager.close(); }
  });
  it("keeps direct launches on non-Linux platforms even when configured", async () => {
    const f = fixture(); vi.spyOn(process, "platform", "get").mockReturnValue("darwin");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const handle = await new ProcessTransport("batch.slice").launch(f.request);
    try { expect(await workerStarted(f.root)).toBe(Number(handle.sessionId)); expect(fs.existsSync(path.join(f.root, "scope-args"))).toBe(false); expect(warn).not.toHaveBeenCalled(); }
    finally { await handle.stop(); await handle.waitForClose?.(); }
  });
  it("rechecks launch authority before a failed scope can fall back", async () => {
    const f = fixture("/bin/sleep 0.1; exit 1"); let allowed = true;
    const launching = new ProcessTransport("batch.slice").launch({ ...f.request, authorize: () => allowed });
    const outcome = launching.then(() => undefined, error => error);
    await vi.waitFor(() => expect(fs.existsSync(path.join(f.root, "scope-args"))).toBe(true));
    allowed = false;
    expect(await outcome).toBeInstanceOf(Error);
    expect(fs.existsSync(path.join(f.root, "started"))).toBe(false);
  });
  it("unconfirmed scope teardown vetoes fallback and retains custody debt", async () => {
    const f = fixture("trap '' TERM; while :; do :; done"); const debt = vi.fn();
    await expect(new ProcessTransport("batch.slice").launch({ ...f.request, onUnconfirmedExit: debt }))
      .rejects.toThrow("termination is unconfirmed");
    expect(debt).toHaveBeenCalledOnce(); expect(fs.existsSync(path.join(f.root, "started"))).toBe(false);
  }, 20_000);

  it("is off by default", async () => {
    const f = fixture(); const handle = await new ProcessTransport().launch(f.request);
    try { expect(await workerStarted(f.root)).toBe(Number(handle.sessionId)); expect(fs.existsSync(path.join(f.root, "scope-args"))).toBe(false); }
    finally { await handle.stop(); await handle.waitForClose?.(); }
  });
  it("execs the scoped worker in place, retaining PID, PGID, native close and exit fence", async () => {
    const f = fixture(); const warn = vi.spyOn(console, "warn");
    const handle = await new ProcessTransport("batch.slice").launch(f.request);
    const pid = Number(handle.sessionId);
    try {
      expect(await workerStarted(f.root)).toBe(pid);
      const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8").slice(fs.readFileSync(`/proc/${pid}/stat`, "utf8").lastIndexOf(")") + 2).split(" ");
      expect(Number(stat[2])).toBe(pid); // field 5 is PGID
      expect(fs.readFileSync(path.join(f.root, "scope-args"), "utf8").split("\n").slice(0, 6)).toEqual(["--user", "--scope", "--slice=batch.slice", "--quiet", "--collect", "--"]);
      expect(await handle.isAlive()).toBe(true); expect(warn).not.toHaveBeenCalled();
    } finally { await handle.stop(); await handle.waitForClose?.(); }
    expect(await handle.isAlive()).toBe(false); expect(handle.lostContact?.()).toBeUndefined();
    const kill = vi.spyOn(process, "kill"); await handle.stop(); expect(kill).not.toHaveBeenCalled();
  });
  it.each(["unavailable", "failed"])("warns once and launches directly when systemd-run is %s", async mode => {
    const f = fixture("exit 1"); if (mode === "unavailable") fs.unlinkSync(path.join(f.root, "systemd-run"));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {}); const transport = new ProcessTransport("batch.slice");
    for (let i = 0; i < 2; i++) {
      if (fs.existsSync(path.join(f.root, "started"))) fs.unlinkSync(path.join(f.root, "started"));
      const handle = await transport.launch(f.request);
      try { expect(await workerStarted(f.root)).toBe(Number(handle.sessionId)); }
      finally { await handle.stop(); await handle.waitForClose?.(); }
    }
    expect(warn).toHaveBeenCalledTimes(1); expect(warn.mock.calls[0]![0]).toContain("launching worker normally");
  });
  it("handles an executable disappearing after lookup with a close-fenced fallback", async () => {
    const f = fixture(); const warn = vi.fn();
    const handle = await spawnDetached(f.worker, [], f.root, undefined, undefined, { executable: path.join(f.root, "missing"), slice: "batch.slice", warn });
    try { expect(await workerStarted(f.root)).toBe(handle.pid); expect(warn).toHaveBeenCalledOnce(); }
    finally { await handle.stop(); await handle.waitForClose(); }
  });
  it("does not replay a worker that fails after successful scope admission", async () => {
    const f = fixture(); fs.writeFileSync(f.worker, 'import fs from "node:fs"; fs.appendFileSync("started", String(process.pid)+"\\n"); process.exit(1);');
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const handle = await new ProcessTransport("batch.slice").launch(f.request); await handle.waitForClose?.();
    expect(fs.readFileSync(path.join(f.root, "started"), "utf8").trim().split("\n")).toEqual([handle.sessionId]);
    expect(warn).not.toHaveBeenCalled(); expect(await handle.isAlive()).toBe(false);
  });
});
