import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { executionCgroups, type CgroupCustody } from "../src/process-cgroup.js";
import { spawnScopedExecution, releaseScopedChild } from "../src/worker/scope-spawn.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";

const available = process.platform === "linux" && fs.existsSync("/sys/fs/cgroup/cgroup.controllers") &&
  spawnSync("systemd-run", ["--user", "--scope", "--quiet", "--collect", "true"], { timeout: 5_000 }).status === 0;
afterEach(() => vi.restoreAllMocks());
const closeWithin = async (closed: Promise<void>) => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try { await Promise.race([closed, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("native close unconfirmed")), 5_000); })]); }
  finally { clearTimeout(timer); }
};
const waitEmpty = async (receipt: CgroupCustody) => { await vi.waitFor(() => expect(receipt.exited()).toBe(true), { timeout: 2_000 }); };
const scoped = async (command: string, args: string[]) => {
  const child = await spawnScopedExecution((binary, argv, opts) => spawn(binary, [...argv], opts), command, args, { stdio: "ignore", detached: true });
  const closed = new Promise<void>(resolve => child.once("close", () => resolve()));
  const receipt = executionCgroups.get(child)!;
  expect(receipt).toBeDefined(); return { child, closed, receipt };
};

describe.skipIf(!available)("real cgroup spawn pin and cleanup", () => {
  it("keeps target behind its gate until pin exists; each spawn gets a distinct exact owned scope", async () => {
    const first = await scoped("/bin/sleep", ["60"]), second = await scoped("/bin/sleep", ["60"]);
    try {
      expect(first.receipt.directory).not.toBe(second.receipt.directory);
      expect(first.receipt.pin.uid).toBe(process.getuid!()); first.receipt.verify(first.receipt.execution!);
      const kill = vi.spyOn(process, "kill");
      releaseScopedChild(first.child); releaseScopedChild(second.child);
      await first.receipt.signal("SIGTERM"); expect(first.receipt.exited()).toBe(false);
      await first.receipt.signal("SIGKILL"); await closeWithin(first.closed); await waitEmpty(first.receipt);
      expect(second.receipt.exited()).toBe(false); expect(kill).not.toHaveBeenCalled();
    } finally { await first.receipt.signal("SIGKILL"); await second.receipt.signal("SIGKILL"); await Promise.all([closeWithin(first.closed), closeWithin(second.closed)]); await waitEmpty(second.receipt); }
  });
  it("kills a real unsampled setsid orphan using only recursive cgroup.kill", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-cgroup-orphan-")), ready = path.join(root, "ready");
    const target = await scoped(process.execPath, ["-e", `const {spawn}=require('node:child_process');const fs=require('node:fs'); const leaf=spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{detached:true,stdio:'ignore'}); leaf.unref(); fs.writeFileSync(${JSON.stringify(ready)},String(leaf.pid));`]);
    try {
      const kill = vi.spyOn(process, "kill"); releaseScopedChild(target.child);
      await vi.waitFor(() => expect(fs.existsSync(ready)).toBe(true)); await closeWithin(target.closed);
      expect(target.receipt.exited()).toBe(false); expect(target.receipt.members()).toContain(Number(fs.readFileSync(ready, "utf8")));
      await target.receipt.signal("SIGKILL"); await waitEmpty(target.receipt); expect(kill).not.toHaveBeenCalled();
    } finally { await target.receipt.signal("SIGKILL"); await closeWithin(target.closed); await waitEmpty(target.receipt); fs.rmSync(root, { recursive: true, force: true }); }
  });
  it("migration between readiness marker and pin never adopts the sibling scope or starts target", async () => {
    const sibling = await scoped("/bin/sleep", ["60"]); releaseScopedChild(sibling.child);
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-cgroup-migrate-")), target = path.join(root, "target");
    let launcher: ChildProcess | undefined, planned = "", moved = false;
    const original = fs.openSync.bind(fs), read = vi.spyOn(fs, "readFileSync"), kill = vi.spyOn(process, "kill");
    const open = vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, flags: number, mode?: fs.Mode) => {
      if (String(file) === planned && launcher?.pid) {
        fs.writeFileSync(`${sibling.receipt.directory}/cgroup.procs`, String(launcher.pid)); moved = true;
      }
      return original(file, flags, mode);
    }) as typeof fs.openSync);
    try {
      await expect(spawnScopedExecution((binary, args, opts) => {
        const unit = args.find(arg => arg.startsWith("--unit="))!.slice(7);
        planned = path.join(path.dirname(sibling.receipt.directory), unit);
        launcher = spawn(binary, [...args], opts); return launcher;
      }, "/bin/sh", ["-c", `printf executed > ${JSON.stringify(target)}`], { stdio: "ignore", detached: true })).rejects.toThrow();
      expect(moved).toBe(true); expect(fs.existsSync(target)).toBe(false);
      expect(read.mock.calls.some(call => String(call[0]).endsWith("/admitted"))).toBe(false);
      expect(kill).not.toHaveBeenCalled(); expect(sibling.receipt.exited()).toBe(false);
    } finally { open.mockRestore(); await sibling.receipt.signal("SIGKILL"); await closeWithin(sibling.closed); await waitEmpty(sibling.receipt); fs.rmSync(root, { recursive: true, force: true }); }
  });
  it("a real migrated member survives original-scope KILL (accepted #7478 residual)", async () => {
    const first = await scoped("/bin/sleep", ["60"]), sibling = await scoped("/bin/sleep", ["60"]);
    try {
      releaseScopedChild(first.child); releaseScopedChild(sibling.child);
      fs.writeFileSync(`${sibling.receipt.directory}/cgroup.procs`, String(first.child.pid));
      const kill = vi.spyOn(process, "kill"); await first.receipt.signal("SIGTERM"); await first.receipt.signal("SIGKILL");
      expect(fs.readFileSync(`/proc/${first.child.pid}/cgroup`, "utf8")).toContain(path.basename(sibling.receipt.directory));
      expect(sibling.receipt.members()).toContain(first.child.pid); expect(kill).not.toHaveBeenCalled();
    } finally { await first.receipt.signal("SIGKILL"); await sibling.receipt.signal("SIGKILL"); await Promise.all([closeWithin(first.closed), closeWithin(sibling.closed)]); await waitEmpty(first.receipt); await waitEmpty(sibling.receipt); }
  });
  it.each([undefined, "fabric-batch.slice"])("ProcessTransport scopes workers at spawn, retains grace and sends no numeric signals (slice=%s)", async slice => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-cgroup-transport-")), worker = path.join(root, "worker.mjs");
    fs.writeFileSync(worker, "setInterval(()=>{},1000)");
    const handle = await new ProcessTransport(slice).launch({ id: "real", name: "real", cwd: root, workerPath: worker, workerArguments: [] });
    const directory = "/sys/fs/cgroup" + fs.readFileSync(`/proc/${handle.sessionId}/cgroup`, "utf8").trim().split("::")[1];
    try {
      expect(directory).toContain(slice ?? "app.slice"); expect(directory).toMatch(/fabric-worker-[0-9a-f-]+[.]scope$/);
      const kill = vi.spyOn(process, "kill"); const started = Date.now(); await handle.stop();
      expect(Date.now() - started).toBeGreaterThanOrEqual(6_900); await handle.waitForClose?.();
      expect(await handle.isAlive()).toBe(false); expect(handle.lostContact?.()).toBeUndefined(); expect(kill).not.toHaveBeenCalled();
    } finally { await handle.stop(); await handle.waitForClose?.(); fs.rmSync(root, { recursive: true, force: true }); }
  }, 12_000);
});
