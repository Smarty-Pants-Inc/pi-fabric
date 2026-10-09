import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { setTimeout as pause } from "node:timers/promises";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { acquireHostLeasePublishLock, hostLeasePath, hostLeasePublishLockPath, HostLeasePublishLockBusyError,
  meshWriterLeaseRecord, prepareHostLeasePublishLock, prepareHostLeasePublishLocks, writeHostLease } from "../src/topology/host-leases.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const setup = (pid = process.pid, host = os.hostname()) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-publish-lock-"));
  roots.push(root);
  const id = "host:atomic-receipt";
  const lease = { id, rootId: "session:owner", identityId: "session:owner", startedAt: 1,
    updatedAt: 1_000, expiresAt: 61_000, writer: { ...meshWriterLeaseRecord(2, "file", 1), pid, host } };
  writeHostLease(root, lease);
  return { root, id, lease, lock: hostLeasePublishLockPath(root, id) };
};
const deadPid = (): number => {
  const child = spawnSync(process.execPath, ["-e", ""], { timeout: 10_000 });
  expect(child.status).toBe(0);
  return child.pid!;
};
const fixture = path.resolve("tests/fixtures/host-lease-publish-lock.mjs");
const waitFor = async (condition: () => boolean): Promise<void> => {
  const deadline = Date.now() + 15_000;
  while (!condition()) {
    if (Date.now() >= deadline) throw new Error("Contender coordination timed out");
    await pause(5);
  }
};

describe("atomic host lease publish receipts (#695 round 4)", () => {
  it("fsyncs a complete same-directory receipt before exposing its exclusive hardlink", () => {
    const { root, id, lock } = setup();
    const sync = vi.spyOn(fs, "fsyncSync");
    const link = fs.linkSync.bind(fs);
    const publish = vi.spyOn(fs, "linkSync").mockImplementation((from, to) => {
      expect(String(to)).toBe(lock);
      expect(path.dirname(String(from))).toBe(path.dirname(lock));
      expect(fs.existsSync(lock)).toBe(false);
      expect(sync).toHaveBeenCalledTimes(1);
      expect(JSON.parse(fs.readFileSync(from, "utf8"))).toMatchObject({ pid: process.pid, token: expect.any(String) });
      link(from, to);
      expect(JSON.parse(fs.readFileSync(to, "utf8"))).toHaveProperty("startTime");
    });
    const release = acquireHostLeasePublishLock(root, id);
    expect(publish).toHaveBeenCalledTimes(1);
    expect(fs.statSync(lock).nlink).toBe(1);
    expect(fs.readdirSync(path.dirname(lock)).filter(name => name.endsWith(".tmp"))).toEqual([]);
    release();
    expect(fs.existsSync(lock)).toBe(false);
  });

  it("real crash after staging/fsync but before link leaves no lock and next acquire succeeds", () => {
    const { root, id, lock } = setup();
    const child = spawnSync(process.execPath, [fixture, root, id, "crash-before-link"], { encoding: "utf8", timeout: 15_000 });
    expect(child.stderr).toBe("");
    expect(child.status).toBe(42);
    expect(fs.existsSync(lock)).toBe(false);
    const staging = fs.readdirSync(path.dirname(lock)).filter(name => name.endsWith(".tmp"));
    expect(staging).toHaveLength(1);
    expect(JSON.parse(fs.readFileSync(path.join(path.dirname(lock), staging[0]!), "utf8"))).toMatchObject({ pid: child.pid });
    const release = acquireHostLeasePublishLock(root, id);
    release();
    expect(fs.existsSync(lock)).toBe(false);
  });

  it("real crash after link leaves a complete recoverable owner even with two hardlinks", async () => {
    const { root, id, lock } = setup();
    const child = spawnSync(process.execPath, [fixture, root, id, "crash-after-link"], { encoding: "utf8", timeout: 15_000 });
    expect(child.stderr).toBe("");
    expect(child.status).toBe(43);
    expect(fs.statSync(lock).nlink).toBe(2);
    expect(JSON.parse(fs.readFileSync(lock, "utf8"))).toMatchObject({ pid: child.pid, token: expect.any(String) });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await prepareHostLeasePublishLock(new MeshStore(root, 65_536, 100), id);
    expect(fs.existsSync(lock)).toBe(false);
    expect(warn).not.toHaveBeenCalled();
    const release = acquireHostLeasePublishLock(root, id);
    release();
  });

  it.each(["write", "fsync", "link"])("%s failure leaves no visible lock or staging leak", fault => {
    const { root, id, lock } = setup();
    const failure = Object.assign(new Error(`injected ${fault}`), { code: "EIO" });
    if (fault === "write") vi.spyOn(fs, "writeFileSync").mockImplementation(() => { throw failure; });
    if (fault === "fsync") vi.spyOn(fs, "fsyncSync").mockImplementation(() => { throw failure; });
    if (fault === "link") vi.spyOn(fs, "linkSync").mockImplementation(() => { throw failure; });
    expect(() => acquireHostLeasePublishLock(root, id)).toThrow(failure);
    expect(fs.existsSync(lock)).toBe(false);
    expect(fs.readdirSync(path.dirname(lock))).toEqual([path.basename(hostLeasePath(root, id))]);
  });

  it.each(["", "{\"pid\":", "{}", "null"])("reclaims malformed legacy receipt %j only with independent local writer death proof", async text => {
    const pid = deadPid();
    const { root, id, lock } = setup(pid);
    fs.writeFileSync(lock, text, { flag: "wx" });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await prepareHostLeasePublishLock(new MeshStore(root, 65_536, 100), id);
    expect(fs.existsSync(lock)).toBe(false);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining(`pid ${pid} proved dead (ESRCH)`));
    const release = acquireHostLeasePublishLock(root, id);
    release();
  });

  it.each(["live", "foreign", "unknown", "missing", "EPERM"])("keeps an arbitrarily old malformed receipt with %s owner evidence and logs loudly", async evidence => {
    const { root, id, lock, lease } = setup(evidence === "foreign" ? deadPid() : process.pid,
      evidence === "foreign" ? "another-host" : os.hostname());
    if (evidence === "unknown") fs.writeFileSync(hostLeasePath(root, id), JSON.stringify({ format: 1, ...lease, writer: undefined }));
    if (evidence === "missing") fs.unlinkSync(hostLeasePath(root, id));
    fs.writeFileSync(lock, "{", { flag: "wx" });
    fs.utimesSync(lock, 1, 1);
    if (evidence === "EPERM") vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(new Error("denied"), { code: "EPERM" }); });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await prepareHostLeasePublishLock(new MeshStore(root, 65_536, 100), id);
    expect(fs.readFileSync(lock, "utf8")).toBe("{");
    expect(() => acquireHostLeasePublishLock(root, id)).toThrow(HostLeasePublishLockBusyError);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("retaining: no provably dead local lease writer"));
  });

  it("all-lock preparation applies the same malformed-receipt recovery", async () => {
    const { root, lock } = setup(deadPid());
    fs.writeFileSync(lock, "");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await prepareHostLeasePublishLocks(new MeshStore(root, 65_536, 100));
    expect(fs.existsSync(lock)).toBe(false);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("proved dead (ESRCH)"));
  });

  it.each(["lock", "lease", "pid"])("recovery rechecks changed %s evidence under custody and never removes a successor", async changed => {
    const pid = deadPid(), { root, id, lock, lease } = setup(pid);
    fs.writeFileSync(lock, "");
    const mesh = new MeshStore(root, 65_536, 100), custody = mesh.custody.bind(mesh);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(mesh, "custody").mockImplementation(<T>(operation: () => T) => custody(() => {
      if (changed === "lock") {
        fs.unlinkSync(lock);
        fs.writeFileSync(lock, JSON.stringify({ pid: process.pid, startTime: null, token: "successor" }));
      } else if (changed === "lease") {
        fs.writeFileSync(hostLeasePath(root, id), JSON.stringify({ format: 1, ...lease, writer: { ...lease.writer, pid: process.pid } }));
      } else {
        const kill = process.kill.bind(process);
        vi.spyOn(process, "kill").mockImplementation(((target: number, signal?: NodeJS.Signals | number) => target === pid ? true : kill(target, signal)) as typeof process.kill);
      }
      return operation();
    }));
    await prepareHostLeasePublishLock(mesh, id);
    expect(fs.existsSync(lock)).toBe(true);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("recovery evidence changed"));
  });

  it("eight concurrent process acquirers produce exactly one winner without deleting its token", async () => {
    const { root, id, lock } = setup();
    const children = Array.from({ length: 8 }, (_, contender) => {
      const child = spawn(process.execPath, [fixture, root, id, "contend", String(contender)], { stdio: ["ignore", "ignore", "pipe"] });
      let stderr = "";
      child.stderr.on("data", chunk => { stderr += chunk; });
      const exited = new Promise<{ code: number | null; stderr: string }>((resolve, reject) => {
        child.once("error", reject);
        child.once("close", code => resolve({ code, stderr }));
      });
      void exited.catch(() => {});
      return { child, exited };
    });
    try {
      await waitFor(() => fs.readdirSync(root).filter(name => name.startsWith("ready-")).length === children.length);
      fs.writeFileSync(path.join(root, "go"), "");
      await waitFor(() => fs.readdirSync(root).filter(name => name.startsWith("result-")).length === children.length);
      const results = children.map((_, contender) => fs.readFileSync(path.join(root, `result-${contender}`), "utf8"));
      expect(results.filter(value => value === "won")).toHaveLength(1);
      expect(results.filter(value => value === "held")).toHaveLength(7);
      expect(JSON.parse(fs.readFileSync(lock, "utf8"))).toHaveProperty("token");
      expect(fs.readdirSync(path.dirname(lock)).filter(name => name.endsWith(".tmp"))).toEqual([]);
      fs.writeFileSync(path.join(root, "release"), "");
      expect(await Promise.all(children.map(({ exited }) => exited))).toEqual(children.map(() => ({ code: 0, stderr: "" })));
      expect(fs.existsSync(lock)).toBe(false);
    } finally {
      for (const { child } of children) if (child.exitCode === null) child.kill();
      await Promise.allSettled(children.map(({ exited }) => exited));
    }
  }, 30_000);
});
