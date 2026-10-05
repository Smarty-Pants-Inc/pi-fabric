import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ownProcessIncarnation } from "../src/core/atomic-write.js";
import { MeshStore } from "../src/mesh/store.js";

// Regression boundary only: production no longer has any foreign-owner age lease.
const FORMER_UNSAFE_BOUND_MS = 120_000;

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// Exercise the real acquisition/reclaim path with a virtual age, not a configurable
// production lease or two-minute sleep. All mesh roots are isolated temporary directories.
describe.skipIf(process.platform !== "linux")("mesh lock PID namespace identity (#4383)", () => {
  const setup = (lockProtocol?: 1 | 2) => {
    vi.useFakeTimers({ now: Date.now() });
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-lock-nspid-"));
    roots.push(root);
    const store = new MeshStore(root, 65536, 100, {
      ...(lockProtocol === undefined ? {} : { lockProtocol }), lockTimeoutMs: 100,
    });
    const lock = path.join(root, ".lock");
    const ownerPath = path.join(lock, "owner");
    const namespace = fs.readlinkSync("/proc/self/ns/pid");
    const foreign = namespace === "pid:[1]" ? "pid:[2]" : "pid:[1]";
    const receipt = (token: string, pid: number, at: number, ns = foreign, start = "1") =>
      `${token}\n${pid}\n${at}\n${start}\n${ns}\n`;
    const hold = (owner: string, at: number) => {
      fs.mkdirSync(lock);
      fs.writeFileSync(ownerPath, owner);
      fs.utimesSync(ownerPath, new Date(at), new Date(at));
    };
    const fences = () => fs.readdirSync(root).filter(name => name.startsWith(".lock.dead."));
    const timeout = async () => {
      const operation = vi.fn();
      const pending = store.exclusive(operation).catch((error: unknown) => error);
      await vi.advanceTimersByTimeAsync(100);
      const error = await pending;
      expect(error).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect(operation).not.toHaveBeenCalled();
      return error as Error;
    };
    return { root, store, lock, ownerPath, namespace, foreign, receipt, hold, fences, timeout };
  };

  // The public default must preserve the same identity/reclaim guarantees as explicit selections.
  for (const protocol of [undefined, 1, 2] as const) {
    const label = protocol ?? "default (1)";
    it(`protocol ${label} records both Linux process identities`, async () => {
      const h = setup(protocol);
      const start = await ownProcessIncarnation();
      await h.store.exclusive(() => {
        const fields = fs.readFileSync(h.ownerPath, "utf8").split("\n");
        expect(fields).toHaveLength(6);
        expect(fields[1]).toBe(String(process.pid));
        expect(fields[3]).toBe(start);
        expect(fields[4]).toBe(h.namespace);
      });
      expect(fs.existsSync(h.lock)).toBe(false);
    });

    it(`protocol ${label} never reclaims a same-namespace live owner even beyond the former unsafe bound`, async () => {
      const h = setup(protocol);
      const old = Date.now() - FORMER_UNSAFE_BOUND_MS - 1000;
      const owner = h.receipt("live", process.pid, old, h.namespace, (await ownProcessIncarnation())!);
      h.hold(owner, old);
      const error = await h.timeout();
      expect(error.message).toContain(`held by pid ${process.pid} (alive`);
      expect(error.message).not.toContain("foreign pid namespace");
      expect(fs.readFileSync(h.ownerPath, "utf8")).toBe(owner);
      expect(h.fences()).toEqual([]);
    });

    it(`protocol ${label} immediately reclaims a reused same-namespace PID with the five-line receipt`, async () => {
      const h = setup(protocol);
      const start = (await ownProcessIncarnation())!;
      h.hold(h.receipt("reused", process.pid, Date.now(), h.namespace, String(BigInt(start) + 1n)), Date.now());
      const operation = vi.fn(() => "recovered");
      await expect(h.store.exclusive(operation)).resolves.toBe("recovered");
      expect(operation).toHaveBeenCalledOnce();
      expect(h.fences()).toHaveLength(1);
    });

    it(`protocol ${label} fails closed on a malformed fifth-line namespace even for a dead PID`, async () => {
      const h = setup(protocol);
      const at = Date.now() - FORMER_UNSAFE_BOUND_MS - 1000;
      const owner = h.receipt("malformed", 999999999, at, "not-a-pid-namespace");
      h.hold(owner, at);
      await h.timeout();
      expect(fs.readFileSync(h.ownerPath, "utf8")).toBe(owner);
      expect(h.fences()).toEqual([]);
    });

    it(`protocol ${label} immediately reclaims a same-namespace dead owner`, async () => {
      const h = setup(protocol);
      h.hold(h.receipt("dead", 999999999, Date.now(), h.namespace), Date.now());
      const operation = vi.fn(() => "recovered");
      await expect(h.store.exclusive(operation)).resolves.toBe("recovered");
      expect(operation).toHaveBeenCalledOnce();
      expect(h.fences()).toHaveLength(1);
    });

    it.each([process.pid, 999999999])(`protocol ${label} protects a young foreign owner with local PID %s without any liveness probe`, async pid => {
      const h = setup(protocol);
      await ownProcessIncarnation(); // populate own-start cache before auditing holder reads
      const at = Date.now() - FORMER_UNSAFE_BOUND_MS + 1000;
      const owner = h.receipt("foreign-young", pid, at);
      h.hold(owner, at);
      const kill = vi.spyOn(process, "kill");
      const read = vi.spyOn(fs, "readFileSync");
      const error = await h.timeout();
      expect(error.message).toContain(`pid ${pid} in a foreign pid namespace (${h.foreign})`);
      expect(kill).not.toHaveBeenCalled();
      expect(read.mock.calls.some(([file]) => String(file) === `/proc/${pid}/stat`)).toBe(false);
      expect(fs.readFileSync(h.ownerPath, "utf8")).toBe(owner);
      expect(h.fences()).toEqual([]);
    });

    it(`protocol ${label} protects an ancient foreign owner against all contenders`, async () => {
      const h = setup(protocol);
      const at = Date.now() - FORMER_UNSAFE_BOUND_MS - 1000;
      h.hold(h.receipt("foreign-old", process.pid, at), at);
      await ownProcessIncarnation();
      const kill = vi.spyOn(process, "kill");
      const rename = vi.spyOn(fs, "renameSync");
      const operations = Array.from({ length: 16 }, () => vi.fn());
      const owner = fs.readFileSync(h.ownerPath, "utf8");
      const pending = operations.map(operation => new MeshStore(h.root, 65536, 100, {
        ...(protocol === undefined ? {} : { lockProtocol: protocol }), lockTimeoutMs: 1000,
      }).exclusive(operation).catch((error: unknown) => error));
      await vi.advanceTimersByTimeAsync(1000);
      expect(await Promise.all(pending)).toEqual(operations.map(() =>
        expect.objectContaining({ code: "FABRIC_MESH_LOCK_TIMEOUT" })));
      expect(operations.every(operation => operation.mock.calls.length === 0)).toBe(true);
      expect(rename).not.toHaveBeenCalled();
      expect(h.fences()).toEqual([]);
      expect(kill).not.toHaveBeenCalled();
      expect(fs.readFileSync(h.ownerPath, "utf8")).toBe(owner);
    });

    it(`protocol ${label} protects a replacement foreign token indefinitely, even with a copied old timestamp`, async () => {
      const h = setup(protocol);
      const at = Date.now() - FORMER_UNSAFE_BOUND_MS - 1000;
      h.hold(h.receipt("previous", process.pid, at), at);
      const replacement = h.receipt("successor", process.pid, at);
      fs.writeFileSync(h.ownerPath, replacement); // publication mtime starts the successor's clock
      await h.timeout();
      expect(fs.readFileSync(h.ownerPath, "utf8")).toBe(replacement);
      expect(h.fences()).toEqual([]);
      await vi.advanceTimersByTimeAsync(FORMER_UNSAFE_BOUND_MS + 1);
      await h.timeout();
      expect(fs.readFileSync(h.ownerPath, "utf8")).toBe(replacement);
      expect(h.fences()).toEqual([]);
    });

    it(`protocol ${label} protects an old namespace receipt when its own namespace cannot be read`, async () => {
      const h = setup(protocol);
      const at = Date.now() - FORMER_UNSAFE_BOUND_MS - 1000;
      const owner = h.receipt("unknown-reader-namespace", 999999999, at, h.namespace);
      h.hold(owner, at);
      const readlink = fs.readlinkSync.bind(fs);
      vi.spyOn(fs, "readlinkSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
        if (String(file) === "/proc/self/ns/pid") throw Object.assign(new Error("procfs unavailable"), { code: "ENOENT" });
        return (readlink as (...args: unknown[]) => unknown)(file, ...args);
      }) as typeof fs.readlinkSync);
      const kill = vi.spyOn(process, "kill");
      await h.timeout();
      expect(kill).not.toHaveBeenCalled();
      expect(fs.readFileSync(h.ownerPath, "utf8")).toBe(owner);
      expect(h.fences()).toEqual([]);
    });

    it(`protocol ${label} never reclaims a foreign owner whose token changes between attempts`, async () => {
      const h = setup(protocol);
      const at = Date.now() - FORMER_UNSAFE_BOUND_MS - 1000;
      h.hold(h.receipt("previous", process.pid, at), at);
      const replacement = h.receipt("successor", process.pid, Date.now());
      const read = fs.readFileSync.bind(fs);
      let ownerReads = 0;
      vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
        if (String(file) === h.ownerPath && ++ownerReads === 2) fs.writeFileSync(h.ownerPath, replacement);
        return (read as (...args: unknown[]) => unknown)(file, ...args);
      }) as typeof fs.readFileSync);
      await h.timeout();
      expect(fs.readFileSync(h.ownerPath, "utf8")).toBe(replacement);
      expect(h.fences()).toEqual([]);
    });
  }
});
