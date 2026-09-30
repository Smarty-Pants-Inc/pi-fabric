import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { readHostLease, readHostLeases, writeHostLease, type FabricHostLease } from "../src/topology/host-leases.js";

// Windows fails an open with EPERM while the owner's heartbeat renames a new lease file over the
// old one. A live host then looked leaseless, and the failed read stayed cached until its next
// renewal: participants vanished from listings (pi-fabric main Test on windows-latest).
describe("host lease files on a transient read failure", () => {
  const roots: string[] = [];
  afterEach(() => {
    vi.restoreAllMocks();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });

  const setup = () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-leases-"));
    roots.push(root);
    const lease = (updatedAt: number): FabricHostLease => ({
      id: "host:a", rootId: "session:a", identityId: "session:a", updatedAt, expiresAt: updatedAt + 60_000,
    });
    const failReads = (times: number) => {
      const read = fs.readFileSync.bind(fs);
      let left = times;
      return vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
        if (left > 0 && String(file).includes("host-leases")) {
          left--;
          throw Object.assign(new Error("EPERM: operation not permitted, open"), { code: "EPERM" });
        }
        return (read as (...args: unknown[]) => unknown)(file, ...rest);
      }) as typeof fs.readFileSync);
    };
    return { root, lease, failReads };
  };

  it.each(["single", "all"])("reparses equal-size atomic replacements with preserved mtime through %s", (reader) => {
    const { root, lease } = setup();
    writeHostLease(root, lease(1_000));
    const dir = path.join(root, "host-leases");
    const file = path.join(dir, fs.readdirSync(dir)[0]!);
    // Use an exactly representable timestamp, independent of filesystem timestamp precision.
    fs.utimesSync(file, 1_000, 1_000);
    const before = fs.statSync(file);
    const read = () => reader === "single"
      ? readHostLease(root, "host:a")
      : readHostLeases(root).get("host:a");
    expect(read()).toEqual(lease(1_000));

    const replace = (text: string) => {
      expect(Buffer.byteLength(text)).toBe(before.size);
      const temporary = file + ".tmp";
      fs.writeFileSync(temporary, text);
      fs.utimesSync(temporary, before.atime, before.mtime);
      // A separate writer's on-disk operation: no writeHostLease or cache invalidation.
      fs.renameSync(temporary, file);
      expect(fs.statSync(file).mtimeMs).toBe(before.mtimeMs);
      expect(fs.statSync(file).size).toBe(before.size);
    };
    const renewed = JSON.stringify({ format: 1, ...lease(2_000) });
    replace(renewed);
    const reads = vi.spyOn(fs, "readFileSync");
    expect(read()).toEqual(lease(2_000));
    expect(readHostLease(root, "host:a")).toEqual(lease(2_000));
    expect(readHostLeases(root).get("host:a")).toEqual(lease(2_000));
    expect(reads.mock.calls.filter(([name]) => name === file)).toHaveLength(1);

    // Changed metadata never bypasses the parser or retains a formerly valid answer.
    replace(renewed.replace('"format":1', '"format":2'));
    expect(read()).toBeUndefined();
    expect(readHostLease(root, "host:a")).toBeUndefined();
    expect(readHostLeases(root).size).toBe(0);
    expect(reads.mock.calls.filter(([name]) => name === file)).toHaveLength(2);
  });

  it("uses ctime when filesystem device and inode values are unavailable", () => {
    const { root, lease } = setup();
    writeHostLease(root, lease(1_000));
    const dir = path.join(root, "host-leases");
    const file = path.join(dir, fs.readdirSync(dir)[0]!);
    const stat = fs.statSync.bind(fs);
    const before = stat(file);
    let ctimeMs = 1_000;
    vi.spyOn(fs, "statSync").mockImplementation(((name: fs.PathLike, ...rest: unknown[]) => {
      const result = (stat as (...args: unknown[]) => fs.Stats)(name, ...rest);
      return name === file
        ? Object.assign(result, { dev: 0, ino: 0, mtimeMs: before.mtimeMs, ctimeMs })
        : result;
    }) as typeof fs.statSync);
    expect(readHostLease(root, "host:a")).toEqual(lease(1_000));
    writeHostLease(root, lease(2_000));
    ctimeMs = 2_000;
    expect(readHostLeases(root).get("host:a")).toEqual(lease(2_000));
  });

  it("shares a valid unchanged cached lease without rereading", () => {
    const { root, lease } = setup();
    writeHostLease(root, lease(1_000));
    const file = path.join(root, "host-leases", fs.readdirSync(path.join(root, "host-leases"))[0]!);
    const reads = vi.spyOn(fs, "readFileSync");
    expect(readHostLeases(root).get("host:a")).toEqual(lease(1_000));
    expect(readHostLease(root, "host:a")).toEqual(lease(1_000));
    expect(readHostLeases(root).get("host:a")).toEqual(lease(1_000));
    expect(reads.mock.calls.filter(([name]) => name === file)).toHaveLength(1);
  });

  it("returns no lease for missing or removed files and directories", () => {
    const { root, lease } = setup();
    expect(readHostLease(root, "host:a")).toBeUndefined();
    expect(readHostLeases(root).size).toBe(0);
    writeHostLease(root, lease(1_000));
    expect(readHostLease(root, "host:a")).toEqual(lease(1_000));
    const dir = path.join(root, "host-leases");
    const file = path.join(dir, fs.readdirSync(dir)[0]!);
    fs.rmSync(file);
    expect(readHostLease(root, "host:a")).toBeUndefined();
    expect(readHostLeases(root).size).toBe(0);
    writeHostLease(root, lease(2_000));
    expect(readHostLeases(root).get("host:a")).toEqual(lease(2_000));
    fs.rmSync(dir, { recursive: true });
    expect(readHostLeases(root).size).toBe(0);
    expect(readHostLease(root, "host:a")).toBeUndefined();
  });

  it("retries a transient EPERM and reads the lease", () => {
    const { root, lease, failReads } = setup();
    writeHostLease(root, lease(1_000));
    failReads(2);
    expect(readHostLease(root, "host:a")?.updatedAt).toBe(1_000);
  });

  it("never caches a read that failed", () => {
    const { root, lease, failReads } = setup();
    writeHostLease(root, lease(1_000));
    const reads = failReads(100);
    expect(readHostLease(root, "host:a")).toBeUndefined();        // no answer yet
    expect(readHostLeases(root).size).toBe(0);
    reads.mockRestore();
    expect(readHostLease(root, "host:a")?.updatedAt).toBe(1_000); // the same file, read now
    expect(readHostLeases(root).get("host:a")?.updatedAt).toBe(1_000);
  });

  it("keeps the last lease while a renewed file cannot be read", () => {
    const { root, lease, failReads } = setup();
    writeHostLease(root, lease(1_000));
    expect(readHostLease(root, "host:a")?.updatedAt).toBe(1_000);
    writeHostLease(root, lease(2_000_000));                      // the renewal, being replaced
    const reads = failReads(100);
    expect(readHostLease(root, "host:a")?.updatedAt).toBe(1_000);
    expect(readHostLeases(root).get("host:a")?.updatedAt).toBe(1_000);
    reads.mockRestore();
    expect(readHostLease(root, "host:a")?.updatedAt).toBe(2_000_000);
  });
});
