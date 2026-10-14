import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshBackgroundRetry, MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// P1 hosted-suite OOM: a permanently failing participant refresh (EACCES, a blocked confirmation) reset its
// backoff on every non-transient failure, so canConsumeMesh() gates and change events retriggered it at once,
// each attempt printing "background operation failed" (millions of lines). A failed background refresh now
// waits out an exponential backoff and repeats its line at most once per warning window.
const roots: string[] = [];
const directories: ParticipantDirectory[] = [];
afterEach(async () => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  await Promise.all(directories.splice(0).map(directory => directory.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const eacces = (): Error => Object.assign(new Error("EACCES: permission denied, open '/mesh/hosts/owner.json'"), { code: "EACCES" });
const failed = (warn: { mock: { calls: unknown[][] } }): string[] =>
  warn.mock.calls.map(call => String(call[0])).filter(line => line.includes("background operation failed"));

describe("background failure backoff (opt-in)", () => {
  it("backs off a repeated non-transient failure and repeats its line once per window", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const retry = new MeshBackgroundRetry("refresh").backoffFailures(1_000, 4_000);
    let attempts = 0;
    for (let now = 0; now < 60_000; now += 10) {
      vi.setSystemTime(now);
      await retry.run(() => { attempts++; throw eacces(); }, false);
    }
    // 0, 1 s, 3 s, 7 s, then every 4 s: 17 attempts in a minute instead of 6000.
    expect(attempts).toBeLessThanOrEqual(18);
    expect(failed(warn)).toHaveLength(1);
    expect(retry.waitMs).toBe(3_010); // last attempt at 59 s; the next is due at 63 s (clock at 59.99 s)
    vi.setSystemTime(63_000);
    expect(await retry.run(() => { throw eacces(); }, false)).toBe("failed"); // classification preserved
    expect(failed(warn)).toHaveLength(2);
    expect(failed(warn)[1]).toMatch(/similar failures suppressed/);
    // A different error is reported at once; a success clears the backoff.
    vi.setSystemTime(70_000);
    await retry.run(() => { throw new Error("superseded"); }, false);
    expect(failed(warn)).toHaveLength(3);
    retry.success();
    expect(retry.waitMs).toBe(0);
    // Typed lock timeouts keep their own retry classification and warning.
    expect(await retry.run(() => { throw new MeshLockTimeoutError("", 1, 1); }, false)).toBe("retry");
  });

  it("leaves the default (queue) boundary unchanged: no backoff for a non-transient failure", async () => {
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const retry = new MeshBackgroundRetry("handler");
    expect(await retry.run(() => { throw new Error("bug"); })).toBe("failed");
    expect(retry.waitMs).toBe(0);
  });
});

describe("participant heartbeat/change refresh under a permanent failure", () => {
  it("bounds attempts and warning lines over a minute of noisy canConsumeMesh/change triggers; explicit refresh recovers at once", { timeout: 30_000 }, async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-refresh-backoff-"));
    roots.push(root);
    const mesh = new MeshStore(root, 65536, 1000, { lockTimeoutMs: 500 });
    const identity = { id: "owner", name: "owner", kind: "main" as const };
    let failing = false;
    let attempts = 0;
    const directory = new ParticipantDirectory(mesh, {
      enabled: true, hostId: "owner", rootId: "root", identity, heartbeatMs: 5_000, leaseMs: 600_000,
      beforePublication: async () => {
        if (!failing) return;
        attempts++;
        throw eacces();
      },
    });
    directories.push(directory);
    let revision = 1;
    const record = (): FabricParticipantRecord => ({ format: 1, id: "actor", kind: "actor", rootId: "root", ownerHostId: "owner",
      ownerIdentityId: "owner", name: "actor", status: "idle", runner: "pi", transport: "process", capabilities: [],
      controlProtocol: "v1", startedAt: 1, updatedAt: revision });
    directory.registerSource(() => [record()]);
    await directory.start();
    expect(directory.canConsumeMesh()).toBe(true);

    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const base = Date.now();
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(base);
    failing = true;
    await directory.refresh().catch(() => undefined); // the explicit attempt that starts the outage
    // A simulated minute: consumers gate every 100 ms, and every tick is also a change event.
    for (let elapsed = 0; elapsed <= 60_000; elapsed += 100) {
      vi.setSystemTime(base + elapsed);
      for (let gate = 0; gate < 3; gate++) expect(directory.canConsumeMesh()).toBe(false);
      revision++;
      directory.scheduleRefresh();
      await new Promise(resolve => setTimeout(resolve, 2)); // let the triggered attempt settle (real clock)
    }
    // Backoff 1 s, 2 s, then the 2.5 s cap (half a heartbeat): about 25 attempts, not one per trigger (>600).
    expect(attempts).toBeGreaterThan(3);
    expect(attempts).toBeLessThanOrEqual(30);
    expect(failed(warn).length).toBeGreaterThanOrEqual(1);
    expect(failed(warn).length).toBeLessThanOrEqual(2);

    // Counterexample: an explicit refresh is not held back by the background backoff and recovers now.
    failing = false;
    await directory.refresh();
    expect(directory.canConsumeMesh()).toBe(true);
  });
});
