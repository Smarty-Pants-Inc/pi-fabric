import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { describe, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import * as fileLock from "../src/residency/file-lock.js";
import { MeshArchive } from "../src/mesh/archive.js";
import { MeshStore, type MeshEvent } from "../src/mesh/store.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { canonicalResidentWakeConfig, residentUnacknowledgedDeliveries } from "../src/residency/wake-index.js";
import * as wake from "../src/residency/wake.js";

// Keep reason: data safety at the committed archive / publication ACK / owner
// admission boundary. Real publish and maintenance, injected native launch only.
// Barriers/faults observe real custody and writes; there are no polling timers.
const from = { id: "publisher", name: "publisher", kind: "main" as const };
const topic = "publication.admission.delivery";
type Delivery = { id: string; sequence?: number | undefined };
const deliveryOf = (event: Delivery): Delivery => ({ id: event.id, sequence: event.sequence });
const barrier = () => {
  let release!: () => void;
  const promise = new Promise<void>(resolve => { release = resolve; });
  return { promise, release };
};

function fixture() {
  // Keep the harness's inherited private temp root and deletion guard intact.
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "wake-publication-"));
  const quarantine = path.join(directory, "product-removals");
  fs.mkdirSync(quarantine);
  let allowed = true;
  const mesh = new MeshStore(path.join(directory, "mesh"), 65_536, 100, {
    stateBackend: "file", canWakeResidents: () => allowed,
  });
  const rootId = "session:publication-admission";
  const root = residentRoot(mesh.root, rootId);
  const config = { rootId, residencyRoot: root, cwd: directory };
  const receiptPath = path.join(root, "wake-failure.json");
  fs.mkdirSync(root, { recursive: true });
  fs.writeFileSync(path.join(root, "config.json"), JSON.stringify(config));
  fs.writeFileSync(path.join(root, "wake-routes.json"), JSON.stringify({
    format: 1, rootId, hostId: "host:publication-admission",
    configJson: canonicalResidentWakeConfig(config),
    actors: [{ id: "actor:listener", name: "listener", topics: [topic] }],
  }));
  // Product removals are namespace-preserving fixture renames, never deletion.
  // One root stays in the journal and never reaches overflow unlink.
  let removals = 0;
  const preserve = (file: fs.PathLike): void => {
    const target = fs.existsSync(file) ? fs.realpathSync(file) : path.resolve(String(file));
    if (!target.startsWith(directory + path.sep)) throw new Error(`non-fixture removal: ${target}`);
    if (fs.existsSync(target)) fs.renameSync(target, path.join(quarantine, `parent-${++removals}`));
  };
  vi.spyOn(fs, "rmSync").mockImplementation(preserve);
  vi.spyOn(fs, "unlinkSync").mockImplementation(preserve);
  vi.spyOn(fs, "rmdirSync").mockImplementation(preserve);
  const launches: Delivery[] = [];
  let launchEffect: (() => Promise<void>) | undefined;
  const launch = vi.fn(async (file: string, saved: ResidentHostConfig) => {
    expect(file).toBe(path.join(root, "config.json"));
    expect(saved).toEqual(config);
    const intent = wake.readWakeJson<wake.ResidentWakeRequest>(wake.residentWakeRequestPath(root));
    expect(intent).toBeDefined();
    launches.push(deliveryOf(intent!));
    await launchEffect?.();
  });
  const dispatch = wake.wakeResidentActors;
  const publicationWakes = vi.spyOn(wake, "wakeResidentActors").mockImplementation(
    (store, events, _launch, recovery, ...rest) => dispatch(store, events, launch, recovery, ...rest));
  const retry = wake.retryResidentWakes;
  const maintenance = vi.spyOn(wake, "retryResidentWakes").mockImplementation(
    (store, _launch, now, ...rest) => retry(store, launch, now, ...rest));
  return {
    directory, root, mesh, config, receiptPath, launches, launch, publicationWakes, maintenance,
    get allowed() { return allowed; }, set allowed(value: boolean) { allowed = value; },
    set launchEffect(value: (() => Promise<void>) | undefined) { launchEffect = value; },
    async prepare() { await wake.ensureResidentWakeArchive(mesh); },
    assertArchived(delivery: Delivery) {
      expect(MeshArchive.fromRoot(mesh.root)?.lookupEntry(delivery.sequence!))
        .toMatchObject({ committed: true, event: delivery });
      expect(fs.existsSync(path.join(mesh.root, "wake-overflow"))).toBe(false);
    },
    close() {
      try { mesh.closeState(); }
      finally { vi.restoreAllMocks(); fs.rmSync(directory, { recursive: true, force: true }); }
    },
  };
}

async function reach(entered: Promise<void>, operation: Promise<unknown>) {
  await Promise.race([entered, operation.then(() => { throw new Error("missed custody barrier"); })]);
}

// Publication must not exchange unserved pending work for a best-effort
// refusal receipt. Once served, ACK is independent of later admission closure.
describe("publication success-before-ACK ordering guard", () => {
  it.skipIf(process.platform === "win32").each([false, true])("rechecks admission before the real intent write (open: %s)", async staysOpen => {
    const f = fixture();
    const entered = barrier();
    const hold = barrier();
    let publishing: Promise<MeshEvent> | undefined;
    let suspended = false;
    const lock = fileLock.lockFile;
    vi.spyOn(fileLock, "lockFile").mockImplementation(async (...args) => {
      const fd = await lock(...args);
      if (!suspended && args[0] === wake.residentWakeIntentLockPath(f.root)) {
        suspended = true;
        entered.release();
        await hold.promise; // Actual intent lock acquired; real write custody is still ahead.
      }
      return fd;
    });
    try {
      await f.prepare();
      publishing = f.mesh.publish({ topic, from, text: "gate changes during intent custody" });
      await reach(entered.promise, publishing);
      const [event] = f.mesh.read({ topic });
      const delivery = deliveryOf(event!);
      f.assertArchived(delivery);
      expect(fs.existsSync(wake.residentWakeRequestPath(f.root))).toBe(false);
      f.allowed = staysOpen;
      hold.release();
      expect(await publishing).toEqual(event);
      expect(f.launches).toEqual(staysOpen ? [delivery] : []);
      expect(fs.existsSync(wake.residentWakeRequestPath(f.root))).toBe(staysOpen);
      expect(residentUnacknowledgedDeliveries(f.mesh.root).get(f.root)).toEqual(staysOpen ? undefined : delivery);
      expect(fs.existsSync(f.receiptPath)).toBe(false);
      await f.mesh.retryResidentWakes();
      expect(f.launches).toEqual(staysOpen ? [delivery] : []);
      f.allowed = true;
      await f.mesh.retryResidentWakes();
      await f.mesh.retryResidentWakes();
      expect(f.launches).toEqual([delivery]);
      expect(residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
      expect(f.mesh.read({ topic })).toEqual([event]);
    } finally {
      hold.release();
      try { await publishing; } finally { f.close(); }
    }
  }, 10_000);

  it.each([
    ["intent-write", false], ["intent-write", true], ["ACK", false], ["ACK", true],
  ] as const)("rechecks %s custody (gate stays open: %s)", async (boundary, staysOpen) => {
    const f = fixture();
    const entered = barrier();
    const hold = barrier();
    let publishing: Promise<MeshEvent> | undefined;
    let suspended = false;
    const exclusive = f.mesh.exclusive.bind(f.mesh);
    vi.spyOn(f.mesh, "exclusive").mockImplementation(async <T>(operation: () => T, timeout?: number): Promise<T> => {
      if (boundary === "ACK" && !suspended && f.launches.length === 1) {
        suspended = true;
        entered.release();
        await hold.promise;
      }
      const hadIntent = fs.existsSync(wake.residentWakeRequestPath(f.root));
      const result = await exclusive(operation, timeout);
      if (boundary === "intent-write" && !suspended && !hadIntent &&
          fs.existsSync(wake.residentWakeRequestPath(f.root))) {
        // This is the actual durable intent transaction's return, while the
        // caller still holds intent.lock, not a test-only no-op transaction.
        suspended = true;
        entered.release();
        await hold.promise;
      }
      return result;
    });
    let refusalWrites = 0;
    const write = atomic.writeJsonAtomic;
    vi.spyOn(atomic, "writeJsonAtomic").mockImplementation((file, value, options) => {
      if (file === f.receiptPath && !f.allowed && typeof (value as { error?: string }).error === "string") {
        refusalWrites++;
        throw Object.assign(new Error("selective refusal persistence EIO"), { code: "EIO" });
      }
      return write(file, value, options);
    });
    try {
      await f.prepare();
      publishing = f.mesh.publish({ topic, from, text: "committed before closure" });
      await reach(entered.promise, publishing);
      expect(f.launches).toHaveLength(boundary === "ACK" ? 1 : 0);
      const [committed] = f.mesh.read({ topic });
      const delivery = deliveryOf(committed!);
      f.assertArchived(delivery);
      expect(wake.readWakeJson(wake.residentWakeRequestPath(f.root))).toMatchObject(delivery);
      expect(residentUnacknowledgedDeliveries(f.mesh.root).get(f.root)).toEqual(delivery);
      // No publisher attempt receipt exists, even after a successful launch.
      expect(fs.existsSync(f.receiptPath)).toBe(false);
      f.allowed = staysOpen;
      hold.release();
      expect(await publishing).toEqual(committed);
      expect(refusalWrites).toBe(0);
      expect(fs.existsSync(f.receiptPath)).toBe(false);
      const served = staysOpen || boundary === "ACK";
      expect(residentUnacknowledgedDeliveries(f.mesh.root).get(f.root)).toEqual(served ? undefined : delivery);
      const initialLaunches = served ? 1 : 0;
      expect(f.launches).toHaveLength(initialLaunches);
      await f.mesh.retryResidentWakes();
      expect(f.launches).toHaveLength(initialLaunches); // Closed gate does not retry.
      f.allowed = true;
      await f.mesh.retryResidentWakes();
      const total = 1; // Delayed successful ACK closes pending even if the owner gate closed.
      expect(f.launches).toEqual(Array.from({ length: total }, () => delivery));
      expect(residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
      expect(fs.existsSync(f.receiptPath)).toBe(false);
      await f.mesh.retryResidentWakes();
      await f.mesh.retryResidentWakes();
      expect(f.launch).toHaveBeenCalledTimes(total);
      expect(f.publicationWakes).toHaveBeenCalledOnce(); // No rescue publication.
      expect(f.mesh.read({ topic })).toEqual([committed]);
      f.assertArchived(delivery);
    } finally {
      hold.release();
      try { await publishing; } finally { f.close(); }
    }
  }, 10_000);

  it.each(["persisted", "EIO"] as const)("a failed launch keeps pending (failure receipt: %s)", async fault => {
    const f = fixture();
    let failedWrites = 0;
    const write = atomic.writeJsonAtomic;
    vi.spyOn(atomic, "writeJsonAtomic").mockImplementation((file, value, options) => {
      if (file === f.receiptPath && typeof (value as { error?: string }).error === "string") {
        failedWrites++;
        if (fault === "EIO") throw Object.assign(new Error("selective failure persistence EIO"), { code: "EIO" });
      }
      return write(file, value, options);
    });
    vi.spyOn(console, "warn").mockImplementation(() => {});
    f.launchEffect = async () => { throw new Error("injected native launch failure"); };
    try {
      await f.prepare();
      const event = await f.mesh.publish({ topic, from, text: "real failure without durable receipt" });
      const delivery = deliveryOf(event);
      expect(failedWrites).toBe(1);
      expect(residentUnacknowledgedDeliveries(f.mesh.root).get(f.root)).toEqual(delivery);
      expect(fs.existsSync(f.receiptPath)).toBe(fault === "persisted");
      f.assertArchived(delivery);
      f.launchEffect = undefined;
      const receipt = wake.readWakeJson<{ nextRetryAt: number }>(f.receiptPath);
      if (receipt) vi.spyOn(Date, "now").mockReturnValue(receipt.nextRetryAt);
      await f.mesh.retryResidentWakes();
      await f.mesh.retryResidentWakes();
      expect(f.launches).toEqual([delivery, delivery]); // One failed attempt, one success.
      expect(residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
      expect(f.mesh.read({ topic })).toEqual([event]);
    } finally { f.close(); }
  });

  it("ACK write EIO after success keeps pending without a generic-failure ACK", async () => {
    const f = fixture();
    let broken = true;
    let ackWrites = 0;
    let failureWrites = 0;
    const writeFile = fs.writeFileSync;
    vi.spyOn(fs, "writeFileSync").mockImplementation((file, data, options) => {
      if (typeof data === "string" && data.includes('"op":"ack"') && JSON.parse(data).root === f.root) {
        ackWrites++;
        if (broken) throw Object.assign(new Error("selective ACK write EIO"), { code: "EIO" });
      }
      return writeFile(file, data, options);
    });
    const write = atomic.writeJsonAtomic;
    vi.spyOn(atomic, "writeJsonAtomic").mockImplementation((file, value, options) => {
      if (file === f.receiptPath && typeof (value as { error?: string }).error === "string") failureWrites++;
      return write(file, value, options);
    });
    vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      await f.prepare();
      const event = await f.mesh.publish({ topic, from, text: "success followed by ACK EIO" });
      const delivery = deliveryOf(event);
      expect(f.launches).toEqual([delivery]);
      expect(ackWrites).toBe(1);
      expect(failureWrites).toBe(0);
      expect(fs.existsSync(f.receiptPath)).toBe(false);
      expect(residentUnacknowledgedDeliveries(f.mesh.root).get(f.root)).toEqual(delivery);
      f.assertArchived(delivery);
      broken = false;
      await f.mesh.retryResidentWakes();
      await f.mesh.retryResidentWakes();
      expect(f.launches).toEqual([delivery, delivery]); // At-least-once after an uncommitted ACK.
      expect(residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
      expect(failureWrites).toBe(0);
      expect(f.mesh.read({ topic })).toEqual([event]);
    } finally { f.close(); }
  });

  it.skipIf(process.platform === "win32")("ACKs the actual adopted newer intent, not the stale captured publication", async () => {
    const f = fixture();
    const entered = barrier();
    const hold = barrier();
    let older: Promise<MeshEvent> | undefined;
    let firstLock = true;
    let failNewerACK = true;
    const lock = fileLock.lockFile;
    vi.spyOn(fileLock, "lockFile").mockImplementation(async (...args) => {
      if (args[0] === wake.residentWakeIntentLockPath(f.root) && firstLock) {
        firstLock = false;
        entered.release();
        await hold.promise; // Newer publisher gets actual intent custody first.
      }
      return lock(...args);
    });
    const writeFile = fs.writeFileSync;
    vi.spyOn(fs, "writeFileSync").mockImplementation((file, data, options) => {
      if (failNewerACK && typeof data === "string" && data.includes('"op":"ack"') && JSON.parse(data).root === f.root) {
        failNewerACK = false;
        throw Object.assign(new Error("newer ACK write EIO"), { code: "EIO" });
      }
      return writeFile(file, data, options);
    });
    vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      await f.prepare();
      older = f.mesh.publish({ topic, from, text: "older waits for intent custody" });
      await reach(entered.promise, older);
      const newer = await f.mesh.publish({ topic, from, text: "newer intent wins" });
      const delivery = deliveryOf(newer);
      expect(failNewerACK).toBe(false);
      expect(residentUnacknowledgedDeliveries(f.mesh.root).get(f.root)).toEqual(delivery);
      expect(fs.existsSync(f.receiptPath)).toBe(false);
      f.allowed = true;
      hold.release();
      const old = await older;
      expect(old.sequence).toBeLessThan(newer.sequence);
      expect(wake.readWakeJson(wake.residentWakeRequestPath(f.root))).toMatchObject(delivery);
      expect(f.launches).toEqual([delivery, delivery]);
      expect(residentUnacknowledgedDeliveries(f.mesh.root).size).toBe(0);
      await f.mesh.retryResidentWakes();
      expect(f.launches).toEqual([delivery, delivery]);
      expect(f.mesh.read({ topic })).toEqual([old, newer]);
      f.assertArchived(delivery);
    } finally {
      hold.release();
      try { await older; } finally { f.close(); }
    }
  }, 10_000);

  it.each(["before-launch", "after-launch"] as const)("fresh maintenance recovers a real publisher exit %s, before ACK", async boundary => {
    const f = fixture();
    try {
      await f.prepare();
      const child = spawnSync("bun", ["--eval", `
        import fs from "node:fs";
        import path from "node:path";
        import { mock } from "bun:test";
        import { MeshStore } from ${JSON.stringify(path.resolve("src/mesh/store.ts"))};
        import * as wake from ${JSON.stringify(path.resolve("src/residency/wake.ts"))};
        const directory = ${JSON.stringify(f.directory)};
        const root = ${JSON.stringify(f.root)};
        const boundary = ${JSON.stringify(boundary)};
        let removals = 0;
        const preserve = file => {
          const target = fs.existsSync(file) ? fs.realpathSync(file) : path.resolve(String(file));
          if (!target.startsWith(directory + path.sep)) throw new Error("non-fixture removal: " + target);
          if (fs.existsSync(target)) fs.renameSync(target, path.join(directory, "product-removals", "child-" + ++removals));
        };
        fs.rmSync = preserve; fs.unlinkSync = preserve; fs.rmdirSync = preserve;
        const mesh = new MeshStore(${JSON.stringify(f.mesh.root)}, 65536, 100, { stateBackend: "file" });
        let launched = false;
        const dispatch = wake.wakeResidentActors;
        mock.module(${JSON.stringify(path.resolve("src/residency/wake.ts"))}, () => ({
          ...wake,
          wakeResidentActors: (store, events, _launch, recovery, ...rest) => dispatch(store, events, async () => {
            launched = true;
            const intent = wake.readWakeJson(wake.residentWakeRequestPath(root));
            fs.writeFileSync(path.join(directory, "child-launch.json"), JSON.stringify({ id: intent.id, sequence: intent.sequence }));
          }, recovery, ...rest),
        }));
        const exclusive = mesh.exclusive.bind(mesh);
        let exited = false;
        mesh.exclusive = async (operation, timeout) => {
          if (boundary === "after-launch" && launched) process.exit(73); // ACK has not acquired custody.
          const hadIntent = fs.existsSync(wake.residentWakeRequestPath(root));
          const result = await exclusive(operation, timeout);
          if (boundary === "before-launch" && !exited && !hadIntent && fs.existsSync(wake.residentWakeRequestPath(root))) {
            exited = true;
            process.exit(73); // Actual durable intent transaction returned; no catch/finally.
          }
          return result;
        };
        await mesh.publish({ topic: ${JSON.stringify(topic)}, from: ${JSON.stringify(from)}, text: "real publisher crash" });
        throw new Error("missed crash boundary");
      `], { cwd: process.cwd(), encoding: "utf8", timeout: 10_000 });
      expect(child.error).toBeUndefined();
      expect(child.status, child.stderr).toBe(73);
      // A newly constructed store has no publishing process's local state.
      f.mesh.closeState();
      const fresh = new MeshStore(f.mesh.root, 65_536, 100, { stateBackend: "file" });
      try {
        const [event] = fresh.read({ topic });
        expect(event).toBeDefined();
        const delivery = deliveryOf(event!);
        f.assertArchived(delivery);
        expect(wake.readWakeJson(wake.residentWakeRequestPath(f.root))).toMatchObject(delivery);
        expect(residentUnacknowledgedDeliveries(fresh.root).get(f.root)).toEqual(delivery);
        expect(fs.existsSync(f.receiptPath)).toBe(false); // Recovery is from pending, not a rescue receipt.
        const launched = wake.readWakeJson<Delivery>(path.join(f.directory, "child-launch.json"));
        expect(launched).toEqual(boundary === "after-launch" ? delivery : undefined);
        await fresh.retryResidentWakes();
        await fresh.retryResidentWakes();
        expect(f.launches).toEqual([delivery]);
        expect(f.maintenance).toHaveBeenCalledTimes(2);
        expect(residentUnacknowledgedDeliveries(fresh.root).size).toBe(0);
        expect(fs.existsSync(f.receiptPath)).toBe(false);
        expect(f.publicationWakes).not.toHaveBeenCalled(); // No parent rescue publication.
        expect(fresh.read({ topic })).toEqual([event]);
        // after-launch: one child success + one recovery success is permitted.
      } finally { fresh.closeState(); }
    } finally { f.close(); }
  }, 15_000);
});
