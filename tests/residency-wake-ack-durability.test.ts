import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { describe, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import { MeshArchive } from "../src/mesh/archive.js";
import { MeshStore, type MeshEvent } from "../src/mesh/store.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { canonicalResidentWakeConfig, residentUnacknowledgedDeliveries } from "../src/residency/wake-index.js";
import * as wake from "../src/residency/wake.js";

// Keep reason: data safety at the durable publication / intent ACK / admission
// boundary (P1). The real archive and real maintenance must retain the exact
// delivery even if persisting the post-ACK refusal receipt fails.
const from = { id: "publisher", name: "publisher", kind: "main" as const };
const topic = "wake.ack.durability";
type Delivery = { id: string; sequence?: number | undefined };
type Receipt = { delivery: Delivery; error?: string; nextRetryAt?: number; claimId?: string };

describe("post-intent ACK refusal receipt durability", () => {
  it.each(["persisted", "EIO"] as const)("retries the archived delivery without rescue publication (receipt: %s)", async fault => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-ack-durability-"));
    const quarantine = path.join(directory, "product-removals");
    fs.mkdirSync(quarantine);
    let allowed = true;
    const mesh = new MeshStore(path.join(directory, "mesh"), 65_536, 100, {
      stateBackend: "file", canWakeResidents: () => allowed,
    });
    const rootId = "session:ack-durability";
    const root = residentRoot(mesh.root, rootId);
    const config = { rootId, residencyRoot: root, cwd: directory };
    const receiptPath = path.join(root, "wake-failure.json");
    fs.mkdirSync(root, { recursive: true });
    fs.writeFileSync(path.join(root, "config.json"), JSON.stringify(config));
    fs.writeFileSync(path.join(root, "wake-routes.json"), JSON.stringify({
      format: 1, rootId, hostId: "host:ack-durability",
      configJson: canonicalResidentWakeConfig(config),
      actors: [{ id: "actor:listener", name: "listener", topics: [topic] }],
    }));

    // Never execute product deletion. Preserve actual namespace changes by
    // renaming existing fixture-local targets; even atomic temp cleanup is caught.
    const removals: string[] = [];
    const preserve = (file: fs.PathLike): void => {
      // Linux releases its pinned owner through /proc/self/fd/<fd>/owner.
      // Resolve that alias before checking fixture custody and renaming it.
      const target = fs.existsSync(file) ? fs.realpathSync(file) : path.resolve(String(file));
      if (!target.startsWith(`${directory}${path.sep}`)) throw new Error(`refusing non-fixture deletion: ${target}`);
      removals.push(target);
      if (fs.existsSync(target)) fs.renameSync(target, path.join(quarantine, String(removals.length)));
    };
    vi.spyOn(fs, "rmSync").mockImplementation(file => preserve(file));
    vi.spyOn(fs, "unlinkSync").mockImplementation(file => preserve(file));
    vi.spyOn(fs, "rmdirSync").mockImplementation(file => preserve(file));

    const launches: Delivery[] = [];
    const launch = vi.fn(async (file: string, saved: ResidentHostConfig) => {
      expect(file).toBe(path.join(root, "config.json"));
      expect(saved).toEqual(config);
      const intent = wake.readWakeJson<wake.ResidentWakeRequest>(wake.residentWakeRequestPath(root));
      expect(intent).toBeDefined();
      launches.push({ id: intent!.id, sequence: intent!.sequence });
    });
    // Both store entry points keep their real implementation and live admission
    // callback. Only replace the native resident launch boundary.
    const dispatch = wake.wakeResidentActors;
    const publicationWakes = vi.spyOn(wake, "wakeResidentActors").mockImplementation(
      (store, events, _launch, recovery, ...rest) => dispatch(store, events, launch, recovery, ...rest));
    const retry = wake.retryResidentWakes;
    const maintenance = vi.spyOn(wake, "retryResidentWakes").mockImplementation(
      (store, _launch, now, ...rest) => retry(store, launch, now, ...rest));

    let entered!: () => void;
    let release!: () => void;
    const ackEntered = new Promise<void>(resolve => { entered = resolve; });
    const ackReleased = new Promise<void>(resolve => { release = resolve; });
    let suspended = false;
    let refusalWrites = 0;
    const writeJson = atomic.writeJsonAtomic;
    vi.spyOn(atomic, "writeJsonAtomic").mockImplementation((file, value, options) => {
      // An initial durable attempt receipt BEFORE ACK is a valid repair. Fail
      // ONLY the error-bearing refusal receipt after admission was revoked.
      const receipt = value as Partial<Receipt> | null;
      if (file === receiptPath && typeof receipt?.error === "string" && !allowed && suspended) {
        refusalWrites++;
        expect(receipt.error).toContain("admission closed");
        expect(launch).not.toHaveBeenCalled();
        if (fault !== "persisted") throw Object.assign(new Error("injected post-ACK refusal receipt EIO"), { code: "EIO" });
      }
      return writeJson(file, value, options);
    });
    const exclusive = mesh.exclusive.bind(mesh);
    vi.spyOn(mesh, "exclusive").mockImplementation(async <T>(operation: () => T, timeout?: number): Promise<T> => {
      // Intent-write custody starts without wake-request.json. Its first
      // subsequent custody request is the real onIntentWritten ACK callback.
      if (!suspended && fs.existsSync(wake.residentWakeRequestPath(root))) {
        suspended = true;
        entered();
        await ackReleased;
        return exclusive(operation, timeout);
      }
      return exclusive(operation, timeout);
    });

    let publishing: Promise<MeshEvent> | undefined;
    try {
      await wake.ensureResidentWakeArchive(mesh);
      publishing = mesh.publish({ topic, from, text: "retain the exact archived delivery" });
      await Promise.race([ackEntered, publishing.then(() => {
        throw new Error("publication returned without reaching post-intent ACK custody");
      })]);
      expect(publicationWakes).toHaveBeenCalledOnce();
      expect(launch).not.toHaveBeenCalled();
      const committed = mesh.read().filter(event => event.topic === topic);
      expect(committed).toHaveLength(1);
      const delivery = { id: committed[0]!.id, sequence: committed[0]!.sequence };
      const archive = MeshArchive.fromRoot(mesh.root)!;
      expect(archive.lookupEntry(delivery.sequence!)).toMatchObject({ committed: true, event: delivery });
      expect(wake.readWakeJson(wake.residentWakeRequestPath(root))).toMatchObject(delivery);
      expect(residentUnacknowledgedDeliveries(mesh.root).get(root)).toEqual(delivery);

      // Revoke in the ACK wait, not before intent writing. ACK itself still runs.
      allowed = false;
      release();
      const published = await publishing;
      expect(published).toEqual(committed[0]);
      expect(refusalWrites, "exercise the post-ACK refusal receipt, not initial attempt writes").toBe(1);
      expect(launch).not.toHaveBeenCalled();
      const receipt = wake.readWakeJson<Receipt>(receiptPath);
      const pending = residentUnacknowledgedDeliveries(mesh.root).get(root);
      expect.soft([pending, receipt?.delivery], "suppressed refusal persistence failure must leave a durable maintenance retry source")
        .toContainEqual(expect.objectContaining(delivery));
      if (fault === "persisted") expect(receipt).toMatchObject({ delivery, error: expect.stringContaining("admission closed") });

      allowed = true;
      const due = Math.max(Date.now(), receipt?.nextRetryAt ?? 0) + wake.RESIDENT_WAKE_RETRY_MIN_MS;
      vi.spyOn(Date, "now").mockReturnValue(due);
      await mesh.retryResidentWakes();
      expect(maintenance).toHaveBeenCalledOnce();
      expect.soft(launches, "real maintenance must launch the exact committed id AND sequence once").toEqual([delivery]);
      expect(residentUnacknowledgedDeliveries(mesh.root).size).toBe(0);
      expect(fs.existsSync(receiptPath)).toBe(false);
      await mesh.retryResidentWakes();
      await mesh.retryResidentWakes();
      expect.soft(launch, "repeated real maintenance must not duplicate the launch").toHaveBeenCalledOnce();
      expect(publicationWakes).toHaveBeenCalledOnce(); // No rescue publication.
      expect(mesh.read().filter(event => event.topic === topic)).toEqual([published]);
      expect(archive.lookupEntry(delivery.sequence!)).toMatchObject({ committed: true, event: delivery });
      expect(fs.existsSync(path.join(mesh.root, "wake-overflow"))).toBe(false);
    } finally {
      release();
      try { await publishing; }
      finally {
        // closeState is still product code: keep its deletion interception active.
        try { mesh.closeState(); }
        finally { vi.restoreAllMocks(); fs.rmSync(directory, { recursive: true, force: true }); }
      }
    }
  }, 10_000);

  it("an older successful publication cannot clear a newer refused publication's retry receipt", async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-ack-overlap-"));
    const quarantine = path.join(directory, "product-removals");
    fs.mkdirSync(quarantine);
    let allowed = true;
    const mesh = new MeshStore(path.join(directory, "mesh"), 65_536, 100, {
      stateBackend: "file", canWakeResidents: () => allowed,
    });
    const rootId = "session:overlap";
    const root = residentRoot(mesh.root, rootId);
    const config = { rootId, residencyRoot: root, cwd: directory };
    const receiptPath = path.join(root, "wake-failure.json");
    fs.mkdirSync(root, { recursive: true });
    fs.writeFileSync(path.join(root, "config.json"), JSON.stringify(config));
    fs.writeFileSync(path.join(root, "wake-routes.json"), JSON.stringify({ format: 1, rootId,
      hostId: "host:overlap", configJson: canonicalResidentWakeConfig(config),
      actors: [{ id: "actor:listener", name: "listener", topics: [topic] }] }));
    let removals = 0;
    const preserve = (file: fs.PathLike): void => {
      const target = fs.existsSync(file) ? fs.realpathSync(file) : path.resolve(String(file));
      if (!target.startsWith(`${directory}${path.sep}`)) throw new Error(`non-fixture deletion: ${target}`);
      if (fs.existsSync(target)) fs.renameSync(target, path.join(quarantine, String(++removals)));
    };
    vi.spyOn(fs, "rmSync").mockImplementation(preserve);
    vi.spyOn(fs, "unlinkSync").mockImplementation(preserve);
    vi.spyOn(fs, "rmdirSync").mockImplementation(preserve);
    let release!: () => void;
    let entered!: () => void;
    const launching = new Promise<void>(resolve => { entered = resolve; });
    const hold = new Promise<void>(resolve => { release = resolve; });
    const launches: Delivery[] = [];
    const launch = vi.fn(async () => {
      const intent = wake.readWakeJson<wake.ResidentWakeRequest>(wake.residentWakeRequestPath(root))!;
      launches.push({ id: intent.id, sequence: intent.sequence });
      if (launches.length === 1) { entered(); await hold; }
    });
    const dispatch = wake.wakeResidentActors;
    vi.spyOn(wake, "wakeResidentActors").mockImplementation((store, events, _launch, recovery, ...rest) =>
      dispatch(store, events, launch, recovery, ...rest));
    const retry = wake.retryResidentWakes;
    vi.spyOn(wake, "retryResidentWakes").mockImplementation((store, _launch, now, ...rest) => retry(store, launch, now, ...rest));
    const exclusive = mesh.exclusive.bind(mesh);
    let revokeNewer = false;
    vi.spyOn(mesh, "exclusive").mockImplementation(async <T>(operation: () => T, timeout?: number): Promise<T> => {
      const intent = wake.readWakeJson<wake.ResidentWakeRequest>(wake.residentWakeRequestPath(root));
      const ack = revokeNewer && intent && intent.id !== launches[0]?.id;
      const result = await exclusive(operation, timeout);
      if (ack) { revokeNewer = false; allowed = false; }
      return result;
    });
    let older: Promise<MeshEvent> | undefined;
    try {
      older = mesh.publish({ topic, from, text: "older launch in flight" });
      await launching;
      const first = launches[0]!;
      revokeNewer = true;
      const newer = await mesh.publish({ topic, from, text: "newer admission refusal" });
      expect(allowed).toBe(false);
      const delivery = { id: newer.id, sequence: newer.sequence };
      expect(delivery.id).not.toBe(first.id);
      const retained = fs.readFileSync(receiptPath, "utf8");
      expect(JSON.parse(retained)).toMatchObject({ delivery, error: expect.stringContaining("admission closed") });
      release();
      await older;
      expect(fs.readFileSync(receiptPath, "utf8"), "older success must leave the newer retry source untouched").toBe(retained);
      allowed = true;
      vi.spyOn(Date, "now").mockReturnValue(JSON.parse(retained).nextRetryAt + wake.RESIDENT_WAKE_RETRY_MIN_MS);
      await mesh.retryResidentWakes();
      await mesh.retryResidentWakes();
      await mesh.retryResidentWakes();
      expect(launches).toEqual([first, delivery]);
      expect(mesh.read({ topic })).toHaveLength(2);
      expect(fs.existsSync(receiptPath)).toBe(false);
    } finally {
      release();
      try { await older; }
      finally {
        try { mesh.closeState(); }
        finally { vi.restoreAllMocks(); fs.rmSync(directory, { recursive: true, force: true }); }
      }
    }
  }, 10_000);

  it("recovers after the publisher process exits after ACK, before recording the admission refusal", async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-ack-exit-"));
    const meshRoot = path.join(directory, "mesh");
    const rootId = "session:ack-exit";
    const root = residentRoot(meshRoot, rootId);
    const receiptPath = path.join(root, "wake-failure.json");
    const quarantine = path.join(directory, "product-removals");
    const config = { rootId, residencyRoot: root, cwd: directory };
    fs.mkdirSync(root, { recursive: true });
    fs.mkdirSync(quarantine);
    fs.writeFileSync(path.join(root, "config.json"), JSON.stringify(config));
    fs.writeFileSync(path.join(root, "wake-routes.json"), JSON.stringify({
      format: 1, rootId, hostId: "host:ack-exit", configJson: canonicalResidentWakeConfig(config),
      actors: [{ id: "actor:listener", name: "listener", topics: [topic] }],
    }));
    let mesh: MeshStore | undefined;
    try {
      // A real separate publisher exits after its real ACK transaction releases
      // custody. No catch/finally in wakeResidentActors can persist the refusal.
      const child = spawnSync("bun", ["--eval", `
        import fs from "node:fs";
        import path from "node:path";
        import { MeshStore } from ${JSON.stringify(path.resolve("src/mesh/store.ts"))};
        import * as wake from ${JSON.stringify(path.resolve("src/residency/wake.ts"))};
        const directory = ${JSON.stringify(directory)};
        const root = ${JSON.stringify(root)};
        let removals = 0;
        const preserve = file => {
          const target = fs.existsSync(file) ? fs.realpathSync(file) : path.resolve(String(file));
          if (!target.startsWith(directory + path.sep)) throw new Error("non-fixture deletion: " + target);
          if (fs.existsSync(target)) fs.renameSync(target, path.join(directory, "product-removals", "child-" + ++removals));
        };
        fs.rmSync = preserve; fs.unlinkSync = preserve; fs.rmdirSync = preserve;
        let allowed = true;
        const mesh = new MeshStore(${JSON.stringify(meshRoot)}, 65536, 100, {
          stateBackend: "file", canWakeResidents: () => allowed,
        });
        await wake.ensureResidentWakeArchive(mesh);
        const exclusive = mesh.exclusive.bind(mesh);
        let ack = false;
        mesh.exclusive = async (operation, timeout) => {
          const exiting = !ack && fs.existsSync(wake.residentWakeRequestPath(root));
          if (exiting) { ack = true; allowed = false; }
          const result = await exclusive(operation, timeout);
          if (exiting) process.exit(73);
          return result;
        };
        await mesh.publish({ topic: ${JSON.stringify(topic)}, from: ${JSON.stringify(from)}, text: "exit after ACK" });
        throw new Error("publisher missed the ACK exit boundary");
      `], { cwd: process.cwd(), encoding: "utf8", timeout: 10_000 });
      expect(child.error).toBeUndefined();
      expect(child.status, child.stderr).toBe(73);

      // Recovery starts in another process with no publisher-local store/cache.
      mesh = new MeshStore(meshRoot, 65_536, 100, { stateBackend: "file" });
      const [event] = mesh.read({ topic });
      expect(event).toBeDefined();
      const delivery = { id: event!.id, sequence: event!.sequence };
      expect(MeshArchive.fromRoot(meshRoot)?.lookupEntry(delivery.sequence!)).toMatchObject({ committed: true, event: delivery });
      expect(wake.readWakeJson(wake.residentWakeRequestPath(root))).toMatchObject(delivery);
      expect(residentUnacknowledgedDeliveries(meshRoot).size, "publisher exited AFTER its durable ACK").toBe(0);
      const receipt = wake.readWakeJson<Receipt>(receiptPath);
      expect.soft(receipt, "a retry receipt must already be durable when the publisher exits after ACK")
        .toMatchObject({ delivery, nextRetryAt: expect.any(Number), claimId: expect.any(String) });
      expect(receipt?.error).toBeUndefined(); // No post-ACK refusal updater executed.
      if (receipt) expect(receipt.nextRetryAt).toBeGreaterThan(Date.now());

      let removals = 0;
      const preserve = (file: fs.PathLike): void => {
        const target = fs.existsSync(file) ? fs.realpathSync(file) : path.resolve(String(file));
        if (!target.startsWith(`${directory}${path.sep}`)) throw new Error(`non-fixture deletion: ${target}`);
        if (fs.existsSync(target)) fs.renameSync(target, path.join(quarantine, `recovery-${++removals}`));
      };
      vi.spyOn(fs, "rmSync").mockImplementation(preserve);
      vi.spyOn(fs, "unlinkSync").mockImplementation(preserve);
      vi.spyOn(fs, "rmdirSync").mockImplementation(preserve);
      const launches: Delivery[] = [];
      const launch = vi.fn(async () => {
        const intent = wake.readWakeJson<wake.ResidentWakeRequest>(wake.residentWakeRequestPath(root))!;
        launches.push({ id: intent.id, sequence: intent.sequence });
      });
      const retry = wake.retryResidentWakes;
      vi.spyOn(wake, "retryResidentWakes").mockImplementation((store, _launch, now, ...rest) => retry(store, launch, now, ...rest));
      vi.spyOn(Date, "now").mockReturnValue(Math.max(Date.now(), receipt?.nextRetryAt ?? 0) + wake.RESIDENT_WAKE_RETRY_MIN_MS);
      await mesh.retryResidentWakes();
      expect.soft(launches, "fresh-process maintenance must recover exactly the ACKed delivery once").toEqual([delivery]);
      await mesh.retryResidentWakes();
      await mesh.retryResidentWakes();
      expect.soft(launch).toHaveBeenCalledOnce();
      expect(mesh.read({ topic })).toEqual([event]); // No rescue publication.
      expect(fs.existsSync(receiptPath)).toBe(false);
      expect(residentUnacknowledgedDeliveries(meshRoot).size).toBe(0);
    } finally {
      try { mesh?.closeState(); }
      finally { vi.restoreAllMocks(); fs.rmSync(directory, { recursive: true, force: true }); }
    }
  }, 15_000);
});
