import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { isMeshLockTimeout } from "../src/core/atomic-write.js";
import { MeshCustodyUnrecoverableError } from "../src/mesh/custody-lock.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { HostLeaseLockBusyError } from "../src/topology/host-lease-lock.js";
import { FabricHostLeaseSupersededError, hostLeasePath, readHostLeaseCurrent, renewHostLease,
  withOwnedHostLease } from "../src/topology/host-leases.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readParticipantFile } from "../src/topology/participant-files.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// Keep reason: a process-boundary release interleaving and lease/record data safety.
// Every root, coordination file and deletion is confined to this suite's private temp directories.
const roots: string[] = [], stores: MeshStore[] = [], directories: ParticipantDirectory[] = [];
afterEach(async () => {
  for (const directory of directories.splice(0)) await directory.close();
  for (const mesh of stores.splice(0)) mesh.closeState();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const identity: MeshIdentity = { id: "contention-owner", name: "owner", kind: "agent" };
const hostId = "contention-host", rootId = "contention-root";
const key = (prefix: string, id: string) => prefix + createHash("sha256").update(id).digest("hex");
const hostKey = key("topology/hosts/", hostId), participantKey = key("topology/participants/", rootId);
const custodyLock = (root: string) => path.join(root, "host-lease-commits",
  createHash("sha256").update(path.basename(hostLeasePath(root, hostId))).digest("hex").slice(0, 16), "custody.lock");
const makeDirectory = (mesh: MeshStore, heartbeat = false) => {
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId, rootId, identity,
    heartbeatMs: 100, leaseMs: 1_000, reapDeadHosts: false, live: () => heartbeat,
    ...(heartbeat ? { random: () => 0.75 } : {}) });
  directories.push(directory);
  const record: FabricParticipantRecord = { format: 1, id: rootId, rootId, ownerHostId: hostId,
    ownerIdentityId: identity.id, kind: "root", name: "root", status: "idle", runner: "pi", transport: "host",
    capabilities: ["fabric"], controlProtocol: "v1", startedAt: Date.now(), updatedAt: Date.now() };
  directory.registerSource(() => [record]);
  return directory;
};
const setup = async (stateBackend: "file" | "sqlite", heartbeat = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "host-lease-contention-")); roots.push(root);
  const mesh = new MeshStore(root, 65_536, 100, { stateBackend, lockTimeoutMs: 80 }); stores.push(mesh);
  expect(mesh.stateBackend).toBe(stateBackend);
  const directory = makeDirectory(mesh, heartbeat);
  // Existing controls use explicit refresh; only the deadline case opts into the real timer.
  if (heartbeat) await directory.start();
  else await directory.refresh();
  const read = () => readHostLeaseCurrent(root, hostId)!;
  expect(read()).toBeDefined();
  return { root, mesh, directory, read, lease: read(), file: hostLeasePath(root, hostId), lock: custodyLock(root) };
};

// The child takes a real MeshStore writer transaction or host-qualified leaseCustody.
// IPC is the event source. Only a synchronous critical section needs the bounded file
// handoff (10 ms safety polling, at most 5 s); it cannot service IPC while paused.
const holdInChild = (root: string, backend: "file" | "sqlite", mode: "writer" | "custody" | "pinned-release") => {
  const control = fs.mkdtempSync(path.join(root, "holder-"));
  const child = spawn(process.execPath, ["--input-type=module", "-e", `
    import fs from "node:fs";
    import path from "node:path";
    import { performance } from "node:perf_hooks";
    import { createJiti } from "jiti";
    import { pathToFileURL } from "node:url";
    const [root, backend, mode, file, lock, control] = process.argv.slice(1);
    const safety = setTimeout(() => process.exit(2), 15000);
    const sleepCell = new Int32Array(new SharedArrayBuffer(4));
    const waitFile = name => {
      const deadline = performance.now() + 5000;
      while (!fs.existsSync(path.join(control, name))) {
        if (performance.now() >= deadline) throw new Error("Holder handoff timed out: " + name);
        Atomics.wait(sleepCell, 0, 0, 10);
      }
    };
    globalThis[Symbol.for("pi-fabric.mesh.sqlite-initialize.test-fixtures")] = "create";
    const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
    const { MeshStore } = await jiti.import("./src/mesh/store.ts");
    const mesh = new MeshStore(root, 65536, 100, { stateBackend: backend });
    const unlink = fs.unlinkSync;
    if (mode === "pinned-release") {
      fs.unlinkSync = (...args) => {
        const name = String(args[0]);
        const ours = /^\\/proc\\/self\\/fd\\/\\d+\\/owner$/.test(name) &&
          fs.realpathSync(path.dirname(name)) === fs.realpathSync(lock);
        const result = unlink(...args); // The actual production unlink happens FIRST.
        if (ours) {
          process.send({ phase: "ownerless", pid: process.pid });
          waitFile("resume"); // Pause only between the pinned unlink and production rmdir.
        }
        return result;
      };
    }
    try {
      if (mode === "writer") {
        await mesh.writeBatch({ identity: { id: "holder", name: "holder", kind: "agent" }, ops: [],
          prepare: () => {
            process.send({ phase: "held", pid: process.pid });
            waitFile("release");
            return [{ kind: "put", key: "test/holder-committed", value: true }];
          } });
      } else {
        await mesh.leaseCustody(file, async () => {
          const released = new Promise(resolve => process.once("message", resolve));
          process.send({ phase: "held", pid: process.pid });
          await released;
        });
      }
    } finally {
      fs.unlinkSync = unlink;
      mesh.closeState();
      clearTimeout(safety);
      process.disconnect();
    }
  `, root, backend, mode, hostLeasePath(root, hostId), custodyLock(root), control], {
    cwd: process.cwd(), stdio: ["ignore", "ignore", "pipe", "ipc"],
  });
  let stderr = "";
  child.stderr!.on("data", chunk => { stderr += chunk; });
  const phases = new Set<string>();
  const pending = new Map<string, { resolve: () => void; reject: (error: Error) => void }>();
  let closed: { code: number | null; signal: NodeJS.Signals | null } | undefined;
  let failed: Error | undefined;
  child.on("message", message => {
    const event = message as { phase: string; pid: number };
    if (event.pid !== child.pid) return;
    phases.add(event.phase); pending.get(event.phase)?.resolve();
  });
  const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(resolve => {
    child.once("error", error => {
      failed = error;
      for (const waiter of pending.values()) waiter.reject(error);
    });
    child.once("close", (code, signal) => {
      closed = { code, signal };
      for (const waiter of pending.values()) waiter.reject(new Error(`Holder exited before phase: ${stderr}`));
      resolve(closed);
    });
  });
  return {
    child,
    wait: (phase: string) => new Promise<void>((resolve, reject) => {
      if (failed) { reject(failed); return; }
      if (phases.has(phase)) { resolve(); return; }
      if (closed) { reject(new Error(`Holder already exited: ${stderr}`)); return; }
      const timer = setTimeout(() => finish(new Error(`Holder ${phase} timed out: ${stderr}`)), 10_000);
      const finish = (error?: Error) => {
        clearTimeout(timer); pending.delete(phase);
        if (error) reject(error); else resolve();
      };
      pending.set(phase, { resolve: () => finish(), reject: finish });
    }),
    beginRelease: () => {
      fs.writeFileSync(path.join(control, "release"), "release");
      if (mode !== "writer" && child.connected) child.send("release");
    },
    finishRelease: async () => {
      fs.writeFileSync(path.join(control, "resume"), "resume");
      expect(await exited, stderr).toEqual({ code: 0, signal: null });
    },
    stop: async () => {
      if (!closed) child.kill("SIGKILL");
      await exited;
    },
  };
};

describe.each(["file", "sqlite"] as const)("%s real host-lease contention", backend => {
  it("renews on the scheduled near-expiry heartbeat and owns custody after the original TTL with the shared writer still held", async () => {
    const clockStart = 1_800_000_000_000;
    // Control only the parent's deadline ordering, not child IPC/I/O or wall-clock speed.
    // SQLite budgets use perf_hooks; keep that clock advancing with the lease clock too.
    vi.useFakeTimers({ now: clockStart, toFake: ["Date", "setTimeout", "clearTimeout"] });
    const monotonic = vi.spyOn(performance, "now").mockImplementation(() => Date.now() - clockStart);
    try {
      const s = await setup(backend, true), holder = holdInChild(s.root, backend, "writer");
      const actualLease = () => JSON.parse(fs.readFileSync(s.file, "utf8"));
      const assertWriterHeld = async () => {
        expect(holder.child.exitCode).toBeNull();
        const blocked = await s.mesh.withTryLock(() => s.mesh.put({ key: "test/must-not-commit", value: true, identity }), 0)
          .then(() => undefined, (error: unknown) => error);
        expect(isMeshLockTimeout(blocked)).toBe(true);
        expect(s.mesh.get("test/holder-committed", { fresh: true })).toBeUndefined();
      };
      try {
        await holder.wait("held");
        const original = actualLease(), confirmedAt = s.directory.confirmedAt();
        expect(original).toEqual({ format: 1, ...s.lease });
        expect(original.updatedAt).toBe(clockStart);
        expect(original.expiresAt).toBe(clockStart + 1_000);
        expect(s.directory.heartbeatArmed).toBe(true);
        await assertWriterHeld();

        // Model a late first scheduled tick: move lease/monotonic clocks forward WITHOUT
        // firing the armed 110 ms jittered timer. Its callback now runs 10 ms before TTL.
        vi.setSystemTime(original.expiresAt - 120);
        await vi.advanceTimersByTimeAsync(109);
        expect(Date.now()).toBe(original.expiresAt - 11);
        expect(actualLease()).toEqual(original); // No direct/manual renewal supplied the proof.
        await vi.advanceTimersByTimeAsync(1);
        const heartbeatAt = Date.now(), renewed = actualLease();
        expect(heartbeatAt).toBe(original.expiresAt - 10);
        expect(heartbeatAt).toBeLessThan(original.expiresAt);
        expect(renewed).toEqual({ ...original, updatedAt: heartbeatAt, expiresAt: heartbeatAt + 1_000 });
        expect(renewed.incarnationToken).toBeTruthy();
        expect(s.directory.confirmedAt()).toBe(confirmedAt); // File liveness is not shared admission.
        await assertWriterHeld();

        // Cross the ORIGINAL expiry before releasing the real child writer. Let the shared
        // publication budget unwind (file clamps to 100 ms; SQLite uses 80 ms), but stop
        // 1 ms before the next 110 ms heartbeat. A failure-path renewal may extend it again.
        await vi.advanceTimersByTimeAsync(109);
        expect(Date.now()).toBe(original.expiresAt + 99);
        const afterExpiry = actualLease();
        expect(afterExpiry).toMatchObject({ id: original.id, rootId: original.rootId,
          identityId: original.identityId, startedAt: original.startedAt, incarnationToken: original.incarnationToken });
        expect(afterExpiry.updatedAt).toBeGreaterThanOrEqual(renewed.updatedAt);
        expect(afterExpiry.expiresAt).toBeGreaterThanOrEqual(renewed.expiresAt);
        expect(afterExpiry.expiresAt).toBeGreaterThan(Date.now());
        expect(s.directory.confirmedAt()).toBe(confirmedAt);
        expect(s.directory.writeStalled()?.message).toContain("write-stalled");
        expect(s.directory.canConsumeMesh()).toBe(false);
        await assertWriterHeld();
        await expect(withOwnedHostLease(s.mesh, renewed, actualLease)).resolves.toEqual(afterExpiry);
        expect(fs.existsSync(s.lock)).toBe(false);
        console.info("near-expiry lease proof", JSON.stringify({ backend, originalExpiresAt: original.expiresAt,
          heartbeatAt, renewedExpiresAt: renewed.expiresAt, checkedAt: Date.now(),
          currentUpdatedAt: afterExpiry.updatedAt, currentExpiresAt: afterExpiry.expiresAt }));

        s.directory.suspend();
        holder.beginRelease(); await holder.finishRelease();
        expect(s.mesh.get("test/holder-committed", { fresh: true })?.value).toBe(true);
        expect(s.mesh.get("test/must-not-commit", { fresh: true })).toBeUndefined();
      } finally {
        s.directory.suspend();
        await holder.stop();
        // Drain any failed assertion's in-flight shared wait before restoring real timers.
        await vi.advanceTimersByTimeAsync(110);
      }
    } finally { monotonic.mockRestore(); vi.useRealTimers(); }
  }, 20_000);

  it("renews and retains owned custody past its budget while another process holds the shared writer", async () => {
    const s = await setup(backend), holder = holdInChild(s.root, backend, "writer");
    try {
      await holder.wait("held");
      const renewed = { ...s.lease, updatedAt: Date.now(), expiresAt: Date.now() + 15_000 };
      const renewal = renewHostLease(s.mesh, renewed, { timeoutMs: 25 }).then(() => undefined, (error: unknown) => error);
      // Hold the real writer longer than the renewal budget, without a fake clock/lock.
      await new Promise(resolve => setTimeout(resolve, 40));
      expect(holder.child.exitCode).toBeNull();
      const blocked = await s.mesh.withTryLock(() => s.mesh.put({ key: "test/must-not-commit", value: true, identity }), 0)
        .then(() => undefined, (error: unknown) => error);
      expect(isMeshLockTimeout(blocked)).toBe(true);
      expect(await renewal).toBeUndefined(); // Baseline GREEN: renewal has no shared-lock dependency.
      expect(s.read()).toEqual(renewed);
      expect(s.read().expiresAt).toBeGreaterThan(Date.now());
      await expect(withOwnedHostLease(s.mesh, renewed, () => readHostLeaseCurrent(s.root, hostId))).resolves.toEqual(renewed);
      expect(fs.existsSync(s.lock)).toBe(false);
      holder.beginRelease(); await holder.finishRelease();
      expect(s.mesh.get("test/holder-committed", { fresh: true })?.value).toBe(true);
      expect(s.mesh.get("test/must-not-commit", { fresh: true })).toBeUndefined();
      await s.directory.refresh();
      expect(s.directory.canConsumeMesh()).toBe(true);
      expect(s.read().incarnationToken).toBe(s.lease.incarnationToken);
      expect(s.directory.get(rootId)).toMatchObject({ ownerIdentityId: identity.id, stale: false });
    } finally { await holder.stop(); }
  }, 20_000);

  it.skipIf(process.platform !== "linux")("treats the real pinned release's transient ownerless view as retryable, then recovers (Linux fd-pinned release only)", async () => {
    const s = await setup(backend), holder = holdInChild(s.root, backend, "pinned-release");
    const alarm = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      await holder.wait("held");
      const ownerPath = path.join(s.lock, "owner"), owner = fs.readFileSync(ownerPath, "utf8"), before = fs.lstatSync(s.lock);
      expect(owner.split("\n")[1]).toBe(String(holder.child.pid));
      const renewed = { ...s.lease, updatedAt: Date.now(), expiresAt: Date.now() + 15_000 };
      await expect(renewHostLease(s.mesh, renewed, { timeoutMs: 25 })).rejects.toBeInstanceOf(HostLeaseLockBusyError);
      expect(fs.readFileSync(ownerPath, "utf8")).toBe(owner);
      expect(s.read()).toEqual(s.lease);
      holder.beginRelease(); await holder.wait("ownerless");
      const during = fs.lstatSync(s.lock);
      expect([during.dev, during.ino]).toEqual([before.dev, before.ino]);
      expect(fs.existsSync(ownerPath)).toBe(false); // A real release, not a fabricated missing read.
      const error = await renewHostLease(s.mesh, renewed, { timeoutMs: 25 }).then(() => undefined, (error: unknown) => error);
      // Soft assertions deliberately let the post-release recovery run on today's RED source.
      expect.soft(error, "an ownerless view during release must remain ordinary contention").toBeInstanceOf(HostLeaseLockBusyError);
      expect.soft(alarm.mock.calls.filter(args => String(args[0]).includes("custody-unrecoverable")),
        "a transient release must not raise the persistent-corruption alarm").toEqual([]);
      expect(s.read()).toEqual(s.lease);
      expect([fs.lstatSync(s.lock).dev, fs.lstatSync(s.lock).ino]).toEqual([before.dev, before.ino]);
      await holder.finishRelease();
      expect(fs.existsSync(s.lock)).toBe(false);
      await renewHostLease(s.mesh, renewed, { timeoutMs: 25 });
      expect(s.read()).toEqual(renewed);
      await expect(withOwnedHostLease(s.mesh, renewed, () => fs.readFileSync(s.file, "utf8")))
        .resolves.toBe(fs.readFileSync(s.file, "utf8"));
      await s.directory.refresh();
      expect(s.directory.canConsumeMesh()).toBe(true);
      expect(s.directory.get(rootId)).toMatchObject({ ownerIdentityId: identity.id, stale: false });
    } finally { await holder.stop(); }
  }, 20_000);

  it.each(["", "not-a-pid"])("keeps a persistent malformed owner PID %j nonretryable and alarmed", async pid => {
    const s = await setup(backend);
    let receipt = "";
    await s.mesh.leaseCustody(s.file, () => { receipt = fs.readFileSync(path.join(s.lock, "owner"), "utf8"); });
    const fields = receipt.split("\n"); fields[1] = pid;
    const malformed = fields.join("\n"), ownerPath = path.join(s.lock, "owner");
    fs.mkdirSync(s.lock); fs.writeFileSync(ownerPath, malformed);
    const before = fs.lstatSync(s.lock), alarm = vi.spyOn(console, "warn").mockImplementation(() => {});
    const error = await renewHostLease(s.mesh, s.lease).then(() => undefined, (error: unknown) => error);
    expect(error).toBeInstanceOf(MeshCustodyUnrecoverableError);
    expect(error).toMatchObject({ code: "FABRIC_MESH_CUSTODY_UNRECOVERABLE", retryable: false, lock: s.lock });
    expect(isMeshLockTimeout(error)).toBe(false);
    expect(alarm.mock.calls.some(args => String(args[0]).includes("custody-unrecoverable"))).toBe(true);
    expect(fs.readFileSync(ownerPath, "utf8")).toBe(malformed);
    expect([fs.lstatSync(s.lock).dev, fs.lstatSync(s.lock).ino]).toEqual([before.dev, before.ino]);
    expect(s.read()).toEqual(s.lease);
  });

  it("never lets contention recovery or stale renewal/close overwrite a successor incarnation or remove its custody", async () => {
    const s = await setup(backend), holder = holdInChild(s.root, backend, "custody");
    try {
      await holder.wait("held");
      await expect(s.directory.refresh()).rejects.toBeInstanceOf(HostLeaseLockBusyError);
      expect(s.read()).toEqual(s.lease);
      holder.beginRelease(); await holder.finishRelease();
    } finally { await holder.stop(); }
    const successor = makeDirectory(s.mesh);
    await successor.refresh();
    const lease = s.read(), host = s.mesh.get(hostKey, { fresh: true }), participant = readParticipantFile(s.root, participantKey);
    expect(lease.incarnationToken).not.toBe(s.lease.incarnationToken);
    expect(host).toBeDefined(); expect(participant).toBeDefined();
    await expect(renewHostLease(s.mesh, s.lease)).rejects.toBeInstanceOf(FabricHostLeaseSupersededError);
    // The directory has not discovered supersession yet: close must check actual ownership,
    // not merely take an already-marked-superseded early return.
    const currentHolder = holdInChild(s.root, backend, "custody");
    try {
      await currentHolder.wait("held");
      const ownerPath = path.join(s.lock, "owner"), owner = fs.readFileSync(ownerPath, "utf8"), before = fs.lstatSync(s.lock);
      await s.directory.close();
      expect(s.read()).toEqual(lease);
      expect(s.mesh.get(hostKey, { fresh: true })).toEqual(host);
      expect(readParticipantFile(s.root, participantKey)).toEqual(participant);
      expect(fs.readFileSync(ownerPath, "utf8")).toBe(owner);
      expect([fs.lstatSync(s.lock).dev, fs.lstatSync(s.lock).ino]).toEqual([before.dev, before.ino]);
      currentHolder.beginRelease(); await currentHolder.finishRelease();
      await expect(s.directory.refresh()).resolves.toBeUndefined(); // Closed refresh is a no-op, never resurrection.
      await expect(s.directory.start()).rejects.toBeInstanceOf(FabricHostLeaseSupersededError);
      expect(s.directory.canConsumeMesh()).toBe(false);
      expect(s.read()).toEqual(lease);
      expect(s.mesh.get(hostKey, { fresh: true })).toEqual(host);
      expect(readParticipantFile(s.root, participantKey)).toEqual(participant);
      await successor.refresh();
      expect(successor.canConsumeMesh()).toBe(true);
      expect(s.read().incarnationToken).toBe(lease.incarnationToken);
    } finally { await currentHolder.stop(); }
  }, 30_000);
});
