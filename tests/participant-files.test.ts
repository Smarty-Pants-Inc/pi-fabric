import { spawn } from "node:child_process";
import childProcess from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import { processIncarnation } from "../src/core/atomic-write.js";
import { MeshStore, RUNTIME_MESH_READ_CACHE_MS, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { LIVENESS_POLICY_KEY, readHostLease, removeHostLease } from "../src/topology/host-leases.js";
import { reapDeadHostRecords } from "../src/topology/host-reaper.js";
import * as participantFiles from "../src/topology/participant-files.js";
import { readParticipantFiles, writeParticipantFile } from "../src/topology/participant-files.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";

// smarty-dev#2004: every participant change rewrote the whole shared state under the mesh lock,
// and every process re-read and parsed it. Records now also live in files of their own.

const roots: string[] = [];
const directories: ParticipantDirectory[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  await Promise.all(directories.splice(0).map((directory) => directory.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const PREFIX = "topology/participants/";
const keyOf = (id: string) => PREFIX + createHash("sha256").update(id).digest("hex");
const identityOf = (name: string): MeshIdentity => ({ id: `session:${name}`, name: "main", kind: "main", sessionId: name });
const meshRoot = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-participant-files-"));
  roots.push(root);
  return path.join(root, "mesh");
};
const record = (name: string, extra: Partial<FabricParticipantRecord> = {}): FabricParticipantRecord => ({
  format: 1, id: `session:${name}`, kind: "root", rootId: `session:${name}`,
  ownerHostId: `session:${name}`, ownerIdentityId: `session:${name}`, name: "main", status: "idle",
  runner: "pi", transport: "host", capabilities: ["steer", "followUp", "fabric"], cwd: "/tmp/project",
  sessionId: name, startedAt: 1, updatedAt: 2, pendingMessages: false, controlProtocol: "v1", label: `P-${name}`,
  ...extra,
});
const directory = (root: string, name: string, source: () => FabricParticipantRecord[]) => {
  const identity = identityOf(name);
  const value = new ParticipantDirectory(new MeshStore(root, 64 * 1024, 1_000), {
    enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 100, leaseMs: 2_000,
  });
  value.registerSource(source);
  directories.push(value);
  return value;
};
const stateParticipants = (root: string) =>
  new MeshStore(root, 64 * 1024, 1_000).listAll(PREFIX, { fresh: true });
const mockNativePlatform = (platform: "darwin" | "win32", start: string) => {
  vi.spyOn(process, "platform", "get").mockReturnValue(platform);
  vi.stubEnv("SystemRoot", "C:\\Windows");
  // A simulated non-Linux host must not accidentally expose this runner's /proc.
  const read = fs.readFileSync.bind(fs);
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
    if (String(file).startsWith("/proc/")) throw Object.assign(new Error("no procfs"), { code: "ENOENT" });
    return (read as (...args: unknown[]) => unknown)(file, ...args);
  }) as typeof fs.readFileSync);
  return vi.spyOn(childProcess, "execFile").mockImplementation(((...args: unknown[]) => {
    const done = args[args.length - 1] as (error: Error | null, stdout: string, stderr: string) => void;
    done(null, start + "\n", "");
    return {} as childProcess.ChildProcess;
  }) as typeof childProcess.execFile);
};
const setPolicy = (root: string) => new MeshStore(root, 64 * 1024, 1_000).put({
  key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files", participants: "files" }, identity: identityOf("owner"),
});

describe("participant files", () => {
  it.each([false, true])("renews a live host through per-key contention (files-only=%s), retries and lets dead hosts expire", async (filesOnly) => {
    vi.useFakeTimers({ now: Date.now() });
    const root = meshRoot();
    if (filesOnly) await setPolicy(root);
    let status: "idle" | "running" = "idle";
    const make = (name: string, source: () => FabricParticipantRecord[]) => {
      // No legacy main-session fallback: assert the actual participant host lease.
      const identity: MeshIdentity = { ...identityOf(name), kind: "agent" };
      const value = new ParticipantDirectory(new MeshStore(root, 64 * 1024, 1_000), {
        enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 100, leaseMs: 2_000,
        reapDeadHosts: false,
      });
      value.registerSource(source);
      directories.push(value);
      return value;
    };
    const alpha = make("alpha", () => [record("alpha", { status }), record("healthy")]);
    const dead = make("dead", () => [record("dead")]);
    const reader = make("reader", () => [record("reader")]);
    let pending: Promise<void> | undefined;
    let lock = "";
    try {
      await alpha.start();
      await dead.refresh(); // no timer: this host genuinely stops renewing
      await reader.refresh();
      const leaseBefore = readHostLease(root, "session:alpha")!;
      lock = path.join(root, "participants", ".locks", keyOf("session:alpha").slice(PREFIX.length));
      fs.mkdirSync(lock, { recursive: true });
      fs.writeFileSync(path.join(lock, "owner"), `${process.pid}\n\nlive-holder\n`);
      if (filesOnly) status = "running";
      else fs.rmSync(path.join(root, "participants", `${keyOf("session:alpha").slice(PREFIX.length)}.json`));
      pending = alpha.refresh().catch(() => undefined);
      await vi.advanceTimersByTimeAsync(3_000); // > lease, still inside the key wait
      expect(readHostLease(root, "session:alpha")!.updatedAt).toBeGreaterThan(leaseBefore.updatedAt);
      expect(reader.list({ scope: "project", includeStale: true }).find((item) => item.id === "session:alpha")).toMatchObject({ stale: false });
      expect(reader.list({ scope: "project", includeStale: true }).find((item) => item.id === "session:dead")).toMatchObject({ stale: true });
      expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toContain("live-holder");
      await vi.advanceTimersByTimeAsync(3_000); // > key timeout; failed keys must be retried
      expect(readHostLease(root, "session:alpha")!.expiresAt).toBeGreaterThan(Date.now());
      fs.rmSync(lock, { recursive: true });
      await vi.advanceTimersByTimeAsync(200);
      await pending;
      await alpha.refresh(); // failed per-key copy/write is retried
      expect(readParticipantFiles(root, { maxAgeMs: 0 }).find((entry) => entry.key === keyOf("session:alpha")))
        .toMatchObject({ value: { status: filesOnly ? "running" : "idle" } });
    } finally {
      if (lock) fs.rmSync(lock, { recursive: true, force: true });
      await vi.advanceTimersByTimeAsync(200);
      await pending;
      await alpha.close();
      vi.useRealTimers();
    }
  });

  it.each([false, true])("renewal does not await a slow Windows holder probe and unknown stays occupied (files-only=%s)", async (filesOnly) => {
    vi.useFakeTimers({ now: Date.now() });
    const root = meshRoot();
    if (filesOnly) await setPolicy(root);
    let status: "idle" | "running" = "idle";
    const alpha = directory(root, "alpha", () => [record("alpha", { status }), record("healthy", { status })]);
    let pending: Promise<void> | undefined;
    const lock = path.join(root, "participants", ".locks", keyOf("session:alpha").slice(PREFIX.length));
    try {
      await alpha.start();
      mockNativePlatform("win32", "639264528000000000");
      const runner = vi.fn<atomic.IncarnationCommandRunner>(() => new Promise(() => {}));
      const native = atomic.createProcessIncarnationReader({ platform: "win32", systemRoot: "C:\\Windows", run: runner });
      vi.spyOn(atomic, "processIncarnation").mockImplementation(native.read);
      vi.spyOn(atomic, "ownProcessIncarnation").mockResolvedValue("win32:639264528000000000");
      fs.mkdirSync(lock, { recursive: true });
      const owner = `${process.pid}\nwin32:639264528000000000\nlive-native-holder\n`;
      fs.writeFileSync(path.join(lock, "owner"), owner);
      if (filesOnly) status = "running";
      else fs.rmSync(path.join(root, "participants", `${keyOf("session:alpha").slice(PREFIX.length)}.json`));
      const leaseBefore = readHostLease(root, "session:alpha")!;
      pending = alpha.refresh();
      await vi.advanceTimersByTimeAsync(3_000); // longer than the lease and one native timeout
      expect(readHostLease(root, "session:alpha")!.updatedAt).toBeGreaterThan(leaseBefore.updatedAt);
      expect(readHostLease(root, "session:alpha")!.expiresAt).toBeGreaterThan(Date.now());
      expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(owner);
      expect(runner).toHaveBeenCalled();
      expect(runner.mock.calls[0]![2].signal.aborted).toBe(true); // timed-out evidence stays unknown
      await vi.advanceTimersByTimeAsync(4_000);
      await pending;
      expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(owner);
      expect(readParticipantFiles(root).find(entry => entry.key === keyOf("session:healthy"))).toBeDefined();
      expect(readHostLease(root, "session:alpha")!.expiresAt).toBeGreaterThan(Date.now());
    } finally {
      fs.rmSync(lock, { recursive: true, force: true });
      await vi.advanceTimersByTimeAsync(3_000);
      await pending;
      await alpha.close();
      vi.useRealTimers();
    }
  });

  it("renews during a contended initial publish, before start has returned", async () => {
    vi.useFakeTimers({ now: Date.now() });
    const root = meshRoot();
    await setPolicy(root);
    const write = participantFiles.writeParticipantFileIf;
    let resume!: () => void;
    const gate = new Promise<void>((resolve) => { resume = resolve; });
    vi.spyOn(participantFiles, "writeParticipantFileIf").mockImplementationOnce(async (...args) => {
      await gate;
      return write(...args);
    });
    const alpha = directory(root, "alpha", () => [record("alpha")]);
    const starting = alpha.start();
    try {
      await vi.advanceTimersByTimeAsync(3_000);
      expect(readHostLease(root, "session:alpha")!.expiresAt).toBeGreaterThan(Date.now());
    } finally {
      resume();
      await starting;
      await alpha.close();
      vi.useRealTimers();
    }
  });

  it.each(["write", "cleanup"] as const)("isolates a failed per-key %s so healthy keys publish and the failure retries", async (operation) => {
    const root = meshRoot();
    await setPolicy(root);
    let includeOld = true;
    let status: "idle" | "running" = "idle";
    const alpha = directory(root, "alpha", () => [record("alpha", { status }), record("healthy", { status }), ...(includeOld ? [record("old")] : [])]);
    await alpha.refresh();
    status = "running";
    includeOld = false;
    const method = operation === "write" ? "writeParticipantFileIf" : "removeParticipantFileIf";
    vi.spyOn(participantFiles, method).mockRejectedValueOnce(new Error("owned key is contended"));
    await expect(alpha.refresh()).resolves.toBeUndefined();
    expect(readParticipantFiles(root, { maxAgeMs: 0 }).find((entry) => entry.key === keyOf("session:healthy")))
      .toMatchObject({ value: { status: "running" } });
    await alpha.refresh();
    expect(readParticipantFiles(root, { maxAgeMs: 0 }).map((entry) => entry.key)).not.toContain(keyOf("session:old"));
    expect(readParticipantFiles(root, { maxAgeMs: 0 }).find((entry) => entry.key === keyOf("session:alpha")))
      .toMatchObject({ value: { status: "running" } });
  });

  it.each(["write", "barrier", "verification", "refused"] as const)("keeps the last state ownership record after a failed migration (%s), then durably retries", async (failure) => {
    const root = meshRoot();
    const alpha = directory(root, "alpha", () => [record("alpha"), record("healthy")]);
    await alpha.refresh();
    const key = keyOf("session:alpha");
    const before = stateParticipants(root).find((entry) => entry.key === key)!;
    const file = path.join(root, "participants", `${key.slice(PREFIX.length)}.json`);
    fs.rmSync(file);
    await setPolicy(root);
    const write = participantFiles.writeParticipantFileIf;
    let fail = true;
    vi.spyOn(participantFiles, "writeParticipantFileIf").mockImplementation(async (mesh, writtenKey, decide, options) => {
      if (writtenKey !== key || !fail) return write(mesh, writtenKey, decide, options);
      fail = false;
      if (failure === "write") throw new Error("migration I/O failed");
      if (failure === "refused") return false;
      if (failure === "barrier") {
        const barrier = vi.spyOn(fs, "fsyncSync").mockImplementationOnce(() => { throw new Error("migration barrier failed"); });
        try { return await write(mesh, writtenKey, decide, options); }
        finally { barrier.mockRestore(); }
      }
      const read = fs.readFileSync.bind(fs);
      const torn = vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...args: unknown[]) => {
        if (String(target) === file && fs.existsSync(file)) return "{}";
        return (read as (...args: unknown[]) => unknown)(target, ...args);
      }) as typeof fs.readFileSync);
      try { return await write(mesh, writtenKey, decide, options); }
      finally { torn.mockRestore(); }
    });
    await alpha.refresh();
    expect(stateParticipants(root).find((entry) => entry.key === key)).toEqual(before);
    expect(stateParticipants(root).find((entry) => entry.key === keyOf("session:healthy"))).toBeUndefined();
    expect(readParticipantFiles(root).find((entry) => entry.key === keyOf("session:healthy"))).toBeDefined();
    const beta = directory(root, "beta", () => [record("beta"), record("alpha")]);
    await beta.refresh();
    expect(stateParticipants(root).find((entry) => entry.key === key)).toEqual(before);
    expect(beta.get("session:alpha", Date.now(), { fresh: true })).toMatchObject({ ownerHostId: "session:alpha", stale: false });
    const barriers = vi.spyOn(fs, "fsyncSync");
    const batch = alpha.mesh.writeBatch.bind(alpha.mesh);
    let verifiedBeforeDeletion = false;
    vi.spyOn(alpha.mesh, "writeBatch").mockImplementation(async (request) => {
      if (request.ops.some((op) => op.kind === "delete" && op.key === key)) {
        expect(barriers).toHaveBeenCalled();
        expect(JSON.parse(fs.readFileSync(file, "utf8"))).toMatchObject({ key, value: { ownerHostId: "session:alpha" } });
        verifiedBeforeDeletion = true;
      }
      return batch(request);
    });
    await alpha.refresh();
    expect(verifiedBeforeDeletion).toBe(true);
    expect(stateParticipants(root).find((entry) => entry.key === key)).toBeUndefined();
    expect(readParticipantFiles(root).find((entry) => entry.key === key)).toMatchObject({ value: { ownerHostId: "session:alpha" } });
  });

  it.each([false, true])("a slow injected Windows probe cannot lapse a live owner observed from another process (files-only=%s)", async (filesOnly) => {
    const root = meshRoot();
    if (filesOnly) await setPolicy(root);
    let commands = 0;
    const native = atomic.createProcessIncarnationReader({
      platform: "win32", systemRoot: "C:\\Windows",
      run: async () => { commands++; await new Promise((resolve) => setTimeout(resolve, 900)); return "639264528000000000\r\n"; },
    });
    const sync = vi.spyOn(childProcess, "execFileSync");
    let status: "idle" | "running" = "idle";
    const identity: MeshIdentity = { ...identityOf("alpha"), kind: "agent" };
    const alpha = new ParticipantDirectory(new MeshStore(root, 64 * 1024, 1_000), {
      enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 100, leaseMs: 300, reapDeadHosts: false,
    });
    alpha.registerSource(() => Array.from({ length: 12 }, (_, index) => record(index === 0 ? "alpha" : `key-${index}`, { kind: "agent", rootId: identity.id, status })));
    directories.push(alpha);
    // Establish the authoritative host before observing the slow update. The first
    // files-only publish may briefly precede its first shared host record; that is
    // not lease expiry (the fresh file lease already prevents ownership takeover).
    await alpha.start();
    const ready = path.join(root, "observer.ready");
    const stop = path.join(root, "observer.stop");
    fs.mkdirSync(root, { recursive: true });
    const code = `
      import { createJiti } from "jiti";
      import { pathToFileURL } from "node:url";
      import fs from "node:fs";
      const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
      const { MeshStore } = await jiti.import("./src/mesh/store.ts");
      const { ParticipantDirectory } = await jiti.import("./src/topology/participant-directory.ts");
      const { readHostLease } = await jiti.import("./src/topology/host-leases.ts");
      const [root, ready, stop] = process.argv.slice(1);
      const identity = { id: "observer", kind: "agent", name: "observer" };
      const observer = new ParticipantDirectory(new MeshStore(root, 65536, 1000), {
        enabled: true, hostId: identity.id, rootId: identity.id, identity, reapDeadHosts: false,
      });
      let samples = 0, lapses = 0, seen = 0;
      fs.writeFileSync(ready, "");
      const deadline = Date.now() + 10000;
      while (!fs.existsSync(stop) && Date.now() < deadline) {
        const lease = readHostLease(root, "session:alpha");
        if (lease) { samples++; if (lease.expiresAt < Date.now()) lapses++; }
        const participant = observer.list({ fresh: true, includeStale: true }).find(p => p.id === "session:alpha");
        if (participant) { seen++; if (participant.stale || participant.ownerHostId !== "session:alpha") lapses++; }
        await new Promise(resolve => setTimeout(resolve, 20));
      }
      console.log(JSON.stringify({ samples, lapses, seen, stopped: fs.existsSync(stop) }));`;
    const child = spawn(process.execPath, ["--input-type=module", "-e", code, root, ready, stop], { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "", stderr = "";
    child.stdout.on("data", (chunk) => { stdout += chunk; });
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    const closed = new Promise<number | null>((resolve, reject) => { child.once("error", reject); child.once("close", resolve); });
    // Avoid an unhandled rejection on early startup failure; still check it below.
    closed.catch(() => undefined);
    try {
      await vi.waitFor(() => expect(fs.existsSync(ready)).toBe(true), { timeout: 10_000, interval: 20 });
      vi.spyOn(atomic, "ownProcessIncarnation").mockImplementation(native.own);
      status = "running";
      await alpha.refresh();
      await new Promise((resolve) => setTimeout(resolve, 100));
      expect(commands).toBe(1); // not one PowerShell per key or heartbeat
      expect(sync).not.toHaveBeenCalled();
      expect(readHostLease(root, "session:alpha")!.expiresAt).toBeGreaterThan(Date.now());
      expect(readParticipantFiles(root)).toHaveLength(12);
    } finally {
      fs.writeFileSync(stop, "");
      expect(await closed, stderr).toBe(0); // join close before removing the owned root
    }
    const result = JSON.parse(stdout.trim());
    expect(result).toMatchObject({ lapses: 0, stopped: true });
    expect(result.samples).toBeGreaterThan(20); // observed while the probe exceeded the lease
    expect(result.seen).toBeGreaterThan(0);
  });

  it("before the fleet owner's switch, write each committed record to the shared state and its file", async () => {
    const root = meshRoot();
    const alpha = directory(root, "alpha", () => [record("alpha")]);
    await alpha.start();
    expect(stateParticipants(root).map((entry) => entry.key)).toEqual([keyOf("session:alpha")]);
    const [file] = readParticipantFiles(root);
    expect(file).toMatchObject({ key: keyOf("session:alpha"), value: { id: "session:alpha", label: "P-alpha" } });
    // A runtime that reads only files (the shared copy gone) still finds the record.
    await new MeshStore(root, 64 * 1024, 1_000).delete({ key: keyOf("session:alpha") });
    const beta = directory(root, "beta", () => [record("beta")]);
    await beta.start();
    expect(beta.peers().map((peer) => peer.id)).toEqual(["session:alpha"]);
  });

  it("under the switch, write records only to files, remove this host's shared copies, and change without the lock", async () => {
    const root = meshRoot();
    let status: "idle" | "running" = "idle";
    const alpha = directory(root, "alpha", () => [record("alpha", { status })]);
    await alpha.start();                                           // an older rollout: a shared copy
    expect(stateParticipants(root)).toHaveLength(1);
    await setPolicy(root);
    await alpha.refresh();
    expect(stateParticipants(root)).toEqual([]);                   // removed from the shared state
    expect(readParticipantFiles(root).map((entry) => entry.key)).toEqual([keyOf("session:alpha")]);

    const beta = directory(root, "beta", () => [record("beta")]);
    await beta.start();
    expect(stateParticipants(root)).toEqual([]);
    const stateFile = path.join(root, "state.json");
    const before = fs.statSync(stateFile).mtimeMs;
    const batches = vi.spyOn(MeshStore.prototype, "writeBatch");
    status = "running";
    alpha.scheduleRefresh();                                       // a change-only refresh
    await vi.waitFor(() => expect(beta.peers().find((peer) => peer.id === "session:alpha")?.status).toBe("running"),
      { timeout: 5_000, interval: 20 });
    expect(batches.mock.calls.filter((call) => (call[0] as { identity: MeshIdentity }).identity.id === "session:alpha"))
      .toHaveLength(0);
    expect(fs.statSync(stateFile).mtimeMs).toBe(before);
    expect(beta.get("session:alpha")).toMatchObject({ status: "running", stale: false });

    await alpha.close();
    expect(readParticipantFiles(root).map((entry) => entry.key)).toEqual([keyOf("session:beta")]);
  });

  it("an older runtime's later shared write wins over a file for the same key", async () => {
    const root = meshRoot();
    const beta = directory(root, "beta", () => [record("beta")]);
    await beta.start();
    const alphaIdentity = identityOf("alpha");
    writeParticipantFile(root, {
      key: keyOf("session:alpha"), value: record("alpha", { status: "idle" }), version: 1,
      updatedAt: Date.now() - 10_000, updatedBy: alphaIdentity,
    });
    await new MeshStore(root, 64 * 1024, 1_000).put({
      key: keyOf("session:alpha"), value: record("alpha", { status: "running" }), identity: alphaIdentity,
    });
    const lists = beta.list({ scope: "project", includeStale: true }).filter((participant) => participant.id === "session:alpha");
    expect(lists).toHaveLength(1);
    expect(lists[0]).toMatchObject({ status: "running" });
  });

  it("re-reads only the files that changed, and nothing while the directory is unchanged", async () => {
    const root = meshRoot();
    const entry = (name: string, updatedAt: number) => ({
      key: keyOf(`session:${name}`), value: record(name), version: 1, updatedAt, updatedBy: identityOf(name),
    });
    for (const name of ["a", "b", "c"]) writeParticipantFile(root, entry(name, 1));
    await new Promise((resolve) => setTimeout(resolve, 150));     // past the directory's timestamp tick
    const reads = vi.spyOn(atomic, "readFileRetrying");
    expect(readParticipantFiles(root)).toHaveLength(3);
    expect(reads).toHaveBeenCalledTimes(3);
    readParticipantFiles(root);
    expect(reads).toHaveBeenCalledTimes(3);
    writeParticipantFile(root, entry("b", 2));
    expect(readParticipantFiles(root).find((item) => item.key === keyOf("session:b"))?.updatedAt).toBe(2);
    expect(reads).toHaveBeenCalledTimes(4);
  });

  it("the dead-host sweep removes the files of a host gone for the window, and keeps a live host's", async () => {
    const root = meshRoot();
    const mesh = new MeshStore(root, 64 * 1024, 1_000);
    const now = Date.now();
    const hostKey = (id: string) => "topology/hosts/" + createHash("sha256").update(id).digest("hex");
    const writer = identityOf("writer");
    await mesh.put({ key: hostKey("session:dead"), value: { format: 1, id: "session:dead", expiresAt: now - 7 * 3_600_000 }, identity: writer });
    await mesh.put({ key: hostKey("session:live"), value: { format: 1, id: "session:live", expiresAt: now + 60_000 }, identity: writer });
    for (const name of ["dead", "live"]) {
      writeParticipantFile(root, {
        key: keyOf(`session:${name}`), value: record(name), version: 1, updatedAt: now - 7 * 3_600_000, updatedBy: identityOf(name),
      });
    }
    expect(await reapDeadHostRecords(mesh, writer, { ownHostId: "session:writer", now })).toBe(2);   // the host and its file
    expect(readParticipantFiles(root).map((entry) => entry.key)).toEqual([keyOf("session:live")]);
  });

  // review/astra F2 on #142: a scan that failed to read a file must not be reused as current.
  it("retries a file that failed to read, for normal and fresh reads, with no later directory change", async () => {
    const root = meshRoot();
    for (const name of ["a", "b"]) {
      writeParticipantFile(root, { key: keyOf(`session:${name}`), value: record(name), version: 1, updatedAt: 1, updatedBy: identityOf(name) });
    }
    await new Promise((resolve) => setTimeout(resolve, 150));     // past the directory's timestamp tick
    const reads = vi.spyOn(atomic, "readFileRetrying");
    reads.mockImplementationOnce(() => { throw Object.assign(new Error("busy"), { code: "EBUSY" }); });
    expect(readParticipantFiles(root)).toHaveLength(1);
    expect(readParticipantFiles(root, { maxAgeMs: 2_000 })).toHaveLength(2);                   // a normal listing
    reads.mockImplementationOnce(() => { throw Object.assign(new Error("busy"), { code: "EBUSY" }); });
    writeParticipantFile(root, { key: keyOf("session:c"), value: record("c"), version: 1, updatedAt: 1, updatedBy: identityOf("c") });
    await new Promise((resolve) => setTimeout(resolve, 150));
    expect(readParticipantFiles(root)).toHaveLength(2);
    expect(readParticipantFiles(root, { maxAgeMs: 0 })).toHaveLength(3);                       // a fresh one
  });

  // review/astra F1 on #142: a takeover judged on an earlier read must not replace an owner that
  // took the record since, in another process; nor may the dead-host sweep remove it.
  describe("ownership under the switch", () => {
    const shared = "actor:shared";
    const actor = (owner: string): FabricParticipantRecord => {
      const { label: _label, ...value } = record("x", {
        id: shared, kind: "actor", rootId: owner, ownerHostId: owner, ownerIdentityId: owner, parentId: owner,
      });
      return value;
    };
    const deadOwned = (root: string, updatedAt = Date.now()) => writeParticipantFile(root, {
      key: keyOf(shared), value: actor("session:dead"), version: 1, updatedAt, updatedBy: identityOf("dead"),
    });
    const stateOwner = (root: string) =>
      (new MeshStore(root, 64 * 1024, 1_000).get(keyOf(shared), { fresh: true })?.value as { ownerHostId?: string })?.ownerHostId;
    const owner = (root: string) =>
      (readParticipantFiles(root, { maxAgeMs: 0 }).find((entry) => entry.key === keyOf(shared))?.value as { ownerHostId?: string })?.ownerHostId;
    // Host B, in a process of its own, takes the record over and exits with its 60 s lease live.
    const takeOverInAnotherProcess = (root: string) => new Promise<void>((resolve, reject) => {
      const code = `
        import { createJiti } from "jiti";
        import { pathToFileURL } from "node:url";
        const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
        const { MeshStore } = await jiti.import("./src/mesh/store.ts");
        const { ParticipantDirectory } = await jiti.import("./src/topology/participant-directory.ts");
        const identity = { id: "session:b", name: "main", kind: "main", sessionId: "b" };
        const directory = new ParticipantDirectory(new MeshStore(process.argv[1], 64 * 1024, 1000), {
          enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 30000, leaseMs: 60000, reapDeadHosts: false,
        });
        directory.registerSource(() => [JSON.parse(process.argv[2])]);
        await directory.refresh();
        process.exit(0);`;
      const child = spawn(process.execPath, ["--input-type=module", "-e", code, root, JSON.stringify(actor("session:b"))],
        { cwd: process.cwd(), stdio: ["ignore", "ignore", "pipe"] });
      let stderr = "";
      child.stderr.on("data", (chunk) => { stderr += chunk; });
      child.once("exit", (status) => status === 0 ? resolve() : reject(new Error(stderr)));
    });

    it("a takeover in another process between this host's read and its write keeps the new owner", async () => {
      const root = meshRoot();
      await setPolicy(root);
      deadOwned(root);
      const write = participantFiles.writeParticipantFileIf;
      vi.spyOn(participantFiles, "writeParticipantFileIf").mockImplementationOnce(async (...args) => {
        await takeOverInAnotherProcess(root);
        return write(...args);
      });
      const c = directory(root, "c", () => [actor("session:c")]);
      await c.start();
      expect(owner(root)).toBe("session:b");
      await c.refresh();                                           // B is live: C leaves it
      expect(owner(root)).toBe("session:b");
      expect(c.get(shared)).toMatchObject({ ownerHostId: "session:b", stale: false });
    }, 60_000);

    // review/astra round 2 F1: with the production read cache, C's cached state has no record of B.
    const cachedDirectory = (root: string, name: string) => {
      const identity = identityOf(name);
      const value = new ParticipantDirectory(new MeshStore(root, 64 * 1024, 1_000, { readCacheMs: RUNTIME_MESH_READ_CACHE_MS }), {
        enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 60_000, leaseMs: 120_000, reapDeadHosts: false,
      });
      directories.push(value);
      return value;
    };

    it("a contender whose cached state predates the new owner leaves it (production read cache)", async () => {
      const root = meshRoot();
      await setPolicy(root);
      deadOwned(root);
      const c = cachedDirectory(root, "c");
      let wants = false;
      c.registerSource(() => (wants ? [actor("session:c")] : []));
      await c.start();                                            // C's state cache: no host B
      const b = cachedDirectory(root, "b");
      b.registerSource(() => [actor("session:b")]);
      await b.start();
      expect(owner(root)).toBe("session:b");
      wants = true;
      await c.refresh();                                          // within C's 2 s cache window
      expect(owner(root)).toBe("session:b");
    });

    it("a contender leaves a record whose new owner paused after its file, before its shared host record", async () => {
      const root = meshRoot();
      await setPolicy(root);
      deadOwned(root);
      let release!: () => void;
      const paused = new Promise<void>((resolve) => { release = resolve; });
      const batch = MeshStore.prototype.writeBatch;
      vi.spyOn(MeshStore.prototype, "writeBatch").mockImplementation(async function (this: MeshStore, input) {
        if (input.identity.id === "session:b") await paused;      // B stops before its host record
        return batch.call(this, input);
      });
      const b = cachedDirectory(root, "b");
      b.registerSource(() => [actor("session:b")]);
      const started = b.start();
      await vi.waitFor(() => expect(owner(root)).toBe("session:b"), { timeout: 5_000, interval: 10 });
      expect(new MeshStore(root, 64 * 1024, 1_000).listAll("topology/hosts/", { fresh: true })).toEqual([]);
      const c = cachedDirectory(root, "c");
      c.registerSource(() => [actor("session:c")]);
      await c.start();
      expect(owner(root)).toBe("session:b");
      release();
      await started;
      expect(owner(root)).toBe("session:b");
    });

    // review/astra round 4: before the switch, a copy delayed past an older runtime's state-only
    // takeover must not look newer than that takeover.
    it("a dual-write copy delayed past a newer state-only owner leaves that owner visible", async () => {
      const root = meshRoot();
      const b = identityOf("b");
      const takeOverStateOnly = async () => {                     // an older runtime: state only
        const mesh = new MeshStore(root, 64 * 1024, 1_000);
        const now = Date.now();
        await mesh.put({
          key: "topology/hosts/" + createHash("sha256").update(b.id).digest("hex"), identity: b,
          value: { format: 1, id: b.id, rootId: b.id, identity: b, startedAt: now, updatedAt: now, expiresAt: now + 60_000 },
        });
        const current = mesh.get(keyOf(shared), { fresh: true });
        await mesh.put({ key: keyOf(shared), value: actor(b.id), identity: b, ...(current ? { ifVersion: current.version } : {}) });
      };
      const write = participantFiles.writeParticipantFileIf;
      vi.spyOn(participantFiles, "writeParticipantFileIf").mockImplementationOnce(async (...args) => {
        await new Promise((resolve) => setTimeout(resolve, 20));   // A pauses after its state commit
        await takeOverStateOnly();
        return write(...args);
      });
      const a = directory(root, "a", () => [actor("session:a")]);
      await a.start();
      const c = directory(root, "c", () => []);
      await c.start();
      expect(c.get(shared, Date.now(), { fresh: true })).toMatchObject({ ownerHostId: b.id, stale: false });
      expect(c.list({ scope: "project", fresh: true }).find((participant) => participant.id === shared))
        .toMatchObject({ ownerHostId: b.id });
      await a.refresh();                                          // A resumes: B is live, A leaves it
      expect(stateOwner(root)).toBe(b.id);
      expect(c.get(shared, Date.now(), { fresh: true })).toMatchObject({ ownerHostId: b.id, stale: false });
    });

    // Security pass S1 on #142: the same with B a new runtime whose own copy failed.
    it("a delayed copy past a new runtime's takeover whose copy failed leaves that owner; its copy is made again", async () => {
      const root = meshRoot();
      const hostKeyOf = (id: string) => "topology/hosts/" + createHash("sha256").update(id).digest("hex");
      let b: ParticipantDirectory | undefined;
      const write = participantFiles.writeParticipantFileIf;
      vi.spyOn(participantFiles, "writeParticipantFileIf")
        .mockImplementationOnce(async (...args) => {              // A's copy: A pauses past its lease
          const mesh = new MeshStore(root, 64 * 1024, 1_000);
          await mesh.delete({ key: hostKeyOf("session:a") });
          removeHostLease(root, "session:a");
          b = directory(root, "b", () => [actor("session:b")]);
          await b.start();                                        // B takes K; its copy fails (below)
          return write(...args);
        })
        .mockImplementationOnce(async () => { throw new Error("copy failed"); });
      const a = directory(root, "a", () => [actor("session:a")]);
      // Removing A's lease is now terminal for its incarnation (#7313): its delayed
      // copy must not recreate the lease or confirm after B has taken the record.
      await expect(a.start()).rejects.toMatchObject({ code: "FABRIC_HOST_LEASE_SUPERSEDED" });
      expect(stateOwner(root)).toBe("session:b");
      expect(owner(root)).not.toBe("session:a");                 // A's delayed copy was dropped
      const c = directory(root, "c", () => []);
      await c.start();
      expect(c.get(shared, Date.now(), { fresh: true })).toMatchObject({ ownerHostId: "session:b", stale: false });
      await expect(a.refresh()).rejects.toMatchObject({ code: "FABRIC_HOST_LEASE_SUPERSEDED" });
      expect(stateOwner(root)).toBe("session:b");                // A never takes K back from live B
      await b!.refresh();
      expect(owner(root)).toBe("session:b");                     // B's copy is made again
    });

    it("without a race, a record whose owner is gone is taken over", async () => {
      const root = meshRoot();
      await setPolicy(root);
      deadOwned(root);
      const c = directory(root, "c", () => [actor("session:c")]);
      await c.start();
      expect(owner(root)).toBe("session:c");
    });

    it("the dead-host sweep keeps a record taken over after its scan", async () => {
      const root = meshRoot();
      const mesh = new MeshStore(root, 64 * 1024, 1_000);
      const now = Date.now();
      deadOwned(root, now - 7 * 3_600_000);                        // no host record, older than the window
      const remove = participantFiles.removeParticipantFileIf;
      vi.spyOn(participantFiles, "removeParticipantFileIf").mockImplementationOnce(async (...args) => {
        writeParticipantFile(root, {
          key: keyOf(shared), value: actor("session:new"), version: 2, updatedAt: Date.now(), updatedBy: identityOf("new"),
        });
        return remove(...args);
      });
      expect(await reapDeadHostRecords(mesh, identityOf("writer"), { ownHostId: "session:writer", now })).toBe(0);
      expect(owner(root)).toBe("session:new");
    });
  });

  // review/astra round 3 on #142: the per-key lock must never be taken from a live holder, and
  // recovering a dead holder's lock must never remove a successor's lock. Separate processes.
  describe("per-key lock recovery", () => {
    const lockOf = (root: string, id: string) =>
      path.join(root, "participants", ".locks", keyOf(id).slice(PREFIX.length));
    const holdLock = (root: string, id: string, pid: number) => {
      const lock = lockOf(root, id);
      fs.mkdirSync(lock, { recursive: true });
      fs.writeFileSync(path.join(lock, "owner"), `${pid}\n\nheld\n`);
      const old = new Date(Date.now() - 3_600_000);         // far past any age threshold
      fs.utimesSync(lock, old, old);
      return lock;
    };
    const exited = () => new Promise<number>((resolve) => {
      const child = spawn(process.execPath, ["-e", ""], { stdio: "ignore" });
      child.once("close", () => resolve(child.pid!));
    });

    it("serializes a paused leftover sweep with recovery rather than restoring an ownerless canonical lock", async () => {
      const root = meshRoot();
      const mesh = new MeshStore(root, 64 * 1024, 1_000);
      const lock = holdLock(root, "session:a", 999999999);
      const old = new Date(Date.now() - 2 * 3_600_000);
      fs.utimesSync(lock, old, old);
      const rename = fs.renameSync.bind(fs);
      const remove = fs.rmSync.bind(fs);
      let armed = true;
      let sweep: Promise<number> | undefined;
      let ownerlessRestore = false;
      vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
        if (String(file).startsWith(`${lock}.`) && String(file).endsWith(".dead") && armed) {
          // An interrupted recursive sweep has unlinked owner but not yet rmdir'd.
          remove(path.join(String(file), "owner"), { force: true });
          throw Object.assign(new Error("paused sweep after owner unlink"), { code: "EBUSY" });
        }
        return remove(file, options);
      });
      vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
        rename(from, to);
        if (String(from) === lock && armed) {
          sweep = reapDeadHostRecords(mesh, identityOf("sweeper"), { ownHostId: "session:sweeper" });
          armed = false;
        } else if (String(to) === lock && !fs.existsSync(path.join(lock, "owner"))) {
          ownerlessRestore = true;
        }
      });
      await participantFiles.writeParticipantFileIf(mesh, keyOf("session:a"), () => ({
        key: keyOf("session:a"), value: record("a"), version: 1, updatedAt: 1, updatedBy: identityOf("a"),
      }));
      await sweep;
      expect(sweep).toBeDefined();
      expect(ownerlessRestore).toBe(false);
      expect(readParticipantFiles(root)).toHaveLength(1);
    });

    it("ordinarily sweeps old staging/tombstone leftovers but leaves recent ones and canonical locks", async () => {
      const root = meshRoot();
      const mesh = new MeshStore(root, 64 * 1024, 1_000);
      const canonical = holdLock(root, "session:a", process.pid);
      const old = new Date(Date.now() - 2 * 3_600_000);
      for (const suffix of ["old.tmp", "old.dead", "recent.tmp", "recent.dead"]) {
        const leftover = `${canonical}.${suffix}`;
        fs.mkdirSync(leftover);
        fs.writeFileSync(path.join(leftover, "owner"), "leftover\n");
        if (suffix.startsWith("old")) fs.utimesSync(leftover, old, old);
      }
      await reapDeadHostRecords(mesh, identityOf("sweeper"), { ownHostId: "session:sweeper" });
      expect(fs.readdirSync(path.dirname(canonical)).sort()).toEqual([
        path.basename(canonical), `${path.basename(canonical)}.recent.dead`, `${path.basename(canonical)}.recent.tmp`,
      ].sort());
    });

    it.each(["EACCES", "EIO", undefined].flatMap(code =>
      (["write", "remove"] as const).map(operation => ({ code, operation })),
    ))("non-ESRCH key probe failure $code preserves $operation exclusion and the exact receipt", async ({ code, operation }) => {
      vi.useFakeTimers({ now: Date.now() });
      const root = meshRoot();
      const mesh = new MeshStore(root, 64 * 1024, 1_000);
      const key = keyOf("session:a");
      writeParticipantFile(root, { key, value: record("a"), version: 1, updatedAt: 1, updatedBy: identityOf("a") });
      const lock = holdLock(root, "session:a", process.pid);
      const owner = fs.readFileSync(path.join(lock, "owner"), "utf8");
      const directory = fs.lstatSync(lock);
      const file = path.join(root, "participants", `${key.slice(PREFIX.length)}.json`);
      const contents = fs.readFileSync(file, "utf8");
      const recovery = vi.spyOn(mesh, "exclusive");
      vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(new Error("unknown probe failure"), { code }); });
      try {
        const decide = vi.fn(() => ({ key, value: record("replacement"), version: 2, updatedAt: 2, updatedBy: identityOf("replacement") }));
        const remove = vi.fn(() => true);
        const pending = (operation === "write"
          ? participantFiles.writeParticipantFileIf(mesh, key, decide)
          : participantFiles.removeParticipantFileIf(mesh, key, remove)).catch((error: unknown) => error);
        await vi.advanceTimersByTimeAsync(5_000);
        expect(await pending).toMatchObject({ message: expect.stringMatching(/Timed out waiting/) });
        expect(decide).not.toHaveBeenCalled();
        expect(remove).not.toHaveBeenCalled();
        expect(recovery).not.toHaveBeenCalled();
        expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(owner);
        expect([fs.lstatSync(lock).dev, fs.lstatSync(lock).ino]).toEqual([directory.dev, directory.ino]);
        expect(fs.readFileSync(file, "utf8")).toBe(contents);
      } finally { vi.useRealTimers(); }
    });

    it.skipIf(!["linux", "darwin", "win32"].includes(process.platform) || (process.platform === "linux" && !fs.existsSync(`/proc/${process.pid}/stat`))).each([false, true])("native participant publication records incarnation and recovers a proven reused PID (unknown probe=%s)", async (unknownProbe) => {
      const root = meshRoot();
      const mesh = new MeshStore(root, 64 * 1024, 1_000);
      const key = keyOf("session:a");
      const start = await processIncarnation(process.pid);
      expect(start).toBeDefined();
      await participantFiles.writeParticipantFileIf(mesh, key, () => {
        expect(fs.readFileSync(path.join(lockOf(root, "session:a"), "owner"), "utf8").split("\n")[1]).toBe(start);
        return undefined;
      });
      const lock = holdLock(root, "session:a", process.pid);
      const different = process.platform === "linux" ? String(BigInt(start!) + 1n)
        : process.platform === "win32" ? `win32:${BigInt(start!.slice(6)) + 1n}` : "darwin:Fri Jan  1 00:00:00 1999";
      fs.writeFileSync(path.join(lock, "owner"), `${process.pid}\n${different}\nreused\n`);
      if (unknownProbe) vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(new Error("unknown probe failure"), { code: "EIO" }); });
      await expect(participantFiles.writeParticipantFileIf(mesh, key, () => ({
        key, value: record("a"), version: 1, updatedAt: 1, updatedBy: identityOf("a"),
      }))).resolves.toBe(true);
    });

    it.skipIf(process.platform !== "linux" || !fs.existsSync(`/proc/${process.pid}/stat`))("keeps unreadable, torn, foreign and probe-denied live Linux key holders", async () => {
      vi.useFakeTimers({ now: Date.now() });
      const root = meshRoot();
      const mesh = new MeshStore(root, 64 * 1024, 1_000);
      const lock = holdLock(root, "session:a", process.pid);
      const start = (await processIncarnation(process.pid))!;
      const read = fs.readFileSync.bind(fs);
      try {
        for (const scenario of ["unreadable", "torn", "permission", "unknown-probe", "unknown-unreadable", "unknown-torn", "unknown-foreign"] as const) {
          const unreadable = scenario.includes("unreadable");
          fs.writeFileSync(path.join(lock, "owner"), scenario.includes("torn")
            ? `${process.pid}\n${start.slice(0, -1) || "0"}`
            : `${process.pid}\n${unreadable ? BigInt(start) + 1n : scenario === "unknown-foreign" ? "win32:639264528000000000" : start}\nheld\n`);
          if (unreadable) {
            vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
              if (String(file) === `/proc/${process.pid}/stat`) throw Object.assign(new Error("unreadable"), { code: "EACCES" });
              return (read as (...args: unknown[]) => unknown)(file, ...args);
            }) as typeof fs.readFileSync);
          }
          if (scenario === "permission") vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(new Error("denied"), { code: "EPERM" }); });
          if (scenario.startsWith("unknown-")) vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(new Error("unknown probe failure"), { code: "EIO" }); });
          const owner = fs.readFileSync(path.join(lock, "owner"), "utf8");
          const decide = vi.fn(() => undefined);
          const pending = participantFiles.writeParticipantFileIf(mesh, keyOf("session:a"), decide).catch((error: unknown) => error);
          await vi.advanceTimersByTimeAsync(5_000);
          expect(await pending).toMatchObject({ message: expect.stringMatching(/Timed out waiting/) });
          expect(decide).not.toHaveBeenCalled();
          expect(fs.existsSync(lock)).toBe(true);
          expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(owner);
          vi.restoreAllMocks();
        }
      } finally {
        // Restore native functions before fixture teardown; never signal a numeric id.
        vi.restoreAllMocks();
        vi.useRealTimers();
      }
    });

    it.each(["darwin", "win32"] as const)("uses native %s incarnation to recover a reused PID", async (platform) => {
      vi.useFakeTimers({ now: Date.now() });
      mockNativePlatform(platform, platform === "darwin" ? "Thu Oct  1 12:00:00 2026" : "639264528000000000");
      const root = meshRoot();
      const lock = holdLock(root, "session:a", process.pid);
      fs.writeFileSync(path.join(lock, "owner"), `${process.pid}\n${platform === "darwin" ? "darwin:Wed Sep 30 12:00:00 2026" : "win32:639263664000000000"}\nheld\n`);
      const key = keyOf("session:a");
      const pending = participantFiles.writeParticipantFileIf(new MeshStore(root, 64 * 1024, 1_000), key, () => ({
        key, value: record("a"), version: 1, updatedAt: 1, updatedBy: identityOf("a"),
      })).catch((error: unknown) => error);
      try {
        await vi.advanceTimersByTimeAsync(5_000);
        expect(await pending).toBe(true);
        expect(childProcess.execFile).toHaveBeenCalled();
      } finally { vi.useRealTimers(); }
    });

    it.each(["darwin", "win32"] as const)("keeps live/unknown %s incarnations, even past stale age", async (platform) => {
      vi.useFakeTimers({ now: Date.now() });
      const identity = platform === "darwin" ? "darwin:Thu Oct  1 12:00:00 2026" : "win32:639264528000000000";
      const native = mockNativePlatform(platform, identity.slice(identity.indexOf(":") + 1));
      const root = meshRoot();
      const lock = holdLock(root, "session:a", process.pid);
      fs.writeFileSync(path.join(lock, "owner"), `${process.pid}\n${identity}\nheld\n`);
      const mesh = new MeshStore(root, 64 * 1024, 1_000);
      try {
        for (const unknown of [false, true]) {
          if (unknown) native.mockImplementation(() => { throw new Error("identity unreadable"); });
          const decide = vi.fn(() => undefined);
          const pending = participantFiles.writeParticipantFileIf(mesh, keyOf("session:a"), decide).catch((error: unknown) => error);
          await vi.advanceTimersByTimeAsync(5_000);
          expect(await pending).toMatchObject({ message: expect.stringMatching(/Timed out waiting/) });
          expect(decide).not.toHaveBeenCalled();
          expect(fs.existsSync(lock)).toBe(true);
        }
      } finally { vi.useRealTimers(); }
    });

    for (const cleanup of ["rmdir", "recursive"] as const) {
      it.skipIf(process.platform === "win32")(`holder release preserves a successor installed during ${cleanup} cleanup`, async () => {
        const root = meshRoot();
        const mesh = new MeshStore(root, 64 * 1024, 1_000);
        const key = keyOf("session:a");
        const lock = lockOf(root, "session:a");
        fs.mkdirSync(path.dirname(lock), { recursive: true });
        const staging = `${lock}.successor.tmp`;
        const successor = `${process.pid}\n\nsuccessor\n`;
        fs.mkdirSync(staging);
        fs.writeFileSync(path.join(staging, "owner"), successor);
        const rm = fs.rmSync.bind(fs);
        let installed = false;
        vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
          const target = String(file);
          if (!installed && (target === lock || (target.startsWith(`${lock}.`) && target.endsWith(".dead")))) {
            // Node's recursive remover unlinks owner before rmdir. An acquiring writer can
            // rename its nonempty staging directory over that now-empty canonical lock.
            rm(path.join(target, "owner"));
            fs.renameSync(staging, lock);
            installed = true;
            if (cleanup === "rmdir") {
              fs.rmdirSync(target); // old release: ENOTEMPTY; detached release: removes only our empty tombstone
              return;
            }
            // A recursive retry must not delete the successor either (no swallowed error proves safety).
          }
          rm(file, options);
        });
        await expect(participantFiles.writeParticipantFileIf(mesh, key, () => ({
          key, value: record("a"), version: 1, updatedAt: 1, updatedBy: identityOf("a"),
        }))).resolves.toBe(true);
        expect(installed).toBe(true);
        expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(successor);
        expect(fs.readdirSync(path.dirname(lock))).toEqual([path.basename(lock)]);
        expect(readParticipantFiles(root, { maxAgeMs: 0 })[0]).toMatchObject({ key, version: 1 });
      });
    }

    it.each(["EPERM", "EBUSY"])("holder release retries transient %s without touching a successor", async (code) => {
      const root = meshRoot();
      const mesh = new MeshStore(root, 64 * 1024, 1_000);
      const key = keyOf("session:a");
      const lock = lockOf(root, "session:a");
      const rename = fs.renameSync.bind(fs);
      const rm = fs.rmSync.bind(fs);
      const targets: string[] = [];
      const successor = `${process.pid}\n\nsuccessor\n`;
      vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
        if (String(source) === lock && String(target).endsWith(".dead")) {
          targets.push(String(target));
          if (targets.length <= 2) throw Object.assign(new Error("Windows sharing violation"), { code });
        }
        rename(source, target);
      });
      vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
        if (String(file).startsWith(`${lock}.`) && String(file).endsWith(".dead")) {
          // Once detached, the canonical name belongs to a successor. Retries and
          // recursive cleanup must target only this holder's unique tombstone.
          fs.mkdirSync(lock);
          fs.writeFileSync(path.join(lock, "owner"), successor);
        }
        rm(file, options);
      });
      await expect(participantFiles.writeParticipantFileIf(mesh, key, () => ({
        key, value: record("a"), version: 1, updatedAt: 1, updatedBy: identityOf("a"),
      }))).resolves.toBe(true);
      expect(targets).toHaveLength(3);
      expect(new Set(targets).size).toBe(1);
      expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(successor);
      expect(fs.readdirSync(path.dirname(lock))).toEqual([path.basename(lock)]);
      expect(readParticipantFiles(root, { maxAgeMs: 0 })[0]).toMatchObject({ key, version: 1 });
    });

    it("holder release cleans its detached lock after success and a failed decision", async () => {
      const root = meshRoot();
      const mesh = new MeshStore(root, 64 * 1024, 1_000);
      const key = keyOf("session:a");
      const lock = lockOf(root, "session:a");
      await expect(participantFiles.writeParticipantFileIf(mesh, key, () => undefined)).resolves.toBe(false);
      expect(fs.existsSync(lock)).toBe(false);
      expect(fs.readdirSync(path.dirname(lock))).toEqual([]);
      await expect(participantFiles.writeParticipantFileIf(mesh, key, () => { throw new Error("decision failed"); }))
        .rejects.toThrow("decision failed");
      expect(fs.existsSync(lock)).toBe(false);
      expect(fs.readdirSync(path.dirname(lock))).toEqual([]);
    });

    it("a live holder past any age keeps its lock; a contender times out and writes nothing", async () => {
      const root = meshRoot();
      const holder = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { stdio: "ignore" });
      const closed = new Promise<void>((resolve) => holder.once("close", () => resolve()));
      try {
        await new Promise((resolve) => holder.once("spawn", resolve));
        const lock = holdLock(root, "session:a", holder.pid!);
        await expect(participantFiles.writeParticipantFileIf(new MeshStore(root, 64 * 1024, 1_000), keyOf("session:a"), () => ({
          key: keyOf("session:a"), value: record("a"), version: 1, updatedAt: 1, updatedBy: identityOf("a"),
        }))).rejects.toThrow(/Timed out waiting for the participant file lock/);
        expect(fs.existsSync(lock)).toBe(true);
        expect(readParticipantFiles(root, { maxAgeMs: 0 })).toEqual([]);
      } finally {
        if (holder.exitCode === null && holder.signalCode === null) holder.kill("SIGKILL");
        await closed;
      }
    }, 30_000);

    it("a cleaner that read a dead holder's lock never removes the successor's lock", async () => {
      const root = meshRoot();
      const lock = holdLock(root, "session:a", await exited());
      const marker = path.join(root, "inside");
      const holding = path.join(root, "holding");
      // Z, another process, recovers the dead lock and holds the key; it enters the operation.
      const code = `
        import { createJiti } from "jiti";
        import { pathToFileURL } from "node:url";
        import fs from "node:fs";
        const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
        const files = await jiti.import("./src/topology/participant-files.ts");
        const { MeshStore } = await jiti.import("./src/mesh/store.ts");
        const [root, key, marker, holding, go] = process.argv.slice(1);
        fs.writeFileSync(go + ".ready", "");
        while (!fs.existsSync(go)) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
        await files.writeParticipantFileIf(new MeshStore(root, 64 * 1024, 1000), key, (current) => {
          fs.mkdirSync(marker);
          fs.writeFileSync(holding, "");
          Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 1000);
          fs.rmSync(marker, { recursive: true, force: true });
          return { key, value: { name: "z" }, version: (current?.version ?? 0) + 1, updatedAt: Date.now(), updatedBy: { id: "z" } };
        });
        process.exit(0);`;
      const go = path.join(root, "go");
      const z = new Promise<void>((resolve, reject) => {
        const child = spawn(process.execPath, ["--input-type=module", "-e", code, root, keyOf("session:a"), marker, holding, go],
          { cwd: process.cwd(), stdio: ["ignore", "ignore", "pipe"] });
        let err = "";
        child.stderr.on("data", (chunk) => { err += chunk; });
        child.once("exit", (status) => status === 0 ? resolve() : reject(new Error(err)));
      });
      await vi.waitFor(() => expect(fs.existsSync(`${go}.ready`)).toBe(true), { timeout: 30_000, interval: 20 });
      let released = false;
      // This process, X, reads the dead holder's owner record; before X acts on it, Z runs.
      const read = fs.readFileSync;
      vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
        const text = (read as (...args: unknown[]) => unknown)(file, ...rest);
        if (!released && file === path.join(lock, "owner")) {
          released = true;
          fs.writeFileSync(go, "");
          const deadline = Date.now() + 20_000;
          while (!fs.existsSync(holding) && Date.now() < deadline) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 20);
        }
        return text;
      }) as typeof fs.readFileSync);
      let overlap = false;
      await participantFiles.writeParticipantFileIf(new MeshStore(root, 64 * 1024, 1_000), keyOf("session:a"), (current) => {
        try { fs.mkdirSync(marker); } catch { overlap = true; }
        fs.rmSync(marker, { recursive: true, force: true });
        return { key: keyOf("session:a"), value: { name: "x" }, version: (current?.version ?? 0) + 1, updatedAt: Date.now(), updatedBy: identityOf("x") };
      });
      vi.mocked(fs.readFileSync).mockRestore();
      await z;
      expect(fs.existsSync(holding)).toBe(true);                   // Z did run while X was pending
      expect(overlap).toBe(false);
      expect(readParticipantFiles(root, { maxAgeMs: 0 })[0]).toMatchObject({ version: 2, value: { name: "x" } });
    }, 60_000);
  });
});
