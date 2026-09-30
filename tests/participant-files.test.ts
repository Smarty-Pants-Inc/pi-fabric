import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import { MeshStore, RUNTIME_MESH_READ_CACHE_MS, type MeshIdentity } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { LIVENESS_POLICY_KEY, removeHostLease } from "../src/topology/host-leases.js";
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
const setPolicy = (root: string) => new MeshStore(root, 64 * 1024, 1_000).put({
  key: LIVENESS_POLICY_KEY, value: { version: 1, hostLeases: "files", participants: "files" }, identity: identityOf("owner"),
});

describe("participant files", () => {
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
      await a.start();
      expect(stateOwner(root)).toBe("session:b");
      expect(owner(root)).not.toBe("session:a");                 // A's delayed copy was dropped
      const c = directory(root, "c", () => []);
      await c.start();
      expect(c.get(shared, Date.now(), { fresh: true })).toMatchObject({ ownerHostId: "session:b", stale: false });
      await a.refresh();
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
      child.once("exit", () => resolve(child.pid!));
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
      try {
        await new Promise((resolve) => holder.once("spawn", resolve));
        const lock = holdLock(root, "session:a", holder.pid!);
        await expect(participantFiles.writeParticipantFileIf(new MeshStore(root, 64 * 1024, 1_000), keyOf("session:a"), () => ({
          key: keyOf("session:a"), value: record("a"), version: 1, updatedAt: 1, updatedBy: identityOf("a"),
        }))).rejects.toThrow(/Timed out waiting for the participant file lock/);
        expect(fs.existsSync(lock)).toBe(true);
        expect(readParticipantFiles(root, { maxAgeMs: 0 })).toEqual([]);
      } finally {
        holder.kill("SIGKILL");
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
