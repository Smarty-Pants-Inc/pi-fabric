import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { COMMIT_OUTBOX_PREFIX, CommitOutbox, withStateFence } from "../src/mesh/commit-outbox.js";
import { MeshStore, type MeshBatchView, type MeshIdentity } from "../src/mesh/store.js";
import { StoreBridgeSide } from "../src/mesh/bridge.js";
import { readHostLease } from "../src/topology/host-leases.js";
import type { FabricHostRecord, FabricParticipantRecord } from "../src/topology/types.js";
import { MeshStateWalCapError } from "../src/mesh/state-sqlite.js";
import { MeshBackgroundRetry } from "../src/core/atomic-write.js";

// smarty-dev#6477 L2b: state-domain fences and the durable commit outbox (plan R3, R11).
const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const identity: MeshIdentity = { id: "session:outbox", name: "main", kind: "main" };
const meshRoot = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-commit-outbox-"));
  roots.push(root);
  return path.join(root, "mesh");
};
const store = (root = meshRoot()) => new MeshStore(root, 64 * 1024, 100);

describe("withStateFence", () => {
  it("runs its body as a state operation that writes nothing, never through exclusive()", async () => {
    const mesh = store();
    await mesh.put({ key: "a/b", value: 1, identity });
    const before = mesh.stateStamp();
    const exclusive = vi.spyOn(mesh, "exclusive");
    const seen = await withStateFence(mesh, identity, (view) => view.get("a/b")?.value);
    expect(seen).toBe(1);
    expect(exclusive).not.toHaveBeenCalled();
    expect(mesh.stateStamp()).toBe(before);
    expect(mesh.get("a/b")?.version).toBe(1);
  });

  it("bounds a zero-wait fence and never retries a body that threw", async () => {
    const mesh = store();
    let runs = 0;
    await expect(withStateFence(mesh, identity, () => { runs += 1; throw new Error("refused"); }, 250)).rejects.toThrow("refused");
    expect(runs).toBe(1);
    // Another live holder of the state write lock (file backend): a zero-wait fence fails typed.
    const other = store(mesh.root);
    const lock = path.join(mesh.root, ".lock");
    fs.mkdirSync(lock);
    fs.writeFileSync(path.join(lock, "owner"), `held\n${process.pid}\n${Date.now()}\n`);
    await expect(withStateFence(other, identity, () => true, 0)).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
    fs.rmSync(lock, { recursive: true, force: true });
    expect(await withStateFence(other, identity, () => true, 0)).toBe(true);
  });

  // smarty-dev#7451 item 8: a WAL-cap refusal wrote nothing, like a lock timeout, so the fence and the residency
  // background retry (MeshBackgroundRetry) retry it with backoff instead of reporting it as a failure.
  it("retries a WAL-cap refusal within its budget, and the background retry backs it off", async () => {
    const mesh = store();
    const refused = () => new MeshStateWalCapError(200 * 1024 * 1024, 96 * 1024 * 1024);
    const tryLock = mesh.withTryLock.bind(mesh);
    vi.spyOn(mesh, "withTryLock").mockImplementationOnce(async () => { throw refused(); }).mockImplementation(tryLock);
    expect(await withStateFence(mesh, identity, () => "ran", 1_000)).toBe("ran");
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const retry = new MeshBackgroundRetry("resident delivery", 100, 400);
    expect(await retry.run(() => { throw refused(); })).toBe("retry");
    expect(retry.waitMs).toBeGreaterThan(0);
  });
});

describe("CommitOutbox", () => {
  const effects = (log: string[]) => ({
    touch: (payload: { n: number }, _view: unknown, replay: boolean) => { log.push(`${payload.n}${replay ? ":replay" : ""}`); },
  });

  it("records effects in a changing commit, runs them after it and retires the rows on the next commit", async () => {
    const mesh = store();
    const log: string[] = [];
    const outbox = new CommitOutbox(mesh, "scope-a", identity, effects(log));
    const commit = (n: number) => {
      const plan = outbox.plan();
      return mesh.writeBatch({ identity, ops: [],
        prepare: () => plan.stage([{ kind: "put", key: `subject/${n}`, value: n }], [{ kind: "touch", key: `k${n}`, payload: { n } }]),
        afterCommit: (view) => {
          // The row is committed by the time the effect runs.
          expect(view.get(outbox.rowKey(`k${n}`))).toBeDefined();
          plan.run(view);
        } });
    };
    await commit(1);
    expect(log).toEqual(["1"]);
    expect(mesh.listAll(outbox.prefix, { fresh: true })).toHaveLength(1);
    expect(outbox.retiring).toBe(1);
    await commit(2);
    expect(log).toEqual(["1", "2"]);
    // Row 1 went with commit 2; row 2 waits for the next one (or retire()).
    expect(mesh.listAll(outbox.prefix, { fresh: true }).map((entry) => entry.key)).toEqual([outbox.rowKey("k2")]);
    await outbox.retire();
    expect(mesh.listAll(outbox.prefix, { fresh: true })).toEqual([]);
    expect(mesh.listAll(COMMIT_OUTBOX_PREFIX, { fresh: true })).toEqual([]);
  });

  it("records nothing for a batch that commits nothing, and still runs its effects", async () => {
    const mesh = store();
    const log: string[] = [];
    const outbox = new CommitOutbox(mesh, "scope-b", identity, effects(log));
    const plan = outbox.plan();
    await mesh.writeBatch({ identity, ops: [], prepare: () => plan.stage([], [{ kind: "touch", key: "k", payload: { n: 7 } }]),
      afterCommit: (view) => { plan.run(view); } });
    expect(log).toEqual(["7"]);
    expect(mesh.stateStamp()).toBeUndefined();
    expect(outbox.retiring).toBe(0);
  });

  it("keeps one row per idempotency key, and leaves a failed effect for recovery", async () => {
    const mesh = store();
    let fail = true;
    const log: string[] = [];
    const outbox = new CommitOutbox(mesh, "scope-c", identity, {
      touch: (payload: { n: number }, _view, replay) => {
        if (fail) throw new Error("disk full");
        log.push(`${payload.n}${replay ? ":replay" : ""}`);
      },
    });
    const plan = outbox.plan();
    await mesh.writeBatch({ identity, ops: [], prepare: () => plan.stage([{ kind: "put", key: "subject/x", value: 1 }],
      [{ kind: "touch", key: "same", payload: { n: 1 } }, { kind: "touch", key: "same", payload: { n: 2 } }]),
    afterCommit: (view) => { plan.run(view); } });
    const rows = mesh.listAll(outbox.prefix, { fresh: true });
    expect(rows).toHaveLength(1);
    expect((rows[0]!.value as { payload: { n: number } }).payload.n).toBe(2);
    expect(log).toEqual([]);
    fail = false;
    // A fresh process of the same writer replays the row once, then deletes it.
    const restarted = new CommitOutbox(store(mesh.root), "scope-c", identity, effects(log));
    expect(await restarted.recover()).toBe(1);
    expect(log).toEqual(["2:replay"]);
    expect(await restarted.recover()).toBe(0);
    expect(mesh.listAll(outbox.prefix, { fresh: true })).toEqual([]);
  });

  it("concurrent batches on SQLite each run exactly their own effect and get their own result (owner review P2)", async () => {
    // ResidentHost.#queueDelivery's shape: a durable batch whose afterCommit decides "chosen".
    const mesh = new MeshStore(meshRoot(), 64 * 1024, 100, { stateBackend: "sqlite" });
    try {
      expect(mesh.stateBackend).toBe("sqlite");
      const log: string[] = [];
      const order: string[] = [];
      const outbox = new CommitOutbox(mesh, "scope-concurrent", identity, effects(log));
      const deliver = async (n: number) => {
        const plan = outbox.plan();
        let ran = -1;
        let mine: string[] = [];
        await mesh.writeBatch({ identity, ops: [],
          prepare: () => {
            order.push(`prepare:${n}`);
            return plan.stage([], [{ kind: "touch", key: `delivery:${n}`, payload: { n } }], { durable: true });
          },
          afterCommit: (view) => {
            order.push(`after:${n}`);
            const before = log.length;
            ran = plan.run(view);
            mine = log.slice(before);
          } });
        return { n, ran, mine };
      };
      const ids = [1, 2, 3, 4, 5, 6];
      const results = await Promise.all(ids.map(deliver));
      // SQLite commits a batch, then re-acquires custody for its afterCommit: another batch's
      // prepare ran in between (the interleaving a shared stage got wrong).
      expect(ids.some((n) => {
        const own = order.indexOf(`prepare:${n}`);
        const after = order.indexOf(`after:${n}`);
        return order.slice(own + 1, after).some((step) => step.startsWith("prepare:"));
      })).toBe(true);
      for (const result of results) {
        expect(result.ran).toBe(1);
        expect(result.mine).toEqual([String(result.n)]);
      }
      expect([...log].sort()).toEqual(ids.map(String));
    } finally {
      mesh.closeState();
    }
  });

  it.each(["file", "sqlite"] as const)("a late older afterCommit leaves a newer same-key row pending; a crash before its effect replays it once (%s, smarty-dev#7023)", async (backend) => {
    const root = meshRoot();
    const mesh = new MeshStore(root, 64 * 1024, 100, { stateBackend: backend });
    const log: string[] = [];
    const outbox = new CommitOutbox(mesh, "scope-late", identity, effects(log));
    const commit = (n: number, key: string, plan = outbox.plan()) => mesh.writeBatch({ identity, ops: [],
      prepare: () => plan.stage([{ kind: "put", key: `subject/${n}`, value: n }], [{ kind: "touch", key, payload: { n } }]),
      // Deferred: this commit's afterCommit has not run yet.
      afterCommit: () => {} }).then(() => plan);
    try {
      const older = await commit(1, "same");
      // The newer same-key commit lands; its afterCommit never runs (the process dies before it).
      await commit(2, "same");
      const newer = mesh.get(outbox.rowKey("same"), { fresh: true });
      expect((newer?.value as { payload: { n: number } }).payload.n).toBe(2);
      // The older afterCommit runs late, on a view that already holds the newer row.
      await mesh.writeBatch({ identity, ops: [], afterCommit: (view) => { expect(older.run(view)).toBe(1); } });
      expect(log).toEqual(["1"]);
      expect(outbox.retiring).toBe(0);
      // A later batch commits and retires what ran: the newer row must survive it.
      const later = outbox.plan();
      await mesh.writeBatch({ identity, ops: [], prepare: () => later.stage([{ kind: "put", key: "subject/3", value: 3 }], [{ kind: "touch", key: "other", payload: { n: 3 } }]),
        afterCommit: (view) => { later.run(view); } });
      await outbox.retire();
      expect(mesh.get(outbox.rowKey("same"), { fresh: true })?.version).toBe(newer?.version);
    } finally {
      mesh.closeState();
    }
    // Restart: the newer effect is pending and runs exactly once.
    const reopened = new MeshStore(root, 64 * 1024, 100, { stateBackend: backend });
    try {
      const restarted = new CommitOutbox(reopened, "scope-late", identity, effects(log));
      expect(await restarted.recover()).toBe(1);
      expect(await restarted.recover()).toBe(0);
      expect(log).toEqual(["1", "3", "2:replay"]);
      expect(reopened.listAll(restarted.prefix, { fresh: true })).toEqual([]);
    } finally {
      reopened.closeState();
    }
  });

  it.each(["file", "sqlite"] as const)("bounds retirement and keeps failed batches and newer recordings pending (%s)", async backend => {
    const mesh = new MeshStore(meshRoot(), 64 * 1024, 100, { stateBackend: backend });
    const log: string[] = [];
    const outbox = new CommitOutbox(mesh, "bounded-retire", identity, effects(log));
    try {
      const plan = outbox.plan();
      await mesh.writeBatch({ identity, ops: [], prepare: () => plan.stage([], Array.from({ length: 137 }, (_, n) => ({
        kind: "touch", key: `k${n}`, payload: { n },
      })), { durable: true }), afterCommit: view => { plan.run(view); } });
      const original = mesh.writeBatch.bind(mesh), sizes: number[] = [];
      const batch = vi.spyOn(mesh, "writeBatch").mockImplementation(async input => {
        sizes.push(input.ops.length);
        if (sizes.length === 2) throw new Error("retirement interrupted");
        return original(input);
      });
      await expect(outbox.retire()).rejects.toThrow("retirement interrupted");
      expect(sizes).toEqual([64, 64]);
      expect(outbox.retiring).toBe(73);
      expect(mesh.listAll(outbox.prefix, { fresh: true })).toHaveLength(73);
      batch.mockRestore();
      // A newer recording after that failure cannot be retired by the old version fence.
      const newer = outbox.plan();
      await mesh.writeBatch({ identity, ops: [], prepare: () => newer.stage([], [{ kind: "touch", key: "k100", payload: { n: 999 } }], { durable: true }) });
      // stage() folds at most 64 independent retirements into the new atomic commit.
      expect(mesh.listAll(outbox.prefix, { fresh: true })).toHaveLength(9);
      await outbox.retire();
      expect(outbox.retiring).toBe(0);
      expect(mesh.listAll(outbox.prefix, { fresh: true }).map(entry => entry.key)).toEqual([outbox.rowKey("k100")]);
      expect(await outbox.recover()).toBe(1);
      expect(log.at(-1)).toBe("999:replay");
      expect(mesh.listAll(outbox.prefix, { fresh: true })).toEqual([]);
    } finally { mesh.closeState(); }
  });

  // Keep: data safety and a dual-fault path (SA-03) that a normal delivery flow cannot reach cheaply.
  describe.each(["file", "sqlite"] as const)("SA-03 completion proof (%s)", backend => {
    const completed = async () => {
      const mesh = new MeshStore(meshRoot(), 64 * 1024, 100, { stateBackend: backend });
      const fault = { active: false };
      const attempts: string[] = [];
      const artifact = path.join(path.dirname(mesh.root), "effect-a");
      const outbox = new CommitOutbox(mesh, "failed-replay", identity, {
        touch: (payload: string) => {
          attempts.push(`${payload}:${fault.active ? "fault" : "ok"}`);
          if (payload === "a") {
            if (fault.active) throw new Error("effect write refused");
            fs.writeFileSync(artifact, "installed");
          }
        },
      });
      const plan = outbox.plan();
      await mesh.writeBatch({ identity, ops: [], prepare: () => plan.stage([], [
        { kind: "touch", key: "a", payload: "a" }, { kind: "touch", key: "b", payload: "b" },
      ], { durable: true }), afterCommit: view => { plan.run(view); } });
      expect(fs.readFileSync(artifact, "utf8")).toBe("installed");
      const row = mesh.get(outbox.rowKey("a"), { fresh: true });
      expect(row).toBeDefined();
      expect(outbox.retiring).toBe(2);
      const refusal = vi.spyOn(mesh, "writeBatch").mockRejectedValueOnce(new Error("retirement refused"));
      await expect(outbox.retire()).rejects.toThrow("retirement refused");
      refusal.mockRestore();
      expect(mesh.get(outbox.rowKey("a"), { fresh: true })).toEqual(row);
      fault.active = true;
      return { mesh, outbox, fault, attempts, artifact, row };
    };

    it.each(["replay", "recover", "run retry"] as const)("retains the exact failed row through direct and next-stage retirement after %s", async route => {
      const { mesh, outbox, fault, attempts, row } = await completed();
      try {
        if (route === "recover") expect(await outbox.recover()).toBe(1); // Only unrelated B succeeds.
        else {
          await mesh.writeBatch({ identity, ops: [], prepare: view => { outbox.plan().replay(view); return []; } });
          if (route === "run retry") await mesh.writeBatch({ identity, ops: [], afterCommit: view => { outbox.plan().run(view); } });
        }
        expect(attempts).toContain("a:fault");
        await outbox.retire();
        expect(mesh.get(outbox.rowKey("a"), { fresh: true })).toEqual(row);
        expect(mesh.get(outbox.rowKey("b"), { fresh: true })).toBeUndefined();
        const next = outbox.plan();
        await mesh.writeBatch({ identity, ops: [], prepare: () => next.stage([
          { kind: "put", key: "subject/next", value: 1 },
        ], []), afterCommit: view => { next.run(view); } });
        expect(mesh.get(outbox.rowKey("a"), { fresh: true })).toEqual(row);
        fault.active = false;
        expect(await outbox.recover()).toBe(1);
        expect(mesh.listAll(outbox.prefix, { fresh: true })).toEqual([]);
        expect(attempts.at(-1)).toBe("a:ok");
      } finally { mesh.closeState(); }
    });

    it.each(["retire", "stage"] as const)("rechecks %s eligibility captured before the failed replay", async route => {
      const { mesh, outbox, fault, row } = await completed();
      try {
        if (route === "retire") {
          const original = mesh.writeBatch.bind(mesh);
          let release!: () => void;
          const gate = new Promise<void>(resolve => { release = resolve; });
          const queued = vi.spyOn(mesh, "writeBatch").mockImplementationOnce(async input => {
            await gate;
            return original(input);
          });
          const retiring = outbox.retire(); // Captures both old successful versions before replay.
          try {
            await original({ identity, ops: [], prepare: view => { outbox.plan().replay(view); return []; } });
          } finally { release(); await retiring; queued.mockRestore(); }
        } else {
          await mesh.writeBatch({ identity, ops: [], prepare: view => {
            const ops = outbox.plan().stage([], [], { durable: true });
            expect(ops.filter(op => op.kind === "delete")).toHaveLength(2);
            outbox.plan().replay(view); // Invalidates A after stage selected its deletion.
            return ops;
          } });
        }
        expect(mesh.get(outbox.rowKey("a"), { fresh: true })).toEqual(row);
        expect(mesh.get(outbox.rowKey("b"), { fresh: true })).toBeUndefined();
        fault.active = false;
        expect(await outbox.recover()).toBe(1);
        expect(mesh.listAll(outbox.prefix, { fresh: true })).toEqual([]);
      } finally { mesh.closeState(); }
    });

    it.each([false, true])("does not erase a newer nested successful completion when the older execution fails=%s", async fails => {
      const mesh = new MeshStore(meshRoot(), 64 * 1024, 100, { stateBackend: backend });
      let nested: (() => void) | undefined;
      const log: number[] = [];
      const outbox = new CommitOutbox(mesh, "newer-completion", identity, {
        touch: (n: number) => {
          log.push(n);
          if (n === 1 && nested) { const run = nested; nested = undefined; run(); if (fails) throw new Error("older failed"); }
        },
      });
      const older = outbox.plan(), newer = outbox.plan();
      let oldView!: MeshBatchView, newView!: MeshBatchView;
      try {
        await mesh.writeBatch({ identity, ops: [], prepare: () => older.stage([], [
          { kind: "touch", key: "same", payload: 1 },
        ], { durable: true }), afterCommit: view => { oldView = view; older.run(view); } });
        // SQLite callback views are live; its public stateView pins a detached snapshot.
        oldView = mesh.stateView()?.view ?? oldView;
        await mesh.writeBatch({ identity, ops: [], prepare: () => newer.stage([], [
          { kind: "touch", key: "same", payload: 2 },
        ], { durable: true }), afterCommit: view => { newView = view; } });
        newView = mesh.stateView()?.view ?? newView;
        expect(newView.version(outbox.rowKey("same"))).toBeGreaterThan(oldView.version(outbox.rowKey("same")));
        nested = () => { newer.run(newView); };
        expect(outbox.plan().replay(oldView)).toBe(fails ? 0 : 1);
        expect(log).toEqual([1, 1, 2]);
        expect(outbox.retiring).toBe(1);
        await outbox.retire();
        expect(mesh.listAll(outbox.prefix, { fresh: true })).toEqual([]);
      } finally { mesh.closeState(); }
    });

    it("keeps a newer proof across an old stamp mismatch and an older empty run view", async () => {
      const mesh = new MeshStore(meshRoot(), 64 * 1024, 100, { stateBackend: backend });
      const outbox = new CommitOutbox(mesh, "stale-views", identity, {
        touch: (n: number) => { if (n === 1) throw new Error("old recording refused"); },
      });
      let absent!: MeshBatchView, current!: MeshBatchView;
      const older = outbox.plan(), newer = outbox.plan();
      try {
        await mesh.writeBatch({ identity, ops: [], afterCommit: view => { absent = view; } });
        absent = mesh.stateView()?.view ?? absent;
        await mesh.writeBatch({ identity, ops: [], prepare: () => older.stage([], [
          { kind: "touch", key: "same", payload: 1 },
        ], { durable: true }) });
        await mesh.writeBatch({ identity, ops: [], prepare: () => newer.stage([], [
          { kind: "touch", key: "same", payload: 2 },
        ], { durable: true }), afterCommit: view => { current = view; newer.run(view); } });
        expect(older.run(current)).toBe(0); // Failed old stamp, then a successful current-row retry.
        expect(outbox.retiring).toBe(1);
        outbox.plan().run(absent); // The empty snapshot predates both recordings.
        expect(outbox.retiring).toBe(1);
        await outbox.retire();
        expect(mesh.listAll(outbox.prefix, { fresh: true })).toEqual([]);
      } finally { mesh.closeState(); }
    });
  });

  it.each(["file", "sqlite"] as const)("SA-04 main immediate withdrawal removes the lease with a retained tombstone (%s)", async backend => {
    const mesh = new MeshStore(meshRoot(), 64 * 1024, 100, { stateBackend: backend });
    const now = Date.now();
    const peer: MeshIdentity = { id: "session:withdrawal-00000000", name: "main", kind: "main", sessionId: "withdrawal" };
    const host = { format: 1, id: peer.id, rootId: peer.id, identity: peer, startedAt: now, updatedAt: now, expiresAt: now + 60_000 } as FabricHostRecord;
    const participant = { format: 1, id: peer.id, rootId: peer.id, kind: "root", ownerHostId: peer.id, ownerIdentityId: peer.id,
      name: "withdrawal", label: "withdrawal", status: "idle", runner: "pi", transport: "host", capabilities: ["steer", "followUp"],
      sessionId: "withdrawal", startedAt: now, updatedAt: now, controlProtocol: "v1" } as FabricParticipantRecord;
    const key = (prefix: string) => prefix + createHash("sha256").update(peer.id).digest("hex");
    const side = new StoreBridgeSide(mesh, "ryzen2", () => now);
    try {
      await side.mirror({ hosts: [{ record: host, expiresAt: now + 60_000 }], participants: [participant] });
      expect(readHostLease(mesh.root, peer.id)?.identityId).toBe(peer.id);
      await side.mirror({ hosts: [], participants: [] });
      expect(mesh.get(key("topology/hosts/"), { fresh: true })).toBeUndefined();
      expect(mesh.get(key("topology/participants/"), { fresh: true })).toBeUndefined();
      let tombstone = 0;
      await mesh.writeBatch({ identity, ops: [], afterCommit: view => { tombstone = view.version(key("topology/hosts/")); } });
      expect(tombstone).toBeGreaterThan(0);
      expect(readHostLease(mesh.root, peer.id)).toBeUndefined();
      // A restarted complete pass replays and retires the successful unlease recording.
      await new StoreBridgeSide(mesh, "ryzen2", () => now).mirror({ hosts: [], participants: [] });
      expect(mesh.listAll(COMMIT_OUTBOX_PREFIX, { fresh: true })).toEqual([]);
    } finally { mesh.closeState(); }
  });

  it("does not take the lock to recover when nothing is pending", async () => {
    const mesh = store();
    const outbox = new CommitOutbox(mesh, "scope-d", identity, effects([]));
    const batch = vi.spyOn(mesh, "writeBatch");
    expect(await outbox.recover()).toBe(0);
    expect(batch).not.toHaveBeenCalled();
  });

  it("crash after COMMIT, before the effect: the restart runs the effect exactly once", async () => {
    const root = meshRoot();
    fs.mkdirSync(root, { recursive: true });
    const effectLog = path.join(path.dirname(root), "effect.log");
    const code = `
      import { createJiti } from "jiti";
      import { pathToFileURL } from "node:url";
      import fs from "node:fs";
      const jiti = createJiti(pathToFileURL(process.cwd() + "/index.js").href);
      const { MeshStore } = await jiti.import("./src/mesh/store.ts");
      const { CommitOutbox } = await jiti.import("./src/mesh/commit-outbox.ts");
      const [root, effectLog] = process.argv.slice(1);
      const identity = { id: "session:outbox", name: "main", kind: "main" };
      const store = new MeshStore(root, 65536, 100);
      // The process dies after the state commit and before its effect writes anything.
      const outbox = new CommitOutbox(store, "crash", identity, {
        touch: () => { process.kill(process.pid, "SIGKILL"); fs.appendFileSync(effectLog, "child\\n"); },
      });
      const plan = outbox.plan();
      await store.writeBatch({ identity, ops: [],
        prepare: () => plan.stage([{ kind: "put", key: "crash/subject", value: 1 }], [{ kind: "touch", key: "subject", payload: { n: 1 } }]),
        afterCommit: view => { plan.run(view); } });
      fs.appendFileSync(effectLog, "survived\\n");`;
    const child = spawn(process.execPath, ["--input-type=module", "-e", code, root, effectLog], { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] });
    let stderr = "";
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    const exit = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
      child.once("error", reject);
      child.once("close", (code, signal) => resolve({ code, signal }));
    });
    // Windows reports a killed child with signalCode null and a non-zero exit code.
    if (process.platform === "win32" && exit.signal === null) expect(exit.code, stderr).not.toBe(0);
    else expect(exit.signal, stderr).toBe("SIGKILL");
    expect(fs.existsSync(effectLog)).toBe(false);
    const mesh = store(root);
    expect(mesh.get("crash/subject", { fresh: true })?.value).toBe(1);
    const runs: number[] = [];
    const restarted = new CommitOutbox(mesh, "crash", identity, {
      touch: (payload: { n: number }, view, replay) => {
        expect(replay).toBe(true);
        expect(view.get("crash/subject")?.value).toBe(1);
        runs.push(payload.n);
      },
    });
    expect(mesh.listAll(restarted.prefix, { fresh: true })).toHaveLength(1);
    expect(await restarted.recover()).toBe(1);
    expect(await new CommitOutbox(store(root), "crash", identity, restarted.handlers).recover()).toBe(0);
    expect(runs).toEqual([1]);
    expect(mesh.listAll(restarted.prefix, { fresh: true })).toEqual([]);
  }, 30_000);
});
