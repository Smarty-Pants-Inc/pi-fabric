import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { COMMIT_OUTBOX_PREFIX, CommitOutbox, withStateFence } from "../src/mesh/commit-outbox.js";
import { MeshStore, type MeshBatchView, type MeshIdentity } from "../src/mesh/store.js";
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

  // SA-03/data safety: a prior successful effect is not proof after its retry fails.
  it.each(["file", "sqlite"].flatMap(backend => [false, true].map(async => ({ backend, async }))))(
    "SA-03 retains the exact failed replay after a successful effect and retirement fault ($backend, async=$async)", async mode => {
    const mesh = new MeshStore(meshRoot(), 64 * 1024, 100, { stateBackend: mode.backend as "file" | "sqlite" });
    let fail = false;
    const handler = (payload: { n: number }) => { if (fail && payload.n === 1) throw new Error("current bytes unreadable"); };
    const outbox = new CommitOutbox(mesh, "dual-fault", identity, mode.async ? {} : { touch: handler },
      mode.async ? { touch: async payload => { await Promise.resolve(); handler(payload); } } : {});
    const replay = async (throughRecovery = false) => {
      let view!: MeshBatchView;
      await mesh.writeBatch({ identity, ops: [], afterCommit: current => { view = current; } });
      if (mode.async) await outbox.plan().replayAsync(view);
      else if (throughRecovery) await outbox.recover();
      else outbox.plan().replay(view);
    };
    try {
      let emptyView!: MeshBatchView;
      await mesh.writeBatch({ identity, ops: [], afterCommit: current => { emptyView = current; } });
      const plan = outbox.plan();
      let view!: MeshBatchView;
      await mesh.writeBatch({ identity, ops: [], prepare: () => plan.stage([], [1, 2].map(n => ({
        kind: "touch", key: `k${n}`, payload: { n },
      })), { durable: true }), afterCommit: current => { view = current; } });
      if (mode.async) await plan.runAsync(view); else plan.run(view);
      expect(outbox.retiring).toBe(2);
      const before = mesh.get(outbox.rowKey("k1"), { fresh: true });
      const retirement = vi.spyOn(mesh, "writeBatch").mockRejectedValueOnce(new Error("retire failed after effect"));
      await expect(outbox.retire()).rejects.toThrow("retire failed after effect");
      retirement.mockRestore();
      fail = true;
      await replay(true);
      await outbox.retire();
      expect(mesh.get(outbox.rowKey("k1"), { fresh: true })).toEqual(before);
      expect(mesh.get(outbox.rowKey("k2"), { fresh: true })).toBeUndefined();
      // The opportunistic stage() retirement path must not use the failed proof either.
      await mesh.writeBatch({ identity, ops: [], prepare: () => outbox.plan().stage([
        { kind: "put", key: "unrelated/change", value: true },
      ], []) });
      expect(mesh.get(outbox.rowKey("k1"), { fresh: true })).toEqual(before);
      fail = false;
      await replay();
      // A late plan on an older empty snapshot cannot prune the newer completion proof.
      const late = outbox.plan();
      if (mode.async) await late.runAsync(emptyView); else late.run(emptyView);
      expect(outbox.retiring).toBe(1);
      await outbox.retire();
      expect(mesh.listAll(outbox.prefix, { fresh: true })).toEqual([]);
    } finally { mesh.closeState(); }
  });

  it.each(["file", "sqlite"] as const)("SA-03 fences a queued retirement against an in-flight retry and a newer completion (%s)", async backend => {
    const mesh = new MeshStore(meshRoot(), 64 * 1024, 100, { stateBackend: backend });
    let block = false;
    let releaseRetry!: () => void;
    const retryGate = new Promise<void>(resolve => { releaseRetry = resolve; });
    const outbox = new CommitOutbox(mesh, "inflight-retry", identity, {}, { touch: async (payload: { n: number }) => {
      if (block && payload.n === 1) { await retryGate; throw new Error("older retry failed"); }
    } });
    const record = async (n: number) => {
      const plan = outbox.plan();
      let view!: MeshBatchView;
      await mesh.writeBatch({ identity, ops: [], prepare: () => plan.stage([], [{ kind: "touch", key: "same", payload: { n } }],
        { durable: true }), afterCommit: current => { view = current; } });
      return { plan, view };
    };
    let retry: Promise<number> | undefined;
    let retiring: Promise<void> | undefined;
    let releaseRetirement!: () => void;
    try {
      const first = await record(1);
      await first.plan.runAsync(first.view);
      const before = mesh.get(outbox.rowKey("same"), { fresh: true });
      const original = mesh.writeBatch.bind(mesh);
      const retirementGate = new Promise<void>(resolve => { releaseRetirement = resolve; });
      const snapshot = vi.spyOn(mesh, "writeBatch").mockImplementationOnce(async input => {
        await retirementGate;
        return original(input);
      });
      retiring = outbox.retire(); // Captures the old done map, but has not acquired writer custody.
      block = true;
      retry = outbox.plan().replayAsync(first.view);
      releaseRetirement();
      await retiring;
      snapshot.mockRestore();
      expect(mesh.get(outbox.rowKey("same"), { fresh: true })).toEqual(before);
      const newer = await record(2);
      const newerRow = mesh.get(outbox.rowKey("same"), { fresh: true });
      await newer.plan.runAsync(newer.view);
      releaseRetry();
      await retry;
      expect(mesh.get(outbox.rowKey("same"), { fresh: true })).toEqual(newerRow);
      expect(outbox.retiring).toBe(1); // The old failure cannot invalidate the newer success.
      await outbox.retire();
      expect(mesh.get(outbox.rowKey("same"), { fresh: true })).toBeUndefined();
    } finally {
      releaseRetirement?.(); releaseRetry();
      await retiring; await retry;
      mesh.closeState();
    }
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
