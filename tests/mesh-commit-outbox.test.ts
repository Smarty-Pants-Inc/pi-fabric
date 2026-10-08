import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { COMMIT_OUTBOX_PREFIX, CommitOutbox, withStateFence } from "../src/mesh/commit-outbox.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

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
});

describe("CommitOutbox", () => {
  const effects = (log: string[]) => ({
    touch: (payload: { n: number }, _view: unknown, replay: boolean) => { log.push(`${payload.n}${replay ? ":replay" : ""}`); },
  });

  it("records effects in a changing commit, runs them after it and retires the rows on the next commit", async () => {
    const mesh = store();
    const log: string[] = [];
    const outbox = new CommitOutbox(mesh, "scope-a", identity, effects(log));
    const commit = (n: number) => mesh.writeBatch({ identity, ops: [],
      prepare: () => outbox.stage([{ kind: "put", key: `subject/${n}`, value: n }], [{ kind: "touch", key: `k${n}`, payload: { n } }]),
      afterCommit: (view) => {
        // The row is committed by the time the effect runs.
        expect(view.get(outbox.rowKey(`k${n}`))).toBeDefined();
        outbox.run(view);
      } });
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
    await mesh.writeBatch({ identity, ops: [], prepare: () => outbox.stage([], [{ kind: "touch", key: "k", payload: { n: 7 } }]),
      afterCommit: (view) => { outbox.run(view); } });
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
    await mesh.writeBatch({ identity, ops: [], prepare: () => outbox.stage([{ kind: "put", key: "subject/x", value: 1 }],
      [{ kind: "touch", key: "same", payload: { n: 1 } }, { kind: "touch", key: "same", payload: { n: 2 } }]),
    afterCommit: (view) => { outbox.run(view); } });
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
      await store.writeBatch({ identity, ops: [],
        prepare: () => outbox.stage([{ kind: "put", key: "crash/subject", value: 1 }], [{ kind: "touch", key: "subject", payload: { n: 1 } }]),
        afterCommit: view => { outbox.run(view); } });
      fs.appendFileSync(effectLog, "survived\\n");`;
    const child = spawn(process.execPath, ["--input-type=module", "-e", code, root, effectLog], { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] });
    let stderr = "";
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    const signal = await new Promise<NodeJS.Signals | null>((resolve, reject) => {
      child.once("error", reject);
      child.once("close", (_code, signal) => resolve(signal));
    });
    expect(signal, stderr).toBe("SIGKILL");
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
