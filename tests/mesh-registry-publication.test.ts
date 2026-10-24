// Local-safe actor registry publication regression for PR #1269.
// Acquisition and registry/presence writes are real. Deletion, recovery and
// pinned release are NOT: requested removals are recorded and retained by rename.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import type { FabricActorInfo } from "../src/actors/types.js";
import type { AgentManager } from "../src/agents/manager.js";
import { runAbortable, throwIfExecutionExpired } from "../src/async-settlement.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import * as fenceLock from "../src/mesh/fence-lock.js";
import { meshInvocationStorage } from "../src/mesh/invocation-context.js";
import { MeshLock, withMeshInvocationContext } from "../src/mesh/mesh-lock.js";
import { MeshStore } from "../src/mesh/store.js";

const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
const identity = { id: "session:test", name: "test", kind: "main" as const };
const request = { name: "subscriber", instructions: "Review.", topics: ["test.work"] };

let root: string;
let serial: number;
let deleteCalls: string[];
const fixtures: Array<{ actors: ActorManager; mesh: MeshStore }> = [];

// Test-owned, non-destructive handoff. Never run a product release/recovery
// implementation, even on a platform with a native deletion helper.
const retain = (file: string): void => {
  expect(path.resolve(file).startsWith(`${path.resolve(root)}${path.sep}`)).toBe(true);
  if (fs.existsSync(file)) fs.renameSync(file, path.join(root, "kept", String(++serial)));
};
const heldCanonical = (lock: string): (() => void) => {
  fs.mkdirSync(lock, { recursive: true });
  // Complete, live, no incarnation: neither mesh nor registry may recover it.
  fs.writeFileSync(path.join(lock, "owner"), `held-by-publication-test\n${process.pid}\n${Date.now()}\n`);
  let released = false;
  return () => {
    if (released) return;
    released = true;
    if (fs.existsSync(lock)) retain(lock);
  };
};
const setup = (overrides: { lockTimeoutMs?: number; presenceRetryMs?: number } = {}) => {
  const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 100, {
    lockProtocol: 1, stateBackend: "file", canWakeResidents: () => false,
    ...(overrides.lockTimeoutMs !== undefined ? { lockTimeoutMs: overrides.lockTimeoutMs } : {}),
  });
  // No runner/worktree/worker is constructed or launched. The actor manager's
  // real admission, registry save and mesh publication paths are the subject.
  const agents = {
    config: structuredClone(DEFAULT_FABRIC_CONFIG.agents),
    resolveKernel: () => undefined,
    resolvePythonRuntime: () => undefined,
    defaultModel: () => undefined,
    assertModelAllowed: vi.fn(),
    resolveModelReasonPolicy: () => ({ model: undefined, thinking: undefined, fallback: false }),
    actorArchiveSources: () => new Map(),
    spawn: vi.fn(() => { throw new Error("Publication must not launch a worker"); }),
  } as unknown as AgentManager;
  const actorRoot = path.join(root, "actors");
  const actors = new ActorManager("test", identity, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, enabled: true }, agents, vi.fn(), {
      actorRoot, persistent: true, actorScope: "session",
      canConsumeMesh: () => false, reapDeadSessionPresence: false,
      meshRetentionSweepPath: false,
      ...(overrides.presenceRetryMs !== undefined ? { presenceRetryMs: overrides.presenceRetryMs } : {}),
    });
  fixtures.push({ actors, mesh });
  return { actors, mesh, actorRoot };
};
const watchPresence = (mesh: MeshStore, entered: ReturnType<typeof deferred>): void => {
  const put = mesh.put.bind(mesh);
  vi.spyOn(mesh, "put").mockImplementation((...args) => {
    const pending = put(...args);
    if (args[0].key.startsWith("actors/test/")) entered.resolve();
    return pending;
  });
};
const assertPublished = (actor: FabricActorInfo, actorRoot: string, mesh: MeshStore): void => {
  expect(new ActorRegistryStore(actorRoot).records()).toEqual([
    expect.objectContaining({ id: actor.id, status: "idle", topics: request.topics }),
  ]);
  expect(mesh.listAll("actors/test/").map(entry => entry.value)).toEqual([
    expect.objectContaining({ id: actor.id, status: "idle" }),
  ]);
};

beforeEach(() => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-registry-publication-"));
  fs.mkdirSync(path.join(root, "kept"));
  serial = 0;
  deleteCalls = [];
  vi.spyOn(fs, "rmSync").mockImplementation(file => {
    deleteCalls.push(String(file));
    retain(String(file));
  });
  vi.spyOn(fs, "unlinkSync").mockImplementation(file => {
    deleteCalls.push(String(file));
    retain(String(file));
  });
  vi.spyOn(fs, "rmdirSync").mockImplementation(() => {
    throw new Error("Recovery/deletion is outside this publication regression");
  });
  vi.spyOn(fenceLock, "pinFenceLock").mockReturnValue(123);
  vi.spyOn(fenceLock, "releaseFenceLock").mockImplementation((lock, record) => {
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(record);
    retain(lock);
  });
});
afterEach(async () => {
  try {
    for (const { actors, mesh } of fixtures.splice(0)) {
      await actors.close(); // Persistent, idle actors: no remove/recovery/worker path.
      expect(actors.agents.spawn).not.toHaveBeenCalled();
      mesh.closeState();
    }
    expect(fs.rmdirSync).not.toHaveBeenCalled();
    expect(deleteCalls.every(file => path.resolve(file).startsWith(`${path.resolve(root)}${path.sep}`))).toBe(true);
  } finally {
    vi.restoreAllMocks();
    // The guarded Vitest run-root teardown owns these retained fixtures.
  }
});

describe("committed actor registry publication", () => {
  it("keeps the ordinary 10s lock budget outside a guest and publishes after canonical lock handoff", async () => {
    const { actors, mesh, actorRoot } = setup();
    const release = heldCanonical(path.join(mesh.root, ".lock"));
    const entered = deferred();
    watchPresence(mesh, entered);
    // A closed guest scope must not supply a deadline to a later ordinary call.
    await withMeshInvocationContext({ deadlineAt: () => 0 }, async () => undefined);
    const acquisitions = vi.spyOn(MeshLock.prototype, "withLock");
    let settled = false;
    const pending = actors.create(request).finally(() => { settled = true; });
    try {
      await Promise.race([entered.promise, pending.then(() => {
        throw new Error("Creation ended before canonical lock contention");
      })]);
      await new Promise(resolve => setTimeout(resolve, 40));
      expect(settled).toBe(false);
      expect(mesh.listAll("actors/test/")).toEqual([]);
      expect(acquisitions.mock.calls.length).toBeGreaterThan(0);
      for (const lock of acquisitions.mock.contexts as MeshLock[]) expect(lock.lockTimeoutMs).toBe(10_000);
      release();
      assertPublished(await pending, actorRoot, mesh);
    } finally { release(); await pending; }
  });

  it.each((["registry", "presence"] as const).flatMap(publication =>
    (["Escape", "deadline", "revocation"] as const).map(ending => [publication, ending] as const)))(
    "finishes committed %s publication after guest observation ends (%s), without authorizing its sibling writer",
    async (publication, ending) => {
      const { actors, mesh, actorRoot } = setup();
      const releaseMesh = heldCanonical(path.join(mesh.root, ".lock"));
      const releaseRegistry = publication === "registry"
        ? heldCanonical(path.join(actorRoot, "actors.json.lock")) : () => {};
      const entered = deferred();
      if (publication === "registry") {
        const withLock = ActorRegistryStore.prototype.withLock;
        vi.spyOn(ActorRegistryStore.prototype, "withLock").mockImplementation(function<T>(
          this: ActorRegistryStore, operation: () => T | Promise<T>, timeoutMs?: number, label?: string,
        ): Promise<T> {
          const pending = withLock.call(this, operation, timeoutMs, label) as Promise<T>;
          entered.resolve();
          return pending;
        });
      } else watchPresence(mesh, entered);
      const ready = deferred();
      const abort = new AbortController();
      const revoked = new AbortController();
      // Registry revocation rejects observation without aborting the enclosing
      // runtime signal stored by execution-service's mesh invocation context.
      const observationSignal = AbortSignal.any([abort.signal, revoked.signal]);
      let deadlineAt = Date.now() + 10_000;
      let committedId: string | undefined;
      let creation!: Promise<FabricActorInfo>;
      let uncommitted!: Promise<{ value?: unknown; error?: unknown }>;
      // Same host dispatch boundary as execution-service: cancellation ends the
      // observation promise and closes its scope while the real create continues.
      const observed = withMeshInvocationContext({ signal: abort.signal, deadlineAt: () => deadlineAt }, async () => {
        const checkActive = () => throwIfExecutionExpired({ signal: observationSignal });
        creation = actors.create(request, {
          beforeCommit: checkActive, checkActive,
          onCommit: id => { committedId = id; },
        });
        await Promise.race([entered.promise, creation.then(() => {
          throw new Error("Creation ended before publication contention");
        })]);
        // A concurrent uncommitted write inherits the ORIGINAL invocation, not
        // the actor publisher's host-owned continuation. Both locks remain held.
        uncommitted = mesh.publish({ topic: "guest.uncommitted", from: identity })
          .then(value => ({ value }), error => ({ error }));
        ready.resolve();
        return runAbortable(observationSignal, () => creation);
      }).then(value => ({ value }), error => ({ error }));
      try {
        await Promise.race([ready.promise, observed.then(outcome => {
          throw new Error(`Observation ended before publication contention: ${"error" in outcome ? String(outcome.error) : "success"}`);
        })]);
        expect(committedId).toBeTruthy();
        expect(actors.list()).toEqual([expect.objectContaining({ id: committedId, status: "idle" })]);
        expect(fs.existsSync(path.join(actorRoot, committedId!))).toBe(true);
        expect(fs.existsSync(path.join(actorRoot, "actors.json"))).toBe(publication === "presence");
        expect(mesh.listAll("actors/test/")).toEqual([]);
        if (ending === "deadline") deadlineAt = Date.now() - 1;
        if (ending === "revocation") revoked.abort(new Error(ending));
        else abort.abort(new Error(ending));
        expect(await observed).toHaveProperty("error");
        if (ending === "revocation") expect(abort.signal.aborted).toBe(false);
        expect(mesh.read({ topic: "guest.uncommitted" })).toEqual([]);
        releaseRegistry();
        releaseMesh();
        const actor = await creation;
        expect(actor.id).toBe(committedId);
        assertPublished(actor, actorRoot, mesh);
        const refused = await uncommitted;
        expect(refused).toHaveProperty("error");
        if (ending === "revocation") expect(refused.error).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
        expect(mesh.read({ topic: "guest.uncommitted" })).toEqual([]);
      } finally {
        abort.abort(new Error("test observation cleanup"));
        releaseRegistry(); releaseMesh();
        await observed;
        await creation?.catch(() => undefined);
        await uncommitted;
      }
    },
  );
});

// PR #1269 review P1-1. Node hands an AsyncLocalStorage store to every asynchronous operation created inside
// run(), timers included (https://nodejs.org/api/async_context.html#asynclocalstoragerunstore-callback-args).
// A failed or cancelled guest call therefore reaches host timers armed during it; their later acquisitions
// are host work, not the call's own wait, and must keep the ordinary lock budget.
describe("closed guest scope reaching host-owned timers", () => {
  const presenceOf = (mesh: MeshStore, id: string) =>
    mesh.listAll("actors/test/").find(entry => entry.key === `actors/test/${id}`)?.value as FabricActorInfo | undefined;
  const until = async (check: () => boolean, timeoutMs: number): Promise<boolean> => {
    const stop = Date.now() + timeoutMs;
    while (!check()) {
      if (Date.now() >= stop) return false;
      await new Promise(resolve => setTimeout(resolve, 20));
    }
    return true;
  };

  it("standalone ALS probe against the real dispatch scope: a self-re-arming timer armed after rejection still publishes", async () => {
    const mesh = new MeshStore(path.join(root, "mesh"), 65_536, 100, {
      lockProtocol: 1, stateBackend: "file", canWakeResidents: () => false,
    });
    const fires: Array<{ closed: boolean | undefined; outcome: string }> = [];
    const rejected = deferred();
    const finished = deferred();
    const abort = new AbortController();
    try {
      const observed = withMeshInvocationContext({ signal: abort.signal, deadlineAt: () => Date.now() + 10_000 }, () => {
        // Background host work the call started; it outlives the rejected dispatch.
        void rejected.promise.then(() => {
          const fire = async (): Promise<void> => {
            const closed = meshInvocationStorage.getStore()?.closed;
            const outcome = await mesh.publish({ topic: "host.timer", from: identity })
              .then(() => "published", (error: { code?: string }) => error?.code ?? String(error));
            fires.push({ closed, outcome });
            if (fires.length < 3) setTimeout(() => void fire(), 5); // re-armed from its own callback
            else finished.resolve();
          };
          setTimeout(() => void fire(), 5);
        });
        return runAbortable(abort.signal, () => new Promise<never>(() => {}));
      }).then(value => ({ value }), error => ({ error }));
      abort.abort(new Error("Escape"));
      expect(await observed).toHaveProperty("error");
      rejected.resolve();
      await finished.promise;
      // Node semantics, unchanged: every fire still sees the closed store.
      expect(fires.map(fire => fire.closed)).toEqual([true, true, true]);
      // The fix: the closed call's refusal no longer extends to later host acquisitions.
      expect(fires.map(fire => fire.outcome)).toEqual(["published", "published", "published"]);
      expect(mesh.read({ topic: "host.timer" })).toHaveLength(3);
    } finally {
      mesh.closeState();
    }
  });

  it.each(["Escape", "deadline"] as const)(
    "keeps actor presence publication working after a guest setter ends (%s) with its presence write in flight",
    async ending => {
      // Short ordinary budget so the second actor's ordinary write fails while the lock is held.
      const { actors, mesh } = setup({ lockTimeoutMs: 300, presenceRetryMs: 25 });
      const first = await actors.create({ ...request, name: "first" });
      const second = await actors.create({ ...request, name: "second" });
      expect(presenceOf(mesh, first.id)?.topics).toEqual(request.topics);
      expect(presenceOf(mesh, second.id)?.topics).toEqual(request.topics);
      let release = heldCanonical(path.join(mesh.root, ".lock"));
      const entered = deferred();
      watchPresence(mesh, entered);
      const abort = new AbortController();
      let deadlineAt = Date.now() + 10_000;
      let setter!: Promise<FabricActorInfo>;
      const observed = withMeshInvocationContext({ signal: abort.signal, deadlineAt: () => deadlineAt }, () => {
        setter = actors.setTopics(first.id, ["test.first"]);
        return runAbortable(abort.signal, () => setter);
      }).then(value => ({ value }), error => ({ error }));
      try {
        await entered.promise; // the setter's presence put is waiting on the held canonical lock
        if (ending === "deadline") deadlineAt = Date.now() - 1;
        else abort.abort(new Error(ending));
        const outcome = await observed;
        if (ending === "Escape") expect(outcome).toHaveProperty("error");
        // The registry change committed; the in-flight presence write was the call's own and was refused.
        expect((await setter).topics).toEqual(["test.first"]);
        expect(presenceOf(mesh, first.id)?.topics).toEqual(request.topics);
        release();
        // 1. The first actor's presence lands through the host retry once the lock is free.
        const firstLanded = await until(() => presenceOf(mesh, first.id)?.topics?.[0] === "test.first", 8_000);
        // 2. A later ordinary presence failure for a second actor is retried and lands too.
        release = heldCanonical(path.join(mesh.root, ".lock"));
        expect((await actors.setTopics(second.id, ["test.second"])).topics).toEqual(["test.second"]);
        expect(presenceOf(mesh, second.id)?.topics).toEqual(request.topics); // its own write timed out
        release();
        const secondLanded = await until(() => presenceOf(mesh, second.id)?.topics?.[0] === "test.second", 8_000);
        expect({ firstLanded, secondLanded }).toEqual({ firstLanded: true, secondLanded: true });
      } finally {
        abort.abort(new Error("test cleanup"));
        release();
        await observed;
        await setter?.catch(() => undefined);
      }
    },
    30_000,
  );
});
