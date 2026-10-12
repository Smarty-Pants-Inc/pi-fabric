import fs from "node:fs";
import path from "node:path";
import { ActorRegistryStore } from "../../src/actors/registry-store.js";
import type { DeadRootFixture } from "./dead-root-fixture.js";

/** A durable actor with a session history (created while the fixture's resident runs). */
export const actorWithTree = async (f: DeadRootFixture, name: string): Promise<{ id: string; dir: string }> => {
  const actor = await f.create(name);
  const dir = path.join(f.config.actorRoot, actor.id);
  fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "session.jsonl"), "history\n");
  return { id: actor.id, dir };
};

/**
 * pi-fabric#952 round 3: a removal accepted before the operator flag existed. `marker`: the registry row is revoked and
 * a pre-change cleanup marker (no operatorCheck field, or the given value) is pending. `record`: the row still carries
 * a pre-change accepted-removal record (no operatorCheck field). Written with no host running (the fixture's resident
 * must be gone already: its close saves its own registry view).
 */
export const pendingRemoval = async (f: DeadRootFixture, actor: { id: string; dir: string }, name: string,
  shape: "marker" | "record", operatorCheck?: unknown, markerResidency: "durable" | "session" = "durable"): Promise<void> => {
  const store = new ActorRegistryStore(f.config.actorRoot);
  await store.withLock(() => {
    const rows = store.snapshot().actors as Array<Record<string, unknown> & { id: string }>;
    store.write(shape === "marker" ? rows.filter(row => row.id !== actor.id)
      : rows.map(row => row.id === actor.id ? { ...row, removal: { requestedAt: Date.now() } } : row), { durable: true });
  });
  if (shape === "marker") {
    fs.writeFileSync(path.join(f.config.actorRoot, `removal-${actor.id}.json`), JSON.stringify({
      id: actor.id, sessionDir: actor.dir, presenceKey: `actors/${f.config.sessionId}/${actor.id}`,
      // The marker's residency is not trusted (pi-fabric#952 round 4): a test may mislabel it.
      owner: { name, rootId: f.config.rootId, residency: markerResidency, requestedAt: Date.now() },
      ...(operatorCheck === undefined ? {} : { operatorCheck }) }));
  }
};
