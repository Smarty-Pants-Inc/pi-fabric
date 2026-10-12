import fs from "node:fs";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { ActorBindingStore } from "../src/actors/binding-store.js";
import { ResidentHost } from "../src/residency/host.js";
import { fixture, mainDirectory } from "./helpers/dead-root-fixture.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { actorWithTree, pendingRemoval } from "./helpers/pending-removal.js";
import { REMOVAL_REFUSED_7858, removalTreeDeleteRefused } from "./helpers/windows-removal.js";

// pi-fabric#952 round 2 (smarty-dev#8090): where the Main publication fence is unavailable (Windows: no flock,
// smarty-dev#7858), a live remove never deletes the tree outside the fence. It revokes, keeps the tree, leaves the
// cleanup pending, and refuses as the offline removal does. The platform reads as win32 only from the step right
// before the tree delete (the bindings delete) until the remove returns, so nothing else in the removal changes.
describe("live actor remove without the Main publication fence", () => {
  it("keeps the tree and leaves the cleanup pending; it never deletes unfenced", async () => {
    const f = await fixture();
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    const remove = ActorBindingStore.prototype.delete;
    try {
      const actor = await f.create("unfenced");
      const dir = path.join(f.config.actorRoot, actor.id);
      fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "session.jsonl"), "history\n");
      ActorBindingStore.prototype.delete = async function (this: ActorBindingStore, ...values: Parameters<ActorBindingStore["delete"]>) {
        const result = await remove.apply(this, values);
        Object.defineProperty(process, "platform", { ...platform, value: "win32" });
        return result;
      };
      let result: Awaited<ReturnType<typeof f.host.actors.remove>>;
      try { result = await f.host.actors.remove(actor.id); }
      finally { Object.defineProperty(process, "platform", platform); ActorBindingStore.prototype.delete = remove; }
      expect(fs.existsSync(dir), "the actor tree").toBe(true);
      expect(fs.readFileSync(path.join(dir, "session.jsonl"), "utf8")).toBe("history\n");
      expect(result).toMatchObject({ removed: true, cleaned: false,
        pending: expect.stringContaining("actor removal is not supported where file ownership cannot be proven (smarty-dev#7858)") });
      expect(f.registered(actor.id)).toBe(false); // revoked: only the tree delete waits
      expect(f.host.actors.cleanupObligation(actor.id)).toBeDefined();
    } finally {
      Object.defineProperty(process, "platform", platform);
      ActorBindingStore.prototype.delete = remove;
      await f.close();
    }
  }, 60_000);

  // A restarted owner (finishPendingRemovals) with pre-change state: only an explicit false finishes on its own.
  for (const [label, shape, flag, kept, residency] of [
    ["a pre-change cleanup marker (no operatorCheck field)", "marker", undefined, true, "durable"],
    ["a pre-change accepted-removal record (no operatorCheck field)", "record", undefined, true, "durable"],
    ["a cleanup marker with a non-boolean operatorCheck", "marker", "no", true, "durable"],
    ["a cleanup marker with an explicit operatorCheck: false", "marker", false, false, "durable"],
  ] as const) {
    it(`after an upgrade, ${label} ${kept ? "keeps the tree until an operator rerun" : "still finishes on its own"}`, async () => {
      const f = await fixture();
      let restarted: ResidentHost | undefined;
      try {
        const name = `upgrade-${shape}-${String(flag)}-${residency}`;
        const actor = await actorWithTree(f, name);
        // The upgrade: the old release's host stops cleanly, leaving its pending removal; a new one starts on the root and
        // finishes what it inherits. (A clean stop, not a crash: Windows' startup lock refuses to restart over a
        // crash-shaped record, host.ts #acquireWindowsLock; the upgrade does not depend on how the old host ended.)
        await f.stopResident();
        await pendingRemoval(f, actor, name, shape, flag, residency);
        restarted = new ResidentHost(f.config, () => {});
        await restarted.start(); // its start finishes inherited removals in the background
        const host = restarted;
        await vi.waitFor(() => expect(kept
          ? /dead-root check was lost/.test(host.actors.cleanupObligation(actor.id)?.removal?.state ?? "") || !fs.existsSync(actor.dir)
          : !fs.existsSync(actor.dir) || /cleanup failed/.test(host.actors.cleanupObligation(actor.id)?.removal?.state ?? "")).toBe(true),
        { timeout: 20_000, interval: 50 });
        // pi-fabric#952 on win32: no fence, so even the explicit false keeps its tree, refused with #7858 (row revoked).
        expect(fs.existsSync(actor.dir), "the actor tree").toBe(kept || removalTreeDeleteRefused);
        if (kept) {
          expect(fs.readFileSync(path.join(actor.dir, "session.jsonl"), "utf8")).toBe("history\n");
          expect(restarted.actors.cleanupObligation(actor.id)?.removal?.state).toMatch(/dead-root check was lost/);
        } else if (removalTreeDeleteRefused) {
          expect(restarted.actors.cleanupObligation(actor.id)?.removal?.state).toMatch(REMOVAL_REFUSED_7858);
          expect(f.registered(actor.id)).toBe(false);
        } else expect(restarted.actors.cleanupObligation(actor.id)).toBeUndefined();
      } finally { await restarted?.close(); await f.close(); }
    }, 60_000);
  }

  // pi-fabric#952 round 5: a readable marker naming another owner root than the registry-loaded actor. The fence would
  // have been the marker's root, so the actor's real Main could publish after the final check: refuse, keep the tree.
  it("a registry actor whose removal marker names another root keeps its tree while its real Main lives", async () => {
    const f = await fixture();
    const main = mainDirectory(f.config.meshRoot, f.config.rootId);
    try {
      const actor = await actorWithTree(f, "other-root-marker");
      // A readable marker for this actor naming another owner root (explicit false: nothing else holds it back).
      fs.writeFileSync(path.join(f.config.actorRoot, `removal-${actor.id}.json`), JSON.stringify({ id: actor.id, sessionDir: actor.dir,
        presenceKey: `actors/${f.config.sessionId}/${actor.id}`, operatorCheck: false,
        owner: { name: "other-root-marker", rootId: "session:another-root", residency: "durable", requestedAt: Date.now() } }));
      await main.refresh(); // the actor's real root Main is live
      const failure = await f.host.actors.remove(actor.id).then(() => undefined, (error: unknown) => String(error));
      expect(fs.existsSync(actor.dir), "the actor's tree").toBe(true);
      expect(fs.readFileSync(path.join(actor.dir, "session.jsonl"), "utf8")).toBe("history\n");
      expect(failure).toMatch(/names owner session:another-root/);
      expect(f.registered(actor.id)).toBe(true); // not revoked: the removal stays pending, with the reason
      expect(f.host.actors.status(actor.id).lastError).toMatch(/names owner session:another-root/);
    } finally { await main.close(); await f.close(); }
  }, 60_000);

  it("a removal marker with a malformed operator flag keeps the tree (fail closed)", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("malformed-marker");
      const dir = path.join(f.config.actorRoot, actor.id);
      fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "session.jsonl"), "history\n");
      // A marker that names this actor correctly but whose operator flag is not `true`.
      fs.writeFileSync(path.join(f.config.actorRoot, `removal-${actor.id}.json`), JSON.stringify({
        id: actor.id, sessionDir: dir, presenceKey: `actors/${f.config.sessionId}/${actor.id}`, operatorCheck: "yes" }));
      await f.host.actors.remove(actor.id).catch(() => undefined);
      expect(fs.existsSync(dir), "the actor tree").toBe(true);
      expect(fs.readFileSync(path.join(dir, "session.jsonl"), "utf8")).toBe("history\n");
    } finally { await f.close(); }
  }, 60_000);
});
