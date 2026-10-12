import fs from "node:fs";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { residentRemovalsPath } from "../src/residency/protocol.js";
import { mainPublicationFencePath, mainPublicationFenced } from "../src/topology/main-publication-fence.js";
import { readParticipantFile } from "../src/topology/participant-files.js";
import { fixture, rootParticipantKey, type DeadRootFixture } from "./helpers/dead-root-fixture.js";
import { processHarness } from "./helpers/process-harness.js";
import { actorWithTree, pendingRemoval } from "./helpers/pending-removal.js";

// smarty-dev#8090 + #8082: the resident host's LIVE operator remove across real processes. The resident host (the
// remover) and the root's Main are separate processes. The host holds the root's Main publication fence across its
// final check and the tree delete (ActorManager#finishCleanup through deleteRootActorTree), so a Main that restarts
// after the operator's check either published first (the tree is kept) or is refused until the delete is done.
describe.skipIf(process.platform !== "linux")("live actor remove across processes (smarty-dev#8090)", () => {
  /** A durable actor with a session history, served by a separate resident host process; the root's Main is dead. */
  const live = async (f: DeadRootFixture, h: ReturnType<typeof processHarness>, name: string,
    pauseAt: Parameters<ReturnType<typeof processHarness>["host"]>[1]) => {
    const actor = await f.create(name);
    const dir = path.join(f.config.actorRoot, actor.id);
    fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "session.jsonl"), "history\n");
    // This process's resident ends as a crash (its records name a dead pid) and the Main is gone (a stale root
    // participant with a dead pid); a new resident host process restarts on the root and serves the operator.
    await f.killResident();
    const host = await h.host(f.config.residencyRoot, pauseAt);
    return { id: actor.id, dir, host };
  };
  // The live path: no --main-stopped, so the CLI sends the remove to the running resident host.
  const removeArgs = (f: DeadRootFixture, id: string) => ["remove", "--resident", f.config.residencyRoot, "--actor", id,
    "--mesh-root", f.config.meshRoot, "--confirm-dead-root", f.config.rootId];
  const participantPid = (f: DeadRootFixture) =>
    (readParticipantFile(f.config.meshRoot, rootParticipantKey(f.config.rootId))?.value as { mainProcess?: { pid?: number } } | undefined)?.mainProcess?.pid;
  const fenced = (f: DeadRootFixture) => mainPublicationFenced(f.config.meshRoot, f.config.rootId);
  const history = (dir: string) => fs.readFileSync(path.join(dir, "session.jsonl"), "utf8");

  it("a Main that restarts after the operator's check and publishes before the cleanup keeps its tree", async () => {
    const f = await fixture();
    const h = processHarness();
    try {
      const actor = await live(f, h, "restarted", "afterBindings");
      const remover = h.remover("none", removeArgs(f, actor.id));
      // The operator's check passed (the Main was dead), the registry row is revoked and the bindings are gone; the
      // tree delete is next. The root's Main restarts now and publishes.
      await actor.host.next("paused");
      const main = await h.main(f.config.meshRoot, f.config.rootId);
      expect(await main.publish()).toMatchObject({ ok: true });
      expect(participantPid(f)).toBe(main.pid);
      actor.host.send("continue");
      const done = await remover.next("exit");
      expect(fs.existsSync(actor.dir), "the restarted Main's actor tree").toBe(true);
      // The final check under the fence sees the fresh participant: the cleanup is refused and kept for later.
      expect(JSON.parse(done.out as string)).toMatchObject({ cleaned: false, pending: expect.stringMatching(/Main is live|live root lease/) });
      expect(history(actor.dir)).toBe("history\n");
      expect(f.registered(actor.id)).toBe(false); // revoked before the cleanup, as before
      expect(fenced(f)).toBe(false);
      expect(await main.publish()).toMatchObject({ ok: true });
    } finally { await h.close(); await f.close(); }
  }, 120_000);

  it("the fence is held across the final check and the delete: a Main publish is refused, then works after the release", async () => {
    const f = await fixture();
    const h = processHarness();
    try {
      const actor = await live(f, h, "fenced", "beforeDelete");
      const main = await h.main(f.config.meshRoot, f.config.rootId);
      const remover = h.remover("none", removeArgs(f, actor.id));
      await actor.host.next("paused");
      expect(fenced(f)).toBe(true);
      expect(await main.publish()).toMatchObject({ ok: false, code: "FABRIC_MAIN_PUBLICATION_FENCED" });
      expect(participantPid(f)).toBe(2147483647); // still the stopped Main's stale record
      expect(history(actor.dir)).toBe("history\n");
      actor.host.send("continue");
      const done = await remover.next("exit");
      expect(done).toMatchObject({ code: 0, err: "" });
      expect(fs.existsSync(actor.dir)).toBe(false);
      expect(fenced(f)).toBe(false);
      expect(fs.existsSync(mainPublicationFencePath(f.config.meshRoot, f.config.rootId))).toBe(false);
      expect(await main.publish()).toMatchObject({ ok: true });
      expect(participantPid(f)).toBe(main.pid);
    } finally { await h.close(); await f.close(); }
  }, 120_000);

  it("a host restarted between an operator remove's accept and its cleanup keeps the tree while the Main lives; a rerun with the Main dead deletes it", async () => {
    const f = await fixture();
    const h = processHarness();
    try {
      const actor = await live(f, h, "restarted-host", "afterBindings");
      const first = h.remover("none", removeArgs(f, actor.id));
      await actor.host.next("paused"); // accepted: the row is revoked and the cleanup marker is durable; the tree is next
      expect(await actor.host.kill("SIGKILL")).toEqual([null, "SIGKILL"]);
      await first.kill("SIGKILL");
      const main = await h.main(f.config.meshRoot, f.config.rootId);
      expect(await main.publish()).toMatchObject({ ok: true }); // the root's Main is alive again
      // A new resident host process: its start finishes the removal it inherits, without the operator's check.
      await h.host(f.config.residencyRoot, "none");
      const states = () => {
        try { return (JSON.parse(fs.readFileSync(residentRemovalsPath(f.config.residencyRoot), "utf8")) as { removals: { id: string; state: string }[] })
          .removals.filter(removal => removal.id === actor.id).map(removal => removal.state); } catch { return []; }
      };
      await vi.waitFor(() => expect(!fs.existsSync(actor.dir) || states().some(state => /check was lost/.test(state))).toBe(true),
        { timeout: 30_000, interval: 100 });
      expect(fs.existsSync(actor.dir), "the live Main's actor tree after the host restart").toBe(true);
      expect(history(actor.dir)).toBe("history\n");
      expect(f.registered(actor.id)).toBe(false);
      // The operator reruns the remove once the Main is stopped: the dead-root check runs again under the fence.
      await main.stop();
      const rerun = h.remover("none", removeArgs(f, actor.id));
      expect(await rerun.next("exit")).toMatchObject({ code: 0, err: "" });
      expect(fs.existsSync(actor.dir)).toBe(false);
      expect(fenced(f)).toBe(false);
    } finally { await h.close(); await f.close(); }
  }, 150_000);

  it("after an upgrade, pre-change pending removals (no operator flag) keep their trees while the Main lives; an operator rerun with the Main dead deletes them", async () => {
    const f = await fixture();
    const h = processHarness();
    try {
      const marker = await actorWithTree(f, "pre-change-marker");
      const record = await actorWithTree(f, "pre-change-record");
      await f.killResident(); // the old release's host is gone, its removals pending
      await pendingRemoval(f, marker, "pre-change-marker", "marker");
      await pendingRemoval(f, record, "pre-change-record", "record");
      const main = await h.main(f.config.meshRoot, f.config.rootId);
      expect(await main.publish()).toMatchObject({ ok: true }); // the root's Main is alive
      await h.host(f.config.residencyRoot, "none"); // the upgraded host starts and finishes what it inherits
      const states = () => {
        try { return (JSON.parse(fs.readFileSync(residentRemovalsPath(f.config.residencyRoot), "utf8")) as { removals: { id: string; state: string }[] })
          .removals.filter(removal => /check was lost/.test(removal.state)).map(removal => removal.id); } catch { return []; }
      };
      await vi.waitFor(() => expect([marker, record].every(actor => !fs.existsSync(actor.dir) || states().includes(actor.id))).toBe(true),
        { timeout: 30_000, interval: 100 });
      for (const actor of [marker, record]) {
        expect(fs.existsSync(actor.dir), `${actor.id}: the live Main's actor tree after the upgrade`).toBe(true);
        expect(history(actor.dir)).toBe("history\n");
      }
      await main.stop();
      for (const actor of [marker, record]) {
        const rerun = h.remover("none", removeArgs(f, actor.id));
        expect(await rerun.next("exit")).toMatchObject({ code: 0, err: "" });
        expect(fs.existsSync(actor.dir)).toBe(false);
      }
      expect(fenced(f)).toBe(false);
    } finally { await h.close(); await f.close(); }
  }, 150_000);

  // The live remover killed at the fence's two creation points (#8082 item 2) and while it holds the fence: the tree is
  // never deleted, no fence outlives its holder (no clock: the kernel drops the lock), and the Main publishes.
  for (const [pauseAt, label] of [["fence-temp", "between the fence's temp write and its link"],
    ["fence-link", "after the fence's link, before its lock"], ["beforeDelete", "while it holds the fence"]] as const) {
    it(`a live remover killed ${label} keeps the tree and leaves no held fence; the Main publishes`, async () => {
      const f = await fixture();
      const h = processHarness();
      try {
        const actor = await live(f, h, `killed-${pauseAt}`, pauseAt);
        const fence = mainPublicationFencePath(f.config.meshRoot, f.config.rootId);
        h.remover("none", removeArgs(f, actor.id));
        const paused = await actor.host.next("paused");
        if (pauseAt === "fence-temp") expect(fs.existsSync(paused.temp as string)).toBe(true);
        if (pauseAt === "beforeDelete") expect(fenced(f)).toBe(true);
        expect(await actor.host.kill("SIGKILL")).toEqual([null, "SIGKILL"]);
        expect(fs.existsSync(fence)).toBe(pauseAt !== "fence-temp"); // a dead holder's record may remain ...
        if (pauseAt !== "fence-temp") expect(JSON.parse(fs.readFileSync(fence, "utf8"))).toMatchObject({ pid: actor.host.pid });
        expect(fenced(f)).toBe(false); // ... but no one holds its lock, so it does not fence
        expect(history(actor.dir)).toBe("history\n");
        const main = await h.main(f.config.meshRoot, f.config.rootId);
        expect(await main.publish()).toMatchObject({ ok: true });
        expect(participantPid(f)).toBe(main.pid);
      } finally { await h.close(); await f.close(); }
    }, 120_000);
  }
});
