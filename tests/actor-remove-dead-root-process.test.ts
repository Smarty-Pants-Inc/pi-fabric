import fs from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { offlineMarkerPath } from "../src/actors/remove-offline.js";
import { mainPublicationFencePath, mainPublicationFenced } from "../src/topology/main-publication-fence.js";
import { readParticipantFile } from "../src/topology/participant-files.js";
import { ROOT_PARTICIPANT_FRESH_MS } from "../src/residency/operator-safety.js";
import { fixture, rootParticipantKey, type DeadRootFixture } from "./helpers/dead-root-fixture.js";
import { processHarness } from "./helpers/process-harness.js";

// smarty-dev#8082: the dead-root removal and its Main publication fence across real processes. The root's Main and
// the `fabric-actors remove` operator are separate processes; the remover is paused at a chosen point and killed
// with a real SIGKILL. The in-process suite (actor-remove-dead-root.test.ts) keeps the step-level races.
describe.skipIf(process.platform !== "linux")("dead-root removal across processes (smarty-dev#8082)", () => {
  const seed = async (f: DeadRootFixture, ...names: string[]) => {
    const actors = [];
    for (const name of names) {
      const actor = await f.create(name);
      const dir = path.join(f.config.actorRoot, actor.id);
      fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "session.jsonl"), "history\n");
      actors.push({ id: actor.id, dir });
    }
    await f.killResident(); // a crashed resident and a stopped Main: a stale root participant with a dead pid
    return actors;
  };
  const removeArgs = (f: DeadRootFixture, id: string) => ["remove", "--resident", f.config.residencyRoot, "--actor", id,
    "--mesh-root", f.config.meshRoot, ...f.confirmed];
  const participantPid = (f: DeadRootFixture) =>
    (readParticipantFile(f.config.meshRoot, rootParticipantKey(f.config.rootId))?.value as { mainProcess?: { pid?: number } } | undefined)?.mainProcess?.pid;
  const fenced = (f: DeadRootFixture) => mainPublicationFenced(f.config.meshRoot, f.config.rootId);
  const archivedTree = (f: DeadRootFixture, id: string) => f.archives().some(file => file.startsWith(`${id}.`) && file.endsWith("/tree"));

  it("a separate Main's publish is refused while the remover holds the fence, the tree goes only with no fresh participant, and the Main publishes after the release", async () => {
    // The refused Main still renews its host lease (only the root participant is fenced). The remover's checks
    // after the tree move see that live lease and stop: the tree is archived, the registry row and the pending
    // record are kept for a rerun once the Main is stopped (remove-offline.ts step 4).
    const f = await fixture();
    const h = processHarness();
    try {
      const [actor, guarded] = await seed(f, "raced", "guarded");
      const main = await h.main(f.config.meshRoot, f.config.rootId);
      const remover = h.remover("beforeTreeMove", removeArgs(f, actor!.id));
      await remover.next("paused");
      // Between the remover's final check and its tree move: the Main process tries to publish.
      expect(fenced(f)).toBe(true);
      expect(await main.publish()).toMatchObject({ ok: false, code: "FABRIC_MAIN_PUBLICATION_FENCED" });
      expect(participantPid(f)).toBe(2147483647); // still the stopped Main's stale record
      expect(fs.existsSync(actor!.dir)).toBe(true);
      remover.send("continue");
      const stopped = await remover.next("exit");
      expect(stopped).toMatchObject({ code: 1, err: "" });
      expect(JSON.parse(stopped.out as string)).toMatchObject({ cleaned: false, pending: expect.stringContaining("Main has a live root lease") });
      expect(fs.existsSync(actor!.dir)).toBe(false);
      expect(archivedTree(f, actor!.id)).toBe(true);
      expect(f.registered(actor!.id)).toBe(true);
      expect(fs.existsSync(offlineMarkerPath(f.config.actorRoot, actor!.id))).toBe(true);
      // Released: the Main publishes.
      expect(fenced(f)).toBe(false);
      expect(await main.publish()).toMatchObject({ ok: true });
      expect(participantPid(f)).toBe(main.pid);
      // A fresh participant exists now: the next removal refuses and keeps the tree.
      const second = h.remover("none", removeArgs(f, guarded!.id));
      const refused = await second.next("exit");
      expect(refused.code).toBe(1);
      expect(refused.err).toMatch(/Main is live|Main has a live root lease/);
      expect(fs.readFileSync(path.join(guarded!.dir, "session.jsonl"), "utf8")).toBe("history\n");
      expect(f.registered(guarded!.id)).toBe(true);
    } finally { await h.close(); await f.close(); }
  }, 120_000);

  it("a remover killed between the fence's temp write and its link leaves no fence; the Main publishes and a rerun removes", async () => {
    const f = await fixture();
    const h = processHarness();
    try {
      const [actor] = await seed(f, "killed-before-link");
      const fence = mainPublicationFencePath(f.config.meshRoot, f.config.rootId);
      const remover = h.remover("fence-temp", removeArgs(f, actor!.id));
      const paused = await remover.next("paused");
      expect(paused.fence).toBe(fence);
      expect(fs.existsSync(paused.temp as string)).toBe(true);
      expect(await remover.kill("SIGKILL")).toEqual([null, "SIGKILL"]);
      // No fence: only the dead remover's temp file, which is never the fence.
      expect(fs.existsSync(fence)).toBe(false);
      expect(fenced(f)).toBe(false);
      expect(fs.existsSync(actor!.dir)).toBe(true);
      const rerun = h.remover("none", removeArgs(f, actor!.id));
      expect(await rerun.next("exit")).toMatchObject({ code: 0, err: "" });
      expect(archivedTree(f, actor!.id)).toBe(true);
      const main = await h.main(f.config.meshRoot, f.config.rootId);
      expect(await main.publish()).toMatchObject({ ok: true });
      expect(participantPid(f)).toBe(main.pid);
    } finally { await h.close(); await f.close(); }
  }, 120_000);

  it("a remover killed after the fence's link leaves a fence no one holds; the next remover replaces it and holds it", async () => {
    const f = await fixture();
    const h = processHarness();
    try {
      const [actor] = await seed(f, "killed-after-link");
      const fence = mainPublicationFencePath(f.config.meshRoot, f.config.rootId);
      const killed = h.remover("fence-link", removeArgs(f, actor!.id));
      await killed.next("paused");
      expect(JSON.parse(fs.readFileSync(fence, "utf8"))).toMatchObject({ pid: killed.pid });
      expect(await killed.kill("SIGKILL")).toEqual([null, "SIGKILL"]);
      // The record names a dead remover; no process holds its lock, so it does not fence (no expiry, no clock).
      expect(fs.existsSync(fence)).toBe(true);
      expect(fenced(f)).toBe(false);
      const main = await h.main(f.config.meshRoot, f.config.rootId);
      const next = h.remover("beforeTreeMove", removeArgs(f, actor!.id));
      await next.next("paused");
      expect(JSON.parse(fs.readFileSync(fence, "utf8"))).toMatchObject({ pid: next.pid });
      expect(fenced(f)).toBe(true);
      expect(await main.publish()).toMatchObject({ ok: false, code: "FABRIC_MAIN_PUBLICATION_FENCED" });
      await main.stop(); // the Main stops cleanly before the move (its lease goes with it)
      next.send("continue");
      expect(await next.next("exit")).toMatchObject({ code: 0, err: "" });
      expect(archivedTree(f, actor!.id)).toBe(true);
      expect(fs.existsSync(fence)).toBe(false);
      const restarted = await h.main(f.config.meshRoot, f.config.rootId);
      expect(await restarted.publish()).toMatchObject({ ok: true });
    } finally { await h.close(); await f.close(); }
  }, 120_000);

  it("a remover killed while it holds the fence: the kernel releases it at once, the Main publishes, and a rerun resumes the removal", async () => {
    const f = await fixture();
    const h = processHarness();
    try {
      const [actor] = await seed(f, "killed-holding");
      const fence = mainPublicationFencePath(f.config.meshRoot, f.config.rootId);
      const main = await h.main(f.config.meshRoot, f.config.rootId);
      const killed = h.remover("beforeTreeMove", removeArgs(f, actor!.id));
      await killed.next("paused");
      expect(fenced(f)).toBe(true);
      expect(await main.publish()).toMatchObject({ ok: false, code: "FABRIC_MAIN_PUBLICATION_FENCED" });
      expect(await killed.kill("SIGKILL")).toEqual([null, "SIGKILL"]);
      expect(JSON.parse(fs.readFileSync(fence, "utf8"))).toMatchObject({ pid: killed.pid });
      expect(fenced(f)).toBe(false);
      expect(fs.existsSync(actor!.dir)).toBe(true); // killed before the move: the tree is kept
      // No expiry: the Main publishes at once after the kill.
      expect(await main.publish()).toMatchObject({ ok: true });
      expect(participantPid(f)).toBe(main.pid);
      // While the Main is alive and fresh, a rerun refuses and keeps the tree.
      const early = h.remover("none", removeArgs(f, actor!.id));
      expect(await early.next("exit")).toMatchObject({ code: 1, err: expect.stringMatching(/Main is live|Main has a live root lease/) });
      expect(fs.readFileSync(path.join(actor!.dir, "session.jsonl"), "utf8")).toBe("history\n");
      await main.stop(); // clean stop withdraws the Main's participant and lease
      // Seed the stopped Main's stale dead-pid record so the rerun can resume from the pending record.
      f.mainParticipant(Date.now() - 2 * ROOT_PARTICIPANT_FRESH_MS);
      const rerun = h.remover("none", removeArgs(f, actor!.id));
      expect(await rerun.next("exit")).toMatchObject({ code: 0, err: "" });
      expect(archivedTree(f, actor!.id)).toBe(true);
      expect(f.registered(actor!.id)).toBe(false);
      expect(fs.existsSync(fence)).toBe(false);
    } finally { await h.close(); await f.close(); }
  }, 120_000);
});
