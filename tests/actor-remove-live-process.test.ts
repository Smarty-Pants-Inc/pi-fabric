import fs from "node:fs";
import { hostedOnly } from "../scripts/hosted-only.mjs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { ROOT_PARTICIPANT_FRESH_MS } from "../src/residency/operator-safety.js";
import { mainPublicationFencePath, mainPublicationFenced } from "../src/topology/main-publication-fence.js";
import { readParticipantFile } from "../src/topology/participant-files.js";
import { EVIDENCE, fixture, rootParticipantKey, type DeadRootFixture } from "./helpers/dead-root-fixture.js";
import { processHarness, type ResidentCall } from "./helpers/process-harness.js";

// smarty-dev#8090 / #8082 item 1 (live path). HOSTED ONLY: these tests really delete actor trees (#4664); never run
// them on a work host. Classification (Decision C): native multi-process integration, not product E2E. Separate
// processes run the real ResidentHost, the real `fabric-actors` CLI (src/actors-cli.ts main over the resident request
// channel) and a root Main (a real ParticipantDirectory), with real flock and a real SIGKILL; source loads through
// jiti, so the installed package and the Pi Main launcher are out of scope (named post-install check: Ryzen 1).
describe.skipIf(!hostedOnly("live actor removal across processes") || process.platform !== "linux")("live resident actor removal across processes (smarty-dev#8090)", () => {
  const STALE_PID = 2147483647;
  const seed = async (f: DeadRootFixture, ...names: string[]) => {
    const actors = [];
    for (const name of names) {
      const actor = await f.create(name);
      const dir = path.join(f.config.actorRoot, actor.id);
      fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "session.jsonl"), "history\n");
      actors.push({ id: actor.id, dir });
    }
    // The in-process host goes; the root is then served by the resident child. Its Main is stopped (stale record).
    await f.killResident();
    return actors;
  };
  const liveArgs = (f: DeadRootFixture, id: string) => ["remove", "--resident", f.config.residencyRoot, "--actor", id,
    "--mesh-root", f.config.meshRoot, "--confirm-dead-root", f.config.rootId];
  const offlineArgs = (f: DeadRootFixture, id: string) => [...liveArgs(f, id), "--main-stopped", "--evidence", EVIDENCE];
  const participantPid = (f: DeadRootFixture) =>
    (readParticipantFile(f.config.meshRoot, rootParticipantKey(f.config.rootId))?.value as { mainProcess?: { pid?: number } } | undefined)?.mainProcess?.pid;
  const fenced = (f: DeadRootFixture) => mainPublicationFenced(f.config.meshRoot, f.config.rootId);
  const history = (dir: string) => fs.readFileSync(path.join(dir, "session.jsonl"), "utf8");
  const treeCalls = (calls: ResidentCall[], dir: string) => calls.filter(call => call.kind === "rm" && call.path === path.resolve(dir));
  const hooks = (calls: ResidentCall[]) => calls.filter(call => call.kind === "hook");

  it("(a) a Main that publishes after the operator acceptance but before the final check keeps the tree: pending, never removed", async () => {
    const f = await fixture();
    const h = processHarness();
    try {
      const [actor] = await seed(f, "raced-before-check");
      const main = await h.main(f.config.meshRoot, f.config.rootId);
      const resident = await h.resident(f.config.residencyRoot, "beforeRemove");
      await resident.arm();
      const cli = h.remover("none", liveArgs(f, actor!.id));
      // Accepted (Main stale), stopped, about to remove as an operator; no fence is taken yet.
      expect(await resident.next("paused", 60_000)).toMatchObject({ at: "beforeRemove", id: actor!.id, options: { operator: true } });
      expect(fenced(f)).toBe(false);
      expect(await main.publish()).toMatchObject({ ok: true });
      expect(participantPid(f)).toBe(main.pid);
      resident.send("continue");
      const exit = await cli.next("exit", 60_000);
      expect(exit.err).toBe("");
      expect(JSON.parse(exit.out as string)).toMatchObject({ cleaned: false, pending: expect.any(String) });
      expect(history(actor!.dir)).toBe("history\n");
      const calls = await resident.report();
      expect(hooks(calls)).toContainEqual({ kind: "hook", name: "beforeCheck", fenced: true });
      expect(hooks(calls).some(call => call.name === "beforeDelete")).toBe(false);
      expect(treeCalls(calls, actor!.dir)).toEqual([]);
      // Released: the live Main keeps publishing.
      expect(fenced(f)).toBe(false);
      expect(await main.publish()).toMatchObject({ ok: true });
    } finally { await h.close(); await f.close(); }
  }, 120_000);

  it("(b) a Main publish during the awaited beforeDelete is refused, the tree goes only under the fence, and the Main publishes afterward", async () => {
    const f = await fixture();
    const h = processHarness();
    try {
      const [actor] = await seed(f, "raced-in-delete");
      const main = await h.main(f.config.meshRoot, f.config.rootId); // started, not yet published
      const resident = await h.resident(f.config.residencyRoot, "beforeDelete");
      await resident.arm();
      const cli = h.remover("none", liveArgs(f, actor!.id));
      expect(await resident.next("paused", 60_000)).toMatchObject({ at: "beforeDelete" });
      expect(fenced(f)).toBe(true);
      expect(await main.publish()).toMatchObject({ ok: false, code: "FABRIC_MAIN_PUBLICATION_FENCED" });
      expect(participantPid(f)).toBe(STALE_PID);
      expect(history(actor!.dir)).toBe("history\n");
      resident.send("continue");
      const exit = await cli.next("exit", 60_000);
      expect(exit).toMatchObject({ code: 0, err: "" });
      expect(JSON.parse(exit.out as string).cleaned).not.toBe(false);
      expect(fs.existsSync(actor!.dir)).toBe(false);
      const calls = await resident.report();
      expect(hooks(calls)).toEqual([{ kind: "hook", name: "beforeCheck", fenced: true }, { kind: "hook", name: "beforeDelete", fenced: true }]);
      expect(treeCalls(calls, actor!.dir)).toEqual([{ kind: "rm", path: path.resolve(actor!.dir), fenced: true }]);
      expect(fs.existsSync(mainPublicationFencePath(f.config.meshRoot, f.config.rootId))).toBe(false);
      expect(fenced(f)).toBe(false);
      expect(await main.publish()).toMatchObject({ ok: true });
      expect(participantPid(f)).toBe(main.pid);
    } finally { await h.close(); await f.close(); }
  }, 120_000);

  it.each(["fence-temp", "beforeDelete"] as const)("(c) a live resident killed at %s keeps the tree; the Main publishes at once and a successor retakes the fence (no expiry)", async (pauseAt) => {
    const f = await fixture();
    const h = processHarness();
    try {
      const [victim, other] = await seed(f, `killed-${pauseAt}`, "successor");
      const fence = mainPublicationFencePath(f.config.meshRoot, f.config.rootId);
      const resident = await h.resident(f.config.residencyRoot, pauseAt);
      await resident.arm();
      const cli = h.remover("none", liveArgs(f, victim!.id));
      const paused = await resident.next("paused", 60_000);
      if (pauseAt === "fence-temp") {
        expect(paused.fence).toBe(fence);
        expect(fs.existsSync(paused.temp as string)).toBe(true);
      } else expect(fenced(f)).toBe(true);
      expect(await resident.kill("SIGKILL")).toEqual([null, "SIGKILL"]);
      await cli.kill("SIGKILL"); // its request is never answered
      if (pauseAt === "fence-temp") expect(fs.existsSync(fence)).toBe(false); // the temp file is never the fence
      else expect(JSON.parse(fs.readFileSync(fence, "utf8"))).toMatchObject({ pid: resident.pid }); // a record no one holds
      expect(fenced(f)).toBe(false);
      expect(history(victim!.dir)).toBe("history\n");
      const main = await h.main(f.config.meshRoot, f.config.rootId);
      expect(await main.publish()).toMatchObject({ ok: true });
      expect(participantPid(f)).toBe(main.pid);
      await main.stop();
      // The stopped Main's record ages out (the fixture's dead-pid record). The resident is dead, so the successor is
      // the real CLI's offline remover; it takes the same fence path over by its kernel lock, not by time.
      f.mainParticipant(Date.now() - 2 * ROOT_PARTICIPANT_FRESH_MS);
      const successor = h.remover("beforeTreeMove", offlineArgs(f, other!.id));
      await successor.next("paused", 60_000);
      expect(JSON.parse(fs.readFileSync(fence, "utf8"))).toMatchObject({ pid: successor.pid });
      expect(fenced(f)).toBe(true);
      successor.send("continue");
      expect(await successor.next("exit", 60_000)).toMatchObject({ code: 0, err: "" });
      expect(fs.existsSync(fence)).toBe(false);
      expect(history(victim!.dir)).toBe("history\n"); // the interrupted live removal never removed it
      const restarted = await h.main(f.config.meshRoot, f.config.rootId);
      expect(await restarted.publish()).toMatchObject({ ok: true });
    } finally { await h.close(); await f.close(); }
  }, 120_000);

  it("counterexample: the Main's ordinary same-root removeActor is allowed while its fresh participant exists, and still fenced", async () => {
    const f = await fixture();
    const h = processHarness();
    try {
      const [actor] = await seed(f, "main-owned");
      const resident = await h.resident(f.config.residencyRoot, "none");
      const main = await h.main(f.config.meshRoot, f.config.rootId);
      expect(await main.publish()).toMatchObject({ ok: true });
      const removed = await main.removeActor(actor!.id);
      expect(removed).toMatchObject({ ok: true, result: { removed: true } });
      expect((removed.result as { cleaned?: boolean }).cleaned).not.toBe(false);
      expect(fs.existsSync(actor!.dir)).toBe(false);
      const calls = await resident.report();
      expect(calls).toContainEqual({ kind: "remove", id: actor!.id, options: expect.not.objectContaining({ operator: true }) });
      expect(treeCalls(calls, actor!.dir)).toEqual([{ kind: "rm", path: path.resolve(actor!.dir), fenced: true }]);
      expect(fenced(f)).toBe(false);
      expect(await main.publish()).toMatchObject({ ok: true });
    } finally { await h.close(); await f.close(); }
  }, 120_000);
});
