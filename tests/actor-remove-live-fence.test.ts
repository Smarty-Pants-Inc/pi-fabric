import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { offlineRemovalHooks } from "../src/actors/remove-offline.js";
import { ResidentHost } from "../src/residency/host.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { mainPublicationFenced, takeMainPublicationFence } from "../src/topology/main-publication-fence.js";
import { readParticipantFile } from "../src/topology/participant-files.js";
import { fixture, mainDirectory, rootParticipantKey, type DeadRootFixture } from "./helpers/dead-root-fixture.js";
import { processHarness } from "./helpers/process-harness.js";

// smarty-dev#8090: every actor-tree delete, live included, goes through the one fenced helper; the fence's liveness is
// the kernel's (flock); a fenced Main refresh is a silent retry at its next heartbeat.
describe.skipIf(process.platform !== "linux")("live actor remove under the Main publication fence (smarty-dev#8090)", () => {
  const seed = async (f: DeadRootFixture, name: string) => {
    const actor = await f.create(name);
    const dir = path.join(f.config.actorRoot, actor.id);
    fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, "session.jsonl"), "history\n");
    return { id: actor.id, dir };
  };
  const live = ["--confirm-dead-root", "session:dead-root"];
  const liveRootLease = (f: DeadRootFixture, expiresAt = Date.now() + 60_000) => writeHostLease(f.config.meshRoot,
    { id: f.config.rootId, rootId: f.config.rootId, identityId: f.config.rootId, updatedAt: Date.now(), expiresAt });
  /** Pause the next fenced final check (offline or live) until `resume()`; `paused` resolves when it is reached. */
  const pauseFinalCheck = () => {
    let resume!: () => void, reached!: () => void;
    const paused = new Promise<void>(resolve => { reached = resolve; });
    const gate = new Promise<void>(resolve => { resume = resolve; });
    offlineRemovalHooks.beforeFinalCheck = async () => { delete offlineRemovalHooks.beforeFinalCheck; reached(); await gate; };
    return { paused, resume };
  };

  it("a fenced Main refresh is refused once, then publishes within one heartbeat, with no warning or error line", async () => {
    const meshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-fenced-heartbeat-"));
    const rootId = "session:fenced-heartbeat";
    const heartbeatMs = 300;
    const lines: unknown[][] = [];
    const warn = vi.spyOn(console, "warn").mockImplementation((...args) => { lines.push(args); });
    const error = vi.spyOn(console, "error").mockImplementation((...args) => { lines.push(args); });
    const directory = mainDirectory(meshRoot, rootId, undefined, { heartbeatMs });
    const published = () => readParticipantFile(meshRoot, rootParticipantKey(rootId)) !== undefined;
    try {
      const release = await takeMainPublicationFence(meshRoot, rootId);
      await directory.start(); // a Main starting under the fence: no throw
      expect(published()).toBe(false);
      await new Promise(resolve => setTimeout(resolve, heartbeatMs + 100)); // one fenced heartbeat, also refused
      expect(published()).toBe(false);
      release();
      const releasedAt = Date.now();
      await vi.waitFor(() => expect(published()).toBe(true), { timeout: heartbeatMs + 200, interval: 10 });
      expect(Date.now() - releasedAt).toBeLessThanOrEqual(heartbeatMs + 200);
      expect(lines).toEqual([]);
    } finally { warn.mockRestore(); error.mockRestore(); await directory.close(); fs.rmSync(meshRoot, { recursive: true, force: true }); }
  }, 30_000);

  it("a Main-initiated removal deletes the tree under the fence, with no check", async () => {
    const f = await fixture();
    const rm = fs.rmSync;
    const fencedAtDelete: boolean[] = [];
    try {
      const actor = await seed(f, "main-initiated");
      const spy = vi.spyOn(fs, "rmSync").mockImplementation(((file: fs.PathLike, options?: fs.RmOptions) => {
        if (String(file) === actor.dir) fencedAtDelete.push(mainPublicationFenced(f.config.meshRoot, f.config.rootId));
        return rm(file, options);
      }) as typeof fs.rmSync);
      try { await f.host.actors.remove(actor.id); } finally { spy.mockRestore(); }
      expect(fencedAtDelete).toEqual([true]);
      expect(fs.existsSync(actor.dir)).toBe(false);
      expect(mainPublicationFenced(f.config.meshRoot, f.config.rootId)).toBe(false);
    } finally { await f.close(); }
  }, 40_000);

  it("a separate Main's publish during a live operator removal's cleanup is refused; the re-check keeps the tree; the Main publishes after", async () => {
    const f = await fixture();
    const h = processHarness();
    try {
      const actor = await seed(f, "live-raced");
      const main = await h.main(f.config.meshRoot, f.config.rootId);
      const gate = pauseFinalCheck();
      const removal = f.cli(actor.id, live);
      await gate.paused;
      // Inside the live cleanup's fenced check-plus-delete: the root's Main process tries to publish.
      expect(mainPublicationFenced(f.config.meshRoot, f.config.rootId)).toBe(true);
      expect(await main.publish()).toMatchObject({ ok: false, code: "FABRIC_MAIN_PUBLICATION_FENCED" });
      gate.resume();
      const result = await removal;
      // The Main is back (its lease is live): the check under the fence refuses and the tree is kept.
      expect(JSON.parse(result.out)).toMatchObject({ cleaned: false, pending: expect.stringContaining("live root lease") });
      expect(fs.readFileSync(path.join(actor.dir, "session.jsonl"), "utf8")).toBe("history\n");
      expect(JSON.parse(fs.readFileSync(path.join(f.config.actorRoot, `removal-${actor.id}.json`), "utf8")))
        .toMatchObject({ operator: { confirmDeadRoot: f.config.rootId } });
      expect(mainPublicationFenced(f.config.meshRoot, f.config.rootId)).toBe(false);
      expect(await main.publish()).toMatchObject({ ok: true });
    } finally {
      delete offlineRemovalHooks.beforeFinalCheck;
      try { await h.close(); } finally { await f.close(); }
    }
  }, 60_000);

  it("a host restart recovers the operator cleanup with its check rebuilt: kept while the Main is live, deleted once it is gone", async () => {
    const f = await fixture();
    let restarted: ResidentHost | undefined;
    try {
      const actor = await seed(f, "restart-recovered");
      const gate = pauseFinalCheck();
      const removal = f.cli(actor.id, live);
      await gate.paused;
      liveRootLease(f); // the root's Main comes back during the cleanup
      gate.resume();
      expect(JSON.parse((await removal).out)).toMatchObject({ cleaned: false });
      expect(fs.existsSync(actor.dir)).toBe(true);
      // The resident host crashes and restarts; the Main is still live.
      await f.killResident();
      liveRootLease(f);
      restarted = new ResidentHost(f.config, () => {});
      await restarted.start();
      expect(await restarted.actors.remove(actor.id)).toMatchObject({ cleaned: false, pending: expect.stringContaining("live root lease") });
      expect(fs.readFileSync(path.join(actor.dir, "session.jsonl"), "utf8")).toBe("history\n");
      // The Main is gone: its lease expired. The recovered cleanup finishes.
      liveRootLease(f, Date.now() - 1);
      expect(await restarted.actors.remove(actor.id)).toEqual({ removed: true });
      expect(fs.existsSync(actor.dir)).toBe(false);
      expect(fs.existsSync(path.join(f.config.actorRoot, `removal-${actor.id}.json`))).toBe(false);
    } finally {
      delete offlineRemovalHooks.beforeFinalCheck;
      try { await restarted?.close(); } finally { await f.close(); }
    }
  }, 60_000);
});
