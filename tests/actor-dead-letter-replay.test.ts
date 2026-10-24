import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentRunRequest, AgentRunResult } from "../src/agents/types.js";
import { ACTOR_DEAD_LETTER_ARCHIVE_MAX_BYTES, ActorManager } from "../src/actors/manager.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { DEFAULT_DEAD_LETTER_REPLAY_MAX_AGE_MS, normalizeDeadLetterReplayMaxAgeMs, type FabricActorActivationFilter, type FabricActorRequest } from "../src/actors/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { normalizeScope, senderStamp } from "../src/scope.js";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";

const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
  vi.restoreAllMocks();
});
const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
const waitFor = async (predicate: () => boolean) => {
  const deadline = Date.now() + 10_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("replay did not reach its checkpoint");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};
const readLines = <T>(file: string): T[] => fs.existsSync(file)
  ? fs.readFileSync(file, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line) as T) : [];
type Entry = { at: number; source: string; event: MeshEvent };

// Real registry, restart, mesh envelopes and queue checkpoint. Worker launches are held at the
// run boundary, so assertions inspect accepted durable work without calling a model. No cleanup
// deletes: the enclosing local probe records/retains product delete targets (#11199).
const NOISE_FILTER: FabricActorActivationFilter = [{ id: "noise", kind: ["noise"] }];
async function backlog(patch: Partial<FabricActorRequest> = {}, queueLimit = 16) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "dead-letter-replay-"));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "runs") });
  closers.push(() => agents.close());
  vi.spyOn(agents, "run").mockImplementation(async (_request, signal) => {
    await new Promise<void>(resolve => {
      if (signal?.aborted) resolve();
      else signal?.addEventListener("abort", () => resolve(), { once: true });
    });
    throw new Error("held test run aborted");
  });
  const open = () => {
    const manager = new ActorManager("test", identity, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, actorQueueLimit: queueLimit }, agents, () => {},
      { actorRoot: path.join(root, "actors"), persistent: true, closeGraceMs: 0, meshRetentionSweepPath: false });
    closers.push(() => manager.close());
    return manager;
  };
  const first = open();
  const actor = await first.create({ name: "backlog", instructions: "Supervise.", topics: ["team.work"], coalesce: false, ...patch });
  await first.close();
  const now = Date.now();
  const entries: Entry[] = [];
  for (let i = 0; i < 10; i++) {
    const event = await mesh.publish({ topic: "team.work", from: identity, kind: i === 7 ? "noise" : "work", text: `event-${i}` });
    // Event timestamps deliberately disagree with recorded age: use entry.at, not event.createdAt.
    event.createdAt = i < 6 ? now : now - 48 * 60 * 60 * 1000;
    entries.push({ at: i < 6 ? now - DEFAULT_DEAD_LETTER_REPLAY_MAX_AGE_MS - 60_000 : now, source: "mesh:team.work", event });
  }
  const directory = path.join(root, "actors", actor.id);
  const file = path.join(directory, "dead-letter.jsonl");
  const archive = path.join(directory, "dead-letter.archive.jsonl");
  fs.writeFileSync(file, entries.map(entry => JSON.stringify(entry) + "\n").join(""));
  const queued = () => {
    const name = fs.readdirSync(directory).find(file => file.startsWith("queue-") && file.endsWith(".json"));
    if (!name) return [];
    return (JSON.parse(fs.readFileSync(path.join(directory, name), "utf8")) as { items: Array<{ payload: MeshEvent }> }).items.map(item => item.payload.id);
  };
  return { root, actor, entries, file, archive, queued, open };
}
const receipts = (manager: ActorManager, id: string) => manager.messages(id, 100)
  .filter(message => message.source === "actor:dead-letter-replay").map(message => message.data);

describe("dead-letter replay age and current activation filter (#10630)", () => {
  it("replays four of ten in order, archives exactly six by recorded time, and reports the counts", async () => {
    const w = await backlog();
    const manager = w.open();
    await waitFor(() => receipts(manager, w.actor.id).length === 1);
    expect(w.queued()).toEqual(w.entries.slice(6).map(entry => entry.event.id));
    expect(readLines<Entry>(w.archive)).toEqual(w.entries.slice(0, 6));
    expect(readLines(w.file)).toEqual([]);
    expect(receipts(manager, w.actor.id)).toEqual([{ replayed: 4, archivedStale: 6, filterSkipped: 0, remaining: 0 }]);
    expect(manager.status(w.actor.id).deadLetterReplayMaxAgeMs).toBe(DEFAULT_DEAD_LETTER_REPLAY_MAX_AGE_MS);
  });

  it.each([0, 2049])("does not resurrect an expired dead letter after a crash with a lagging saved mesh cursor (#1065) (padding: %i)", async paddingCount => {
    // Data-safety regression: entry.at is expired but the mesh envelope is recent enough
    // for normal saved-cursor catch-up. Archival must not erase the only restart seed
    // until this monitor has safely passed the event and checkpointed that boundary.
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "dead-letter-cursor-crash-"));
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const cursorPath = path.join(root, "actor-mesh-cursor.json");
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "runs") });
    closers.push(() => agents.close());
    const runs: AgentRunRequest[] = [];
    vi.spyOn(agents, "run").mockImplementation(async (request, signal) => {
      runs.push(request);
      await new Promise<void>(resolve => {
        if (signal?.aborted) resolve();
        else signal?.addEventListener("abort", () => resolve(), { once: true });
      });
      throw new Error("held cursor regression run aborted");
    });
    let polling = false;
    let monitor!: ActorMeshMonitor;
    const start = ActorMeshMonitor.prototype.start;
    vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(function (this: ActorMeshMonitor) {
      monitor = this;
      if (polling) start.call(this);
    });
    const open = () => {
      const manager = new ActorManager("cursor-crash", identity, mesh,
        { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, actorQueueLimit: 16 }, agents, () => {},
        { actorRoot: path.join(root, "actors"), persistent: true, closeGraceMs: 0, meshRetentionSweepPath: false,
          meshCursorPath: cursorPath, meshReplayAgeMs: 60_000, canConsumeMesh: () => polling });
      closers.push(() => manager.close());
      return manager;
    };
    const initial = open();
    const actor = await initial.create({ name: "cursor-backlog", instructions: "Supervise.", topics: ["team.work"],
      coalesce: false, batchWindowMs: 0 });
    await initial.close();
    await mesh.publish({ topic: "unrelated.cursor-anchor", from: identity, kind: "anchor", text: "before expired work" });
    const laggingCursor = { format: 1, ...mesh.latestCursor() };
    const stale = await mesh.publish({ topic: "team.work", from: identity, kind: "work", text: "expired backlog, recent envelope" });
    const entry: Entry = { at: Date.now() - DEFAULT_DEAD_LETTER_REPLAY_MAX_AGE_MS - 60_000, source: "mesh:team.work", event: stale };
    const paddingEntries: Entry[] = [];
    // Publish real recent envelopes so saved-cursor catch-up encounters every stale ID.
    // publishBatch can commit a prefix, so advance only by its returned events.
    while (paddingEntries.length < paddingCount) {
      const events = await mesh.publishBatch(Array.from({ length: Math.min(256, paddingCount - paddingEntries.length) }, () => ({
        topic: "team.work", from: identity, kind: "work", text: "p",
      })));
      expect(events.length).toBeGreaterThan(0);
      paddingEntries.push(...events.map(event => ({ at: entry.at, source: entry.source, event })));
    }
    const staleEntries = [entry, ...paddingEntries];
    const staleIds = new Set(staleEntries.map(value => value.event.id));
    // The large row must exercise the delivery-seed bound, not archive rotation.
    expect(Buffer.byteLength(staleEntries.map(value => `${JSON.stringify(value)}\n`).join("")))
      .toBeLessThanOrEqual(1024 * 1024);
    const liveBacklog = await mesh.publish({ topic: "team.work", from: identity, kind: "work", text: "fresh backlog must replay even while the cursor lags" });
    const liveEntry: Entry = { at: Date.now(), source: "mesh:team.work", event: liveBacklog };
    const directory = path.join(root, "actors", actor.id);
    const file = path.join(directory, "dead-letter.jsonl");
    const archive = path.join(directory, "dead-letter.archive.jsonl");
    const queued = () => fs.readdirSync(directory)
      .filter(name => name.startsWith("queue-") && name.endsWith(".json"))
      .flatMap(name => (JSON.parse(fs.readFileSync(path.join(directory, name), "utf8")) as {
        items: Array<{ payload: MeshEvent }>;
      }).items.map(item => item.payload.id));
    fs.writeFileSync(cursorPath, JSON.stringify(laggingCursor));
    fs.writeFileSync(file, [...staleEntries, liveEntry].map(value => `${JSON.stringify(value)}\n`).join(""));

    const beforeCrash = open();
    await beforeCrash.setActivationFilter(actor.id, null); // real archival/rewrite opportunity
    // The monitor never started and consumption was held, so close cannot flush a newer
    // cursor. This is the process-loss boundary, not a graceful cursor checkpoint.
    await beforeCrash.close();
    const crashBacklog = readLines<Entry>(file);
    const crashArchive = readLines<Entry>(archive);
    const crashQueued = queued();
    expect(JSON.parse(fs.readFileSync(cursorPath, "utf8"))).toEqual(laggingCursor);

    const fresh = await mesh.publish({ topic: "team.work", from: identity, kind: "work", text: "unrelated fresh work must still deliver" });
    polling = true;
    const restarted = open(); // real start, readCursor, saved-cursor catch-up and dispatch
    await waitFor(() => queued().includes(fresh.id));
    await waitFor(() => restarted.meshCaughtUp());
    expect(queued().filter(id => staleIds.has(id))).toEqual([]);
    expect(runs.filter(request => staleEntries.some(value => request.task.includes(value.event.id)))).toEqual([]);
    expect(crashBacklog).toEqual(staleEntries);
    expect(crashArchive).toEqual([]);
    expect(crashQueued).toContain(liveBacklog.id); // only stale archival waits for the cursor
    expect(crashQueued.filter(id => staleIds.has(id))).toEqual([]);
    await waitFor(() => runs.some(request => request.task.includes(liveBacklog.id)));

    // The existing monitor lifecycle must retry after catch-up; no direct replay callback
    // and no second timer is needed. Its safe cursor can now authorize durable archival.
    monitor.schedule();
    await waitFor(() => readLines<Entry>(file).length === 0 && readLines<Entry>(archive).length === staleEntries.length);
    const saved = JSON.parse(fs.readFileSync(cursorPath, "utf8")) as { last: { sequence: number; id: string } };
    const lastStale = staleEntries.at(-1)!.event;
    expect(saved.last.sequence).toBeGreaterThanOrEqual(lastStale.sequence);
    if (saved.last.sequence === lastStale.sequence) expect(saved.last.id).toBe(lastStale.id);
    expect(readLines<Entry>(archive)).toEqual(staleEntries);
    expect(readLines<Entry>(file)).toEqual([]);
    await restarted.setActivationFilter(actor.id, null); // another replay opportunity cannot re-archive it
    expect(readLines<Entry>(archive)).toEqual(staleEntries);
    expect(readLines<Entry>(file)).toEqual([]);
    expect(queued().filter(id => staleIds.has(id))).toEqual([]);
    expect(runs.filter(request => staleEntries.some(value => request.task.includes(value.event.id)))).toEqual([]);
  });

  it("bounds the stale archive, rotates one generation, and notices dropped older data", async () => {
    const w = await backlog();
    const oldArchive = Buffer.alloc(ACTOR_DEAD_LETTER_ARCHIVE_MAX_BYTES - 16, 0x78);
    fs.writeFileSync(w.archive, oldArchive);
    fs.writeFileSync(`${w.archive}.1`, Buffer.alloc(ACTOR_DEAD_LETTER_ARCHIVE_MAX_BYTES + 16, 0x79));
    for (const entry of w.entries) entry.at = Date.now() - DEFAULT_DEAD_LETTER_REPLAY_MAX_AGE_MS - 60_000;
    fs.writeFileSync(w.file, w.entries.map(entry => JSON.stringify(entry) + "\n").join(""));
    const manager = w.open();
    await waitFor(() => receipts(manager, w.actor.id).length === 1);
    expect(fs.statSync(w.archive).size).toBeLessThanOrEqual(ACTOR_DEAD_LETTER_ARCHIVE_MAX_BYTES);
    expect(fs.statSync(`${w.archive}.1`).size).toBeLessThanOrEqual(ACTOR_DEAD_LETTER_ARCHIVE_MAX_BYTES);
    expect(fs.readFileSync(`${w.archive}.1`)).toEqual(oldArchive);
    expect(readLines<Entry>(w.archive)).toEqual(w.entries);
    expect(manager.messages(w.actor.id)).toContainEqual(expect.objectContaining({
      data: expect.objectContaining({ reason: "dead_letter_archive", deadLetterArchiveDroppedBytes: ACTOR_DEAD_LETTER_ARCHIVE_MAX_BYTES + 16 }),
    }));
  });

  it("counts discarded current-prefix and overwritten malformed backup bytes exactly once", async () => {
    const w = await backlog();
    const retainedHistory = Buffer.from(`${JSON.stringify(w.entries[0])}\n`);
    const discardedPrefix = Buffer.concat([Buffer.alloc(ACTOR_DEAD_LETTER_ARCHIVE_MAX_BYTES + 16, 0x78), Buffer.from("\n")]);
    const oldBackup = Buffer.alloc(ACTOR_DEAD_LETTER_ARCHIVE_MAX_BYTES + 31, 0xff);
    fs.writeFileSync(w.archive, Buffer.concat([discardedPrefix, retainedHistory]));
    fs.writeFileSync(`${w.archive}.1`, oldBackup);
    for (const entry of w.entries) entry.at = Date.now() - DEFAULT_DEAD_LETTER_REPLAY_MAX_AGE_MS - 60_000;
    fs.writeFileSync(w.file, w.entries.map(entry => `${JSON.stringify(entry)}\n`).join(""));
    const manager = w.open();
    await waitFor(() => receipts(manager, w.actor.id).length === 1);
    expect(fs.readFileSync(`${w.archive}.1`)).toEqual(retainedHistory);
    expect(readLines<Entry>(w.archive)).toEqual(w.entries);
    const notice = manager.messages(w.actor.id).find(message =>
      (message.data as { reason?: unknown } | undefined)?.reason === "dead_letter_archive");
    expect(notice?.data).toEqual(expect.objectContaining({
      deadLetterArchiveDroppedBytes: discardedPrefix.length + oldBackup.length,
      deadLetterArchiveRotated: true,
    }));
    expect(notice?.data).not.toHaveProperty("deadLetterArchiveDropped");
  });

  it.each(["EPERM", "EIO"])("syncs the writable fallback backup before truncating, but refuses other rename failures (%s)", async code => {
    const w = await backlog();
    const line = `${JSON.stringify(w.entries[0])}\n`;
    fs.writeFileSync(w.archive, line.repeat(Math.floor(ACTOR_DEAD_LETTER_ARCHIVE_MAX_BYTES / Buffer.byteLength(line))));
    const before = fs.readFileSync(w.archive, "utf8");
    const backup = `${w.archive}.1`;
    const temporary = `${backup}.${process.pid}.tmp`;
    const rename = fs.renameSync, open = fs.openSync, sync = fs.fsyncSync, unlink = fs.unlinkSync;
    const steps: string[] = [];
    let fallbackFd: number | undefined;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(from) === temporary && String(to) === backup) throw Object.assign(new Error("injected rename refusal"), { code });
      return rename(from, to);
    });
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
      const fd = open(file, flags, mode);
      if (String(file) === backup && flags === "r+") { fallbackFd = fd; steps.push("backup-open-writable"); }
      if (String(file) === w.archive && flags === "w") steps.push("archive-truncate");
      return fd;
    });
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (fd === fallbackFd) { steps.push("backup-sync"); fallbackFd = undefined; }
      return sync(fd);
    });
    vi.spyOn(fs, "unlinkSync").mockImplementation(file => {
      if (String(file) === temporary) { steps.push("temporary-unlink-recorded"); return; }
      return unlink(file);
    });
    const manager = w.open();
    await manager.setActivationFilter(w.actor.id, null);
    if (code === "EPERM") {
      await waitFor(() => receipts(manager, w.actor.id).length === 1);
      expect(steps.slice(0, 4)).toEqual(["backup-open-writable", "backup-sync", "temporary-unlink-recorded", "archive-truncate"]);
      expect(fs.readFileSync(backup, "utf8")).toBe(before);
      expect(readLines<Entry>(w.archive)).toEqual(w.entries.slice(0, 6));
    } else {
      expect(fs.readFileSync(w.archive, "utf8")).toBe(before);
      expect(readLines<Entry>(w.file)).toEqual(w.entries);
      expect(receipts(manager, w.actor.id)).toEqual([]);
      expect(steps).toEqual([]);
    }
  });

  it("retains each oversized stale prefix until a restart durably archives it", async () => {
    const w = await backlog();
    let previous: Entry[] = [];
    for (let batch = 0; batch < 3; batch++) {
      const entries: Entry[] = Array.from({ length: 32 }, (_, index) => ({
        ...w.entries[0]!,
        event: { ...w.entries[0]!.event, id: `batch-${batch}-${index}`, text: "x".repeat(40_000) },
      }));
      let bytes = 0, archivedCount = 0;
      for (const entry of [...entries].reverse()) {
        const size = Buffer.byteLength(JSON.stringify(entry)) + 1;
        if (bytes + size > ACTOR_DEAD_LETTER_ARCHIVE_MAX_BYTES) break;
        bytes += size;
        archivedCount++;
      }
      expect(archivedCount).toBeGreaterThan(0);
      expect(archivedCount).toBeLessThan(entries.length);
      const suffix = entries.slice(-archivedCount);
      const prefix = entries.slice(0, -archivedCount);
      // The preceding batch was completely archived before installing any new work.
      if (batch > 0) expect(readLines(w.file)).toEqual([]);
      const overwrittenBackupBytes = fs.existsSync(`${w.archive}.1`) ? fs.statSync(`${w.archive}.1`).size : 0;
      fs.writeFileSync(w.file, entries.map(entry => JSON.stringify(entry) + "\n").join(""));
      const manager = w.open();
      // Replay is synchronous up to the presence write. Close immediately, before queued
      // microtasks/timers can archive the retained prefix and hide the first checkpoint.
      const configured = manager.setActivationFilter(w.actor.id, null);
      const receipt = receipts(manager, w.actor.id).at(-1);
      const notices = manager.messages(w.actor.id).filter(message =>
        (message.data as { reason?: unknown } | undefined)?.reason === "dead_letter_archive");
      await manager.close();
      await configured;
      expect(readLines<Entry>(w.archive)).toEqual(suffix);
      expect(fs.statSync(w.archive).size).toBeLessThanOrEqual(ACTOR_DEAD_LETTER_ARCHIVE_MAX_BYTES);
      expect(readLines<Entry>(w.file)).toEqual(prefix);
      expect(receipt).toEqual({ replayed: 0, archivedStale: archivedCount, filterSkipped: 0, remaining: prefix.length });
      if (batch === 0) expect(notices).toEqual([]); // retained input is not dropped history
      if (batch > 0) {
        expect(fs.statSync(`${w.archive}.1`).size).toBeLessThanOrEqual(ACTOR_DEAD_LETTER_ARCHIVE_MAX_BYTES);
        expect(readLines<Entry>(`${w.archive}.1`)).toEqual(previous);
        expect(notices.at(-1)?.data).toEqual(expect.objectContaining({ deadLetterArchiveDroppedBytes: overwrittenBackupBytes }));
        expect(notices.at(-1)?.data).not.toHaveProperty("deadLetterArchiveDropped");
      }

      const restarted = w.open();
      const retried = restarted.setActivationFilter(w.actor.id, null);
      const retryReceipt = receipts(restarted, w.actor.id).at(-1);
      await restarted.close();
      await retried;
      expect(readLines(w.file)).toEqual([]);
      expect(readLines<Entry>(w.archive)).toEqual(prefix);
      expect(readLines<Entry>(`${w.archive}.1`)).toEqual(suffix);
      expect(retryReceipt).toEqual({ replayed: 0, archivedStale: prefix.length, filterSkipped: 0, remaining: 0 });
      expect(new Set([...readLines<Entry>(w.archive), ...readLines<Entry>(`${w.archive}.1`)].map(entry => entry.event.id)))
        .toEqual(new Set(entries.map(entry => entry.event.id)));
      previous = prefix;
    }
  });

  it("appends only the fitting stale suffix without rotating existing archive history", async () => {
    const w = await backlog();
    const staleAt = Date.now() - DEFAULT_DEAD_LETTER_REPLAY_MAX_AGE_MS - 60_000;
    const oversized: Entry = {
      ...w.entries[0]!, at: staleAt,
      event: { ...w.entries[0]!.event, id: "oversized-stale", text: "x".repeat(ACTOR_DEAD_LETTER_ARCHIVE_MAX_BYTES) },
    };
    const suffixEntry: Entry = {
      ...w.entries[1]!, at: staleAt,
      event: { ...w.entries[1]!.event, id: "fitting-stale-suffix", text: "suffix" },
    };
    const stale = [oversized, suffixEntry];
    const lineBytes = (entry: Entry) => Buffer.byteLength(JSON.stringify(entry)) + 1;
    const totalBytes = stale.reduce((total, entry) => total + lineBytes(entry), 0);
    let suffixBytes = 0;
    let suffixStart = stale.length;
    for (let index = stale.length - 1; index >= 0; index--) {
      const size = lineBytes(stale[index]!);
      if (suffixBytes + size > ACTOR_DEAD_LETTER_ARCHIVE_MAX_BYTES) break;
      suffixBytes += size;
      suffixStart = index;
    }
    const prefix = stale.slice(0, suffixStart);
    const suffix = stale.slice(suffixStart);
    const history: Entry = { ...w.entries[2]!, event: { ...w.entries[2]!.event, id: "existing-history", text: "history" } };
    const backup: Entry = { ...w.entries[3]!, event: { ...w.entries[3]!.event, id: "existing-backup", text: "backup" } };
    const historyText = `${JSON.stringify(history)}\n`;
    const backupText = `${JSON.stringify(backup)}\n`;
    const headroom = ACTOR_DEAD_LETTER_ARCHIVE_MAX_BYTES - suffixBytes;
    expect(totalBytes).toBeGreaterThan(ACTOR_DEAD_LETTER_ARCHIVE_MAX_BYTES);
    expect(prefix).toEqual([oversized]);
    expect(suffix).toEqual([suffixEntry]);
    expect(lineBytes(history)).toBeLessThanOrEqual(headroom);
    expect(lineBytes(history) + suffixBytes).toBeLessThanOrEqual(ACTOR_DEAD_LETTER_ARCHIVE_MAX_BYTES);
    fs.writeFileSync(w.archive, historyText);
    fs.writeFileSync(`${w.archive}.1`, backupText);
    fs.writeFileSync(w.file, stale.map(entry => JSON.stringify(entry) + "\n").join(""));
    const manager = w.open();
    const configured = manager.setActivationFilter(w.actor.id, null);
    const receipt = receipts(manager, w.actor.id).at(-1);
    const notices = manager.messages(w.actor.id).filter(message =>
      (message.data as { reason?: unknown } | undefined)?.reason === "dead_letter_archive");
    await manager.close();
    await configured;
    expect(readLines<Entry>(w.file)).toEqual(prefix);
    expect(fs.readFileSync(w.archive, "utf8")).toBe(historyText + suffix.map(entry => `${JSON.stringify(entry)}\n`).join(""));
    expect(fs.readFileSync(`${w.archive}.1`, "utf8")).toBe(backupText);
    expect(notices).toEqual([]);
    expect(receipt).toEqual({ replayed: 0, archivedStale: suffix.length, filterSkipped: 0, remaining: prefix.length });
  });

  it.each([false, true])("retains a single oversized stale entry without rotating or claiming stale progress (live work: %s)", async mixedLive => {
    const w = await backlog();
    const oversized: Entry = { ...w.entries[0]!, event: { ...w.entries[0]!.event, text: "x".repeat(ACTOR_DEAD_LETTER_ARCHIVE_MAX_BYTES) } };
    const live = mixedLive ? w.entries.slice(6) : [];
    const entries = [oversized, ...live];
    const history = `${JSON.stringify(w.entries[1])}\n`;
    fs.writeFileSync(w.archive, history);
    fs.writeFileSync(w.file, entries.map(entry => JSON.stringify(entry) + "\n").join(""));
    const manager = w.open();
    const configured = manager.setActivationFilter(w.actor.id, null);
    const replayReceipts = receipts(manager, w.actor.id);
    await manager.close();
    await configured;
    expect(readLines<Entry>(w.file)).toEqual([oversized]);
    expect(fs.readFileSync(w.archive, "utf8")).toBe(history);
    expect(fs.existsSync(`${w.archive}.1`)).toBe(false);
    expect(w.queued()).toEqual(live.map(entry => entry.event.id));
    expect(replayReceipts).toEqual(mixedLive
      ? [{ replayed: live.length, archivedStale: 0, filterSkipped: 0, remaining: 1 }]
      : []);
    expect(manager.messages(w.actor.id).filter(message =>
      (message.data as { reason?: unknown } | undefined)?.reason === "dead_letter_archive")).toEqual([]);
  });

  it("archives stale entries behind a full replay batch without overtaking remaining fresh work", async () => {
    const w = await backlog({}, 2);
    const freshIds = new Set([0, 3, 6, 9]);
    for (const [index, entry] of w.entries.entries()) {
      entry.at = Date.now() - (freshIds.has(index) ? 0 : DEFAULT_DEAD_LETTER_REPLAY_MAX_AGE_MS + 60_000);
    }
    fs.writeFileSync(w.file, w.entries.map(entry => JSON.stringify(entry) + "\n").join(""));
    const manager = w.open();
    await waitFor(() => receipts(manager, w.actor.id).length === 1);
    const fresh = w.entries.filter((_entry, index) => freshIds.has(index));
    expect(w.queued()).toEqual(fresh.slice(0, 2).map(entry => entry.event.id));
    expect(readLines<Entry>(w.file)).toEqual(fresh.slice(2));
    expect(readLines<Entry>(w.archive)).toEqual(w.entries.filter((_entry, index) => !freshIds.has(index)));
    expect(receipts(manager, w.actor.id)).toEqual([{ replayed: 2, archivedStale: 6, filterSkipped: 0, remaining: 2 }]);
  });

  it("0 restores unlimited replay across restart, including all six old entries", async () => {
    const w = await backlog({ deadLetterReplayMaxAgeMs: 0 });
    const manager = w.open();
    await waitFor(() => receipts(manager, w.actor.id).length === 1);
    expect(w.queued()).toEqual(w.entries.map(entry => entry.event.id));
    expect(fs.existsSync(w.archive)).toBe(false);
    expect(receipts(manager, w.actor.id)).toEqual([{ replayed: 10, archivedStale: 0, filterSkipped: 0, remaining: 0 }]);
    expect(manager.definition(w.actor.id).deadLetterReplayMaxAgeMs).toBe(0);
    await manager.setActivationFilter(w.actor.id, null);
    expect(manager.status(w.actor.id).deadLetterReplayMaxAgeMs).toBe(0);
  });

  it("sets the replay age through live filter configuration and preserves it on reload", async () => {
    const w = await backlog();
    const manager = w.open();
    await manager.setActivationFilter(w.actor.id, null, undefined, undefined, 0);
    await waitFor(() => receipts(manager, w.actor.id).length === 1);
    expect(w.queued()).toEqual(w.entries.map(entry => entry.event.id));
    expect(fs.existsSync(w.archive)).toBe(false);
    await expect(manager.setActivationFilter(w.actor.id, null, undefined, undefined, -1)).rejects.toThrow(/deadLetterReplayMaxAgeMs/);
    expect(manager.status(w.actor.id).deadLetterReplayMaxAgeMs).toBe(0);
    await manager.close();
    const reloaded = w.open();
    expect(reloaded.status(w.actor.id).deadLetterReplayMaxAgeMs).toBe(0);
  });

  // smarty-dev#16416 round 2: setActivationFilter refuses a NEW filter that would skip routed dead-letter backlog
  // (queued-work guard). These cases test replay under a filter already set when the backlog formed: the actor is
  // created with it, and re-applying it (which also triggers a replay) skips nothing newly.
  it("uses the current head-skip filter before replay and keeps unmatched events in order", async () => {
    const w = await backlog({ activationFilter: NOISE_FILTER });
    // Configure synchronously before the restart's queued replay microtask.
    const manager = w.open();
    await manager.setActivationFilter(w.actor.id, [{ id: "noise", kind: ["noise"] }]);
    await waitFor(() => receipts(manager, w.actor.id).length === 1);
    expect(w.queued()).toEqual(w.entries.slice(6).filter(entry => entry.event.kind !== "noise").map(entry => entry.event.id));
    expect(manager.status(w.actor.id).filterSkipped.count).toBe(1);
    expect(manager.messages(w.actor.id)).toContainEqual(expect.objectContaining({ reason: "filtered: noise", data: { filteredItemId: w.entries[7]!.event.id } }));
    expect(receipts(manager, w.actor.id)).toEqual([{ replayed: 3, archivedStale: 6, filterSkipped: 1, remaining: 0 }]);
  });

  it("archives raw stale entries before coalescing live keys by sequence, preserving FIFO and the current filter", async () => {
    const w = await backlog({ coalesceKey: "payload.number", activationFilter: NOISE_FILTER });
    const now = Date.now();
    const staleAge = DEFAULT_DEAD_LETTER_REPLAY_MAX_AGE_MS + 60_000;
    // [key, mesh sequence, recorded age]: stale duplicates outrank every live event.
    // Both live keys disagree with recorded-age order; key 88 also disagrees with file order.
    const rows: Array<[number | undefined, number, number]> = [
      [77, 900, staleAge], [77, 800, staleAge],
      [77, 20, 2_000], [88, 60, 5_000], [77, 40, 4_000], [88, 30, 1_000],
      [undefined, 50, 3_000], [88, 700, staleAge], [55, 70, 1_000], [undefined, 10, 6_000],
    ];
    for (const [index, entry] of w.entries.entries()) {
      const [number, sequence, age] = rows[index]!;
      entry.event.data = { payload: number === undefined ? {} : { number } };
      entry.event.sequence = sequence;
      entry.event.kind = index === 8 ? "noise" : "work";
      entry.at = now - age;
    }
    fs.writeFileSync(w.file, w.entries.map(entry => JSON.stringify(entry) + "\n").join(""));
    const manager = w.open();
    await manager.setActivationFilter(w.actor.id, [{ id: "noise", kind: ["noise"] }]);
    await waitFor(() => receipts(manager, w.actor.id).length === 1);
    expect(w.queued()).toEqual([4, 3, 6, 9].map(index => w.entries[index]!.event.id));
    expect(readLines<Entry>(w.archive)).toEqual([0, 1, 7].map(index => w.entries[index]!));
    expect(readLines(w.file)).toEqual([]);
    expect(manager.status(w.actor.id).filterSkipped.count).toBe(1);
    expect(manager.messages(w.actor.id)).toContainEqual(expect.objectContaining({
      reason: "filtered: noise", data: { filteredItemId: w.entries[8]!.event.id },
    }));
    expect(receipts(manager, w.actor.id)).toEqual([{ replayed: 4, archivedStale: 3, filterSkipped: 1, remaining: 0 }]);
  });

  it.each([
    { ordering: "filtered entry is older", filteredSequence: 10, replayableSequence: 20 },
    { ordering: "filtered entry is newer", filteredSequence: 20, replayableSequence: 10 },
  ])("keeps same-key replayable work when a live entry is skipped ($ordering)", async ({ filteredSequence, replayableSequence }) => {
    const w = await backlog({ coalesceKey: "payload.number", activationFilter: NOISE_FILTER });
    const now = Date.now();
    const filtered: Entry = {
      ...w.entries[0]!, at: now,
      event: { ...w.entries[0]!.event, id: `filtered-${filteredSequence}`, kind: "noise", sequence: filteredSequence,
        data: { payload: { number: 77 } } },
    };
    const replayable: Entry = {
      ...w.entries[1]!, at: now,
      event: { ...w.entries[1]!.event, id: `replayable-${replayableSequence}`, kind: "work", sequence: replayableSequence,
        data: { payload: { number: 77 } } },
    };
    // Both entries are live and share a coalesce key. The filter is skip-only: it must
    // acknowledge the matching entry without letting it erase the other entry.
    fs.writeFileSync(w.file, [filtered, replayable].map(entry => JSON.stringify(entry) + "\n").join(""));
    const manager = w.open();
    await manager.setActivationFilter(w.actor.id, [{ id: "noise", kind: ["noise"] }]);
    await waitFor(() => receipts(manager, w.actor.id).length === 1);
    expect(w.queued()).toEqual([replayable.event.id]);
    expect(readLines<Entry>(w.file)).toEqual([]);
    expect(manager.messages(w.actor.id)).toContainEqual(expect.objectContaining({
      reason: "filtered: noise", data: { filteredItemId: filtered.event.id },
    }));
    expect(receipts(manager, w.actor.id)).toEqual([{ replayed: 1, archivedStale: 0, filterSkipped: 1, remaining: 0 }]);
  });

  it("retains eligible same-key work untouched beyond replay capacity while skipping newer noise", async () => {
    const w = await backlog({ coalesceKey: "payload.number", activationFilter: NOISE_FILTER }, 2);
    const entries = w.entries.slice(6).map((entry, index) => ({
      ...entry,
      event: { ...entry.event, sequence: index + 1, kind: index === 3 ? "noise" : "work",
        data: { payload: { number: index < 2 ? 11 + index : 77 } } },
    }));
    fs.writeFileSync(w.file, entries.map(entry => `${JSON.stringify(entry)}\n`).join(""));
    const manager = w.open();
    const configured = manager.setActivationFilter(w.actor.id, [{ id: "noise", kind: ["noise"] }]);
    const replayReceipts = receipts(manager, w.actor.id);
    await manager.close();
    await configured;
    // The first item can already be in flight; the second keeps the queue at half full
    // so its synchronous refill cannot consume the retained same-key entry.
    expect(w.queued()).toEqual(entries.slice(0, 2).map(entry => entry.event.id));
    expect(readLines<Entry>(w.file)).toEqual([entries[2]!]);
    expect(manager.messages(w.actor.id)).toContainEqual(expect.objectContaining({
      reason: "filtered: noise", data: { filteredItemId: entries[3]!.event.id },
    }));
    expect(replayReceipts).toEqual([{ replayed: 2, archivedStale: 0, filterSkipped: 1, remaining: 1 }]);
  });

  it("removes and receipts every raw entry when a same-key live batch is entirely filtered", async () => {
    const w = await backlog({ coalesceKey: "payload.number", activationFilter: NOISE_FILTER });
    const entries = w.entries.slice(6, 8).map(entry => ({
      ...entry, event: { ...entry.event, kind: "noise", data: { payload: { number: 77 } } },
    }));
    fs.writeFileSync(w.file, entries.map(entry => `${JSON.stringify(entry)}\n`).join(""));
    const manager = w.open();
    await manager.setActivationFilter(w.actor.id, [{ id: "noise", kind: ["noise"] }]);
    await waitFor(() => receipts(manager, w.actor.id).length === 1);
    expect(w.queued()).toEqual([]);
    expect(readLines<Entry>(w.file)).toEqual([]);
    expect(manager.status(w.actor.id).filterSkipped.count).toBe(2);
    expect(receipts(manager, w.actor.id)).toEqual([{ replayed: 0, archivedStale: 0, filterSkipped: 2, remaining: 0 }]);
  });

  it("an expired current filter does not skip a fresh dead letter", async () => {
    const w = await backlog();
    const manager = w.open();
    await manager.setActivationFilter(w.actor.id, [{ id: "noise", kind: ["noise"] }], undefined, Date.now() - 1);
    await waitFor(() => receipts(manager, w.actor.id).length === 1);
    expect(w.queued()).toEqual(w.entries.slice(6).map(entry => entry.event.id));
    expect(manager.status(w.actor.id).filterSkipped.count).toBe(0);
  });

  it("retains every entry if rewriting crashes after the archive append; retry may duplicate the archive", async () => {
    const w = await backlog();
    let failRewrite = true;
    const rename = fs.renameSync;
    const remove = fs.rmSync;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (String(to) === w.file && failRewrite) throw new Error("injected crash after archive append");
      return rename(from, to);
    });
    vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
      if (String(target) === w.file && failRewrite) throw new Error("injected crash during empty-backlog removal");
      return remove(target, options);
    });
    const manager = w.open();
    await waitFor(() => readLines(w.archive).length >= 6 && w.queued().length === 4);
    expect(readLines<Entry>(w.file)).toEqual(w.entries); // archive append never truncates the backlog
    expect(readLines<Entry>(w.archive).slice(0, 6)).toEqual(w.entries.slice(0, 6));
    expect(receipts(manager, w.actor.id)).toEqual([]); // no false successful-rewrite receipt
    failRewrite = false;
    await manager.setActivationFilter(w.actor.id, null);
    await waitFor(() => readLines(w.file).length === 0);
    expect(w.queued()).toEqual(w.entries.slice(6).map(entry => entry.event.id));
    const archivedIds = readLines<Entry>(w.archive).map(entry => entry.event.id);
    expect(archivedIds.length).toBeGreaterThanOrEqual(12);
    expect(new Set([...archivedIds, ...w.queued()])).toEqual(new Set(w.entries.map(entry => entry.event.id)));
  });

  it("fails closed on an archive append error, leaving the complete backlog replayable", async () => {
    const w = await backlog();
    const open = fs.openSync;
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
      if (String(file) === w.archive) throw new Error("archive unavailable");
      return open(file, flags, mode);
    });
    const manager = w.open();
    await manager.setActivationFilter(w.actor.id, null);
    expect(readLines<Entry>(w.file)).toEqual(w.entries);
    expect(w.queued()).toEqual([]);
    expect(receipts(manager, w.actor.id)).toEqual([]);
  });

  it("validates the option and preserves it through global create, update, reload and import", () => {
    for (const bad of [-1, 0.5, Infinity, NaN, "0", Number.MAX_SAFE_INTEGER + 1]) {
      expect(() => normalizeDeadLetterReplayMaxAgeMs(bad)).toThrow(/deadLetterReplayMaxAgeMs/);
    }
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "dead-letter-template-"));
    const registry = new GlobalActorRegistry(root, 64 * 1024);
    const template = registry.create({ name: "backlog", instructions: "Supervise.", deadLetterReplayMaxAgeMs: 0 });
    registry.update(template.id, { activationFilter: ["hold"] });
    const reloaded = new GlobalActorRegistry(root, 64 * 1024);
    expect(reloaded.toRequest(reloaded.resolve(template.id)!).deadLetterReplayMaxAgeMs).toBe(0);
    registry.update(template.id, { deadLetterReplayMaxAgeMs: 12_345 });
    expect(reloaded.toRequest(reloaded.resolve(template.id)!).deadLetterReplayMaxAgeMs).toBe(12_345);
    expect(() => registry.update(template.id, { deadLetterReplayMaxAgeMs: -1 })).toThrow(/deadLetterReplayMaxAgeMs/);
    for (const action of ["create", "setActivationFilter"]) {
      const descriptor = AGENTS_ACTION_DESCRIPTORS.find(item => item.name === action)!;
      expect(descriptor.inputSchema.properties).toHaveProperty("deadLetterReplayMaxAgeMs");
    }
  });
});

const replayActorScope = normalizeScope({ principal: { id: "actor-owner" }, grants: [{ resource: "mesh:team.work", actions: ["read"] }] });
const replayForeignScope = normalizeScope({ principal: { id: "other-owner" }, grants: [{ resource: "mesh:team.work", actions: ["read"] }] });
const replayCases = [
  { key: "foreign", mode: "foreign" as const, sender: senderStamp(replayForeignScope), scope: replayActorScope },
  { key: "foreign-unscoped", mode: "foreign" as const, sender: senderStamp(replayForeignScope), scope: undefined },
  { key: "legacy", mode: "legacy" as const, sender: undefined, scope: replayActorScope },
  { key: "same", mode: "same" as const, sender: senderStamp(replayActorScope), scope: replayActorScope },
] as const;
type ReplayCase = (typeof replayCases)[number];
type ReplayFrame = { source: string; payload: MeshEvent; id: string; untrusted?: true };
const replayFrame = (request: AgentRunRequest): ReplayFrame => {
  const separator = request.task.lastIndexOf("\n\n");
  return JSON.parse(request.task.slice(separator + 2)) as ReplayFrame;
};

// Hold one real activation, fill queue+overflow, let the final event take the persistent
// dead-letter path, then restart while its replayed activation is held.
const senderReplayFixture = async (scenario: ReplayCase) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "dead-letter-sender-replay-"));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "runs") });
  const runs: AgentRunRequest[] = [];
  let release!: () => void;
  const held = new Promise<void>(resolve => { release = resolve; });
  let replayId: string | undefined;
  let releaseReplay!: () => void;
  const replayHeld = new Promise<void>(resolve => { releaseReplay = resolve; });
  vi.spyOn(agents, "run").mockImplementation(async (request, signal, onSpawned) => {
    runs.push(request);
    const id = `recorded-${runs.length}`;
    onSpawned?.({ id, name: "recorded", status: "running", runner: "pi", transport: "process", cwd: process.cwd() });
    const aborted = new Promise<void>(resolve => signal?.addEventListener("abort", () => resolve(), { once: true }));
    if (runs.length === 1) await Promise.race([held, aborted]);
    if (replayId && request.task.includes(replayId)) await Promise.race([replayHeld, aborted]);
    return { id, name: "recorded", task: request.task, status: "completed", runner: "pi", transport: "process", cwd: process.cwd(),
      startedAt: Date.now(), updatedAt: Date.now(), turns: 1, toolCalls: 0, inferenceStarted: true, text: "",
      value: { action: "silent" }, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } } satisfies AgentRunResult;
  });
  let monitor!: ActorMeshMonitor;
  vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(function (this: ActorMeshMonitor) { monitor = this; });
  const open = () => new ActorManager("sender-replay", identity, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, actorQueueLimit: 1 }, agents, () => {},
    { actorRoot: path.join(root, "actors"), persistent: true, closeGraceMs: 0, meshRetentionSweepPath: false, canConsumeMesh: () => false });
  let manager = open();
  closers.push(async () => { await manager.close(); await agents.close(); });
  const actor = await manager.create({ name: `sender-${scenario.key}`, instructions: "Inspect.", topics: ["team.work"], coalesce: false,
    batchWindowMs: 0, ...(scenario.scope ? { principalScope: scenario.scope } : {}) });
  const dir = path.join(root, "actors", actor.id);
  const queue = () => {
    const name = fs.readdirSync(dir).find(file => file.startsWith("queue-") && file.endsWith(".json"));
    return name ? JSON.parse(fs.readFileSync(path.join(dir, name), "utf8")) as { items: Array<{ payload: MeshEvent; sender?: ReturnType<typeof senderStamp> }> } : { items: [] };
  };
  let sequence = 0;
  const from: MeshIdentity = { id: "agent:sender", name: "sender", kind: "agent" };
  const makeEvent = (id: string, ref: string, sender = scenario.sender): MeshEvent => ({
    id, sequence: ++sequence, topic: "team.work", kind: "work", from, data: { ref }, verification: "mesh",
    principal: { id: "principal:sender", binding: "voice-call" }, createdAt: Date.now(),
    ...(sender === undefined ? {} : { sender }),
  });
  const deliver = (event: MeshEvent) => { if (monitor.callbacks.onEvent(event) !== true) throw new Error("event not accepted"); };
  deliver(makeEvent(`${scenario.key}-blocker`, "blocker", senderStamp(replayActorScope)));
  await waitFor(() => runs.length === 1);
  const live = makeEvent(`${scenario.key}-live`, "live");
  deliver(live);
  await waitFor(() => queue().items.some(item => item.payload.id === live.id));
  for (let index = 0; index < 8; index++) deliver(makeEvent(`${scenario.key}-filler-${index}`, `filler-${index}`));
  const replay = makeEvent(`${scenario.key}-replay`, "replay");
  replayId = replay.id;
  deliver(replay);
  const deadLetter = path.join(dir, "dead-letter.jsonl");
  const letters = readLines<Entry>(deadLetter);
  expect(letters).toHaveLength(1);
  expect(letters[0]!.event).toEqual(replay);
  const persistedLive = queue().items.find(item => item.payload.id === live.id);
  expect(persistedLive).toBeDefined();
  release();
  await waitFor(() => runs.some(request => { try { return replayFrame(request).payload.id === replay.id; } catch { return false; } }));
  const liveRequest = runs.find(request => { try { return replayFrame(request).payload.id === live.id; } catch { return false; } })!;
  const replayRequest = runs.find(request => { try { return replayFrame(request).payload.id === replay.id; } catch { return false; } })!;
  await manager.close();
  const persistedRestart = queue().items.find(item => item.payload.id === replay.id);
  expect(persistedRestart).toBeDefined();
  manager = open();
  await waitFor(() => runs.filter(request => { try { return replayFrame(request).payload.id === replay.id; } catch { return false; } }).length >= 2);
  const restartedRequest = runs.filter(request => { try { return replayFrame(request).payload.id === replay.id; } catch { return false; } }).at(-1)!;
  releaseReplay();
  return { actor, live, replay, letters, persistedLive: persistedLive!, persistedRestart: persistedRestart!, liveRequest, replayRequest, restartedRequest, scope: scenario.scope };
};

describe("dead-letter replay preserves mesh sender trust framing (#14094)", () => {
  it.each(replayCases)("keeps $key sender identity and trust framing across live admission and replay", async scenario => {
    const result = await senderReplayFixture(scenario);
    const live = replayFrame(result.liveRequest), replay = replayFrame(result.replayRequest), restarted = replayFrame(result.restartedRequest);
    const liveHeader = result.liveRequest.task.slice(0, result.liveRequest.task.indexOf("\n\n"));
    const replayHeader = result.replayRequest.task.slice(0, result.replayRequest.task.indexOf("\n\n"));
    const restartedHeader = result.restartedRequest.task.slice(0, result.restartedRequest.task.indexOf("\n\n"));
    expect(replayHeader).toBe(liveHeader);
    expect(restartedHeader).toBe(liveHeader);
    // The live checkpoint and restart checkpoint both carry sender separately from the full
    // MeshEvent payload; a missing legacy sender remains absent rather than being synthesized.
    if (scenario.sender) {
      expect(result.persistedLive.payload.sender).toEqual(scenario.sender);
      expect(result.persistedLive.sender).toEqual(scenario.sender);
      expect(result.letters[0]!.event.sender).toEqual(scenario.sender);
      expect(result.persistedRestart.sender).toEqual(scenario.sender);
    } else {
      expect(result.persistedLive.payload).not.toHaveProperty("sender");
      expect(result.persistedLive).not.toHaveProperty("sender");
      expect(result.letters[0]!.event).not.toHaveProperty("sender");
      expect(result.persistedRestart).not.toHaveProperty("sender");
    }
    expect(result.letters[0]!.event).toEqual(result.replay);
    expect(live.payload).toEqual(result.live);
    expect(replay.payload).toEqual(result.replay);
    expect(result.liveRequest.inheritedScope).toEqual(result.scope);
    expect(result.replayRequest.inheritedScope).toEqual(result.scope);
    expect(result.restartedRequest.inheritedScope).toEqual(result.scope);
    expect(result.replayRequest.provenance).toEqual(result.liveRequest.provenance);
    expect(result.restartedRequest.provenance).toEqual(result.replayRequest.provenance);
    expect(restarted.payload).toEqual(result.replay);
    if (scenario.mode === "foreign" || scenario.mode === "legacy") {
      expect(liveHeader).toContain("UNTRUSTED input from a different or narrower principal");
      expect(live.untrusted).toBe(true);
      expect(replay.untrusted).toBe(true);
      expect(restarted.untrusted).toBe(true);
    } else {
      expect(liveHeader).not.toContain("UNTRUSTED");
      expect(live).not.toHaveProperty("untrusted");
      expect(replay).not.toHaveProperty("untrusted");
      expect(restarted).not.toHaveProperty("untrusted");
    }
  });
});
