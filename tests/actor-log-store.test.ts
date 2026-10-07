import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorLogStore, ACTOR_MESSAGE_HISTORY_LIMIT } from "../src/actors/log-store.js";
import type { FabricActorMessage } from "../src/actors/types.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-logs-"));
  roots.push(root);
  return { root, actor: { sessionFile: path.join(root, "actor", "session.jsonl"), lastRunId: "latest" }, store: new ActorLogStore({ maxEventBytes: 8192 }, { eventContextChars: 1000 }, { actorRunArchiveMs: 100 }) };
};

describe("ActorLogStore", () => {
  it("reads live message and retention limits instead of snapshotting configuration", () => {
    const { root, actor } = setup();
    const mesh = { maxEventBytes: 8192 };
    const config = { eventContextChars: 1000 };
    const retention = { actorRunArchiveMs: 1000 };
    const store = new ActorLogStore(mesh, config, retention);
    const history: FabricActorMessage[] = [];
    const message = (): FabricActorMessage => ({ id: "message", actorId: "a", actorName: "a", direction: "out", source: "test", createdAt: 1, text: "x".repeat(500) });
    store.recordMessage(history, message());
    expect(history.at(-1)?.text).toHaveLength(500);
    config.eventContextChars = 10;
    store.recordMessage(history, message());
    expect(history.at(-1)?.text).toBe("x".repeat(10) + "\n[actor message truncated]");
    config.eventContextChars = 1000;
    mesh.maxEventBytes = 4600;
    store.recordMessage(history, message());
    expect(Buffer.byteLength(JSON.stringify(history.at(-1)))).toBeLessThanOrEqual(504);
    const directory = path.join(root, "actor", "runs", "old");
    fs.mkdirSync(directory, { recursive: true });
    fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ status: "completed", finishedAt: 1, transport: "process", sessionId: "2147483647" }));
    store.pruneRuns(actor, 1000);
    expect(store.retainedRunIds(actor)).toEqual(["old"]);
    retention.actorRunArchiveMs = 100;
    store.pruneRuns(actor, 1000);
    expect(store.retainedRunIds(actor)).toEqual([]);
  });

  it("resumes at most one run per step and refreshes the mutable latest-run fence", () => {
    const { root, actor, store } = setup();
    for (const id of ["a", "b", "latest"]) {
      const run = path.join(root, "actor", "runs", id);
      fs.mkdirSync(run, { recursive: true });
      fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ status: "completed", finishedAt: 1, transport: "process", sessionId: "2147483647" }));
    }
    const slices = store.pruneRunsInSlices(actor, 1000);
    expect(slices.next().done).toBe(false); // session backups only
    expect(slices.next().done).toBe(false); // directory census only
    expect(store.retainedRunIds(actor)).toEqual(["a", "b", "latest"]);
    expect(slices.next().done).toBe(false); // a only
    expect(store.retainedRunIds(actor)).toEqual(["b", "latest"]);
    actor.lastRunId = "b"; // New lastRunId between yields is not collectible.
    while (!slices.next().done) {}
    expect(store.retainedRunIds(actor)).toEqual(["b", "latest"]);
  });

  it("passes terminal event retention settings to its existing archive sweep without touching lastRunId", () => {
    const { root, actor } = setup();
    const retention = { actorRunArchiveMs: 10000, terminalRunEventsAgeMs: 100, terminalRunEventsMaxBytes: 1024 };
    const store = new ActorLogStore({ maxEventBytes: 8192 }, { eventContextChars: 1000 }, retention);
    const log = '{"text":"' + "x".repeat(100) + '"}\n';
    for (const id of ["old", "latest"]) {
      const run = path.join(root, "actor", "runs", id);
      fs.mkdirSync(run, { recursive: true });
      fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ status: "completed", finishedAt: 1, transport: "process", sessionId: "2147483647" }));
      fs.writeFileSync(path.join(run, "events.jsonl"), log.repeat(100));
    }
    store.pruneRuns(actor, 1000);
    expect(fs.statSync(path.join(root, "actor", "runs", "old", "events.jsonl")).size).toBeLessThanOrEqual(1024);
    expect(fs.readFileSync(path.join(root, "actor", "runs", "latest", "events.jsonl"), "utf8")).toBe(log.repeat(100));
  });

  it("bounds caller messages identically to retained history and keeps the newest 100", () => {
    const { store } = setup();
    const history: FabricActorMessage[] = [];
    for (let i = 0; i < 102; i++) {
      const message: FabricActorMessage = { id: String(i), actorId: "a", actorName: "a", direction: "out", source: "test", createdAt: i, text: "🙂\"".repeat(10000), data: { huge: "x".repeat(10000) } };
      store.recordMessage(history, message);
      expect(message).toEqual(history.at(-1));
      expect(message).not.toBe(history.at(-1));
      expect(Buffer.byteLength(JSON.stringify(message))).toBeLessThanOrEqual(4096);
    }
    expect(history).toHaveLength(ACTOR_MESSAGE_HISTORY_LIMIT);
    expect(history[0]?.id).toBe("2");
    expect(history.at(-1)?.id).toBe("101");
  });

  it("prunes only expired terminal archives, protecting the latest and active runs", () => {
    const { root, actor, store } = setup();
    for (const [id, status, finishedAt] of [["old", "completed", 1], ["latest", "completed", 1], ["active", "running", 1], ["recent", "failed", 950], ["unknown", undefined, 1]] as const) {
      const dir = path.join(root, "actor", "runs", id);
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, "status.json"), JSON.stringify({ status, finishedAt, transport: "process", sessionId: "2147483647" }));
    }
    store.pruneRuns(actor, 1000);
    expect(store.retainedRunIds(actor)).toEqual(["active", "latest", "recent", "unknown"]);
  });

  it("archives matching dispatch receipts and rejects a receipt for another run", async () => {
    const { root, actor, store } = setup();
    const source = path.join(root, "source");
    fs.mkdirSync(source, { mode: 0o700 });
    const receipt = { runId: "latest", ledger: path.join(root, "ledger.jsonl"),
      decision: { decisionId: "decision", routeClass: "bounded-lookup", mode: "live" } };
    const file = path.join(source, "route-dispatch-receipt.json");
    fs.writeFileSync(file, JSON.stringify(receipt), { mode: 0o600 });
    await store.retainRun(actor, "latest", source);
    expect(JSON.parse(fs.readFileSync(path.join(root, "actor", "runs", "latest", "route-dispatch-receipt.json"), "utf8"))).toEqual(receipt);
    await expect(store.retainRun(actor, "other", source)).rejects.toThrow("Mismatched route dispatch receipt");
    expect(fs.existsSync(path.join(root, "actor", "runs", "other", "route-dispatch-receipt.json"))).toBe(false);
    expect(JSON.parse(fs.readFileSync(file, "utf8"))).toEqual(receipt);
  });

  it("copies the archive protocol and nested runs, tolerating missing sources and nested-copy failure", async () => {
    const { root, actor, store } = setup();
    await store.retainRun(actor, "missing", undefined);
    expect(store.retainedRunIds(actor)).toEqual([]);
    const source = path.join(root, "source");
    fs.mkdirSync(path.join(source, "nested"), { recursive: true });
    for (const file of ["events.jsonl", "status.json", "task.txt", "relaunches.jsonl", "private.txt", "nested/child"]) fs.writeFileSync(path.join(source, file), file);
    await store.retainRun(actor, "latest", source);
    const dest = path.join(root, "actor", "runs", "latest");
    expect(fs.readdirSync(dest).sort()).toEqual(["events.jsonl", "nested", "relaunches.jsonl", "status.json", "task.txt"]);
    expect(fs.readFileSync(path.join(dest, "nested", "child"), "utf8")).toBe("nested/child");
    vi.spyOn(fs, "cpSync").mockImplementation(() => { throw new Error("nested unavailable"); });
    await expect(store.retainRun(actor, "other", source)).resolves.toBeUndefined();
    expect(store.retainedRunIds(actor)).toEqual(["latest", "other"]);
  });
});
