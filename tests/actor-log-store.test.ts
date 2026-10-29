import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorLogStore, ActorRunArchiveRefusedError, ACTOR_MESSAGE_HISTORY_LIMIT } from "../src/actors/log-store.js";
import { diagnostics } from "../src/core/diagnostics.js";
import * as privateDir from "../src/storage/private-dir.js";
import * as atomicWrite from "../src/core/atomic-write.js";
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

  it("keeps terminal event traces byte-for-byte regardless of archival age and size settings", () => {
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
    expect(fs.readFileSync(path.join(root, "actor", "runs", "old", "events.jsonl"), "utf8")).toBe(log.repeat(100));
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

  // Keep: data safety; these are real source/archive trees at the custody boundary, not mock cleanup counts.
  it("returns no-source explicitly and verifies durable copies including repeat nested archives", async () => {
    const { root, actor, store } = setup();
    for (const source of [undefined, path.join(root, "absent")]) {
      await expect(store.retainRun(actor, "missing", source)).resolves.toEqual({ archived: false, reason: "no-source" });
    }
    expect(store.retainedRunIds(actor)).toEqual([]);
    const source = path.join(root, "source");
    fs.mkdirSync(path.join(source, "nested", "child"), { recursive: true });
    for (const file of ["events.jsonl", "status.json", "task.txt", "relaunches.jsonl", "private.txt", "nested/child/events.jsonl"]) fs.writeFileSync(path.join(source, file), file);
    const dest = path.join(root, "actor", "runs", "latest");
    const result = { archived: true, receipt: { runId: "latest", directory: dest } };
    await expect(store.retainRun(actor, "latest", source)).resolves.toEqual(result);
    await expect(store.retainRun(actor, "latest", source)).resolves.toEqual(result);
    expect(fs.readdirSync(dest).sort()).toEqual(["events.jsonl", "nested", "relaunches.jsonl", "status.json", "task.txt"]);
    expect(fs.readFileSync(path.join(dest, "nested", "child", "events.jsonl"), "utf8")).toBe("nested/child/events.jsonl");
    if (process.platform !== "win32") expect(fs.statSync(path.join(dest, "nested", "child")).mode & 0o777).toBe(0o700);
  });

  it.skipIf(process.platform === "win32")("throws a typed, logged refusal for a real 0777 volume ancestor and leaves its source intact", async () => {
    const { root, store } = setup();
    const volume = path.join(root, "volume"), source = path.join(root, "source");
    fs.mkdirSync(volume); fs.mkdirSync(source);
    fs.writeFileSync(path.join(source, "events.jsonl"), "only copy\n");
    // An owned anchor itself self-heals; the unsafe local-path volume ABOVE that anchor must not be changed.
    const actor = { sessionFile: path.join(volume, "mesh", "actors", "a", "session.jsonl") };
    fs.chmodSync(volume, 0o777);
    const logged = vi.spyOn(diagnostics, "error");
    try {
      await expect(store.retainRun(actor, "latest", source)).rejects.toMatchObject({
        name: "ActorRunArchiveRefusedError", code: "FABRIC_ACTOR_RUN_ARCHIVE_REFUSED", runId: "latest",
      });
      expect(fs.readFileSync(path.join(source, "events.jsonl"), "utf8")).toBe("only copy\n");
      expect(logged).toHaveBeenCalledWith("src/actors/log-store.ts", expect.stringContaining("writable by group or others"));
      expect(fs.statSync(volume).mode & 0o777).toBe(0o777);
    } finally { fs.chmodSync(volume, 0o700); }
  });

  it.each(["events.jsonl", "nested/child/events.jsonl"])("rejects a source symlink at %s even without O_NOFOLLOW", async file => {
    const { root, actor, store } = setup();
    const source = path.join(root, "source"), entry = path.join(source, file), target = path.join(root, "trace.jsonl");
    fs.mkdirSync(path.dirname(entry), { recursive: true });
    fs.writeFileSync(target, "only trace\n");
    fs.symlinkSync(target, entry);
    // Windows has no O_NOFOLLOW. Exercise the same source-open behavior on
    // POSIX too, while keeping real files and the production copy/receipt path.
    const open = fs.openSync;
    vi.spyOn(fs, "openSync").mockImplementation((name, flags, mode) =>
      open(name, name === entry && typeof flags === "number" ? flags & ~(fs.constants.O_NOFOLLOW ?? 0) : flags, mode));
    await expect(store.retainRun(actor, "latest", source)).rejects.toBeInstanceOf(ActorRunArchiveRefusedError);
    expect(fs.lstatSync(entry).isSymbolicLink()).toBe(true);
    expect(fs.readFileSync(target, "utf8")).toBe("only trace\n");
    expect(fs.existsSync(path.join(root, "actor", "runs", "latest", file))).toBe(false);
    fs.unlinkSync(entry);
    fs.writeFileSync(entry, "only trace\n");
    await expect(store.retainRun(actor, "latest", source)).resolves.toMatchObject({ archived: true });
    expect(fs.readFileSync(path.join(root, "actor", "runs", "latest", file), "utf8")).toBe("only trace\n");
  });

  it.each(["events.jsonl", "nested/child/events.jsonl"])("rejects a refused %s entry, retaining bytes for a successful retry", async file => {
    const { root, actor, store } = setup();
    const source = path.join(root, "source"), dest = path.join(root, "actor", "runs", "latest");
    fs.mkdirSync(path.dirname(path.join(source, file)), { recursive: true });
    fs.writeFileSync(path.join(source, file), "retained bytes\n");
    const write = privateDir.writeArchiveFile;
    const failure = vi.spyOn(privateDir, "writeArchiveFile").mockImplementation((held, name, content, durable) =>
      name === path.basename(file) ? false : write(held, name, content, durable));
    await expect(store.retainRun(actor, "latest", source)).rejects.toBeInstanceOf(ActorRunArchiveRefusedError);
    expect(fs.readFileSync(path.join(source, file), "utf8")).toBe("retained bytes\n");
    failure.mockRestore();
    await expect(store.retainRun(actor, "latest", source)).resolves.toMatchObject({ archived: true });
    expect(fs.readFileSync(path.join(dest, file), "utf8")).toBe("retained bytes\n");
  });

  it.each(["copy", "directory-sync", "file-sync"])("never returns a receipt after a %s failure, even without a route receipt", async stage => {
    const { root, actor, store } = setup();
    const source = path.join(root, "source"); fs.mkdirSync(source);
    fs.writeFileSync(path.join(source, "events.jsonl"), "only trace\n");
    const fail = () => { throw new Error(`${stage} failed`); };
    if (stage === "copy") vi.spyOn(privateDir, "writeArchiveFile").mockImplementation(fail);
    else if (stage === "directory-sync") vi.spyOn(atomicWrite, "syncDirectoryChain").mockImplementation(fail);
    else vi.spyOn(fs, "fsyncSync").mockImplementation(fail);
    await expect(store.retainRun(actor, "latest", source)).rejects.toMatchObject({
      code: "FABRIC_ACTOR_RUN_ARCHIVE_REFUSED", message: expect.stringContaining(`${stage} failed`),
    });
    expect(fs.readFileSync(path.join(source, "events.jsonl"), "utf8")).toBe("only trace\n");
  });

  it("rejects an existing non-file destination instead of silently skipping the trace", async () => {
    const { root, actor, store } = setup();
    const source = path.join(root, "source"), dest = path.join(root, "actor", "runs", "latest");
    fs.mkdirSync(source); fs.writeFileSync(path.join(source, "events.jsonl"), "only trace\n");
    fs.mkdirSync(path.join(dest, "events.jsonl"), { recursive: true });
    await expect(store.retainRun(actor, "latest", source)).rejects.toBeInstanceOf(ActorRunArchiveRefusedError);
    expect(fs.readFileSync(path.join(source, "events.jsonl"), "utf8")).toBe("only trace\n");
  });
});
