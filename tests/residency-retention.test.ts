import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { cancellationError } from "../src/async-settlement.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { ResidentHost, sweepResidentRuns } from "../src/residency/host.js";
import * as expiry from "../src/residency/request-expiry.js";
import { ResidentRequestRetention } from "../src/residency/retention.js";
import { acknowledgeResidentResponse, abandonResidentRequest, commitResidentRequest, readResidentRequestDecision, registerResidentCancellation, residentHostId, residentRoot, residentHostStateNote, residentCommandForOwner, type ResidentCommand, type ResidentHostConfig } from "../src/residency/protocol.js";

const roots: string[] = [];
const nativePlatform = process.platform;
afterEach(() => { Object.defineProperty(process, "platform", { value: nativePlatform }); vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const root = () => { const value = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-request-retention-")); roots.push(value); return value; };
const write = (root: string, dir: string, id: string, value: unknown) => {
  fs.mkdirSync(path.join(root, dir), { recursive: true, mode: 0o700 });
  fs.writeFileSync(path.join(root, dir, `${id}.json`), JSON.stringify(value), { mode: 0o600 });
};
const seed = (root: string, time: number, acknowledged = true) => {
  const requestId = expiry.newResidentRequestId(time);
  const command = { format: 3, operation: "removeActor", requestId, rootId: "session:retention", id: "entity", createdAt: time } as ResidentCommand;
  commitResidentRequest(root, command, "entity", "host");
  const response = { format: 1 as const, requestId, ok: true, completedAt: time };
  write(root, "responses", requestId, response);
  if (acknowledged) acknowledgeResidentResponse(root, response, time, 3);
  return command;
};
const sweep = (root: string, now: number, live = new Set<string>()) => new ResidentRequestRetention(root).sweep(now, live, 10_000);
const exists = (root: string, dir: string, id: string) => fs.existsSync(path.join(root, dir, `${id}.json`));

describe("resident terminal event retention", () => {
  const day = 24 * 60 * 60 * 1000;
  const now = 4 * day;
  const log = Buffer.from(Array.from({ length: 15000 }, (_, sequence) => JSON.stringify({ sequence, text: "🙂".repeat(10) }) + "\n").join(""));
  const make = (dir: string, id: string, status: string, finishedAt = day) => {
    const run = path.join(dir, "runs", id);
    fs.mkdirSync(run, { recursive: true });
    fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ status, finishedAt }));
    fs.writeFileSync(path.join(run, "events.jsonl"), log);
    fs.writeFileSync(path.join(run, "reply.json"), '{"text":"keep reply"}');
    return run;
  };

  it.each([
    ["startup", false], ["startup", true], ["streaming", false], ["streaming wildcard", false],
  ] as const)("%s retention (retainRuns=%s) requires checked descendant exit before touching an aged parent log", (phase, retainRuns) => {
    const dir = root();
    const unknown = [undefined, "malformed", String(process.pid)].map((sessionId, index) => {
      const run = make(dir, `unknown-${index}`, "completed");
      write(run, "nested/child", "status", { status: "completed", transport: "process", sessionId });
      fs.writeFileSync(path.join(run, "nested/child/events.jsonl"), log);
      const aged = new Date(day); fs.utimesSync(run, aged, aged);
      return run;
    });
    const exited = make(dir, "checked-exited", "completed");
    write(exited, "nested/child", "status", { status: "completed", transport: "process", sessionId: "2147483647" });
    const snapshots = unknown.map(run => ["events.jsonl", "status.json", "reply.json", "nested/child/status.json", "nested/child/events.jsonl"]
      .map(name => [name, fs.readFileSync(path.join(run, name))] as const));
    if (phase === "startup") sweepResidentRuns(path.join(dir, "runs"), now, 10000, { retainRuns });
    else {
      const retention = new ResidentRequestRetention(dir);
      try { retention.sweep(now, new Set(phase === "streaming wildcard" ? ["*"] : []), 10000); }
      finally { retention.close(); }
    }
    // Native byte equality preserves the full snapshot guarantee without Vitest's
    // per-byte deep comparison of multi-megabyte Buffers consuming the CI timeout.
    for (const [index, run] of unknown.entries()) {
      for (const [name, before] of snapshots[index]!) {
        expect(fs.readFileSync(path.join(run, name)).equals(before), `${run}/${name} must remain byte-identical`).toBe(true);
      }
    }
    expect(fs.statSync(path.join(exited, "events.jsonl")).size).toBeLessThanOrEqual(256 * 1024);
  });

  it("uses the existing streaming sweep to bound terminal residency runs, preserving results, latest references and idempotency", () => {
    const dir = root();
    const actorRoot = path.join(dir, "actor-registry");
    write(actorRoot, ".", "actors", { actors: [{ id: "actor", lastRunId: "latest" }] });
    const compacted = ["completed", "failed", "stopped", "timed_out"].map(status => make(dir, status, status));
    const preserved = [make(dir, "live", "running"), make(dir, "queued", "queued"), make(dir, "unknown", "unknown"),
      make(dir, "young", "completed", now - 1000), make(dir, "latest", "failed"), make(dir, "held", "completed")];
    write(dir, "results", "completed", { text: "keep saved result" });
    const result = fs.readFileSync(path.join(dir, "results", "completed.json"));
    const statuses = compacted.map(run => fs.readFileSync(path.join(run, "status.json")));
    const retention = new ResidentRequestRetention(dir, [actorRoot], { terminalRunEventsMaxBytes: 128 * 1024 });
    // A wildcard from the budget-limited request-reference scan cannot starve the
    // run phase: terminal/exit safety is independently checked for every candidate.
    retention.sweep(now, new Set(["*", "held"]), 10000);
    const bounded = compacted.map(run => fs.readFileSync(path.join(run, "events.jsonl")));
    for (const [index, run] of compacted.entries()) {
      const tail = bounded[index]!;
      expect(tail.length).toBeLessThanOrEqual(128 * 1024);
      expect(JSON.parse(tail.subarray(0, tail.indexOf(0x0a)).toString())).toMatchObject({ fabricTruncated: true });
      const events = tail.subarray(tail.indexOf(0x0a) + 1);
      expect(events.equals(log.subarray(log.length - events.length))).toBe(true);
      expect(JSON.parse(events.toString().trim().split("\n").at(-1)!)).toMatchObject({ sequence: 14999 });
      expect(fs.readFileSync(path.join(run, "status.json"))).toEqual(statuses[index]);
      expect(fs.readFileSync(path.join(run, "reply.json"), "utf8")).toBe('{"text":"keep reply"}');
    }
    for (const run of preserved) expect(fs.readFileSync(path.join(run, "events.jsonl")).equals(log)).toBe(true);
    expect(fs.readFileSync(path.join(dir, "results", "completed.json"))).toEqual(result);
    const rename = vi.spyOn(fs, "renameSync");
    retention.sweep(now + 60001, new Set(["held"]), 10000);
    expect(rename.mock.calls.some(call => String(call[1]).endsWith("events.jsonl"))).toBe(false);
    for (const [index, run] of compacted.entries()) expect(fs.readFileSync(path.join(run, "events.jsonl")).equals(bounded[index]!)).toBe(true);
    retention.close();
  });

  it("also bounds explicitly retained runs at fenced startup and protects persisted lastRunId from cleanup", () => {
    const dir = root(); const actorRoot = path.join(dir, "actor-registry");
    write(actorRoot, ".", "actors", { actors: [{ id: "actor", lastRunId: "latest" }] });
    const old = make(dir, "old", "completed");
    const latest = make(dir, "latest", "failed");
    for (const run of [old, latest]) fs.utimesSync(run, day / 1000, day / 1000);
    const runs = path.join(dir, "runs");
    expect(sweepResidentRuns(runs, now, 10000, { actorRoots: [actorRoot], retainRuns: true })).toEqual([]);
    expect(fs.statSync(path.join(old, "events.jsonl")).size).toBeLessThanOrEqual(256 * 1024);
    expect(fs.existsSync(path.join(old, "status.json"))).toBe(true);
    expect(fs.readFileSync(path.join(latest, "events.jsonl")).equals(log)).toBe(true);
    fs.utimesSync(old, day / 1000, day / 1000);
    expect(sweepResidentRuns(runs, now, 10000, { actorRoots: [actorRoot] })).toEqual([old]);
    expect(fs.readFileSync(path.join(latest, "events.jsonl")).equals(log)).toBe(true);
  });

  it.each(["startup", "streaming"])("compacts with a valid registry above 1 MiB at %s, preserving its latest run", (phase) => {
    const dir = root(); const actorRoot = path.join(dir, "actor-registry");
    write(actorRoot, ".", "actors", { actors: [{ id: "actor", lastRunId: "latest", instructions: "normal actor",
      messages: Array.from({ length: 100 }, () => ({ text: "x".repeat(12_000) })) }] });
    const registry = fs.readFileSync(path.join(actorRoot, "actors.json"));
    expect(registry.length).toBeGreaterThan(1024 * 1024);
    const old = make(dir, "old", "completed"); const latest = make(dir, "latest", "failed");
    if (phase === "startup") {
      sweepResidentRuns(path.join(dir, "runs"), now, 10000, { actorRoots: [actorRoot], retainRuns: true });
    } else {
      const collector = new ResidentRequestRetention(dir, [actorRoot]);
      try { collector.sweep(now, new Set(), 10000); } finally { collector.close(); }
    }
    expect(fs.statSync(path.join(old, "events.jsonl")).size).toBeLessThanOrEqual(256 * 1024);
    expect(fs.readFileSync(path.join(latest, "events.jsonl")).equals(log)).toBe(true);
    expect(fs.readFileSync(path.join(actorRoot, "actors.json")).equals(registry)).toBe(true);
  });

  it.each(["slow references", "slow safety walk"])("makes progress with the production 5-ms budget despite %s", (slow) => {
    const dir = root(); const actorRoot = path.join(dir, "actor-registry");
    write(actorRoot, ".", "actors", { actors: [{ id: "actor", lastRunId: "latest" }] });
    const old = make(dir, "old", "completed");
    const latest = make(dir, "latest", "failed"); const live = make(dir, "live", "completed");
    const worker = make(dir, "worker", "completed");
    write(worker, ".", "status", { status: "completed", finishedAt: day, transport: "process", sessionId: String(process.pid) });
    if (slow === "slow safety walk") {
      for (let i = 0; i < 20; i++) {
        const child = path.join(old, "nested", String(i));
        write(child, ".", "status", { status: "completed", finishedAt: day, transport: "process", sessionId: "2147483647" });
      }
    }
    let elapsed = 0; let registryReads = 0;
    vi.spyOn(performance, "now").mockImplementation(() => elapsed);
    const read = fs.readFileSync;
    vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof read>) => {
      const file = String(args[0]);
      if (file === path.join(actorRoot, "actors.json")) { registryReads++; if (slow === "slow references") elapsed += 6; }
      if (slow === "slow safety walk" && file.startsWith(old) && file.endsWith("status.json")) elapsed += 1;
      return read(...args);
    });
    const collector = new ResidentRequestRetention(dir, [actorRoot]);
    try {
      for (let slice = 0; slice < 100 && collector.due(now); slice++) collector.sweep(now, new Set(["live"]), 5);
      expect(fs.statSync(path.join(old, "events.jsonl")).size).toBeLessThanOrEqual(256 * 1024);
      for (const run of [latest, live, worker]) expect(fs.readFileSync(path.join(run, "events.jsonl")).equals(log)).toBe(true);
      expect(registryReads).toBeGreaterThan(0);
    } finally { collector.close(); }
  });

  it.each(["new latest", "live set", "live worker", "unreadable registry"])("refreshes the %s veto between over-budget reference preparation and compaction", (change) => {
    const dir = root(); const actorRoot = path.join(dir, "actor-registry");
    write(actorRoot, ".", "actors", { actors: [{ id: "actor", lastRunId: "latest" }] });
    const old = make(dir, "old", "completed"); const latest = make(dir, "latest", "failed");
    let elapsed = 0;
    vi.spyOn(performance, "now").mockImplementation(() => elapsed);
    const read = fs.readFileSync;
    vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof read>) => {
      if (String(args[0]) === path.join(actorRoot, "actors.json")) elapsed += 6;
      return read(...args);
    });
    const collector = new ResidentRequestRetention(dir, [actorRoot]);
    try {
      collector.sweep(now, new Set(), 5);
      expect(fs.readFileSync(path.join(old, "events.jsonl")).equals(log)).toBe(true);
      if (change === "new latest") {
        write(actorRoot, ".", "actors-next", { actors: [{ id: "actor", lastRunId: "old" }, { id: "other", lastRunId: "latest" }] });
        fs.renameSync(path.join(actorRoot, "actors-next.json"), path.join(actorRoot, "actors.json"));
      } else if (change === "unreadable registry") {
        fs.writeFileSync(path.join(actorRoot, "actors.json"), "{");
      } else if (change === "live worker") {
        write(old, ".", "status", { status: "completed", finishedAt: day, transport: "process", sessionId: String(process.pid) });
      }
      for (let slice = 0; slice < 100 && collector.due(now); slice++) collector.sweep(now, new Set(change === "live set" ? ["old"] : []), 5);
      for (const run of [old, latest]) expect(fs.readFileSync(path.join(run, "events.jsonl")).equals(log)).toBe(true);
    } finally { collector.close(); }
  });

  it("vetoes a new latest-run reference published during a long compaction safety walk", () => {
    const dir = root(); const actorRoot = path.join(dir, "actor-registry");
    write(actorRoot, ".", "actors", { actors: [] });
    const old = make(dir, "old", "completed");
    vi.spyOn(performance, "now").mockReturnValue(0);
    const read = fs.readFileSync; let changed = false;
    vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof read>) => {
      if (!changed && String(args[0]) === path.join(old, "status.json")) {
        changed = true;
        write(actorRoot, ".", "actors", { actors: [{ id: "actor", lastRunId: "old" }] });
      }
      return read(...args);
    });
    const collector = new ResidentRequestRetention(dir, [actorRoot]);
    try {
      collector.sweep(now, new Set(), 5);
      expect(changed).toBe(true);
      expect(fs.readFileSync(path.join(old, "events.jsonl")).equals(log)).toBe(true);
    } finally { collector.close(); }
  });

  it("eventually compacts via ResidentHost's actual 5-ms polling path after an over-budget registry read", async () => {
    const dir = root(); const actorRoot = path.join(dir, "actor-registry");
    const old = make(dir, "old", "completed"); const latest = make(dir, "latest", "failed");
    const held = make(dir, "held", "completed"); const worker = make(dir, "worker", "completed");
    write(worker, ".", "status", { status: "completed", finishedAt: day, transport: "process", sessionId: String(process.pid) });
    // Protect fixtures from startup, so only the production poll can compact old.
    write(actorRoot, ".", "actors", { actors: ["old", "latest", "held"].map(id => ({ id, lastRunId: id })) });
    const config: ResidentHostConfig = {
      format: 1, rootId: "session:retention", sessionId: "retention", cwd: dir, projectRoot: dir,
      meshRoot: path.join(dir, "mesh"), actorRoot, residencyRoot: dir,
      fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
      mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorScope: "project" }, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath: "unused", fabricExtensionPath: "unused", piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    };
    let elapsed = 0; let registryReads = 0;
    vi.spyOn(performance, "now").mockImplementation(() => elapsed);
    const read = fs.readFileSync;
    vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof read>) => {
      if (String(args[0]) === path.join(actorRoot, "actors.json")) { elapsed += 6; registryReads++; }
      return read(...args);
    });
    const slices = vi.spyOn(ResidentRequestRetention.prototype, "sweep");
    const host = new ResidentHost(config);
    try {
      await host.start();
      expect(fs.readFileSync(path.join(old, "events.jsonl")).equals(log)).toBe(true);
      vi.spyOn(host.agents, "retentionReferences").mockReturnValue(new Set(["held"]));
      write(actorRoot, ".", "actors", { actors: [{ id: "actor", lastRunId: "latest" }] });
      const deadline = Date.now() + 2000;
      while (fs.statSync(path.join(old, "events.jsonl")).size > 256 * 1024 && Date.now() < deadline) {
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      expect(fs.statSync(path.join(old, "events.jsonl")).size).toBeLessThanOrEqual(256 * 1024);
      for (const run of [latest, held, worker]) expect(fs.readFileSync(path.join(run, "events.jsonl")).equals(log)).toBe(true);
      expect(registryReads).toBeGreaterThan(0);
      expect(slices.mock.calls.length).toBeGreaterThan(1);
      expect(slices.mock.calls.every(call => call[2] === 5)).toBe(true);
    } finally { await host.close(); }
  });

  it("fails closed for unreadable actor lastRunId references, and honors custom age", () => {
    const dir = root(); const actorRoot = path.join(dir, "actor-registry");
    write(actorRoot, ".", "actors", { actors: [{ id: "actor", lastRunId: 123 }] });
    const run = make(dir, "old", "completed");
    new ResidentRequestRetention(dir, [actorRoot]).sweep(now, new Set(), 10000);
    expect(fs.readFileSync(path.join(run, "events.jsonl")).equals(log)).toBe(true);
    new ResidentRequestRetention(dir, [], { terminalRunEventsAgeMs: 7 * day }).sweep(now, new Set(), 10000);
    expect(fs.readFileSync(path.join(run, "events.jsonl")).equals(log)).toBe(true);
  });
});

describe("bounded resident request retention", () => {
  const now = Date.now();
  const old = now - expiry.RESIDENT_REQUEST_RETENTION_MS - 1_000;
  it("collects old terminal acknowledged decisions/responses and refuses replay even with a refreshed createdAt", () => {
    const dir = root(); const command = seed(dir, old);
    sweep(dir, now);
    for (const kind of ["decisions", "responses", "acknowledgements"]) expect(exists(dir, kind, command.requestId)).toBe(false);
    expect(() => commitResidentRequest(dir, { ...command, createdAt: now }, "duplicate", "host")).toThrow(/expired.*do not replay/i);
    expect(readResidentRequestDecision(dir, command.requestId)).toBeUndefined();
    expect(residentHostStateNote(dir)).toMatch(/residency retention:.*0 entries.*0 bytes/);
  });
  it.each(["unacknowledged", "recent", "request", "processing", "live", "unknown writer", "pending"])("keeps %s references", (kind) => {
    const dir = root(); const command = seed(dir, kind === "recent" ? now : old, kind !== "unacknowledged");
    if (kind === "request" || kind === "processing") write(dir, kind === "request" ? "requests" : kind, command.requestId, command);
    if (kind === "pending") write(dir, "acknowledgements", command.requestId, { format: 1, requestFormat: 3, requestId: command.requestId, completedAt: old, acknowledgedAt: old, pending: "entity" });
    sweep(dir, now, new Set(kind === "unknown writer" ? ["*"] : kind === "live" || kind === "pending" ? ["entity"] : []));
    expect(exists(dir, "decisions", command.requestId)).toBe(true);
    expect(exists(dir, "responses", command.requestId)).toBe(true);
  });
  it("keeps unreadable, unknown and legacy entries and reports capacity", () => {
    const dir = root(); const command = seed(dir, old);
    fs.writeFileSync(path.join(dir, "decisions", `${command.requestId}.json`), "{");
    const legacy = { ...command, format: 1, requestId: "legacy" } as ResidentCommand;
    commitResidentRequest(dir, legacy, "legacy-entity", "host");
    fs.writeFileSync(path.join(dir, "decisions", "orphan.tmp"), "uncertain publisher");
    sweep(dir, now);
    expect(exists(dir, "decisions", command.requestId)).toBe(true);
    expect(exists(dir, "responses", command.requestId)).toBe(true);
    expect(exists(dir, "decisions", "legacy")).toBe(true);
    expect(residentHostStateNote(dir)).toMatch(/unknown=[1-9]/);
    expect(residentHostStateNote(dir)).toMatch(/legacy=[1-9]/);
  });
  it("keeps an unreadable response or acknowledgement, never inferring acknowledgement from absence", () => {
    const dir = root(); const a = seed(dir, old); const b = seed(dir, old);
    fs.writeFileSync(path.join(dir, "responses", `${a.requestId}.json`), "{");
    fs.writeFileSync(path.join(dir, "acknowledgements", `${b.requestId}.json`), "null");
    sweep(dir, now);
    expect(exists(dir, "decisions", a.requestId)).toBe(true);
    expect(exists(dir, "decisions", b.requestId)).toBe(true);
  });
  it("fails closed on an unreadable expiry floor and retains its fences", () => {
    const dir = root(); const command = seed(dir, old);
    fs.writeFileSync(path.join(dir, "request-expiry.json"), "{");
    sweep(dir, now);
    expect(exists(dir, "decisions", command.requestId)).toBe(true);
    expect(() => commitResidentRequest(dir, { ...command, requestId: expiry.newResidentRequestId(now) }, "new", "host")).toThrow(/expiry/i);
    expect(residentHostStateNote(dir)).toMatch(/expiry.*unreadable/i);
  });
  it.each([...new Set([nativePlatform, "win32"])] as NodeJS.Platform[])("retries the platform durable barrier before collection, including after restart (%s)", (platform) => {
    Object.defineProperty(process, "platform", { value: platform });
    const dir = root(); const command = seed(dir, old);
    let renamed = false;
    const rename = fs.renameSync;
    const sync = fs.fsyncSync;
    const renameFault = vi.spyOn(fs, "renameSync").mockImplementation((...args) => {
      rename(...args);
      if (args[1] === path.join(dir, "request-expiry.json")) renamed = true;
    });
    const syncFault = vi.spyOn(fs, "fsyncSync").mockImplementation((...args) => {
      // Windows has no directory fsync: fail its applicable pre-rename file
      // barrier instead. Unix retains the published-but-not-durable rename race.
      if (platform === "win32" ? fs.fstatSync(args[0]).isFile() : renamed) {
        throw new Error(platform === "win32" ? "file barrier unavailable" : "directory barrier unavailable");
      }
      return sync(...args);
    });
    sweep(dir, now);
    expect(renamed).toBe(platform !== "win32");
    expect(exists(dir, "decisions", command.requestId)).toBe(true);
    // A new collector retries the file barrier on Windows, or the visible-but-unsynced Unix rename.
    sweep(dir, now);
    expect(exists(dir, "decisions", command.requestId)).toBe(true);
    expect(exists(dir, "responses", command.requestId)).toBe(true);
    expect(residentHostStateNote(dir)).toMatch(/collection disabled/);
    syncFault.mockRestore(); renameFault.mockRestore();
    sweep(dir, now);
    expect(exists(dir, "decisions", command.requestId)).toBe(false);
  });
  it("negotiation preserves an existing generation request ID, including an expired replay", () => {
    const dir = root(); const command = seed(dir, old);
    sweep(dir, now);
    const negotiated = residentCommandForOwner(command, { requestExpiry: 1 } as Parameters<typeof residentCommandForOwner>[1]);
    expect(negotiated.requestId).toBe(command.requestId);
    expect(() => commitResidentRequest(dir, negotiated, "duplicate", "host")).toThrow(/expired/i);
  });
  it("persists a monotonic floor across restart and clock rollback", () => {
    const dir = root(); const command = seed(dir, old);
    sweep(dir, now); sweep(dir, old - 10_000);
    expect(() => commitResidentRequest(dir, command, "duplicate", "host")).toThrow(/expired/i);
    expect(() => abandonResidentRequest(path.join(dir, "requests"), path.join(dir, "responses"), command.requestId)).toThrow(/expired/i);
  });
  it("checks expiry again after publishing a fence to close the collector/link race", () => {
    const dir = root(); const command = seed(dir, old);
    fs.rmSync(path.join(dir, "decisions", `${command.requestId}.json`));
    fs.rmSync(path.join(dir, "acknowledgements", `${command.requestId}.json`));
    const link = fs.linkSync;
    vi.spyOn(fs, "linkSync").mockImplementation((...args) => { sweep(dir, now); return link(...args); });
    expect(() => commitResidentRequest(dir, command, "duplicate", "host")).toThrow(/expired/i);
  });
  it.each(["abandonment", "cancellation"] as const)("rejects expired %s after collection vacates a committed fence during its CAS", (kind) => {
    const dir = root(); const command = seed(dir, old);
    const link = fs.linkSync;
    vi.spyOn(fs, "linkSync").mockImplementation((...args) => {
      sweep(dir, now);
      expect(exists(dir, "decisions", command.requestId)).toBe(false);
      // Exchange files published after collection must not be removed as safe abandonment.
      write(dir, "requests", command.requestId, command);
      write(dir, "responses", command.requestId, { late: true });
      return link(...args);
    });
    let error: unknown;
    if (kind === "cancellation") {
      const controller = new AbortController();
      registerResidentCancellation(controller.signal, dir, command);
      error = cancellationError(controller.signal, new Error("ordinary cancellation"));
    } else {
      try { abandonResidentRequest(path.join(dir, "requests"), path.join(dir, "responses"), command.requestId, 3); }
      catch (caught) { error = caught; }
    }
    expect(error).toBeInstanceOf(expiry.ResidentRequestExpiredError);
    expect(error).toMatchObject({ code: "RESIDENT_REQUEST_EXPIRED" });
    expect((error as Error).message).toMatch(/do not replay or reassign/);
    expect(exists(dir, "acknowledgements", command.requestId)).toBe(false);
    expect(exists(dir, "requests", command.requestId)).toBe(true);
    expect(exists(dir, "responses", command.requestId)).toBe(true);
  });
  it.each(["main", "nested"] as const)("%s client reports expired, not ordinary timeout, when collection wins the abandonment CAS race", async (kind) => {
    const dir = root(); const rootId = "session:expiry-race"; const meshRoot = path.join(dir, "mesh");
    const residencyRoot = residentRoot(meshRoot, rootId);
    fs.mkdirSync(residencyRoot, { recursive: true });
    fs.writeFileSync(path.join(residencyRoot, "owner.json"), JSON.stringify({ format: 1, hostId: residentHostId(rootId), pid: process.pid, requestFence: 1, requestExpiry: 1, commands: ["removeActor"] }));
    const config: ResidentHostConfig = {
      format: 1, rootId, sessionId: "expiry-race", cwd: dir, projectRoot: dir, meshRoot, actorRoot: path.join(dir, "actors"), residencyRoot,
      fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: DEFAULT_FABRIC_CONFIG.mesh, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath: "unused", fabricExtensionPath: "unused", piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    };
    const mesh = new MeshStore(meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
    const participants = new ParticipantDirectory(mesh, { enabled: false, hostId: rootId, rootId, identity: { id: rootId, name: "main", kind: "main" } });
    const main = new ResidencyClient({ config, mesh, participants, commandTimeoutMs: 0,
      mainAgent: { id: rootId, local: true, matches: id => id === rootId, info: () => { throw new Error("unused"); }, deliverAgent: () => { throw new Error("unused"); } } });
    const nested = new ResidentActorClient(meshRoot, rootId, 0);
    let requestId = "";
    const clock = vi.spyOn(Date, "now").mockReturnValue(old);
    const rename = fs.renameSync; const link = fs.linkSync;
    vi.spyOn(fs, "renameSync").mockImplementation((...args) => {
      rename(...args);
      if (path.dirname(String(args[1])) !== path.join(residencyRoot, "requests")) return;
      const command = JSON.parse(fs.readFileSync(args[1], "utf8")) as ResidentCommand;
      requestId = command.requestId;
      expect(command.format).toBe(3);
      commitResidentRequest(residencyRoot, command, "entity", "host");
      acknowledgeResidentResponse(residencyRoot, { format: 1, requestId, ok: true, completedAt: old }, old, 3);
      fs.rmSync(args[1]); // Simulate an already completed, consumed exchange with a lost reply.
      clock.mockRestore();
    });
    vi.spyOn(fs, "linkSync").mockImplementation((...args) => {
      if (JSON.parse(fs.readFileSync(args[0], "utf8")).state === "abandoned") {
        sweep(residencyRoot, now);
        expect(exists(residencyRoot, "decisions", requestId)).toBe(false);
      }
      return link(...args);
    });
    try {
      await expect((kind === "main" ? main : nested).removeActor("entity")).rejects.toMatchObject({ code: "RESIDENT_REQUEST_EXPIRED" });
      expect(exists(residencyRoot, "acknowledgements", requestId)).toBe(false);
    } finally { await main.close(); await participants.close(); }
  });
  it("resumes a budgeted scan without starving later entries", () => {
    const dir = root(); const commands = Array.from({ length: 40 }, () => seed(dir, old));
    const collector = new ResidentRequestRetention(dir);
    for (let i = 0; i < 500 && commands.some(c => exists(dir, "decisions", c.requestId)); i++) collector.sweep(now, new Set(), 0.1);
    expect(commands.some(c => exists(dir, "decisions", c.requestId))).toBe(false);
    collector.close();
  });
  it("retains a generation-looking legacy decision: rollback may still execute its format-1 envelope", () => {
    const dir = root(); const requestId = expiry.newResidentRequestId(old);
    commitResidentRequest(dir, { format: 1, operation: "removeActor", requestId, rootId: "root", id: "entity", createdAt: old }, "entity", "host");
    // Even an incorrect format-3 ack cannot authorize collection of a legacy decision.
    acknowledgeResidentResponse(dir, { format: 1, requestId, ok: true, completedAt: old }, old, 3);
    sweep(dir, now);
    expect(exists(dir, "decisions", requestId)).toBe(true);
    expect(residentHostStateNote(dir)).toMatch(/unknown=[1-9]/);
  });
  it("collects acknowledged abandonment but keeps a recent acknowledgement of an old outcome", () => {
    const dir = root(); const abandoned = expiry.newResidentRequestId(old);
    const clock = vi.spyOn(Date, "now").mockReturnValue(old);
    abandonResidentRequest(path.join(dir, "requests"), path.join(dir, "responses"), abandoned, 3);
    clock.mockRestore();
    const recentAck = seed(dir, old);
    acknowledgeResidentResponse(dir, { format: 1, requestId: recentAck.requestId, ok: true, completedAt: old }, now, 3);
    sweep(dir, now);
    expect(exists(dir, "decisions", abandoned)).toBe(false);
    expect(exists(dir, "decisions", recentAck.requestId)).toBe(true);
  });
  it.each(["running", "queued", "unreadable"])("keeps persisted %s agent references after host loss", (status) => {
    const dir = root(); const command = seed(dir, old);
    write(dir, "agents", "entity", status === "unreadable" ? null : { id: "entity", handle: { status } });
    sweep(dir, now);
    expect(exists(dir, "decisions", command.requestId)).toBe(true);
    expect(exists(dir, "responses", command.requestId)).toBe(true);
  });
  it.each(["live", "registry unreadable", "removal pending", "removal unreadable"])("keeps persisted actor references: %s", (kind) => {
    const dir = root(); const command = seed(dir, old); const actors = path.join(dir, "actor-root");
    fs.mkdirSync(actors);
    fs.writeFileSync(path.join(actors, "actors.json"), kind === "registry unreadable" ? "{" : JSON.stringify({ actors: kind === "live" ? [{ id: "entity", status: "running" }] : [] }));
    if (kind.startsWith("removal")) fs.writeFileSync(path.join(actors, "removal-entity.json"), kind === "removal unreadable" ? "{" : JSON.stringify({ id: "entity" }));
    new ResidentRequestRetention(dir, [actors]).sweep(now, new Set(), 10_000);
    expect(exists(dir, "decisions", command.requestId)).toBe(true);
    expect(exists(dir, "responses", command.requestId)).toBe(true);
    if (kind.includes("unreadable")) expect(residentHostStateNote(dir)).toMatch(/unknown=[1-9]/);
  });
  it.each(["stopped alone", "writer gone", "live overrides proof"])("requires owner-confirmed writer exit for a stopped registry row: %s", (kind) => {
    const dir = root(); const command = seed(dir, old); const actors = path.join(dir, "actor-root");
    fs.mkdirSync(actors);
    fs.writeFileSync(path.join(actors, "actors.json"), JSON.stringify({ actors: [{ id: "entity", status: "stopped" }] }));
    const live = new Set(kind === "live overrides proof" ? ["entity"] : []);
    const gone = new Set(kind === "stopped alone" ? [] : ["entity"]);
    const collector = new ResidentRequestRetention(dir, [actors]);
    try { collector.sweep(now, live, 10_000, gone); } finally { collector.close(); }
    expect(exists(dir, "decisions", command.requestId)).toBe(kind !== "writer gone");
    expect(exists(dir, "acknowledgements", command.requestId)).toBe(kind !== "writer gone");
  });
  it("an acknowledgement publication failure retains the response", () => {
    const dir = root(); const command = seed(dir, old, false);
    fs.writeFileSync(path.join(dir, "acknowledgements"), "not a directory");
    expect(acknowledgeResidentResponse(dir, { format: 1, requestId: command.requestId, ok: true, completedAt: old }, old, 3)).toBe(false);
    sweep(dir, now);
    expect(exists(dir, "decisions", command.requestId)).toBe(true);
    expect(exists(dir, "responses", command.requestId)).toBe(true);
  });
  it("partial deletion still cannot reopen replay and leaves the acknowledgement for a later scan", () => {
    const dir = root(); const command = seed(dir, old);
    const remove = fs.rmSync;
    const fault = vi.spyOn(fs, "rmSync").mockImplementation((...args) => {
      if (args[0] === path.join(dir, "decisions", `${command.requestId}.json`)) throw new Error("disk fault");
      return remove(...args);
    });
    sweep(dir, now);
    expect(exists(dir, "responses", command.requestId)).toBe(false);
    expect(exists(dir, "decisions", command.requestId)).toBe(true);
    expect(exists(dir, "acknowledgements", command.requestId)).toBe(true);
    expect(() => commitResidentRequest(dir, command, "duplicate", "host")).toThrow(/expired/i);
    fault.mockRestore(); sweep(dir, now);
    expect(exists(dir, "decisions", command.requestId)).toBe(false);
  });
  it("the real host maintenance path collects and replay returns a definitive expired error without dispatch", async () => {
    const dir = root(); const command = seed(dir, old);
    const config: ResidentHostConfig = {
      format: 1, rootId: command.rootId, sessionId: "retention", cwd: dir, projectRoot: dir,
      meshRoot: path.join(dir, "mesh"), actorRoot: path.join(dir, "actors"), residencyRoot: dir,
      fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: DEFAULT_FABRIC_CONFIG.mesh,
      retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: "unused", fabricExtensionPath: "unused", piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    };
    const host = new ResidentHost(config);
    try {
      await host.start();
      expect(exists(dir, "decisions", command.requestId)).toBe(false);
      const remove = vi.spyOn(host.actors, "remove");
      write(dir, "requests", command.requestId, { ...command, createdAt: Date.now() });
      const deadline = Date.now() + 2_000;
      while (!exists(dir, "responses", command.requestId) && Date.now() < deadline) await new Promise(r => setTimeout(r, 10));
      expect(JSON.parse(fs.readFileSync(path.join(dir, "responses", `${command.requestId}.json`), "utf8"))).toMatchObject({ ok: false, errorCode: "RESIDENT_REQUEST_EXPIRED" });
      expect(remove).not.toHaveBeenCalled();
      expect(readResidentRequestDecision(dir, command.requestId)).toBeUndefined();
    } finally { await host.close(); }
  });
});
