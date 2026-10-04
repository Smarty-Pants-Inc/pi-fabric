import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import { ActorManager } from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { deliverActorToMain } from "../src/actors/main-delivery.js";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

// smarty-dev#1439: a long-lived actor's session grows until compaction fails. resetSession
// and actors.maxSessionBytes start its next run on a fresh session and keep its mailbox.

const roots: string[] = [];
const managers: Array<{ close(): Promise<void> }> = [];

const waitFor = async (predicate: () => boolean, timeoutMs = process.env.CI ? 10_000 : 3_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Timed out waiting for actor state");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
};

interface SessionRun { task: string; prior: string[] }

const setup = (options: {
  maxSessionBytes?: number;
  canManageActor?: (id: string) => boolean | undefined;
  resolvePiModel?: (model: string) => string | Promise<string>;
} = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-reset-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/session-worker.mjs"),
    runRoot: path.join(root, "runs"),
  });
  const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
  const deliveries: string[] = [];
  const sendMessage = vi.fn();
  const pi = { sendMessage } as unknown as ExtensionAPI;
  const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, (request) => {
    if (request.message.text) deliveries.push(request.message.text);
    deliverActorToMain(pi, identity, request);
  }, {
    actorRoot: path.join(root, "actors"),
    persistent: true,
    preparationRetryMs: 25,
    ...(options.maxSessionBytes !== undefined ? { maxSessionBytes: options.maxSessionBytes } : {}),
    ...(options.canManageActor ? { canManageActor: options.canManageActor } : {}),
    ...(options.resolvePiModel ? { resolvePiModel: options.resolvePiModel } : {}),
  });
  managers.push(actors, agents);
  // The fake worker appends each run's turns to its --session file, as Pi does: a run's
  // prior context is what the file held when it started. `gate` holds a run open.
  const runs: SessionRun[] = [];
  let gate: Promise<void> | undefined;
  const run = agents.run.bind(agents);
  vi.spyOn(agents, "run").mockImplementation(async (request, signal) => {
    const file = request.sessionFile!;
    const prior = fs.existsSync(file) ? fs.readFileSync(file, "utf8").split("\n").filter((line) => {
      if (!line) return false;
      try { return JSON.parse(line).type !== "session"; } catch { return true; }
    }) : [];
    runs.push({ task: request.task, prior });
    await gate;
    return run(request, signal);
  });
  return {
    actors, agents, runs, root, mesh, deliveries, sendMessage,
    hold: () => {
      let release!: () => void;
      gate = new Promise((resolve) => { release = resolve; });
      return () => { gate = undefined; release(); };
    },
  };
};

const backups = (sessionFile: string): string[] =>
  fs.readdirSync(path.dirname(sessionFile))
    .filter((name) => name.startsWith("session.jsonl.") && name.endsWith(".bak"))
    .sort();

const sessionHeader = (file: string) => JSON.parse(fs.readFileSync(file, "utf8").split("\n", 1)[0]!);

const resets = (actors: ActorManager, id: string) =>
  actors.messages(id, 50).filter((message) => (message.data as { sessionReset?: unknown } | undefined)?.sessionReset);

afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(managers.splice(0).map((manager) => manager.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("actor session rotation safety (smarty-dev#2847)", () => {
  it.each(["archive", "registry"] as const)("#2479 R3 F26 finishes a Claude size reset after failed %s confirmation", async fault => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "claude-reset-")); roots.push(root);
    const invocationLog = path.join(root, "claude.jsonl"), previousLog = process.env.FAKE_CLAUDE_LOG;
    process.env.FAKE_CLAUDE_LOG = invocationLog;
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("src/worker.ts"), claudeBinary: path.resolve("tests/fixtures/fake-claude.mjs"), runRoot: path.join(root, "runs"),
    });
    const actors = new ActorManager("claude-reset", { id: "session:claude-reset", name: "main", kind: "main" },
      new MeshStore(path.join(root, "mesh"), 65536, 100), { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
      { actorRoot: path.join(root, "actors"), persistent: true, maxSessionBytes: 64, preparationRetryMs: 25 });
    managers.push(actors, agents);
    const launch = vi.spyOn(agents, "run"), namespace = atomic.syncPathNamespace, write = atomic.AtomicFileWriter.prototype.write;
    let unavailable = true, failures = 0, archived: string | undefined;
    try {
      const actor = await actors.create({ name: "native size reset", instructions: "Work.", residency: "durable", runner: "claude", model: "claude/haiku", transport: "process" });
      await actors.ask(actor.id, "first native context"); await waitFor(() => actors.inFlightCount() === 0, 15000);
      const registry = path.join(root, "actors", "actors.json"), pending = `${actor.sessionFile}.archive-pending.json`;
      expect(JSON.parse(fs.readFileSync(registry, "utf8")).actors[0].runnerSessionId).toBe("11111111-1111-4111-8111-111111111111");
      expect(fs.statSync(actor.sessionFile!).size).toBeGreaterThan(64);
      const contents = fs.readFileSync(actor.sessionFile!, "utf8");
      const prior = ["20000101T000000000Z", "20000102T000000000Z", "20000103T000000000Z"].map(stamp => `${actor.sessionFile}.${stamp}.bak`);
      for (const file of prior) fs.copyFileSync(actor.sessionFile!, file);
      vi.spyOn(atomic, "syncPathNamespace").mockImplementation((file, inode) => {
        if (file.startsWith(`${actor.sessionFile}.`) && file.endsWith(".bak")) {
          archived = file;
          if (unavailable && fault === "archive") { failures++; throw new Error("archive reset confirmation unavailable"); }
        }
        namespace(file, inode);
      });
      vi.spyOn(atomic.AtomicFileWriter.prototype, "write").mockImplementation(function(this: atomic.AtomicFileWriter, contents, options) {
        if (this.file === registry && archived && unavailable && fault === "registry") { failures++; throw new Error("reset registry unavailable"); }
        return write.call(this, contents, options);
      });
      actors.tell(actor.id, "after native reset");
      await waitFor(() => failures > 0, 15000); await new Promise(resolve => setTimeout(resolve, 150));
      expect(launch).toHaveBeenCalledTimes(1);
      expect(fs.existsSync(pending)).toBe(true);
      expect(prior.every(file => fs.existsSync(file))).toBe(true);
      unavailable = false;
      await waitFor(() => launch.mock.calls.length === 2 && actors.inFlightCount() === 0, 15000);
      expect(launch.mock.calls[1]![0].runnerSessionId).toBeUndefined();
      const invocations = fs.readFileSync(invocationLog, "utf8").trim().split("\n").map(line => JSON.parse(line));
      expect(invocations).toHaveLength(2); expect(invocations[1].argv).not.toContain("--resume");
      expect(fs.existsSync(pending)).toBe(false);
      expect(fs.readFileSync(archived!, "utf8")).toBe(contents);
      expect(backups(actor.sessionFile!)).toHaveLength(1);
    } finally { unavailable = false; if (previousLog === undefined) delete process.env.FAKE_CLAUDE_LOG; else process.env.FAKE_CLAUDE_LOG = previousLog; }
  }, 40000);

  it.each(["size", "requested"] as const)("#2479 R2 F6 blocks %s boundary continuation until the complete archive receipt is confirmed", async (trigger) => {
    const { actors, runs, hold } = setup({ maxSessionBytes: trigger === "size" ? 64 : 0 });
    const actor = await actors.create({ name: `receipt boundary ${trigger}`, instructions: "Work.", residency: "durable", transport: "process" });
    await actors.ask(actor.id, "complete transcript first");
    await waitFor(() => actors.inFlightCount() === 0);
    const prior = ["20000101T000000000Z", "20000102T000000000Z", "20000103T000000000Z"].map(stamp => `${actor.sessionFile}.${stamp}.bak`);
    for (const file of prior) fs.copyFileSync(actor.sessionFile!, file);
    let release: (() => void) | undefined;
    let active: Promise<unknown> | undefined;
    if (trigger === "requested") {
      release = hold();
      active = actors.ask(actor.id, "complete transcript second");
      await waitFor(() => runs.length === 2);
    }
    const namespace = atomic.syncPathNamespace, events: string[] = [];
    let unavailable = true, failures = 0, archived: string | undefined;
    const confirmation = vi.spyOn(atomic, "syncPathNamespace").mockImplementation((file, inode) => {
      if (file.startsWith(`${actor.sessionFile}.`) && file.endsWith(".bak")) {
        archived = file;
        if (unavailable) { failures++; throw Object.assign(new Error("archive receipt unavailable"), { code: "ENOENT" }); }
        namespace(file, inode); events.push("confirmed"); return;
      }
      namespace(file, inode);
    });
    const rename = fs.renameSync.bind(fs), rm = fs.rmSync.bind(fs), unlink = fs.unlinkSync.bind(fs);
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (trigger === "requested" && from === actor.sessionFile && String(to).endsWith(".bak")) {
        // Main's settled-run retention may prune before a reset receipt exists.
        // Seed the historical referents at the archive boundary being faulted.
        for (const file of prior) fs.copyFileSync(from, file);
        events.length = 0;
      }
      if (to === actor.sessionFile) events.push("replacement");
      rename(from, to);
    });
    vi.spyOn(fs, "rmSync").mockImplementation((file, options) => { if (prior.includes(String(file))) events.push("prune"); rm(file, options); });
    vi.spyOn(fs, "unlinkSync").mockImplementation(file => { if (prior.includes(String(file))) events.push("prune"); unlink(file); });
    const reset = trigger === "requested" ? actors.resetSession(actor.id).then(() => undefined, error => error) : undefined;
    actors.tell(actor.id, "queued boundary continuation");
    release?.();
    if (active) await active;
    let snapshot!: { exists: boolean; prior: boolean; runs: number; events: string[]; contents: string };
    try {
      await waitFor(() => failures > 0);
      if (reset) expect(await reset).toBeInstanceOf(Error);
      await new Promise(resolve => setTimeout(resolve, 150));
      snapshot = { exists: fs.existsSync(actor.sessionFile!), prior: prior.every(file => fs.existsSync(file)), runs: runs.length,
        events: [...events], contents: fs.readFileSync(archived!, "utf8") };
    } finally { unavailable = false; }
    await waitFor(() => runs.length === (trigger === "size" ? 2 : 3) && actors.inFlightCount() === 0);
    confirmation.mockRestore();
    expect(snapshot.exists).toBe(false);
    expect(snapshot.prior).toBe(true);
    expect(snapshot.runs).toBe(trigger === "size" ? 1 : 2);
    expect(snapshot.events).toEqual([]);
    expect(snapshot.contents).toContain("complete transcript first");
    if (trigger === "requested") expect(snapshot.contents).toContain("complete transcript second");
    expect(events[0]).toBe("confirmed");
    expect(events).toContain("replacement");
    expect(runs.at(-1)!.prior).toEqual([]);
  });

  it("#2479 R2 F6 retains the original archive obligation across immediate idle reset retry", async () => {
    const { actors, runs } = setup();
    const actor = await actors.create({ name: "receipt idle retry", instructions: "Work.", residency: "durable", transport: "process" });
    await actors.ask(actor.id, "complete idle transcript");
    await waitFor(() => actors.inFlightCount() === 0);
    const contents = fs.readFileSync(actor.sessionFile!, "utf8");
    const prior = ["20000101T000000000Z", "20000102T000000000Z", "20000103T000000000Z"].map(stamp => `${actor.sessionFile}.${stamp}.bak`);
    for (const file of prior) fs.copyFileSync(actor.sessionFile!, file);
    const namespace = atomic.syncPathNamespace, events: string[] = [];
    let unavailable = true, archived: string | undefined;
    const confirmation = vi.spyOn(atomic, "syncPathNamespace").mockImplementation((file, inode) => {
      if (file.startsWith(`${actor.sessionFile}.`) && file.endsWith(".bak")) {
        archived = file;
        if (unavailable) throw new Error("archive receipt unavailable");
        namespace(file, inode); events.push("confirmed"); return;
      }
      namespace(file, inode);
    });
    const rename = fs.renameSync.bind(fs), rm = fs.rmSync.bind(fs), unlink = fs.unlinkSync.bind(fs);
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => { if (to === actor.sessionFile) events.push("replacement"); rename(from, to); });
    vi.spyOn(fs, "rmSync").mockImplementation((file, options) => { if (prior.includes(String(file))) events.push("prune"); rm(file, options); });
    vi.spyOn(fs, "unlinkSync").mockImplementation(file => { if (prior.includes(String(file))) events.push("prune"); unlink(file); });
    let retry: unknown, snapshot!: { exists: boolean; prior: boolean; events: string[] };
    try {
      await expect(actors.resetSession(actor.id)).rejects.toThrow("archive receipt unavailable");
      retry = await actors.resetSession(actor.id).then(() => undefined, error => error);
      snapshot = { exists: fs.existsSync(actor.sessionFile!), prior: prior.every(file => fs.existsSync(file)), events: [...events] };
    } finally { unavailable = false; }
    await actors.resetSession(actor.id);
    confirmation.mockRestore();
    expect(retry).toBeInstanceOf(Error);
    expect(snapshot).toEqual({ exists: false, prior: true, events: [] });
    expect(fs.readFileSync(archived!, "utf8")).toBe(contents);
    expect(events[0]).toBe("confirmed");
    expect(events.indexOf("confirmed")).toBeLessThan(events.indexOf("replacement"));
    expect(events.indexOf("confirmed")).toBeLessThan(events.indexOf("prune"));
    expect(runs).toHaveLength(1);
    await actors.ask(actor.id, "after receipt recovery");
    expect(runs.at(-1)!.prior).toEqual([]);
  });
  it.each(["intent", "retirement"] as const)("#2479 R2 F6 retains archive debt through a failed journal %s barrier", async (fault) => {
    const { actors } = setup();
    const actor = await actors.create({ name: `receipt journal ${fault}`, instructions: "Work.", residency: "durable", transport: "process" });
    await actors.ask(actor.id, "complete journal transcript");
    await waitFor(() => actors.inFlightCount() === 0);
    const contents = fs.readFileSync(actor.sessionFile!, "utf8"), pending = `${actor.sessionFile}.archive-pending.json`;
    const write = atomic.writeJsonAtomic, namespace = atomic.syncDirectoryChain;
    let unavailable = true, archive: string | undefined;
    vi.spyOn(atomic, "writeJsonAtomic").mockImplementation((file, value, options) => {
      if (file === pending && unavailable && fault === "intent") throw new Error("intent barrier unavailable");
      write(file, value, options);
    });
    vi.spyOn(atomic, "syncDirectoryChain").mockImplementation(directory => {
      if (directory === path.dirname(actor.sessionFile!) && unavailable && fault === "retirement") throw new Error("retirement barrier unavailable");
      namespace(directory);
    });
    const rename = fs.renameSync.bind(fs);
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => { rename(from, to); if (from === actor.sessionFile) archive = String(to); });
    let retry: unknown;
    try {
      await expect(actors.resetSession(actor.id)).rejects.toThrow(`${fault} barrier unavailable`);
      // Both injections leave no journal name. The live obligation must survive.
      expect(fs.existsSync(pending)).toBe(false);
      retry = await actors.resetSession(actor.id).then(() => undefined, error => error);
      expect(retry).toBeInstanceOf(Error);
      if (fault === "intent") expect(fs.readFileSync(actor.sessionFile!, "utf8")).toBe(contents);
      else expect(fs.existsSync(actor.sessionFile!)).toBe(false);
    } finally { unavailable = false; }
    await actors.resetSession(actor.id);
    expect(fs.readFileSync(archive!, "utf8")).toBe(contents);
    expect(fs.existsSync(pending)).toBe(false);
    expect(sessionHeader(actor.sessionFile!)).toMatchObject({ type: "session", version: 3 });
  });

  it("#2479 R2 F6 refuses an archive inode substitute and retries the original inode", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "receipt inode retry", instructions: "Work.", residency: "durable", transport: "process" });
    await actors.ask(actor.id, "complete inode transcript");
    await waitFor(() => actors.inFlightCount() === 0);
    const contents = fs.readFileSync(actor.sessionFile!, "utf8");
    const namespace = atomic.syncPathNamespace;
    let archive: string | undefined;
    const confirmation = vi.spyOn(atomic, "syncPathNamespace").mockImplementation((file, inode) => {
      if (file.startsWith(`${actor.sessionFile}.`) && file.endsWith(".bak")) { archive = file; throw new Error("archive receipt unavailable"); }
      namespace(file, inode);
    });
    await expect(actors.resetSession(actor.id)).rejects.toThrow("archive receipt unavailable");
    confirmation.mockRestore();
    const held = `${archive}.held`;
    fs.renameSync(archive!, held);
    fs.writeFileSync(archive!, contents); // Identical visible bytes, a different inode.
    try {
      await expect(actors.resetSession(actor.id)).rejects.toThrow("archive inode changed");
      expect(fs.existsSync(actor.sessionFile!)).toBe(false);
      expect(fs.existsSync(`${actor.sessionFile}.archive-pending.json`)).toBe(true);
    } finally { fs.rmSync(archive!); fs.renameSync(held, archive!); }
    await actors.resetSession(actor.id);
    expect(fs.readFileSync(archive!, "utf8")).toBe(contents);
    expect(fs.existsSync(`${actor.sessionFile}.archive-pending.json`)).toBe(false);
  });

  it("#2479 R3 F6 does not treat ENOENT from archive confirmation as an absent source", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "archive missing namespace", instructions: "Work.", residency: "durable" });
    await actors.ask(actor.id, "first");
    const prior = ["20000101T000000000Z", "20000102T000000000Z", "20000103T000000000Z"].map(stamp => `${actor.sessionFile}.${stamp}.bak`);
    for (const file of prior) fs.copyFileSync(actor.sessionFile!, file);
    const namespace = atomic.syncPathNamespace;
    const confirmation = vi.spyOn(atomic, "syncPathNamespace").mockImplementation((file, inode) => {
      if (file.startsWith(`${actor.sessionFile}.`) && file.endsWith(".bak")) throw Object.assign(new Error("archive namespace disappeared"), { code: "ENOENT" });
      namespace(file, inode);
    });
    try {
      await expect(actors.resetSession(actor.id)).rejects.toThrow("archive namespace disappeared");
      expect(fs.existsSync(actor.sessionFile!)).toBe(false);
      expect(prior.every(file => fs.existsSync(file))).toBe(true);
    } finally { confirmation.mockRestore(); }
  });
  it.each(["reset", "repair"] as const)("#2479 R3 F6 confirms complete preserved session contents before %s replacement or pruning", async (kind) => {
    const { actors } = setup();
    const actor = await actors.create({ name: `archive ${kind}`, instructions: "Work.", residency: "durable", transport: "process" });
    await actors.ask(actor.id, "first");
    if (kind === "repair") fs.writeFileSync(actor.sessionFile!, "headerless latest append\n");
    const contents = fs.readFileSync(actor.sessionFile!, "utf8"), events: string[] = [], descriptors = new Map<number, string>();
    const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs), rename = fs.renameSync.bind(fs), rm = fs.rmSync.bind(fs);
    vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); descriptors.set(fd, String(file)); return fd; });
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => { events.push(`sync:${descriptors.get(fd)}`); sync(fd); });
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => { events.push(`rename:${to}`); rename(from, to); });
    vi.spyOn(fs, "rmSync").mockImplementation((file, options) => { if (String(file).endsWith(".bak")) events.push(`prune:${file}`); rm(file, options); });
    if (kind === "reset") await actors.resetSession(actor.id);
    else await actors.ask(actor.id, "recover");
    const archiveEvent = events.find(event => event.startsWith(`rename:${actor.sessionFile}.`) && event.endsWith(".bak"))!;
    expect(archiveEvent).toBeDefined();
    const archive = archiveEvent.slice("rename:".length), archiveAt = events.indexOf(archiveEvent), headerAt = events.indexOf(`rename:${actor.sessionFile}`);
    const fileBarrier = events.findIndex(event => event === `sync:${actor.sessionFile}` || event === `sync:${archive}`);
    expect(fileBarrier).toBeGreaterThanOrEqual(0);
    expect(fileBarrier).toBeLessThan(headerAt);
    const pruneAt = events.findIndex(event => event.startsWith("prune:"));
    if (pruneAt >= 0) expect(fileBarrier).toBeLessThan(pruneAt);
    if (process.platform !== "win32") expect(events.slice(archiveAt + 1, headerAt)).toContain(`sync:${path.dirname(actor.sessionFile!)}`);
    expect(fs.readFileSync(archive, "utf8")).toBe(contents);
  });
  it("#2479 syncs the archived namespace before publishing a replacement session header", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "archive audit", instructions: "Work.", residency: "durable" });
    await actors.ask(actor.id, "first");
    const events: string[] = [], descriptors = new Map<number, string>();
    const open = fs.openSync.bind(fs), sync = fs.fsyncSync.bind(fs), rename = fs.renameSync.bind(fs);
    const opened = vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => { const fd = open(file, flags, mode); descriptors.set(fd, String(file)); return fd; });
    const synced = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => { events.push(`sync:${descriptors.get(fd)}`); sync(fd); });
    const renamed = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => { events.push(`rename:${to}`); rename(from, to); });
    const writer = vi.spyOn(atomic, "writeJsonAtomic");
    try {
      await actors.resetSession(actor.id);
      const archive = events.findIndex(event => event.startsWith(`rename:${actor.sessionFile}.`) && event.endsWith(".bak"));
      const header = events.indexOf(`rename:${actor.sessionFile}`);
      expect(archive).toBeGreaterThanOrEqual(0);
      if (process.platform !== "win32") expect(events.slice(archive + 1, header)).toContain(`sync:${path.dirname(actor.sessionFile!)}`);
      expect(writer.mock.calls.find(([file]) => file === actor.sessionFile)?.[2]?.durable).toBe(true);
    } finally { opened.mockRestore(); synced.mockRestore(); renamed.mockRestore(); writer.mockRestore(); }
  });
  it("defers rotation during an in-flight run, preserves its appends, and atomically seeds the next session", async () => {
    const { actors, runs, hold } = setup();
    const actor = await actors.create({ name: "durable", instructions: "Work.", residency: "durable", transport: "process" });
    await actors.ask(actor.id, "first");
    const originalHeader = sessionHeader(actor.sessionFile!);
    const release = hold();
    const inFlight = actors.ask(actor.id, "in-flight append");
    await waitFor(() => runs.length === 2);
    let settled = false;
    const reset = actors.resetSession(actor.id).then(() => { settled = true; });
    try {
      await new Promise((resolve) => setTimeout(resolve, 30));
      expect(settled).toBe(false);
      expect(backups(actor.sessionFile!)).toEqual([]);
      expect(sessionHeader(actor.sessionFile!)).toEqual(originalHeader);
    } finally { release(); }
    await inFlight;
    await reset;
    const [backup] = backups(actor.sessionFile!);
    const archived = path.join(path.dirname(actor.sessionFile!), backup!);
    expect(sessionHeader(archived)).toEqual(originalHeader);
    expect(fs.readFileSync(archived, "utf8")).toContain("in-flight append");
    expect(sessionHeader(actor.sessionFile!)).toMatchObject({ type: "session", version: 3 });
    expect(sessionHeader(actor.sessionFile!).id).not.toBe(originalHeader.id);
    await expect(actors.ask(actor.id, "next activation")).resolves.toMatchObject({ direction: "out" });
  });

  it("defers a rotation requested while the drain is launching at a size-reset boundary", async () => {
    const { actors, agents, mesh } = setup({ maxSessionBytes: 64 });
    const actor = await actors.create({ name: "admission", instructions: "Work." });
    await actors.ask(actor.id, "first");
    await waitFor(() => actors.inFlightCount() === 0);
    let release!: () => void;
    let entered!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const ready = new Promise<void>((resolve) => { entered = resolve; });
    const put = mesh.put.bind(mesh);
    let blocked = false;
    const spy = vi.spyOn(mesh, "put").mockImplementation(async (request) => {
      if (!blocked && request.key === `actors/test/${actor.id}`) { blocked = true; entered(); await gate; }
      return put(request);
    });
    const next = actors.ask(actor.id, "launching append");
    await ready;
    let settled = false;
    const reset = actors.resetSession(actor.id).then(() => { settled = true; });
    let settledAtGate = false;
    let resetsAtGate = 0;
    try {
      await new Promise((resolve) => setTimeout(resolve, 30));
      settledAtGate = settled;
      resetsAtGate = resets(actors, actor.id).length;
    } finally { release(); spy.mockRestore(); }
    // Even a failing assertion on the base must first settle the work the test launched.
    await next;
    await reset;
    expect(settledAtGate).toBe(false);
    expect(resetsAtGate).toBe(1);
    expect(resets(actors, actor.id)).toHaveLength(2);
    expect(agents.list().filter((run) => run.status === "running")).toEqual([]);
    expect(backups(actor.sessionFile!).some((name) => fs.readFileSync(path.join(path.dirname(actor.sessionFile!), name), "utf8").includes("launching append"))).toBe(true);
  });

  it.each(["headerless", "invalid-json", "invalid-header"])("repairs a %s session at activation and preserves the orphan", async (malformed) => {
    const { actors } = setup();
    const actor = await actors.create({ name: "repair", instructions: "Work.", residency: "durable", transport: "process" });
    const orphan = malformed === "headerless" ? '{"type":"message","id":"orphan","message":{"role":"user","content":"kept event"}}\n' : malformed === "invalid-json" ? 'partial-json\n' : '{"type":"session","id":null}\n';
    fs.mkdirSync(path.dirname(actor.sessionFile!), { recursive: true });
    fs.writeFileSync(actor.sessionFile!, orphan);
    await expect(actors.ask(actor.id, "recover activation")).resolves.toMatchObject({ direction: "out" });
    expect(sessionHeader(actor.sessionFile!)).toMatchObject({ type: "session", version: 3, id: expect.any(String) });
    const kept = backups(actor.sessionFile!).filter((name) => name.includes("orphan-noheader"));
    expect(kept).toHaveLength(1);
    const backup = path.join(path.dirname(actor.sessionFile!), kept[0]!);
    expect(fs.readFileSync(backup, "utf8")).toBe(orphan);
    for (const word of ["next", "another", "third"]) {
      await actors.ask(actor.id, word);
      await actors.resetSession(actor.id);
    }
    expect(fs.readFileSync(backup, "utf8")).toBe(orphan); // ordinary pruning cannot lose the orphan
  });

  it("commits each new header by temp-file rename before any native writer launches", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "atomic", instructions: "Work." });
    const rename = fs.renameSync.bind(fs);
    const publications: string[] = [];
    const spy = vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      if (target === actor.sessionFile) {
        expect(String(source)).toMatch(/\.tmp$/);
        expect(sessionHeader(String(source))).toMatchObject({ type: "session", version: 3, cwd: process.cwd() });
        expect(fs.existsSync(actor.sessionFile!)).toBe(false);
        publications.push(String(source));
      }
      return rename(source, target);
    });
    try {
      await actors.ask(actor.id, "initial append");
      await actors.resetSession(actor.id);
      await actors.ask(actor.id, "append after rotation");
      expect(publications).toHaveLength(2);
      expect(fs.readdirSync(path.dirname(actor.sessionFile!)).filter((name) => name.endsWith(".tmp"))).toEqual([]);
    } finally { spy.mockRestore(); }
  });

  it("fails loudly if repair cannot preserve the malformed file", async () => {
    const { actors, mesh } = setup();
    const actor = await actors.create({ name: "repair-error", instructions: "Work." });
    const orphan = '{"type":"message","id":"orphan"}\n';
    fs.mkdirSync(path.dirname(actor.sessionFile!), { recursive: true });
    fs.writeFileSync(actor.sessionFile!, orphan);
    const rename = fs.renameSync.bind(fs);
    const spy = vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      if (source === actor.sessionFile) throw Object.assign(new Error("cannot preserve orphan"), { code: "EACCES" });
      return rename(source, target);
    });
    try {
      await expect(actors.ask(actor.id, "repair")).rejects.toThrow("cannot preserve orphan");
      expect(fs.readFileSync(actor.sessionFile!, "utf8")).toBe(orphan);
      await waitFor(() => mesh.read({ topic: "ops.owner" }).some((event) => event.kind === "actor.session.error"));
      expect(mesh.read({ topic: "ops.owner" }).filter((event) => event.kind === "actor.session.error")).toHaveLength(1);
    } finally { spy.mockRestore(); }
  });

  it.each(["halt", "close"] as const)("keeps a repair alarm passive after %s interrupts model resolution", async (operation) => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    let holdResolution = false;
    let resolutionHeld = false;
    const { actors, agents, mesh, deliveries, sendMessage } = setup({
      resolvePiModel: (model) => {
        // Arm after creation: unpinned owner defaults resolve at drain admission,
        // not enqueue. Hold that resolution without depending on a call count.
        if (holdResolution) {
          resolutionHeld = true;
          return gate.then(() => model);
        }
        return model;
      },
    });
    const actor = await actors.create({ name: "interrupted-repair", instructions: "Work.", model: "test/actor", delivery: "mailbox", transport: "process" });
    const orphan = '{"type":"message","id":"orphan"}\n';
    fs.mkdirSync(path.dirname(actor.sessionFile!), { recursive: true });
    fs.writeFileSync(actor.sessionFile!, orphan);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    // Attach the rejection handler before interrupting; settle all started work even on failure.
    holdResolution = true;
    const activation = actors.ask(actor.id, "cancelled activation").then(() => undefined, (error: Error) => error);
    let closing: Promise<void> | undefined;
    try {
      await waitFor(() => resolutionHeld);
      expect(actors.inFlightCount()).toBe(1);
      expect(launch).not.toHaveBeenCalled();
      if (operation === "halt") expect(actors.haltAll()).toEqual({ halted: 1 });
      else closing = actors.close();
    } finally {
      release();
      await activation;
      if (closing) await closing;
      await waitFor(() => actors.inFlightCount() === 0);
    }
    expect(await activation).toBeInstanceOf(Error);
    if (operation === "halt") expect(actors.halted).toBe(true);
    const alarms = mesh.read({ topic: "ops.owner" }).filter((event) => event.kind === "actor.session.repaired");
    expect(alarms).toHaveLength(1);
    expect(alarms[0]).toMatchObject({ data: { actorId: actor.id, archived: expect.stringContaining("orphan-noheader") } });
    const evidence = actors.messages(actor.id, 50).filter((message) => message.text?.includes("session repaired"));
    expect(evidence).toHaveLength(1);
    expect(evidence[0]).toMatchObject({ source: "fabric-host", data: alarms[0]!.data });
    const archived = (alarms[0]!.data as { archived: string }).archived;
    expect(fs.readFileSync(archived, "utf8")).toBe(orphan);
    expect(sessionHeader(actor.sessionFile!)).toMatchObject({ type: "session", version: 3 });
    expect(launch).not.toHaveBeenCalled();
    expect(agents.list()).toEqual([]);
    expect(sendMessage).not.toHaveBeenCalled(); // No Pi delivery can start a new Main turn.
    expect(deliveries).toEqual([]);
  });

  it("publishes the repair alarm once on ops.owner, including for a silent mailbox actor", async () => {
    const { actors, mesh, deliveries, sendMessage } = setup();
    const actor = await actors.create({ name: "alarm", instructions: "Work.", delivery: "mailbox" });
    fs.mkdirSync(path.dirname(actor.sessionFile!), { recursive: true });
    fs.writeFileSync(actor.sessionFile!, '{"type":"message","id":"orphan"}\n');
    await actors.ask(actor.id, "repair").catch(() => undefined);
    await actors.ask(actor.id, "healthy").catch(() => undefined);
    await waitFor(() => actors.inFlightCount() === 0);
    const alarms = mesh.read({ topic: "ops.owner" }).filter((event) => event.kind === "actor.session.repaired");
    expect(alarms).toHaveLength(1);
    expect(alarms[0]).toMatchObject({ from: { id: "session:test" }, data: { actorId: actor.id, archived: expect.stringContaining("orphan-noheader") } });
    expect(deliveries.filter((text) => text.includes("session repaired"))).toHaveLength(1);
    expect(sendMessage).toHaveBeenCalledTimes(1);
    expect(sendMessage.mock.calls[0]?.[1]).toMatchObject({ deliverAs: "followUp", triggerTurn: true });
  });
});

describe("actor session reset (smarty-dev#1439)", () => {
  it("resets an idle actor: the next run has no prior context and keeps the definition and mailbox", async () => {
    const { actors, runs } = setup();
    const actor = await actors.create({ name: "keeper", instructions: "Remember words.", topics: ["words"] });
    await actors.ask(actor.id, "REMEMBER PELICAN");
    await actors.ask(actor.id, "RECALL");
    expect(runs[1]!.prior.length).toBeGreaterThan(0);
    const before = actors.definition(actor.id);
    const messagesBefore = actors.messages(actor.id, 50).length;

    // agents.resetSession({ id: "keeper" }) forwards the name unchanged to
    // this owning-host path; it must not need a UUID or a global-template reset.
    const info = await actors.resetSession("keeper");
    expect(info).toMatchObject({ id: actor.id, status: "idle" });
    expect(sessionHeader(actor.sessionFile!)).toMatchObject({ type: "session", version: 3 });
    expect(backups(actor.sessionFile!)).toHaveLength(1);
    expect(actors.definition(actor.id)).toEqual(before);
    const [logged] = resets(actors, actor.id);
    expect(logged).toMatchObject({ direction: "out", source: "fabric-host", reason: "session reset (requested)" });
    expect(logged!.data).toMatchObject({ sessionReset: { trigger: "requested", archived: expect.stringMatching(/session\.jsonl\.\d{8}T\d{9}Z\.bak$/) } });
    expect(actors.messages(actor.id, 50)).toHaveLength(messagesBefore + 1);

    await actors.ask(actor.id, "RECALL");
    expect(runs[2]!.prior).toEqual([]);
  });

  it("terminal stop cancels a pending reset without rotating or resuming the actor", async () => {
    const { actors, runs, hold } = setup();
    const actor = await actors.create({ name: "cancel repair", instructions: "Work." });
    await actors.ask(actor.id, "first");
    const header = sessionHeader(actor.sessionFile!);
    const release = hold();
    const active = actors.ask(actor.id, "held activation").catch(error => error);
    await waitFor(() => runs.length === 2);
    const reset = actors.resetSession(actor.id).catch(error => error);
    try {
      await actors.stop(actor.id);
      expect(await reset).toMatchObject({ name: "ActorSessionResetCancelledError", code: "ACTOR_SESSION_RESET_CANCELLED", id: actor.id });
      expect(backups(actor.sessionFile!)).toEqual([]);
      expect(sessionHeader(actor.sessionFile!)).toEqual(header);
    } finally { release(); await active; }
    expect(resets(actors, actor.id)).toEqual([]);
    expect(actors.status(actor.id).status).toBe("stopped");
    expect(() => actors.ask(actor.id, "still stopped")).toThrow(/stopped/);
  });

  it("waits for a run in flight: it finishes on the old session, queued work runs on the new one", async () => {
    const { actors, runs, hold } = setup();
    const actor = await actors.create({ name: "busy", instructions: "Work." });
    await actors.ask(actor.id, "first");
    const release = hold();
    const inFlight = actors.ask(actor.id, "second");
    await waitFor(() => runs.length === 2);
    const queued = actors.ask(actor.id, "third");
    let settled = false;
    const reset = actors.resetSession(actor.id).then((info) => { settled = true; return info; });
    await new Promise((resolve) => setTimeout(resolve, 50));
    // Not interrupted, not yet reset.
    expect(settled).toBe(false);
    expect(runs).toHaveLength(2);
    expect(fs.existsSync(actor.sessionFile!)).toBe(true);
    release();
    await expect(inFlight).resolves.toMatchObject({ direction: "out" });
    await expect(reset).resolves.toMatchObject({ id: actor.id });
    await queued;
    expect(runs[1]!.prior.length).toBeGreaterThan(0);
    expect(runs[2]).toMatchObject({ prior: [] });
    expect(runs[2]!.task).toContain("third");
    // The archive holds both runs of the old session.
    const [backup] = backups(actor.sessionFile!);
    const archived = fs.readFileSync(path.join(path.dirname(actor.sessionFile!), backup!), "utf8");
    expect(archived).toContain("first");
    expect(archived).toContain("second");
    expect(fs.readFileSync(actor.sessionFile!, "utf8")).not.toContain("second");
  });

  it("resets a session past actors.maxSessionBytes at the next run boundary, and 0 disables it", async () => {
    const { actors, runs } = setup({ maxSessionBytes: 64 });
    const actor = await actors.create({ name: "growing", instructions: "Grow." });
    await actors.ask(actor.id, "one");
    const size = fs.statSync(actor.sessionFile!).size;
    expect(size).toBeGreaterThan(64);
    await actors.ask(actor.id, "two");
    expect(runs[1]!.prior).toEqual([]);
    const [logged] = resets(actors, actor.id);
    expect(logged).toMatchObject({ source: "fabric-host", data: { sessionReset: { trigger: "size", bytes: size } } });
    expect(logged!.reason).toMatch(/^session reset \(size limit\)/);

    const disabled = setup({ maxSessionBytes: 0 });
    const other = await disabled.actors.create({ name: "unbounded", instructions: "Grow." });
    await disabled.actors.ask(other.id, "one");
    await disabled.actors.ask(other.id, "two");
    expect(disabled.runs[1]!.prior.length).toBeGreaterThan(0);
    expect(resets(disabled.actors, other.id)).toEqual([]);
  });

  it("leaves a session at or under the limit alone", async () => {
    const { actors, runs } = setup({ maxSessionBytes: 10_000 });
    const actor = await actors.create({ name: "small", instructions: "Stay small." });
    await actors.ask(actor.id, "one");
    await actors.ask(actor.id, "two");
    expect(runs[1]!.prior.length).toBeGreaterThan(0);
    expect(backups(actor.sessionFile!)).toEqual([]);
  });

  it("keeps only the newest backup", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "churn", instructions: "Churn." });
    const archived: string[] = [];
    for (const word of ["a", "b", "c", "d"]) {
      await actors.ask(actor.id, word);
      const info = await actors.resetSession(actor.id);
      expect(info.id).toBe(actor.id);
      const data = resets(actors, actor.id).at(-1)!.data as { sessionReset: { archived: string } };
      archived.push(path.basename(data.sessionReset.archived));
    }
    expect(new Set(archived).size).toBe(4);
    expect(backups(actor.sessionFile!)).toEqual(archived.slice(-1).sort());
    // A reset without a session file archives nothing, but creates its header atomically.
    fs.rmSync(actor.sessionFile!);
    await actors.resetSession(actor.id);
    expect(resets(actors, actor.id).at(-1)!.data).toMatchObject({ sessionReset: { archived: null, bytes: 0 } });
    expect(backups(actor.sessionFile!)).toEqual(archived.slice(-1).sort());
  });

  it("orders same-millisecond backups by their suffix when it prunes", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "fast", instructions: "Churn." });
    const dir = path.dirname(actor.sessionFile!);
    fs.mkdirSync(dir, { recursive: true });
    const stamp = "20260927T150000000Z";
    for (const name of [`session.jsonl.${stamp}.bak`, `session.jsonl.${stamp}-1.bak`]) fs.writeFileSync(path.join(dir, name), "{}\n");
    fs.writeFileSync(actor.sessionFile!, JSON.stringify({ type: "session", version: 3, id: "valid", timestamp: new Date().toISOString(), cwd: process.cwd() }) + "\n");
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(new Date("2026-09-27T15:00:00.000Z"));
    try { await actors.resetSession(actor.id); } finally { vi.useRealTimers(); }
    expect(backups(actor.sessionFile!)).toEqual([`session.jsonl.${stamp}-2.bak`]);
    expect(backups(actor.sessionFile!)).not.toContain(`session.jsonl.${stamp}-1.bak`);
    expect(backups(actor.sessionFile!)).not.toContain(`session.jsonl.${stamp}.bak`);
  });

  // review/astra F1 on #101: a pruned name was reused, sorted oldest and deleted at once.
  it("keeps the newest backup when every reset lands in the same millisecond", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "frozen", instructions: "Churn." });
    const archived: string[] = [];
    for (const word of ["alpha", "bravo", "charlie", "delta"]) {
      await actors.ask(actor.id, word);
      vi.useFakeTimers({ toFake: ["Date"] });
      vi.setSystemTime(new Date("2026-09-27T15:00:00.000Z"));
      try {
        await actors.resetSession(actor.id);
      } finally {
        vi.useRealTimers();
      }
      const data = resets(actors, actor.id).at(-1)!.data as { sessionReset: { archived: string } };
      expect(fs.existsSync(data.sessionReset.archived)).toBe(true);
      expect(fs.readFileSync(data.sessionReset.archived, "utf8")).toContain(word);
      archived.push(path.basename(data.sessionReset.archived));
    }
    expect(new Set(archived).size).toBe(4);
    const kept = backups(actor.sessionFile!);
    expect(kept).toHaveLength(1);
    expect(kept.sort()).toEqual(archived.slice(-1).sort());
    const dir = path.dirname(actor.sessionFile!);
    const contents = kept.map((name) => fs.readFileSync(path.join(dir, name), "utf8")).join("\n");
    expect(contents).not.toContain("charlie");
    expect(contents).toContain("delta");
    expect(contents).not.toContain("bravo");
  });

  it("rejects a reset of an actor another host owns", async () => {
    let owns = true;
    const { actors } = setup({ canManageActor: () => owns });
    const actor = await actors.create({ name: "leased", instructions: "Observe." });
    await actors.ask(actor.id, "one");
    owns = false;
    await expect(actors.resetSession("leased")).rejects.toThrow("owned by another host");
    expect(fs.existsSync(actor.sessionFile!)).toBe(true);
    expect(backups(actor.sessionFile!)).toEqual([]);
  });

  it("reads actors.maxSessionBytes from config, 20 MiB by default", () => {
    expect(DEFAULT_FABRIC_CONFIG.actors.maxSessionBytes).toBe(20 * 1024 * 1024);
    expect(normalizeFabricConfig({ actors: { maxSessionBytes: 0 } }).actors.maxSessionBytes).toBe(0);
    expect(normalizeFabricConfig({ actors: { maxSessionBytes: 1234.7 } }).actors.maxSessionBytes).toBe(1234);
    expect(normalizeFabricConfig({ actors: { maxSessionBytes: -5 } }).actors.maxSessionBytes).toBe(0);
    expect(normalizeFabricConfig({}).actors.maxSessionBytes).toBe(20 * 1024 * 1024);
  });
});
