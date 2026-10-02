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
import { ResidentHost } from "../src/residency/host.js";
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
  it.each(["unacknowledged", "recent", "request", "processing", "live", "pending"])("keeps %s references", (kind) => {
    const dir = root(); const command = seed(dir, kind === "recent" ? now : old, kind !== "unacknowledged");
    if (kind === "request" || kind === "processing") write(dir, kind === "request" ? "requests" : kind, command.requestId, command);
    if (kind === "pending") write(dir, "acknowledgements", command.requestId, { format: 1, requestFormat: 3, requestId: command.requestId, completedAt: old, acknowledgedAt: old, pending: "entity" });
    sweep(dir, now, new Set(kind === "live" || kind === "pending" ? ["entity"] : []));
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
