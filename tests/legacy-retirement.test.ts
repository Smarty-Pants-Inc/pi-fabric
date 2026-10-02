import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import { legacyRetirementDiagnostic } from "../src/residency/legacy-retirement.js";
import { residentHostId, type ResidentHostConfig, type ResidentHostOwner } from "../src/residency/protocol.js";
import type { FabricParticipantSource } from "../src/topology/types.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";

const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); roots.splice(0).forEach(root => fs.rmSync(root, { recursive: true, force: true })); });
const fixture = (pid = process.pid) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-retirement-")); roots.push(root);
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:retirement", sessionId: "retirement", cwd: root, projectRoot: root,
    meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
    fullCodeMode: false, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: "unused", fabricExtensionPath: "new-release",
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  for (const dir of [config.residencyRoot, config.actorRoot, ...["requests", "processing", "agents"].map(dir => path.join(config.residencyRoot, dir))]) fs.mkdirSync(dir, { recursive: true });
  const owner: ResidentHostOwner = { format: 1, hostId: residentHostId(config.rootId), pid, token: "legacy-token", startedAt: Date.now(), readyAt: Date.now() };
  fs.writeFileSync(path.join(config.residencyRoot, "owner.json"), JSON.stringify(owner));
  const participants = { list: () => [], self: () => ({ kind: "root", id: config.rootId, sessionId: config.sessionId, ownerHostId: config.rootId }) } as unknown as FabricParticipantSource;
  const client = new ResidencyClient({ config, mesh: new MeshStore(config.meshRoot, 65536, 100), participants,
    mainAgent: { id: config.rootId, local: true } as FabricMainAgentTarget, startupTimeoutMs: 1 });
  return { config, owner, participants, client };
};

const deferred = async (f: ReturnType<typeof fixture>) => {
  const before = fs.readFileSync(path.join(f.config.residencyRoot, "owner.json"), "utf8");
  const kill = vi.spyOn(process, "kill");
  await f.client.reconcileRelease();
  expect(JSON.parse(fs.readFileSync(path.join(f.config.residencyRoot, "handover-deferred.json"), "utf8")).reason)
    .toBe(legacyRetirementDiagnostic);
  expect(kill.mock.calls.filter(([, signal]) => signal !== 0)).toEqual([]);
  expect(fs.readFileSync(path.join(f.config.residencyRoot, "owner.json"), "utf8")).toBe(before);
  expect(fs.existsSync(path.join(f.config.residencyRoot, "retirement.lock"))).toBe(false);
  kill.mockRestore();
};

describe("legacy retirement installer-drain deferral", () => {
  it.each(["running", "queued", "in-flight", "durable queue", "unknown queue"])("defers a %s actor behind an idle participant without suspending or sampling", async kind => {
    const f = fixture();
    const actor = { id: "actor", rootId: f.config.rootId, residency: "durable", status: kind === "running" || kind === "queued" ? kind : "idle",
      ...(kind === "in-flight" ? { inFlightRun: { id: "run" } } : {}) };
    fs.mkdirSync(path.join(f.config.actorRoot, actor.id));
    fs.writeFileSync(path.join(f.config.actorRoot, "actors.json"), JSON.stringify({ actors: [actor] }));
    if (kind.includes("queue")) fs.writeFileSync(path.join(f.config.actorRoot, actor.id, "queue-legacy.json"), kind === "unknown queue" ? "{" : JSON.stringify({ items: [{ id: "accepted-event" }] }));
    const sample = vi.spyOn(f.participants, "list").mockReturnValue([{ id: actor.id, ownerHostId: f.owner.hostId, status: "idle" }] as ReturnType<FabricParticipantSource["list"]>);
    try { await deferred(f); expect(sample).not.toHaveBeenCalled(); }
    finally { await f.client.close(); }
  });

  it("defers even an apparently idle legacy owner instead of suspending and killing it", async () => {
    const f = fixture();
    try { await deferred(f); await expect(f.client.ensureHost()).resolves.toMatchObject(f.owner); }
    finally { await f.client.close(); }
  });

  it("defers an orphaned owner pointing to an unrelated live process without signalling", async () => {
    const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { stdio: "ignore" });
    const exited = new Promise<void>(resolve => child.once("close", () => resolve()));
    const f = fixture(child.pid!);
    try { await deferred(f); expect(child.exitCode).toBeNull(); }
    finally { await f.client.close(); child.kill(); await exited; }
  });
});

// These formerly tested per-signal /proc authentication. None of these samples
// supplies missing whole-attempt custody/exit receipts, so no signal path exists.
describe("legacy evidence cannot authorize retirement", () => {
  it.each(["start time", "command line", "owner token", "environment", "missing launch", "parent command"])("defers %s mismatch to installer drain without process authentication", async kind => {
    const f = fixture();
    fs.writeFileSync(path.join(f.config.residencyRoot, "host.lock"), JSON.stringify({ pid: f.owner.pid, token: kind === "owner token" ? "different" : f.owner.token }));
    fs.writeFileSync(path.join(f.config.residencyRoot, "launcher.log"), JSON.stringify({ evidence: kind, completeExitReceipts: false }));
    const list = vi.spyOn(f.participants, "list");
    try { await deferred(f); expect(list).not.toHaveBeenCalled(); }
    finally { await f.client.close(); }
  });
});
