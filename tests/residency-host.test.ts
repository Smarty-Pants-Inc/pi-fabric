import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { execFileSync, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { projectOf, repositoryOf } from "../src/topology/project-identity.js";
import { residentDeliveryPrefix } from "../src/residency/protocol.js";
import type { FabricParticipantRecord } from "../src/topology/types.js";
import { removeParticipantFileIf, writeParticipantFile } from "../src/topology/participant-files.js";
import { LIVENESS_POLICY_KEY } from "../src/topology/host-leases.js";
import { lockFile } from "../src/residency/file-lock.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ResidentHost, sweepResidentRuns } from "../src/residency/host.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentRequestRetention } from "../src/residency/retention.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { RESIDENT_HOST_FORMAT, residentResultPath, type ResidentHostConfig } from "../src/residency/protocol.js";
import { processStartTime, residentProcessAlive } from "../src/residency/process-identity.js";
import { residentWatchdogAlarmPath, latchResidentWatchdogAlarm, RESIDENT_WATCHDOG_BLOCKED } from "../src/residency/watchdog-admission.js";

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-host-review-"));
  const config: ResidentHostConfig = {
    format: RESIDENT_HOST_FORMAT, rootId: "session:review", sessionId: "review",
    cwd: process.cwd(), projectRoot: process.cwd(), meshRoot: path.join(root, "mesh"),
    actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
    fullCodeMode: true, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: DEFAULT_FABRIC_CONFIG.mesh,
    retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("dist/agents/worker.js"),
    fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
  };
  fs.mkdirSync(config.residencyRoot);
  const configPath = path.join(config.residencyRoot, "config.json");
  fs.writeFileSync(configPath, JSON.stringify(config));
  const idle = vi.fn();
  const host = new ResidentHost(config, idle);
  return { root, config, host, idle };
};

describe("resident watchdog fail-closed admission (F1)", () => {
  it.skipIf(process.platform !== "linux")("rechecks an alarm latched during asynchronous fence acquisition before restoring actors", async () => {
    const { root, config, host } = fixture();
    // lockFile yields before its native flock helper settles; latch during that
    // yield to exercise the final admission check, not only the entry precheck.
    const starting = host.start();
    latchResidentWatchdogAlarm(config.residencyRoot, { reason: "stale-lease" });
    try {
      await expect(starting).rejects.toThrow(RESIDENT_WATCHDOG_BLOCKED);
      expect(host.actors).toBeUndefined();
      expect(fs.existsSync(path.join(config.residencyRoot, "owner.json"))).toBe(false);
      expect(fs.existsSync(path.join(config.meshRoot, "host-leases"))).toBe(false);
      expect(fs.existsSync(residentWatchdogAlarmPath(config.residencyRoot))).toBe(true);
      // The rejection releases the fence rather than leaving a live lock helper.
      const fd = await lockFile(path.join(config.residencyRoot, "host.lock"), 0, true);
      fs.closeSync(fd);
    } finally { await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.each(["latched", "torn", "directory"] as const)("blocks client and host startup with a %s alarm, without renewing the lease", async mode => {
    const { root, config, host } = fixture();
    const mesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
    const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: "caller", rootId: config.rootId, identity: { id: "caller", name: "caller", kind: "main" } });
    const client = new ResidencyClient({ config, mesh, participants, mainAgent: { local: false } as FabricMainAgentTarget });
    const alarmPath = residentWatchdogAlarmPath(config.residencyRoot);
    if (mode === "latched") latchResidentWatchdogAlarm(config.residencyRoot, { reason: "stale-lease" });
    else if (mode === "torn") fs.writeFileSync(alarmPath, "{");
    else fs.mkdirSync(alarmPath);
    const errorPath = path.join(config.residencyRoot, "error.json");
    fs.writeFileSync(errorPath, '{"error":"prior alarm must survive"}');
    const alarmBefore = mode === "directory" ? undefined : fs.readFileSync(alarmPath, "utf8");
    try {
      await expect(client.ensureHost()).rejects.toThrow(RESIDENT_WATCHDOG_BLOCKED);
      await expect(client.ensureHost()).rejects.toThrow(RESIDENT_WATCHDOG_BLOCKED); // client retry cannot clear debt
      await expect(host.start()).rejects.toThrow(RESIDENT_WATCHDOG_BLOCKED); // direct host cannot bypass launcher
      expect(host.actors).toBeUndefined(); // persisted actor work was not restored
      expect(fs.existsSync(path.join(config.residencyRoot, "owner.json"))).toBe(false);
      expect(fs.existsSync(path.join(config.residencyRoot, "launcher.log"))).toBe(false);
      expect(fs.existsSync(path.join(config.residencyRoot, "host.lock"))).toBe(false);
      expect(fs.existsSync(path.join(config.meshRoot, "host-leases"))).toBe(false);
      expect(fs.readFileSync(errorPath, "utf8")).toBe('{"error":"prior alarm must survive"}');
      if (alarmBefore !== undefined) expect(fs.readFileSync(alarmPath, "utf8")).toBe(alarmBefore);
    } finally { await client.close(); await host.close(); await participants.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });
});

describe("Astra resident lease delivery fence", () => {
  it("F2 binding resolution loses its lease without tell/sequence delivery, then resumes its owned claim once", async () => {
    const { root, config, host } = fixture();
    let sender: FabricControlPlane | undefined;
    let release!: () => void, entered!: () => void;
    const waiting = new Promise<void>(resolve => { release = resolve; });
    const preparing = new Promise<void>(resolve => { entered = resolve; });
    let healthy = true;
    let result: Promise<unknown> | undefined;
    try {
      await host.start();
      vi.spyOn(host.participants, "canConsumeMesh").mockImplementation(() => healthy);
      vi.spyOn(host.actors, "owns").mockReturnValue(true);
      vi.spyOn(host.actors, "status").mockReturnValue({ rootId: config.rootId } as ReturnType<typeof host.actors.status>);
      vi.spyOn(host.actors, "resolveActivationBinding").mockImplementation(async () => { entered(); await waiting; return {}; });
      const tell = vi.spyOn(host.actors, "tell").mockReturnValue({ messageId: "lease-delivery-once" } as ReturnType<typeof host.actors.tell>);
      const identity = { id: config.rootId, name: "Main", kind: "main" as const, sessionId: config.sessionId };
      sender = new FabricControlPlane(new MeshStore(config.meshRoot, 65536, 1000), identity, { enabled: true, hostId: identity.id, pollMs: 20, acknowledgementTimeoutMs: 5000 });
      sender.start(() => ({ accepted: false }));
      result = sender.request(host.hostId, "actor", "followUp", { message: "accepted work" }).catch(error => error);
      await preparing; healthy = false; release(); await delay(150);
      expect(tell).not.toHaveBeenCalled();
      const seenRoot = path.join(config.meshRoot, "control-seen", createHash("sha256").update(host.hostId).digest("hex").slice(0, 32));
      const seen = new MeshStore(seenRoot, 65536, 1000);
      expect(seen.listAll("topology/control-seen/").every(entry => !(entry.value as { sequence?: number }).sequence)).toBe(true);
      healthy = true;
      expect(await result).toMatchObject({ acknowledged: true, messageId: "lease-delivery-once" });
      expect(tell).toHaveBeenCalledOnce();
    } finally {
      healthy = true; release?.(); await result; await sender?.close();
      vi.restoreAllMocks(); await host.close(); fs.rmSync(root, { recursive: true, force: true });
    }
  }, 15000);
});

describe("#3662 resident actor delivery routing", () => {
  it.each(["read-denied", "stat-denied", "invalid-json", "invalid-envelope", "invalid-participant"] as const)(
    "S1 keeps file-only %s lineage at its mailbox until confirmed withdrawal", async (fault) => {
      const { root, config, host } = fixture();
      const integratorId = "session:11111111-1111-4111-8111-111111111111";
      config.cwd = root;
      config.projectRoot = root;
      config.project = projectOf(root);
      vi.stubEnv("SMARTY_LEAD_SESSION", "");
      fs.mkdirSync(path.join(root, ".local"));
      fs.writeFileSync(path.join(root, ".local", "lead"), integratorId);
      let readFault: ReturnType<typeof vi.spyOn> | undefined;
      let statFault: ReturnType<typeof vi.spyOn> | undefined;
      try {
        await host.start();
        await host.mesh.put({ key: LIVENESS_POLICY_KEY, identity: host.identity, value: { version: 1, participants: "files" } });
        const publishRoot = async (id: string) => {
          const identity = { id, name: "main", kind: "main" as const };
          const hash = createHash("sha256").update(id).digest("hex");
          const key = `topology/participants/${hash}`;
          const participant: FabricParticipantRecord = {
            format: 1, id, rootId: id, kind: "root", ownerHostId: id, ownerIdentityId: id,
            name: "main", status: "idle", runner: "pi", transport: "host", role: "project-agent",
            project: config.project!, cwd: root, capabilities: ["steer", "followUp", "fabric"],
            startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1",
          };
          await host.mesh.put({ key: `topology/hosts/${hash}`, identity, value: {
            format: 1, id, rootId: id, identity, startedAt: 1, updatedAt: Date.now(), expiresAt: Date.now() + 120_000,
          } });
          const entry = { key, value: participant, version: 1, updatedAt: Date.now(), updatedBy: identity };
          writeParticipantFile(config.meshRoot, entry);
          return { key, participant, file: path.join(config.meshRoot, "participants", `${hash}.json`), text: JSON.stringify({ format: 1, ...entry }) };
        };
        await publishRoot(integratorId);
        const original = await publishRoot(config.rootId);
        // No lookup warmed the root slot before the fault. Denials persist through every retry.
        if (fault === "invalid-json") fs.writeFileSync(original.file, "{torn");
        if (fault === "invalid-envelope") fs.writeFileSync(original.file, JSON.stringify({ format: 99 }));
        if (fault === "invalid-participant") fs.writeFileSync(original.file, JSON.stringify({
          format: 1, key: original.key, value: { ...original.participant, kind: "invalid" },
          version: 1, updatedAt: Date.now(), updatedBy: host.identity,
        }));
        if (fault === "read-denied") {
          const read = fs.readFileSync;
          readFault = vi.spyOn(fs, "readFileSync").mockImplementation((...args: Parameters<typeof fs.readFileSync>) => {
            if (String(args[0]) === original.file) throw Object.assign(new Error("denied"), { code: "EACCES" });
            return read(...args);
          });
        }
        if (fault === "stat-denied") {
          const stat = fs.statSync;
          statFault = vi.spyOn(fs, "statSync").mockImplementation((...args: Parameters<typeof fs.statSync>) => {
            if (String(args[0]) === original.file) throw Object.assign(new Error("denied"), { code: "EACCES" });
            return stat(...args);
          });
        }
        expect(host.mesh.get(original.key, { fresh: true })).toBeUndefined();
        expect(host.participants.get(config.rootId, Date.now(), { fresh: true })).toBeUndefined();
        expect(host.participants.lastKnown(config.rootId)).toBeUndefined();
        expect(host.participants.get(integratorId, Date.now(), { fresh: true })?.id).toBe(integratorId);
        const send = async (text: string, total: number) => {
          host.actors.onDeliver({
            actor: { id: "actor:supervisor", name: "supervisor", project: config.project } as Parameters<typeof host.actors.onDeliver>[0]["actor"],
            message: { id: text, actorId: "actor:supervisor", actorName: "supervisor", direction: "out", source: "actor", createdAt: Date.now(), text },
            delivery: "steer", triggerTurn: true,
          });
          await vi.waitFor(() => expect(host.mesh.listAll("residency/deliveries/", { fresh: true })).toHaveLength(total));
        };
        await send("while unknown", 1);
        expect(host.mesh.listAll(residentDeliveryPrefix(config.rootId))).toHaveLength(1);
        expect(host.mesh.listAll(residentDeliveryPrefix(integratorId))).toHaveLength(0);
        readFault?.mockRestore();
        statFault?.mockRestore();
        fs.writeFileSync(original.file, original.text);
        await send("after repair", 2);
        expect(host.mesh.listAll(residentDeliveryPrefix(config.rootId))).toHaveLength(2);
        expect(host.mesh.listAll(residentDeliveryPrefix(integratorId))).toHaveLength(0);
        // Confirmed withdrawal, not a parsing omission, finally permits exact-bound inheritance.
        expect(await removeParticipantFileIf(host.mesh, original.key, () => true)).toBe(true);
        expect(host.participants.lineageAlive(config.rootId)).toBe(true); // Removal is not positive proof.
        await host.mesh.put({
          key: `topology/lineage-closures/${createHash("sha256").update(config.rootId).digest("hex")}`,
          identity: { id: config.rootId, name: "main", kind: "main" },
          value: { format: 1, rootId: config.rootId, ownerHostId: config.rootId, ownerIdentityId: config.rootId, closedAt: Date.now() },
        });
        expect(host.participants.lineageAlive(config.rootId)).toBe(false);
        await send("after withdrawal", 3);
        expect(host.mesh.listAll(residentDeliveryPrefix(config.rootId))).toHaveLength(2);
        expect(host.mesh.listAll(residentDeliveryPrefix(integratorId))).toHaveLength(1);
      } finally {
        readFault?.mockRestore();
        statFault?.mockRestore();
        await host.close();
        vi.unstubAllEnvs();
        fs.rmSync(root, { recursive: true, force: true });
      }
    },
  );
  it.each([
    ["expired lease with live lineage", true, true, "root"],
    ["dead root without bound integrator", false, false, "root"],
    ["dead root with exact bound integrator", false, true, "integrator"],
    ["dead root with actor repository different from host cwd", false, true, "integrator"],
  ] as const)("routes %s through the host mailbox path", async (_case, rootPresent, bound, target) => {
    const { root, config, host } = fixture();
    const integratorId = "session:11111111-1111-4111-8111-111111111111";
    config.cwd = root;
    config.projectRoot = root;
    let actorProject = root;
    if (_case === "dead root with actor repository different from host cwd") {
      actorProject = path.join(root, "actor-project");
      fs.mkdirSync(actorProject);
      for (const [cwd, origin] of [[root, "https://forge.test/team/host.git"], [actorProject, "https://forge.test/team/actors.git"]]) {
        execFileSync("git", ["init", "-q"], { cwd, stdio: "ignore" });
        execFileSync("git", ["config", "remote.origin.url", origin!], { cwd, stdio: "ignore" });
      }
    }
    const project = projectOf(actorProject);
    const repository = repositoryOf(project);
    config.project = project;
    vi.stubEnv("SMARTY_LEAD_SESSION", "");
    if (bound) {
      fs.mkdirSync(path.join(root, ".local"));
      fs.writeFileSync(path.join(root, ".local", "lead"), integratorId);
    }
    try {
      await host.start();
      const publishRoot = async (id: string, startedAt: number, expiresAt: number) => {
        const identity = { id, name: "main", kind: "main" as const };
        const participant: FabricParticipantRecord = {
          format: 1, id, rootId: id, kind: "root", ownerHostId: id, ownerIdentityId: id,
          name: "main", status: "idle", runner: "pi", transport: "host", role: "project-agent",
          project, ...(repository ? { repository } : {}), cwd: root, capabilities: ["steer", "followUp", "fabric"],
          startedAt, updatedAt: Date.now(), controlProtocol: "v1",
        };
        const key = (prefix: string) => prefix + createHash("sha256").update(id).digest("hex");
        await host.mesh.put({ key: key("topology/hosts/"), identity, value: {
          format: 1, id, rootId: id, identity, startedAt, updatedAt: Date.now(), expiresAt,
        } });
        await host.mesh.put({ key: key("topology/participants/"), identity, value: participant });
      };
      if (rootPresent) await publishRoot(config.rootId, 1, Date.now() - 60_000);
      else await host.mesh.put({
        key: `topology/lineage-closures/${createHash("sha256").update(config.rootId).digest("hex")}`,
        identity: { id: config.rootId, name: "main", kind: "main" },
        value: { format: 1, rootId: config.rootId, ownerHostId: config.rootId, ownerIdentityId: config.rootId, closedAt: Date.now() },
      });
      if (bound) await publishRoot(integratorId, rootPresent ? 99 : 2, Date.now() + 120_000);
      if (!rootPresent) await publishRoot("session:newer-project-agent", 99, Date.now() + 120_000);
      const liveRoots = host.participants.list({ scope: "project", kinds: ["root"], fresh: true });
      expect(liveRoots.some(candidate => candidate.id === config.rootId)).toBe(false);
      if (rootPresent) expect(host.participants.lastKnown(config.rootId)?.participant.stale).toBe(true);
      host.actors.onDeliver({
        actor: { id: "actor:supervisor", name: "supervisor", project: config.project } as Parameters<typeof host.actors.onDeliver>[0]["actor"],
        message: { id: "message", actorId: "actor:supervisor", actorName: "supervisor", direction: "out", source: "actor", createdAt: Date.now(), text: "directive" },
        delivery: "steer", triggerTurn: true,
      });
      await vi.waitFor(() => expect(host.mesh.listAll("residency/deliveries/").length).toBe(1));
      const expected = target === "integrator" ? integratorId : config.rootId;
      const deliveries = host.mesh.listAll(residentDeliveryPrefix(expected));
      expect(deliveries).toHaveLength(1);
      expect(deliveries[0]?.value).toMatchObject({ rootId: expected, message: "directive", delivery: "steer" });
      expect(host.mesh.listAll(residentDeliveryPrefix("session:newer-project-agent"))).toHaveLength(0);
    } finally {
      await host.close();
      vi.unstubAllEnvs();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
const RESIDENT_RUN_RETENTION_MS = 24 * 60 * 60 * 1_000;
describe("resident loaded-path census metadata", () => {
  it("publishes the startup generation rather than a later desired configuration", async () => {
    const { root, config, host } = fixture();
    const loaded = config.fabricExtensionPath;
    try {
      await host.start();
      const ownerPath = path.join(config.residencyRoot, "owner.json");
      const owner = fs.readFileSync(ownerPath, "utf8");
      expect(JSON.parse(owner)).toMatchObject({ pid: process.pid, fabricExtensionPath: loaded });
      fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify({
        ...config, fabricExtensionPath: path.join(root, "replacement", "dist", "index.js"),
      }));
      expect(fs.readFileSync(ownerPath, "utf8")).toBe(owner);
      expect(host.config.fabricExtensionPath).toBe(loaded);
    } finally {
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("resident retention config reload", () => {
  it("applies a same-release client reload to an already-running host's next sweep without replacing its owner", async () => {
    const { root, config, host } = fixture();
    // Do not mutate the shared defaults, and leave the host's initial config immutable.
    config.retention = { ...config.retention };
    let client: ResidencyClient | undefined;
    try {
      await host.start();
      const ownerPath = path.join(config.residencyRoot, "owner.json");
      const owner = fs.readFileSync(ownerPath, "utf8");
      const run = path.join(config.residencyRoot, "runs", "reload-retention");
      fs.mkdirSync(run, { recursive: true });
      const log = Buffer.from((JSON.stringify({ text: "x".repeat(100) }) + "\n").repeat(3000));
      const status = JSON.stringify({ status: "completed", finishedAt: Date.now() - 8 * 60 * 60 * 1000 });
      fs.writeFileSync(path.join(run, "status.json"), status);
      fs.writeFileSync(path.join(run, "events.jsonl"), log);
      fs.writeFileSync(path.join(run, "reply.json"), '{"text":"keep"}');
      await delay(150);
      expect(fs.readFileSync(path.join(run, "events.jsonl"))).toEqual(log);
      // The real reload creates a new client; ensureHost publishes desired config and reuses the owner.
      const next = { ...config, retention: { ...config.retention,
        terminalRunEventsAgeMs: 6 * 60 * 60 * 1000, terminalRunEventsMaxBytes: 128 * 1024 } };
      client = new ResidencyClient({ config: next, mesh: host.mesh, participants: host.participants,
        mainAgent: { local: false } as FabricMainAgentTarget });
      expect((await client.ensureHost()).pid).toBe(process.pid);
      // Advance only the sample clock; keep the real host poll and its production 5-ms transaction.
      const due = vi.spyOn(ResidentRequestRetention.prototype, "due").mockReturnValue(true);
      const nativeSweep = ResidentRequestRetention.prototype.sweep;
      const sweep = vi.spyOn(ResidentRequestRetention.prototype, "sweep").mockImplementation(function (this: ResidentRequestRetention, now, ...args) {
        return nativeSweep.call(this, now + 60_001, ...args);
      });
      const deadline = Date.now() + 2500;
      while (fs.statSync(path.join(run, "events.jsonl")).size > 128 * 1024 && Date.now() < deadline) await delay(10);
      due.mockRestore(); sweep.mockRestore();
      expect(fs.statSync(path.join(run, "events.jsonl")).size).toBeLessThanOrEqual(128 * 1024);
      expect(fs.readFileSync(path.join(run, "status.json"), "utf8")).toBe(status);
      expect(fs.readFileSync(path.join(run, "reply.json"), "utf8")).toBe('{"text":"keep"}');
      expect(fs.readFileSync(ownerPath, "utf8")).toBe(owner);
      expect(DEFAULT_FABRIC_CONFIG.retention.terminalRunEventsMaxBytes).toBe(256 * 1024);
    } finally {
      vi.restoreAllMocks(); await client?.close(); await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("resident tracked result preservation", () => {
  it.each(["native", "win32-injected"] as const)("releases the host fence and closes delivery/participant work even if agent close fails (%s)", async (platformCase) => {
    const { root, config, host } = fixture();
    const successor = new ResidentHost(config);
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    const getuid = Object.getOwnPropertyDescriptor(process, "getuid");
    let fault: ReturnType<typeof vi.spyOn> | undefined;
    try {
      if (platformCase === "win32-injected") {
        Object.defineProperty(process, "platform", { ...platform, value: "win32" });
        Object.defineProperty(process, "getuid", { configurable: true, writable: true, value: undefined });
      }
      await host.start();
      await expect(successor.start()).rejects.toThrow(/already running/);
      expect(successor.actors).toBeUndefined();
      const closeAgents = host.agents.close.bind(host.agents);
      fault = vi.spyOn(host.agents, "close").mockImplementation(async () => {
        await closeAgents();
        throw new Error("fixture agent close failure");
      });
      const closeParticipants = vi.spyOn(host.participants, "close");
      await expect(host.close()).rejects.toThrow("fixture agent close failure");
      expect(closeParticipants).toHaveBeenCalledOnce();
      expect(fs.existsSync(path.join(config.residencyRoot, "owner.json"))).toBe(false);
      const lock = path.join(config.residencyRoot, "host.lock");
      if (process.platform === "linux") {
        // Only Linux uses the persistent flock inode; non-Linux uses a record fence.
        const fd = await lockFile(lock, 0);
        fs.closeSync(fd);
      } else {
        expect(fs.existsSync(lock)).toBe(false);
      }
      // Exercise the product's actual fence on every platform, not flock on Windows.
      await successor.start();
      expect(JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "owner.json"), "utf8")).pid).toBe(process.pid);
      await successor.close();
      expect(fs.existsSync(path.join(config.residencyRoot, "owner.json"))).toBe(false);
      if (process.platform !== "linux") expect(fs.existsSync(lock)).toBe(false);
      closeParticipants.mockRestore();
    } finally {
      fault?.mockRestore();
      try {
        await host.close();
        await successor.close();
      } finally {
        Object.defineProperty(process, "platform", platform);
        if (getuid) Object.defineProperty(process, "getuid", getuid);
        else Reflect.deleteProperty(process, "getuid");
        fs.rmSync(root, { recursive: true, force: true });
      }
    }
  });
  it.each(["LARGE_RESULT", "FAIL_DIRECTIVE"])("F1 save failure keeps the worker's %s completion through close and two host/client restarts", async (task) => {
    const { root, config, host: first } = fixture();
    config.workerPath = path.resolve("tests/fixtures/fake-worker.mjs");
    config.agents = { ...config.agents, retainRuns: false, budgetUsd: 0 };
    config.piModels = { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" };
    let host = first;
    let client: ResidencyClient | undefined;
    let obstructed: string | undefined;
    const launch = ProcessTransport.prototype.launch;
    const fault = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      // Use the real id before the child can finish; atomic rename onto a directory must fail.
      obstructed ??= residentResultPath(config.residencyRoot, request.id);
      fs.mkdirSync(obstructed, { recursive: true });
      return launch.call(this, request);
    });
    // A public spawn is made by a real session caller, never by the hidden resident executor.
    const callerIdentity = { id: config.rootId, name: "Main", kind: "main" as const, sessionId: config.sessionId };
    const callerMesh = new MeshStore(config.meshRoot, config.mesh.maxEventBytes, config.mesh.maxReadEvents);
    const callerParticipants = new ParticipantDirectory(callerMesh, {
      enabled: true, hostId: config.rootId, rootId: config.rootId, identity: callerIdentity,
    });
    callerParticipants.registerSource(() => [{
      format: 1, id: config.rootId, rootId: config.rootId, kind: "root", name: "Main", status: "idle",
      ownerHostId: config.rootId, ownerIdentityId: config.rootId, sessionId: config.sessionId,
      runner: "pi", transport: "host", capabilities: ["fabric"], controlProtocol: "v1",
      startedAt: Date.now(), updatedAt: Date.now(),
    }]);
    const connect = () => new ResidencyClient({
      config, mesh: callerMesh, participants: callerParticipants,
      mainAgent: { local: false } as FabricMainAgentTarget,
    });
    try {
      await callerParticipants.start();
      await host.start();
      client = connect();
      const handle = await client.spawnAgent({ task, transport: "process", residency: "durable" }, AbortSignal.timeout(5_000));
      fault.mockRestore();
      const original = await host.agents.wait(handle.id, { timeoutMs: 5_000 });
      expect(original.status).toBe(task === "FAIL_DIRECTIVE" ? "failed" : "completed");
      expect(original.text).toBe(task === "LARGE_RESULT" ? "x".repeat(100_000) : "fake worker complete");
      const run = host.agents.runDirectory(handle.id)!;
      const worker = fs.readFileSync(path.join(run, "status.json"), "utf8");
      expect(JSON.parse(worker)).toMatchObject({ status: original.status, text: original.text });
      expect(original.error).toBe(JSON.parse(worker).error);
      expect(fs.statSync(obstructed!).isDirectory()).toBe(true); // no authoritative saved result
      // Counterexample: an unobstructed public completion must not pin all tracked runs.
      const saved = await client.spawnAgent({ task: "saved normally", transport: "process", residency: "durable" }, AbortSignal.timeout(5_000));
      await host.agents.wait(saved.id, { timeoutMs: 5_000 });
      const savedRun = host.agents.runDirectory(saved.id)!;
      expect(JSON.parse(fs.readFileSync(residentResultPath(config.residencyRoot, saved.id), "utf8")).status).toBe("completed");
      await client.close();
      await host.close();
      expect(fs.existsSync(savedRun)).toBe(false);
      expect(fs.existsSync(run), "failed save must not delete the only completion").toBe(true);
      expect(fs.readFileSync(path.join(run, "status.json"), "utf8")).toBe(worker);
      const expected = { id: handle.id, status: original.status, text: original.text, residency: "durable",
        ...(original.error === undefined ? {} : { error: original.error }) };
      for (let restart = 1; restart <= 2; restart++) {
        host = new ResidentHost(config);
        await host.start();
        client = connect();
        expect(client.statusAgent(handle.id), `restart ${restart}`).toMatchObject(expected);
        expect(await client.waitAgent(handle.id, AbortSignal.timeout(2_000))).toMatchObject(expected);
        expect(fs.readFileSync(path.join(run, "status.json"), "utf8")).toBe(worker);
        expect(fs.statSync(obstructed!).isDirectory()).toBe(true);
        await client.close();
        await host.close();
      }
    } finally {
      fault.mockRestore();
      await client?.close();
      await host.close();
      await callerParticipants.close();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  }, 20_000);
});

describe("resident orphan retention", () => {
  it("F1 retains a public task's only completion until a valid saved terminal result authorizes collection", async () => {
    const { root, config, host } = fixture();
    const id = "a".repeat(32);
    const runs = path.join(config.residencyRoot, "runs");
    const run = path.join(runs, id);
    const result = {
      id, name: "orphaned public task", task: "work", status: "completed", text: "original completion",
      // Already-exited orphan: persist a usable PID whose absence is checked below.
      runner: "pi", transport: "process", sessionId: "2147483647", cwd: config.cwd, startedAt: 1, updatedAt: 2, finishedAt: 2,
      turns: 1, toolCalls: 0, usage: { input: 1, output: 2, cacheRead: 0, cacheWrite: 0, cost: 0 },
    };
    expect(() => process.kill(Number(result.sessionId), 0)).toThrow(expect.objectContaining({ code: "ESRCH" }));
    fs.mkdirSync(run, { recursive: true });
    fs.writeFileSync(path.join(run, "status.json"), JSON.stringify(result));
    const metadataPath = path.join(config.residencyRoot, "agents", `${id}.json`);
    fs.mkdirSync(path.dirname(metadataPath), { recursive: true });
    fs.writeFileSync(metadataPath, JSON.stringify({
      format: RESIDENT_HOST_FORMAT, rootId: config.rootId, id, runDirectory: run,
      handle: { ...result, status: "running", text: "", residency: "durable" }, createdAt: 1, updatedAt: 1,
    }));
    const expiredAt = Date.now() - RESIDENT_RUN_RETENTION_MS - 60_000;
    fs.utimesSync(run, expiredAt / 1_000, expiredAt / 1_000);
    const resultPath = residentResultPath(config.residencyRoot, id);
    try {
      // Lost host: the detached worker wrote status.json, but onSettled never saved results/id.
      await host.start();
      expect(fs.existsSync(run)).toBe(true);
      expect(fs.existsSync(resultPath)).toBe(false);
      fs.mkdirSync(path.dirname(resultPath), { recursive: true });
      for (const malformed of [
        "{", "null", "[]", JSON.stringify({ ...result, id: "b".repeat(32) }),
        JSON.stringify({ ...result, status: "running" }),
        JSON.stringify({ id, status: "completed" }),
        JSON.stringify({ ...result, text: null }),
        JSON.stringify({ ...result, usage: { ...result.usage, output: "invalid" } }),
      ]) {
        fs.writeFileSync(resultPath, malformed);
        expect(sweepResidentRuns(runs), malformed).toEqual([]);
        expect(fs.existsSync(run), malformed).toBe(true);
        expect(JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8"))).toEqual(result);
      }
      // A saved result cannot turn uncertain public metadata into permission to delete.
      fs.writeFileSync(resultPath, JSON.stringify(result));
      const metadata = fs.readFileSync(metadataPath, "utf8");
      const parsedMetadata = JSON.parse(metadata);
      for (const malformed of [
        "{", "null", "[]", JSON.stringify({ id }),
        JSON.stringify({ ...parsedMetadata, handle: null }),
        JSON.stringify({ ...parsedMetadata, runDirectory: path.join(runs, "unrelated") }),
      ]) {
        fs.writeFileSync(metadataPath, malformed);
        expect(sweepResidentRuns(runs), malformed).toEqual([]);
        expect(fs.existsSync(run), malformed).toBe(true);
        expect(JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8"))).toEqual(result);
      }
      fs.writeFileSync(metadataPath, metadata);
      // Counterexample: this SAME public task becomes collectable once its authoritative copy exists.
      expect(sweepResidentRuns(runs)).toEqual([run]);
      expect(fs.existsSync(run)).toBe(false);
      expect(JSON.parse(fs.readFileSync(resultPath, "utf8"))).toEqual(result);
      expect(fs.existsSync(metadataPath)).toBe(true);
    } finally { await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("F6 sweeps old terminal untracked runs after lease publication, preserving recent/live/unknown/unresolved runs", async () => {
    const { root, config, host } = fixture();
    const runs = path.join(config.residencyRoot, "runs");
    const now = Date.now();
    const make = (name: string, status: Record<string, unknown>, age = RESIDENT_RUN_RETENTION_MS + 60_000) => {
      const run = path.join(runs, name);
      fs.mkdirSync(run, { recursive: true });
      fs.writeFileSync(path.join(run, "status.json"), JSON.stringify(status));
      fs.utimesSync(run, (now - age) / 1_000, (now - age) / 1_000);
      return run;
    };
    const old = make("terminal-old", { status: "completed" });
    const actor = make("actor-old", { status: "completed", actorId: "actor-without-public-metadata",
      transport: "process", sessionId: "2147483647", processStartTime: "1" });
    const recent = make("terminal-recent", { status: "completed" }, 1_000);
    const live = make("live", { status: "completed", transport: "process", sessionId: String(process.pid) });
    const unknown = make("unknown", { status: "running" });
    const malformed = make("malformed", { status: "completed", transport: "process", sessionId: "unknown" });
    const deadWithoutBirth = make("dead-without-birth", { status: "running", transport: "process", sessionId: "2147483647" });
    const unresolved = make("unresolved", { status: "completed" });
    fs.writeFileSync(path.join(unresolved, "unresolved-worker.json"), "{}");
    fs.utimesSync(unresolved, (now - RESIDENT_RUN_RETENTION_MS - 60_000) / 1_000, (now - RESIDENT_RUN_RETENTION_MS - 60_000) / 1_000);
    try {
      await host.start();
      // Startup does not inspect archives; the streaming collector runs later.
      expect(fs.existsSync(old)).toBe(true);
      await vi.waitFor(() => {
        expect(fs.existsSync(old)).toBe(false);
        expect(fs.existsSync(actor)).toBe(false);
      });
      for (const run of [recent, live, unknown, malformed, deadWithoutBirth, unresolved]) expect(fs.existsSync(run), run).toBe(true);
      // Inject the clock: a preserved terminal survivor becomes eligible on a later host start.
      expect(sweepResidentRuns(runs, now + RESIDENT_RUN_RETENTION_MS + 60_000)).toEqual([recent]);
      expect(sweepResidentRuns(runs, now + 10 * RESIDENT_RUN_RETENTION_MS, 0)).toEqual([]);
    } finally { await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });
});

describe("resident host ownership", () => {
  it("followUp advisory A2 resident-owned running task ACK and persisted replay preserve the warning", async () => {
    const { root, config, host } = fixture();
    config.workerPath = path.resolve("tests/fixtures/fake-worker.mjs");
    config.piModels = { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" };
    let sender: FabricControlPlane | undefined;
    try {
      await host.start();
      sender = new FabricControlPlane(host.mesh, { id: "session:sender", name: "Sender", kind: "main" },
        { enabled: true, hostId: "session:sender", pollMs: 20, acknowledgementTimeoutMs: 2000 });
      sender.start(() => ({ accepted: false }));
      const child = await host.agents.spawn({ task: "HANG", transport: "process" });
      await vi.waitFor(() => expect(fs.existsSync(path.join(host.agents.runDirectory(child.id)!, "status.json"))).toBe(true));
      const receipt = await sender.request(host.hostId, child.id, "followUp", { message: "later" }, host.identity.id);
      const warning = { code: "FABRIC_FOLLOW_UP_RUNNING_TASK", targetId: child.id, kind: "agent", status: "running",
        message: "followUp to a running task waits until its current run finishes; use agents.steer for a correction needed before completion." };
      const command = host.mesh.read({ topic: "fabric.control.command", limit: 10 })[0]!;
      await host.mesh.publish({ topic: command.topic, kind: command.kind, from: command.from, to: command.to!, data: command.data });
      await vi.waitFor(() => expect(host.mesh.read({ topic: "fabric.control.ack", limit: 10 })).toHaveLength(2));
      const entries = fs.readFileSync(path.join(host.agents.runDirectory(child.id)!, "steer.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
      expect(entries).toHaveLength(1); expect(entries[0]).toMatchObject({ type: "follow_up", message: "later" });
      expect(entries[0]).not.toHaveProperty("warning");
      for (const ack of host.mesh.read({ topic: "fabric.control.ack", limit: 10 })) expect(ack.data).toMatchObject({ accepted: true, warning });
      expect(receipt).toEqual({ queued: true, messageId: expect.any(String), routed: "mesh", acknowledged: true, warning });
    } finally { await sender?.close(); await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.each(["ask", "followUp"] as const)("preserves owner-default provenance through %s admission without pinning resolved defaults", async (operation) => {
    const { root, config, host } = fixture();
    let handler!: Parameters<typeof host.control.start>[0];
    const control = vi.spyOn(FabricControlPlane.prototype, "start").mockImplementation((accept) => { handler = accept; });
    try {
      await host.start();
      control.mockRestore();
      vi.spyOn(host.actors, "owns").mockReturnValue(true);
      vi.spyOn(host.actors, "status").mockReturnValue({ rootId: config.rootId } as ReturnType<typeof host.actors.status>);
      const binding = vi.spyOn(host.actors, "resolveActivationBinding").mockResolvedValue({ model: "provider/current", thinking: "high" });
      const tell = vi.spyOn(host.actors, "tell").mockReturnValue({ messageId: "accepted" } as ReturnType<typeof host.actors.tell>);
      const ask = vi.spyOn(host.actors, "ask").mockResolvedValue({ id: "accepted" } as Awaited<ReturnType<typeof host.actors.ask>>);
      const principal = { id: "paul", binding: "voice-call" as const };
      const command = { operation, principal, targetId: "actor", commandId: "owner-defaults", message: "keep working", binding: { model: "provider/pinned" }, bindingProvenance: { kind: "owner-defaults" as const, rootId: config.rootId } } as Parameters<typeof handler>[0];
      const from = { id: config.rootId, name: "Main", kind: "main" as const, sessionId: config.sessionId };
      const signal = new AbortController().signal;
      await expect(handler(command, from, signal, "mesh")).resolves.toMatchObject({ accepted: true, messageId: "accepted" });
      const options = { overrides: { model: "provider/pinned" } };
      const provenance = expect.objectContaining({ principal });
      if (operation === "ask") expect(ask).toHaveBeenCalledWith("actor", "keep working", undefined, signal, { ...options, provenance });
      else {
        expect(binding).toHaveBeenCalledWith("actor", options);
        expect(tell).toHaveBeenCalledWith("actor", "keep working", undefined, { ...options, provenance });
      }
      await expect(handler(command, { ...from, id: "session:foreign" }, signal)).resolves.toMatchObject({ accepted: false, error: "Invalid actor owner-default binding provenance" });
      expect(operation === "ask" ? ask : tell).toHaveBeenCalledOnce();
      const { bindingProvenance: _ignored, binding: _pinned, ...resolved } = command;
      for (const foreignBinding of [undefined, {}, { thinking: "medium" as const }]) {
        await expect(handler({ ...resolved, ...(foreignBinding ? { binding: foreignBinding } : {}) }, { ...from, id: "session:foreign" }, signal, "bridge")).resolves.toMatchObject({ accepted: true });
        const foreignOptions = { binding: foreignBinding ?? {} };
        if (operation === "ask") expect(ask).toHaveBeenLastCalledWith("actor", "keep working", undefined, signal, { ...foreignOptions, provenance });
        else {
          expect(binding).toHaveBeenLastCalledWith("actor", foreignOptions);
          expect(tell).toHaveBeenLastCalledWith("actor", "keep working", undefined, { ...foreignOptions, provenance });
        }
      }
    } finally {
      control.mockRestore();
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
  it("publishes the request fence and process birth identity together, then releases ownership", async () => {
    const { root, config, host } = fixture();
    const ownerPath = path.join(config.residencyRoot, "owner.json");
    try {
      await host.start();
      const owner = JSON.parse(fs.readFileSync(ownerPath, "utf8"));
      expect(owner).toMatchObject({ requestFence: 1, callerBoundSpawn: 1, requestExpiry: 1, creationIdempotency: 1, commands: expect.arrayContaining(["spawnBound", "setModel", "setTools"]), pid: process.pid, hostId: host.hostId });
      expect(owner.processStartTime).toBe(processStartTime(process.pid));
      expect(residentProcessAlive(owner.pid, owner.processStartTime)).toBe(true);
      await host.close();
      expect(fs.existsSync(ownerPath)).toBe(false);
      expect(JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "closed.json"), "utf8"))).toMatchObject({
        format: 1, pid: owner.pid, processStartTime: owner.processStartTime, token: owner.token, closedAt: expect.any(Number),
      });
    } finally { await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });
  it("never follows mutable config alone without a Main intent and attested launcher custody", async () => {
    const { root, config, host, idle } = fixture();
    const next = path.join(root, "other-package");
    fs.mkdirSync(path.join(next, "dist/residency"), { recursive: true });
    fs.writeFileSync(path.join(next, "package.json"), JSON.stringify({ name: "pi-fabric" }));
    fs.writeFileSync(path.join(next, "dist/residency/launcher.js"), "// must not run");
    let handler!: Parameters<typeof host.control.start>[0];
    const control = vi.spyOn(FabricControlPlane.prototype, "start").mockImplementation((accept) => { handler = accept; });
    try {
      await host.start();
      control.mockRestore();
      const ownerPath = path.join(config.residencyRoot, "owner.json");
      const owner = fs.readFileSync(ownerPath, "utf8");
      vi.spyOn(host.actors, "listOwned").mockReturnValue([{ residency: "durable", status: "idle", queued: 0 }] as ReturnType<typeof host.actors.listOwned>);
      vi.spyOn(host.actors, "owns").mockReturnValue(true);
      vi.spyOn(host.actors, "status").mockReturnValue({ rootId: config.rootId } as ReturnType<typeof host.actors.status>);
      vi.spyOn(host.actors, "resolveActivationBinding").mockResolvedValue({});
      const tell = vi.spyOn(host.actors, "tell").mockReturnValue({ messageId: "accepted" } as ReturnType<typeof host.actors.tell>);
      fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify({ ...config, fabricExtensionPath: path.join(next, "dist/index.js") }));
      await delay(150);
      expect(idle).not.toHaveBeenCalled();
      expect(fs.readFileSync(ownerPath, "utf8")).toBe(owner);
      await expect(handler({ operation: "followUp", targetId: "actor", commandId: "after-config", message: "keep working" } as Parameters<typeof handler>[0], host.identity, new AbortController().signal)).resolves.toMatchObject({ accepted: true, messageId: "accepted" });
      expect(tell).toHaveBeenCalledOnce();
    } finally {
      control.mockRestore();
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("rejects delayed and new admissions during ordinary close", async () => {
    const { root, config, host } = fixture();
    let handler!: Parameters<typeof host.control.start>[0];
    const control = vi.spyOn(FabricControlPlane.prototype, "start").mockImplementation((accept) => { handler = accept; });
    let resolve: ((binding: Awaited<ReturnType<typeof host.actors.resolveActivationBinding>>) => void) | undefined;
    let closing: Promise<void> | undefined;
    try {
      await host.start();
      control.mockRestore();
      vi.spyOn(host.actors, "owns").mockReturnValue(true);
      vi.spyOn(host.actors, "status").mockReturnValue({ rootId: config.rootId } as ReturnType<typeof host.actors.status>);
      vi.spyOn(host.actors, "resolveActivationBinding").mockImplementation(() => new Promise((done) => { resolve = done; }));
      const tell = vi.spyOn(host.actors, "tell");
      const admission = handler({ operation: "followUp", targetId: "actor", commandId: "delayed", message: "keep me" } as Parameters<typeof handler>[0], host.identity, new AbortController().signal);
      closing = host.close();
      await expect(handler({ operation: "followUp", targetId: "actor", message: "retry me" } as Parameters<typeof handler>[0], host.identity, new AbortController().signal)).resolves.toMatchObject({ accepted: false, error: "Fabric resident host is closing; retry" });
      await expect(host.lifecycle.deliver({ to: "actor" } as Parameters<typeof host.lifecycle.deliver>[0], {} as Parameters<typeof host.lifecycle.deliver>[1])).rejects.toThrow("Fabric resident host is closing; retry");
      resolve!({});
      await expect(admission).resolves.toMatchObject({ accepted: false, error: "Fabric resident host is closing; retry" });
      expect(tell).not.toHaveBeenCalled();
      await closing;
    } finally {
      resolve?.({});
      control.mockRestore();
      await closing;
      await host.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.skipIf(process.platform !== "linux").each([
    ["host.lock", true], ["host.lock", false], ["owner.json", true],
  ] as const)("R3 refuses a live legacy owner in %s (birth identity %s)", async (file, birth) => {
    const { root, config, host } = fixture();
    const child = spawn("sleep", ["60"], { stdio: "ignore" });
    const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
    try {
      await new Promise<void>((resolve, reject) => { child.once("spawn", resolve); child.once("error", reject); });
      const record = JSON.stringify({ pid: child.pid, ...(birth ? { processStartTime: processStartTime(child.pid!) } : {}) });
      const recordPath = path.join(config.residencyRoot, file);
      fs.writeFileSync(recordPath, record);
      await expect(host.start()).rejects.toThrow(/already running/);
      expect(host.actors).toBeUndefined();
      expect(fs.readFileSync(recordPath, "utf8")).toBe(record);
      const fd = await lockFile(path.join(config.residencyRoot, "host.lock"), 0);
      fs.closeSync(fd); // Refusal released the flock; it did not overwrite legacy bytes.
    } finally { child.kill(); await exited; await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.skipIf(process.platform !== "linux")("R2 two concurrent starters fence all actor/control execution before one wins", async () => {
    const { root, config, host: first } = fixture();
    const second = new ResidentHost(config);
    const control = vi.spyOn(FabricControlPlane.prototype, "start");
    try {
      expect(first.actors).toBeUndefined(); expect(second.actors).toBeUndefined();
      const results = await Promise.allSettled([first.start(), second.start()]);
      expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(1);
      const rejected = results.find((r) => r.status === "rejected") as PromiseRejectedResult;
      expect(rejected.reason.constructor.name).toBe("ResidentHostAlreadyRunning");
      const loser = results[0]!.status === "rejected" ? first : second;
      expect(loser.actors).toBeUndefined();
      expect(control).toHaveBeenCalledTimes(1);
    } finally { control.mockRestore(); await Promise.all([first.close(), second.close()]); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.skipIf(process.platform !== "linux")("R2 stale diagnostic file naming a dead PID cannot block or replace the kernel inode", async () => {
    const { root, config, host } = fixture();
    const lock = path.join(config.residencyRoot, "host.lock");
    fs.writeFileSync(lock, JSON.stringify({ pid: -1, token: "stale" }));
    const inode = fs.statSync(lock).ino;
    // Our own PID is not a different live legacy owner.
    fs.writeFileSync(path.join(config.residencyRoot, "owner.json"), JSON.stringify({ pid: process.pid }));
    try {
      await host.start();
      expect(fs.statSync(lock).ino).toBe(inode);
      await host.close();
      expect(fs.statSync(lock).ino).toBe(inode);
    } finally { await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.skipIf(process.platform !== "linux")("R2 SIGKILL releases host ownership without replacing its lock file", async () => {
    const { root, config, host: successor } = fixture();
    const lock = path.join(config.residencyRoot, "host.lock");
    const child = spawn("bun", ["-e", `const {ResidentHost}=await import(${JSON.stringify(path.resolve("src/residency/host.ts"))}); const host=new ResidentHost(${JSON.stringify(config)}); await host.start(); console.log('ready');`], { stdio: ["ignore", "pipe", "pipe"] });
    const done = new Promise<void>((resolve) => child.once("exit", () => resolve()));
    try {
      await new Promise<void>((resolve, reject) => { child.stdout!.once("data", () => resolve()); child.once("error", reject); child.once("exit", () => reject(new Error("fixture died before ready"))); });
      const inode = fs.statSync(lock).ino;
      // Diagnostic content lies: only the OS fence must reject this contender.
      fs.rmSync(path.join(config.residencyRoot, "owner.json"));
      fs.writeFileSync(lock, JSON.stringify({ pid: -1, token: "corrupt-diagnostic" }));
      await expect(successor.start()).rejects.toThrow(/already running/);
      child.kill("SIGKILL"); await done;
      await successor.start();
      expect(fs.statSync(lock).ino).toBe(inode);
    } finally { child.kill("SIGKILL"); await done; await successor.close(); fs.rmSync(root, { recursive: true, force: true }); }
  }, 20_000);

  // Preserve round one's unknown-Linux-identity regression: unreadable is not dead.
  it.skipIf(process.platform !== "linux")("never reclaims a live host's lock on an unreadable start time", async () => {
    const { root, config, host } = fixture();
    const contender = new ResidentHost(config);
    let eio: ReturnType<typeof vi.spyOn> | undefined;
    try {
      await host.start();
      const lock = path.join(config.residencyRoot, "host.lock");
      const live = fs.readFileSync(lock, "utf8");
      const read = fs.readFileSync.bind(fs) as typeof fs.readFileSync;
      eio = vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
        if (String(file) === `/proc/${process.pid}/stat`) throw Object.assign(new Error("EIO"), { code: "EIO" });
        return read(file, options as never);
      }) as typeof fs.readFileSync);
      expect(residentProcessAlive(process.pid, "1")).toBe(true);
      await expect(contender.start()).rejects.toThrow(/already running|starting/);
      expect(fs.readFileSync(lock, "utf8")).toBe(live);
    } finally { eio?.mockRestore(); await Promise.all([host.close(), contender.close()]); fs.rmSync(root, { recursive: true, force: true }); }
    expect(residentProcessAlive(process.pid, "0")).toBe(processStartTime(process.pid) === undefined);
  });

  it("rejects a proven reused PID but conservatively accepts unknown or legacy owners", () => {
    expect(residentProcessAlive(process.pid)).toBe(true);
    expect(residentProcessAlive(-1)).toBe(false);
    const start = processStartTime(process.pid);
    if (start !== undefined) {
      expect(start).toMatch(/^\d+$/);
      expect(residentProcessAlive(process.pid, start)).toBe(true);
      expect(residentProcessAlive(process.pid, "0")).toBe(false);
    } else {
      expect(residentProcessAlive(process.pid, "0")).toBe(true);
    }
  });
});
