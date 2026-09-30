import { execFileSync } from "node:child_process";
import { markUnresolvedWorker } from "../src/storage/retention.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { FabricMainAgentDeliveryRequest, FabricMainAgentTarget } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { projectOf } from "../src/topology/project-identity.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import {
  RESIDENT_HOST_FORMAT,
  residentDeliveryPrefix,
  residentHostId,
  residentResultPath,
  residentRoot,
  type ResidentHostConfig,
  type ResidentHostOwner,
} from "../src/residency/protocol.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { launchLog, same, startedAtMs } from "./helpers/owned-processes.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { actorParticipantRecord } from "../src/topology/records.js";
import type { FabricParticipantSource } from "../src/topology/types.js";
import type { FabricActorMessage } from "../src/actors/types.js";

const repo = process.cwd();
const hostPath = path.resolve("dist/residency/launcher.js");
const fakeWorker = path.resolve("tests/fixtures/fake-worker.mjs");
const hasResidentHost = fs.existsSync(hostPath);
const roots: string[] = [];

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

const git = (cwd: string, ...args: string[]): string =>
  execFileSync("git", args, { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });

// Git normalizes worktree paths its own way (forward slashes, Windows 8.3
// short names resolved), while Node never expands the short form from
// os.tmpdir(), so path text can never match there. Assert the worktree's
// branch registration instead — that is what these checks mean.
const worktreeBranches = (repository: string): string[] =>
  git(repository, "worktree", "list", "--porcelain")
    .split("\n")
    .filter((line) => line.startsWith("branch refs/heads/"))
    .map((line) => line.slice("branch refs/heads/".length));

const initRepository = (directory: string): void => {
  fs.mkdirSync(directory, { recursive: true });
  git(directory, "init", "-q");
  git(directory, "config", "user.email", "pi-fabric-tests@example.invalid");
  git(directory, "config", "user.name", "Pi Fabric tests");
  fs.writeFileSync(path.join(directory, "README.md"), "test repository\n");
  git(directory, "add", ".");
  git(directory, "commit", "-qm", "initial");
};

const waitFor = async (predicate: () => boolean, timeoutMs = 7_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Timed out waiting for durable residency state");
    await delay(25);
  }
};

const mainTarget = (
  identity: MeshIdentity,
  deliveries: FabricMainAgentDeliveryRequest[],
): FabricMainAgentTarget => ({
  id: identity.id,
  local: true,
  matches: (id) => id === "main" || id === identity.id,
  info: () => {
    throw new Error("not used by residency tests");
  },
  deliverAgent: (request) => {
    deliveries.push(request);
    return { queued: true, messageId: randomId(), routed: "main" };
  },
});

const randomId = (): string => Math.random().toString(16).slice(2);

interface RootHarness {
  root: string;
  mesh: MeshStore;
  meshConfig: typeof DEFAULT_FABRIC_CONFIG.mesh;
  identity: MeshIdentity;
  participants: ParticipantDirectory;
  mainAgent: FabricMainAgentTarget;
  deliveries: FabricMainAgentDeliveryRequest[];
  config: ResidentHostConfig;
}

const rootHarness = async (name: string): Promise<RootHarness> => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-${name}-`));
  roots.push(root);
  const meshRoot = path.join(root, "mesh");
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
  const mesh = new MeshStore(meshRoot, meshConfig.maxEventBytes, meshConfig.maxReadEvents);
  const identity: MeshIdentity = {
    id: `session:${name}:${randomId()}`,
    name: "main",
    kind: "main",
    sessionId: name,
  };
  const participants = new ParticipantDirectory(mesh, {
    enabled: true,
    hostId: identity.id,
    rootId: identity.id,
    identity,
    heartbeatMs: 50,
    leaseMs: 300,
  });
  participants.registerSource(() => [{
    format: 1,
    id: identity.id,
    kind: "root",
    rootId: identity.id,
    ownerHostId: identity.id,
    ownerIdentityId: identity.id,
    name: "main",
    status: "idle",
    residency: "session",
    runner: "pi",
    transport: "host",
    capabilities: ["steer", "followUp", "fabric"],
    cwd: repo,
    sessionId: name,
    startedAt: Date.now(),
    updatedAt: Date.now(),
    controlProtocol: "v1",
  }]);
  await participants.start();
  const residencyRoot = residentRoot(meshRoot, identity.id);
  const deliveries: FabricMainAgentDeliveryRequest[] = [];
  return {
    root,
    mesh,
    meshConfig,
    identity,
    participants,
    mainAgent: mainTarget(identity, deliveries),
    deliveries,
    config: {
      format: RESIDENT_HOST_FORMAT,
      rootId: identity.id,
      sessionId: name,
      cwd: repo,
      projectRoot: repo,
      meshRoot,
      actorRoot: path.join(meshRoot, "actors"),
      sessionActorRoot: path.join(meshRoot, "actors", name),
      residencyRoot,
      fullCodeMode: true,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 10_000 },
      mesh: meshConfig,
      retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath: fakeWorker,
      fabricExtensionPath: path.resolve("dist/index.js"),
      piBinary: "pi",
      claudeBinary: "claude",
      vedaBinary: "veda",
      piModels: {
        available: [
          { provider: "provider", id: "visible", name: "Visible" },
          { provider: "deepseek", id: "deepseek-chat", name: "DeepSeek Chat" },
        ],
        aliases: {},
        defaultModel: "provider/visible",
      },
    },
  };
};

const stopResident = async (config: ResidentHostConfig): Promise<void> => {
  const ownerPath = path.join(config.residencyRoot, "owner.json");
  const owner = (() => {
    try {
      return JSON.parse(fs.readFileSync(ownerPath, "utf8")) as ResidentHostOwner;
    } catch {
      return undefined;
    }
  })();
  if (owner?.pid) {
    const alive = (): boolean => {
      try {
        process.kill(owner.pid, 0);
        return true;
      } catch {
        return false;
      }
    };
    try {
      process.kill(owner.pid, "SIGTERM");
    } catch {
      // Process already exited.
    }
    // The host still writes under mesh/ after it removes owner.json: wait for the process
    // itself to exit, not only its marker, before the root is removed.
    await waitFor(() => !alive(), 20_000).catch(() => {
      try { process.kill(owner.pid, "SIGKILL"); } catch { /* exited */ }
    });
  }
};

afterEach(async () => {
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) {
    const residencyDirectory = path.join(root, "mesh", "residency");
    try {
      for (const entry of fs.readdirSync(residencyDirectory)) {
        const config = JSON.parse(
          fs.readFileSync(path.join(residencyDirectory, entry, "config.json"), "utf8"),
        ) as ResidentHostConfig;
        await stopResident(config);
      }
    } catch {
      // No resident host was created.
    }
    // A host that already removed owner.json (idle exit) may still be finishing its last mesh
    // writes: retry ENOTEMPTY briefly instead of failing the suite on a loaded runner.
    fs.rmSync(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 100 });
  }
});

describe("durable cwd validation", () => {
  it("rejects invalid recursive cwd before creating a resident host or request", async () => {
    const state = await rootHarness("resident-cwd-rejection");
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });

    try {
      await expect(
        client.spawnAgent({
          task: "must remain recursive",
          cwd: path.join(state.root, "missing"),
          recursive: true,
          residency: "durable",
        }),
      ).rejects.toThrow(/Invalid Fabric agent cwd.*ENOENT.*call spawn in the next message/);
      expect(fs.existsSync(path.join(state.config.residencyRoot, "owner.json"))).toBe(false);
      expect(fs.existsSync(path.join(state.config.residencyRoot, "requests"))).toBe(false);
    } finally {
      await client.close();
      await state.participants.close();
    }
  });
});

// smarty-dev#883: a timed-out start left its launcher (and Pi child) running
// with no owner. The client must end the launcher it spawned.
describe.skipIf(process.platform === "win32")("resident host start timeout", () => {
  it("ends the launcher and its child when the start budget runs out", async () => {
    const state = await rootHarness("resident-start-timeout");
    const client = new ResidencyClient({
      config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent,
      hostPath: path.resolve("tests/fixtures/stalled-launcher.mjs"), startupTimeoutMs: 300,
    });
    try {
      await expect(client.ensureHost()).rejects.toThrow(/Timed out after \d+ms starting Fabric resident host/);
      const log = fs.readFileSync(path.join(state.config.residencyRoot, "launcher.log"), "utf8");
      const launcher = (JSON.parse(log.trim().split("\n")[0]!) as { pid: number }).pid;
      const child = Number(fs.readFileSync(path.join(state.config.residencyRoot, "stalled-child.pid"), "utf8"));
      const alive = (pid: number) => { try { process.kill(pid, 0); return true; } catch { return false; } };
      // SIGTERM went to the whole group before the rejection; wait for both exits.
      await waitFor(() => !alive(launcher) && !alive(child), 30_000);
    } finally {
      await client.close();
      await state.participants.close();
    }
  });
});

// Known Windows limitation: cross-spawn of the pi binary through bun's
// node_modules shims hangs before the child starts, so the launcher never
// reaches its spawn trace. Durable residency E2E stays POSIX-only until that
// spawn path is resolved; the launcher logic tests below run everywhere.
describe("durable completion receipts", () => {
  const seedCompletion = async (state: RootHarness, status = "completed") => {
    const id = "a".repeat(32);
    const runDirectory = path.join(state.config.residencyRoot, "runs", id);
    const agentsPath = path.join(state.config.residencyRoot, "agents");
    fs.mkdirSync(runDirectory, { recursive: true });
    fs.mkdirSync(agentsPath, { recursive: true });
    const result = {
      id, name: "durable worker", status, text: "authoritative full result", task: "work",
      runner: "pi", transport: "process", cwd: state.root, startedAt: 1, updatedAt: 2, finishedAt: 2,
      turns: 1, toolCalls: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    };
    fs.writeFileSync(path.join(runDirectory, "status.json"), JSON.stringify(result));
    const metadataPath = path.join(agentsPath, `${id}.json`);
    fs.writeFileSync(metadataPath, JSON.stringify({
      format: RESIDENT_HOST_FORMAT, rootId: state.identity.id, id, runDirectory, handle: result, createdAt: 1, updatedAt: 2,
    }));
    const key = `${residentDeliveryPrefix(state.identity.id)}receipt-test`;
    await state.mesh.put({
      key, identity: { id: residentHostId(state.identity.id), name: "resident", kind: "main" }, ifVersion: 0,
      value: {
        format: RESIDENT_HOST_FORMAT, id: "receipt-test", rootId: state.identity.id,
        agentCompletionId: id, from: { id, name: result.name, kind: "agent" },
        delivery: "followUp", triggerTurn: true, message: "truncated legacy summary",
        data: { fabricTruncated: true }, createdAt: 2,
      },
    });
    return { id, result, runDirectory, metadataPath, key };
  };

  // smarty-dev#878: a resident host whose root is gone sends its actors' messages to the project's
  // project agent. That Main accepts an actor message from another root's resident host, and
  // still nothing from a writer that is not a resident host.
  it("delivers an actor message that another root's resident host addressed to this root", { timeout: 30_000 }, async () => {
    const state = await rootHarness("retargeted-delivery");
    const put = (id: string, writer: string, message: string) => state.mesh.put({
      key: `${residentDeliveryPrefix(state.identity.id)}${id}`,
      identity: { id: writer, name: "writer", kind: "agent" }, ifVersion: 0,
      value: {
        format: RESIDENT_HOST_FORMAT, id, rootId: state.identity.id,
        from: { id: "actor-1", name: "supervisor", kind: "actor" },
        delivery: "steer", triggerTurn: true, message, createdAt: 1,
      },
    });
    await put("from-resident", residentHostId("session:departed-root"), "steer from the departed root's actor");
    await put("from-intruder", "session:intruder", "steer from a writer that is not a resident host");
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent });
    try {
      client.start();
      await waitFor(() => state.deliveries.length >= 1);
      await new Promise((resolve) => setTimeout(resolve, 300));
      expect(state.deliveries.map((delivery) => delivery.message)).toEqual(["steer from the departed root's actor"]);
    } finally {
      await client.close();
      await state.participants.close();
    }
  });

  // smarty-dev#2236: one text reply from a resident actor (lucky-asc-router on gpt-6-sol, run
  // 5b660655: one run, one outgoing message, one fabric.actor.output) reached Main twice, 1.5 s
  // apart, because the client delivered a record before deleting it: any drain that saw the record
  // again delivered it again. A record is delivered only by the drainer that claims it.
  describe("a resident actor's delivery record", () => {
    const actorReply = async (state: RootHarness, policy: { delivery: "steer" | "followUp"; triggerTurn: boolean } = { delivery: "followUp", triggerTurn: true }) => {
      const id = "sol-text-reply";
      const key = `${residentDeliveryPrefix(state.identity.id)}${id}`;
      await state.mesh.put({
        key, identity: { id: residentHostId(state.identity.id), name: "resident", kind: "main" }, ifVersion: 0,
        value: {
          format: RESIDENT_HOST_FORMAT, id, rootId: state.identity.id,
          from: { id: "a50e8177eef74cf3a5adb16c068961f7", name: "lucky-asc-router", kind: "actor" },
          ...policy, message: "asc-router: <redacted one-line relay>", createdAt: 1,
        },
      });
      return key;
    };

    // A real Main whose Pi only queues what it is sent (prompt preflight, a settle): nothing is in
    // the session until the test appends it. Review round 2 on pi-fabric#160 (finding 1, S1).
    type Sent = { message: { details: Record<string, any> }; options?: { deliverAs?: string; triggerTurn?: boolean } | undefined };
    const realMain = async (state: RootHarness, entries: unknown[], modules?: { MainAgentController: typeof import("../src/main-agent.js").MainAgentController }, idle = true) => {
      const { MainAgentController } = modules ?? await import("../src/main-agent.js");
      const handlers = new Map<string, Array<(event: unknown, ctx: unknown) => void>>();
      const sent: Sent[] = [];
      const pi = {
        on: (name: string, fn: (event: unknown, ctx: unknown) => void) => { handlers.set(name, [...(handlers.get(name) ?? []), fn]); },
        sendMessage: (message: Sent["message"], options: Sent["options"]) => { sent.push({ message, options }); },
        getThinkingLevel: () => "off",
      };
      const ctx = { isIdle: () => idle, hasPendingMessages: () => false, signal: { aborted: false }, sessionManager: { getEntries: () => entries } };
      const main = new MainAgentController(pi as never, state.identity.id, true, repo, state.identity.sessionId);
      main.attachFollowUpDrain(ctx as never, 60_000, path.join(state.config.meshRoot, "main-followups", "root.json"));
      const emit = (name: string, event: unknown) => { for (const fn of handlers.get(name) ?? []) fn(event, ctx); };
      const append = (item: Sent) => entries.push({ type: "custom_message", customType: "pi-fabric-agent-message", details: item.message.details });
      return { main, sent, emit, append };
    };
    const settle = { outcome: "completed", context: { pendingMessages: [] } };

    it("is delivered once when its delete fails after it was read", { timeout: 30_000 }, async () => {
      const state = await rootHarness("delivery-delete-fails");
      const key = await actorReply(state);
      const main = await realMain(state, []);
      const original = state.mesh.delete.bind(state.mesh);
      let failures = 1;
      vi.spyOn(state.mesh, "delete").mockImplementation(async (input) => {
        if (failures-- > 0) throw new Error("Timed out waiting for the Fabric mesh lock");
        return original(input);
      });
      const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: main.main });
      try {
        client.start();
        await waitFor(() => state.mesh.get(key) === undefined);
        await new Promise((resolve) => setTimeout(resolve, 200));
        expect(main.sent).toHaveLength(1);
        expect(main.sent[0]!.message.details.deliveryId).toBe(`resident:${state.identity.id}:sol-text-reply`);
      } finally {
        await client.close();
        await state.participants.close();
      }
    });

    it("is delivered once when a second drainer read it before the first deleted it", { timeout: 30_000 }, async () => {
      const state = await rootHarness("delivery-stale-drainer");
      const key = await actorReply(state);
      const main = await realMain(state, []);
      const stale = state.mesh.listAll(residentDeliveryPrefix(state.identity.id));
      const first = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: main.main });
      // A second store on the same mesh whose recent-parse cache still holds the record.
      const staleMesh = new MeshStore(state.config.meshRoot, state.meshConfig.maxEventBytes, state.meshConfig.maxReadEvents);
      vi.spyOn(staleMesh, "listAll").mockImplementation(() => structuredClone(stale));
      const second = new ResidencyClient({ config: state.config, mesh: staleMesh, participants: state.participants, mainAgent: main.main });
      try {
        first.start();
        await waitFor(() => state.mesh.get(key) === undefined && main.sent.length > 0);
        await first.close();
        second.start();
        await new Promise((resolve) => setTimeout(resolve, 200));
        expect(main.sent).toHaveLength(1);
      } finally {
        await first.close();
        await second.close();
        await state.participants.close();
      }
    });

    // Review round 1 on pi-fabric#160: a Main that dies before it took the reply must find it after
    // its restart (a delete-first claim lost it).
    it("survives a Main that dies before taking it, and reaches the restarted Main once", { timeout: 30_000 }, async () => {
      const state = await rootHarness("delivery-main-dies");
      const key = await actorReply(state);
      const dying = { ...state.mainAgent, deliverAgent: () => { throw new Error("Main process exited"); } };
      const before = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: dying });
      try {
        before.start();
        await new Promise((resolve) => setTimeout(resolve, 300));
      } finally {
        await before.close();
      }
      expect(state.mesh.get(key)).toBeDefined();
      const main = await realMain(state, []);
      const after = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: main.main });
      try {
        after.start();
        await waitFor(() => state.mesh.get(key) === undefined);
        await new Promise((resolve) => setTimeout(resolve, 200));
        expect(main.sent).toHaveLength(1);
      } finally {
        await after.close();
        await state.participants.close();
      }
    });

    // Review round 2 on pi-fabric#160: deliverAgent returned while the reply sat only in Pi's
    // volatile queue behind a prompt preflight; the record was deleted, then Main died.
    it("is journalled by Main before its record goes, and a restarted Main replays it exactly once", { timeout: 30_000 }, async () => {
      const state = await rootHarness("delivery-preflight-dies");
      const key = await actorReply(state);
      const entries: unknown[] = [];
      const dying = await realMain(state, entries);
      const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: dying.main });
      try {
        client.start();
        await waitFor(() => state.mesh.get(key) === undefined);
      } finally {
        await client.close();
      }
      expect(dying.sent).toHaveLength(1);                                           // queued in Pi, never in the session
      // Main dies (no close); a new controller on the same journal and session.
      const restarted = await realMain(state, entries);
      restarted.emit("agent_before_settle", settle);
      expect(restarted.sent.map((item) => item.message.details.id)).toEqual([dying.sent[0]!.message.details.id]);
      restarted.append(restarted.sent[0]!);
      restarted.emit("agent_settled", { outcome: "completed" });
      restarted.emit("agent_before_settle", settle);
      expect(restarted.sent).toHaveLength(1);
      await state.participants.close();
    });

    // Review round 2 on pi-fabric#160: a module-local set of delivered ids does not survive a
    // release reload, which loads a fresh copy of every module.
    it("is not delivered twice after a failed delete and a release reload", { timeout: 30_000 }, async () => {
      const state = await rootHarness("delivery-release-reload");
      const key = await actorReply(state);
      const entries: unknown[] = [];
      const old = await realMain(state, entries);
      const original = state.mesh.delete.bind(state.mesh);
      const failing = vi.spyOn(state.mesh, "delete").mockRejectedValue(new Error("Timed out waiting for the Fabric mesh lock"));
      const before = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: old.main });
      try {
        before.start();
        await waitFor(() => old.sent.length > 0 && failing.mock.calls.length > 0);
      } finally {
        await before.close();
      }
      old.append(old.sent[0]!);                                                     // the session holds it
      old.emit("agent_settled", { outcome: "completed" });
      failing.mockImplementation(original);
      vi.resetModules();                                                            // a new release generation
      const [{ ResidencyClient: FreshClient }, freshMain] = await Promise.all([
        import("../src/residency/client.js"),
        import("../src/main-agent.js"),
      ]);
      const fresh = await realMain(state, entries, freshMain);
      const after = new FreshClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: fresh.main });
      try {
        after.start();
        await waitFor(() => state.mesh.get(key) === undefined);
        await new Promise((resolve) => setTimeout(resolve, 200));
        expect(fresh.sent).toHaveLength(0);
      } finally {
        await after.close();
        await state.participants.close();
      }
    });

    // Security round 3 on pi-fabric#160 (S2): a passive reply (a non-triggering followUp; a nextTurn
    // delivery reaches Main as one, src/residency/host.ts) keeps its policy across a restart and a
    // release reload: appended once, triggerTurn false, and no Main turn of its own.
    it("keeps a passive reply passive when a restarted, reloaded Main replays it", { timeout: 30_000 }, async () => {
      const state = await rootHarness("delivery-passive-replay");
      const key = await actorReply(state, { delivery: "followUp", triggerTurn: false });
      const entries: unknown[] = [];
      const dying = await realMain(state, entries, undefined, false);             // Main is streaming
      const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: dying.main });
      try {
        client.start();
        await waitFor(() => state.mesh.get(key) === undefined);
      } finally {
        await client.close();
      }
      expect(dying.sent.map((item) => item.options)).toEqual([{ deliverAs: "followUp", triggerTurn: false }]);
      // Main dies before Pi appends it; the restart loads a new release generation.
      vi.resetModules();
      const fresh = await realMain(state, entries, await import("../src/main-agent.js"), false);
      fresh.emit("agent_before_settle", settle);
      expect(fresh.sent.map((item) => item.message.details.id)).toEqual([dying.sent[0]!.message.details.id]);
      expect(fresh.sent[0]!.options).toEqual({ deliverAs: "followUp", triggerTurn: false });
      expect(fresh.sent[0]!.message.details.triggerTurn).toBe(false);
      fresh.append(fresh.sent[0]!);
      fresh.emit("agent_settled", { outcome: "completed" });
      fresh.emit("agent_before_settle", settle);
      expect(fresh.sent).toHaveLength(1);                                           // once, no triggering release
      expect(fs.existsSync(path.join(state.config.meshRoot, "main-followups", "root.json"))).toBe(false);
      await state.participants.close();
    });

    // Astra round 3 finding 2 on pi-fabric#160: shutdown closes Main's journal first; a drain in
    // that window must keep the record, and the next Main gets it once.
    it("is kept when Main's journal is already closed, and the next Main takes it once", { timeout: 30_000 }, async () => {
      const state = await rootHarness("delivery-journal-closed");
      const key = await actorReply(state);
      const closing = await realMain(state, []);
      closing.main.closeFollowUpDrain();
      const during = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: closing.main });
      try {
        during.start();
        await new Promise((resolve) => setTimeout(resolve, 300));
      } finally {
        await during.close();
      }
      expect(closing.sent).toHaveLength(0);
      expect(state.mesh.get(key)).toBeDefined();
      const next = await realMain(state, []);
      const after = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: next.main });
      try {
        after.start();
        await waitFor(() => state.mesh.get(key) === undefined);
        expect(next.sent).toHaveLength(1);
      } finally {
        await after.close();
        await state.participants.close();
      }
    });

    it("is kept for a later drain when Main refuses it", { timeout: 30_000 }, async () => {
      const state = await rootHarness("delivery-refused");
      const key = await actorReply(state);
      const main = await realMain(state, []);
      const deliver = main.main.deliverAgent.bind(main.main);
      let refusals = 1;
      let keptAtRefusal = false;
      vi.spyOn(main.main, "deliverAgent").mockImplementation((request) => {
        // Review round 1 on pi-fabric#160: until Main took it, the record is the only copy.
        if (refusals-- > 0) {
          keptAtRefusal = state.mesh.get(key) !== undefined;
          throw new Error("Main's followUp queue is full");
        }
        return deliver(request);
      });
      const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: main.main });
      try {
        client.start();
        await waitFor(() => main.sent.length > 0 && state.mesh.get(key) === undefined);
        expect(keptAtRefusal).toBe(true);
        await new Promise((resolve) => setTimeout(resolve, 200));
        expect(main.sent).toHaveLength(1);
      } finally {
        await client.close();
        await state.participants.close();
      }
    });
  });

  // review/astra on 3257dba, D1: the durable fallback cleanup keeps a possibly live worker's files.
  it("refuses the fallback cleanup of a durable run marked with an unresolved worker", async () => {
    const state = await rootHarness("unresolved-cleanup");
    const seeded = await seedCompletion(state, "failed");
    markUnresolvedWorker(seeded.runDirectory, "the Herdr server has been unreachable for 300 s");
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent });
    try {
      await expect(client.cleanupAgent(seeded.id)).rejects.toThrow(/may still be running/);
      expect(fs.existsSync(seeded.runDirectory)).toBe(true);
      expect(fs.existsSync(seeded.metadataPath)).toBe(true);
    } finally {
      await client.close();
      await state.participants.close();
    }
  });

  it("refuses the fallback cleanup of a completed durable run whose nested child is marked", async () => {
    const state = await rootHarness("unresolved-nested-cleanup");
    const seeded = await seedCompletion(state, "completed");
    markUnresolvedWorker(path.join(seeded.runDirectory, "nested", "child"), "the Herdr server has been unreachable for 300 s");
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent });
    try {
      await expect(client.cleanupAgent(seeded.id)).rejects.toThrow(/may still be running/);
      expect(fs.existsSync(path.join(seeded.runDirectory, "nested", "child"))).toBe(true);
    } finally {
      await client.close();
      await state.participants.close();
    }
  });

  it("retracts an already queued completion on late wait and persists the receipt across reconnects", async () => {
    const state = await rootHarness("late-completion-wait");
    const seeded = await seedCompletion(state);
    const onBackgroundComplete = vi.fn();
    const onResultConsumed = vi.fn();
    const options = { config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent, onBackgroundComplete, onResultConsumed };
    const client = new ResidencyClient(options);
    const reconnect = new ResidencyClient(options);
    try {
      client.start();
      await waitFor(() => onBackgroundComplete.mock.calls.length > 0);
      expect(onBackgroundComplete.mock.calls[0]![0].text).toBe("authoritative full result");
      expect(state.deliveries).toHaveLength(0);
      expect(state.mesh.listAll(residentDeliveryPrefix(state.identity.id))).toHaveLength(1);
      expect((await client.waitAgent(seeded.id)).status).toBe("completed");
      expect(onResultConsumed).toHaveBeenCalledWith(seeded.id);
      expect(JSON.parse(fs.readFileSync(seeded.metadataPath, "utf8")).completionConsumedAt).toBeGreaterThan(0);
      await client.close();
      onBackgroundComplete.mockClear();
      reconnect.start();
      await waitFor(() => state.mesh.listAll(residentDeliveryPrefix(state.identity.id)).length === 0);
      expect(onBackgroundComplete).not.toHaveBeenCalled();
      expect(state.deliveries).toHaveLength(0);
    } finally {
      await client.close();
      await reconnect.close();
      await state.participants.close();
    }
  });

  it("retains an unread envelope across disconnect and acknowledges only actual inbox delivery", async () => {
    const state = await rootHarness("unread-completion-resume");
    const seeded = await seedCompletion(state);
    const onBackgroundComplete = vi.fn();
    const options = { config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent, onBackgroundComplete };
    const client = new ResidencyClient(options);
    const reconnect = new ResidencyClient(options);
    try {
      client.start();
      await waitFor(() => onBackgroundComplete.mock.calls.length > 0);
      await client.close();
      expect(JSON.parse(fs.readFileSync(seeded.metadataPath, "utf8")).completionConsumedAt).toBeUndefined();
      onBackgroundComplete.mockClear();
      reconnect.start();
      await waitFor(() => onBackgroundComplete.mock.calls.length > 0);
      onBackgroundComplete.mock.calls[0]![1]();
      await waitFor(() => state.mesh.listAll(residentDeliveryPrefix(state.identity.id)).length === 0);
      expect(JSON.parse(fs.readFileSync(seeded.metadataPath, "utf8")).completionConsumedAt).toBeGreaterThan(0);
    } finally {
      await client.close();
      await reconnect.close();
      await state.participants.close();
    }
  });

  it("honors disabled completion notifications for envelopes from an older resident host", async () => {
    const state = await rootHarness("disabled-completion-resume");
    await seedCompletion(state);
    state.config.agents.notifyOnComplete = false;
    const onBackgroundComplete = vi.fn();
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent, onBackgroundComplete });
    try {
      client.start();
      await waitFor(() => state.mesh.listAll(residentDeliveryPrefix(state.identity.id)).length === 0);
      expect(onBackgroundComplete).not.toHaveBeenCalled();
      expect(state.deliveries).toHaveLength(0);
    } finally {
      await client.close();
      await state.participants.close();
    }
  });

  // smarty-dev#1882: an idle resident host removes runs/; status must not fall back to the spawn handle.
  it("reads the saved terminal record after the host removed the run directory", async () => {
    const state = await rootHarness("saved-terminal-record");
    const seeded = await seedCompletion(state);
    const resultPath = residentResultPath(state.config.residencyRoot, seeded.id);
    fs.mkdirSync(path.dirname(resultPath), { recursive: true });
    fs.writeFileSync(resultPath, JSON.stringify(seeded.result));
    fs.rmSync(path.join(state.config.residencyRoot, "runs"), { recursive: true, force: true });
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent });
    try {
      expect(client.statusAgent(seeded.id)).toMatchObject({ status: "completed", text: "authoritative full result", residency: "durable" });
      expect(await client.waitAgent(seeded.id, AbortSignal.timeout(2_000))).toMatchObject({ status: "completed", text: "authoritative full result" });
      await expect(client.cleanupAgent(seeded.id)).resolves.toEqual({ cleaned: true });
      expect(fs.existsSync(resultPath)).toBe(false);
      expect(client.hasAgent(seeded.id)).toBe(false);
    } finally {
      await client.close();
      await state.participants.close();
    }
  });

  it("reports a run with no record, no run directory and no live host as failed, and a live run as running", async () => {
    const state = await rootHarness("lost-terminal-record");
    const seeded = await seedCompletion(state, "running");
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent });
    try {
      fs.rmSync(path.join(seeded.runDirectory, "status.json"));
      expect(client.statusAgent(seeded.id).status).toBe("running");
      fs.rmSync(path.join(state.config.residencyRoot, "runs"), { recursive: true, force: true });
      expect(client.statusAgent(seeded.id)).toMatchObject({ status: "failed", error: expect.stringMatching(/record lost/) });
      expect((await client.waitAgent(seeded.id, AbortSignal.timeout(2_000))).status).toBe("failed");
    } finally {
      await client.close();
      await state.participants.close();
    }
  });

  // review/astra on pi-fabric#136: while a host lives, a stopped attempt may still resume or retry.
  it("treats a terminal status.json as settled only once no host owns the run or the host saved it", async () => {
    const state = await rootHarness("settled-terminal-record");
    const seeded = await seedCompletion(state, "stopped");
    const ownerPath = path.join(state.config.residencyRoot, "owner.json");
    fs.writeFileSync(ownerPath, JSON.stringify({
      format: RESIDENT_HOST_FORMAT, hostId: residentHostId(state.identity.id), pid: process.pid, token: "t", startedAt: 1, readyAt: 1,
    }));
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent });
    try {
      expect(client.settledAgent(seeded.id)).toBeUndefined();
      const resultPath = residentResultPath(state.config.residencyRoot, seeded.id);
      fs.mkdirSync(path.dirname(resultPath), { recursive: true });
      fs.writeFileSync(resultPath, JSON.stringify({ ...seeded.result, status: "completed" }));
      expect(client.settledAgent(seeded.id)).toMatchObject({ status: "completed" });
      fs.rmSync(resultPath);
      fs.rmSync(ownerPath);
      expect(client.settledAgent(seeded.id)).toMatchObject({ status: "stopped" });
    } finally {
      await client.close();
      await state.participants.close();
    }
  });

  it("does not consume a result when a durable wait is aborted", async () => {
    const state = await rootHarness("aborted-completion-wait");
    const seeded = await seedCompletion(state, "running");
    const onBackgroundComplete = vi.fn();
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent, onBackgroundComplete });
    try {
      await expect(client.waitAgent(seeded.id, AbortSignal.abort())).rejects.toThrow("aborted");
      expect(JSON.parse(fs.readFileSync(seeded.metadataPath, "utf8")).completionConsumedAt).toBeUndefined();
      fs.writeFileSync(path.join(seeded.runDirectory, "status.json"), JSON.stringify({ ...seeded.result, status: "completed" }));
      client.start();
      await waitFor(() => onBackgroundComplete.mock.calls.length > 0);
      expect(state.deliveries).toHaveLength(0);
    } finally {
      await client.close();
      await state.participants.close();
    }
  });
});

describe.skipIf(!hasResidentHost || process.platform === "win32")("durable participant residency", () => {
  it.skipIf(process.platform !== "linux")("replaces owner and host lock whose live PID has a different start time", { timeout: 45_000 }, async () => {
    const state = await rootHarness("resident-reused-pid");
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh,
      participants: state.participants, mainAgent: state.mainAgent, hostPath });
    fs.mkdirSync(state.config.residencyRoot, { recursive: true });
    const stale = { format: RESIDENT_HOST_FORMAT, hostId: client.hostId, pid: process.pid,
      processStartTime: "0", token: "stale", startedAt: 0, readyAt: 0 };
    for (const file of ["owner.json", "host.lock"]) fs.writeFileSync(path.join(state.config.residencyRoot, file), JSON.stringify(stale));
    try {
      const actor = await client.createActor({ name: "PID survivor", instructions: "Keep mailbox.", residency: "durable" });
      const owner = await client.ensureHost();
      expect(owner.pid).not.toBe(process.pid);
      expect(owner.processStartTime).toBe(processStartTime(owner.pid));
      await client.removeActor(actor.id);
    } finally {
      await client.close();
      // The fail-on-base probe leaves the planted owner in place; never signal it.
      const ownerPath = path.join(state.config.residencyRoot, "owner.json");
      if (JSON.parse(fs.readFileSync(ownerPath, "utf8")).pid === process.pid) fs.rmSync(ownerPath);
      await stopResident(state.config); await state.participants.close();
    }
  });

  it.each([
    ["SIGTERM", false], ["SIGKILL", false], ["SIGKILL", true],
  ] as const)("relaunches a dead durable host on ordinary tell after %s (fresh dead mesh lock: %s)", { timeout: 100_000 }, async (signal, deadMeshLock) => {
    const state = await rootHarness(`resident-relaunch-${signal}`);
    const launches = launchLog(state.root);
    for (const [key, value] of Object.entries(launches.env)) vi.stubEnv(key, value);
    const agents = new AgentManager(repo, state.config.agents, {
      workerPath: fakeWorker, runRoot: path.join(state.root, "parent-runs"),
      mainAgentId: state.identity.id, meshRoot: state.config.meshRoot,
      projectRoot: repo, hostId: state.identity.id, identityId: state.identity.id,
    });
    const actors = new ActorManager(state.config.sessionId, state.identity, state.mesh,
      state.meshConfig, agents, () => {}, {
        actorRoot: state.config.actorRoot, persistent: true, claimResidency: "session", rootId: state.identity.id,
        canManageActor: (id) => state.participants.get(id)?.ownerHostId === state.identity.id,
      });
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh,
      participants: state.participants, mainAgent: state.mainAgent, hostPath });
    // Deliberately do not start the watchdog: this is the ordinary message path.
    const control = new FabricControlPlane(state.mesh, state.identity, {
      enabled: true, hostId: state.identity.id, pollMs: 20, acknowledgementTimeoutMs: 3_000,
    });
    control.start(() => ({ accepted: false }));
    const router = new AgentMessageRouter(agents, actors, state.mainAgent, state.participants,
      control, (binding) => binding, client);
    const ownerPath = path.join(state.config.residencyRoot, "owner.json");
    const owner = () => JSON.parse(fs.readFileSync(ownerPath, "utf8")) as ResidentHostOwner;
    const registry = () => JSON.parse(fs.readFileSync(path.join(state.config.actorRoot, "actors.json"), "utf8")) as
      { actors: Array<{ id: string; messages: Array<{ text?: string; data?: { message?: string } }>; queue: unknown[] }> };
    let senderRestart: Promise<void> | undefined;
    try {
      const actor = await client.createActor({ name: "restart survivor", instructions: "Keep id and mailbox.", residency: "durable", coalesce: false });
      await router.routeMessage(actor.id, "before death", undefined, "followUp");
      await waitFor(() => (registry().actors.find((item) => item.id === actor.id)?.messages.length ?? 0) >= 2);
      const before = registry().actors.find((item) => item.id === actor.id)!;
      const killed = owner();
      // Never signal a PID that outlived our launch identity.
      const recorded = launches.owned().find((entry) => entry.pid === killed.pid)!;
      expect(recorded).toBeDefined();
      expect(same(recorded)).toBe(true);
      if (killed.processStartTime) expect(killed.processStartTime).toBe(recorded.started);
      process.kill(killed.pid, signal);
      await waitFor(() => !same(recorded), 20_000);
      // Send immediately: a dead holder is still unreclaimable during MeshStore's 30 s stale
      // window. Recovery keeps the tell pending; latency is bounded by that window plus boot/delivery.
      if (deadMeshLock) {
        // Deterministically model SIGKILL inside a mesh write, without changing MeshStore's fence.
        // Pause our own mesh publishers so they cannot race this owner-file publication.
        await state.participants.close();
        await control.close();
        const meshLock = path.join(state.config.meshRoot, ".lock");
        await waitFor(() => !fs.existsSync(meshLock), 35_000);
        fs.mkdirSync(meshLock);
        fs.writeFileSync(path.join(meshLock, "owner"), `dead-host\n${killed.pid}\n${Date.now()}\n`);
        // Resume the sender after publishing the complete dead identity.
        senderRestart = state.participants.start().catch(() => undefined);
        control.start(() => ({ accepted: false }));
      }
      const started = Date.now();
      const result = await router.routeMessage(actor.id, `after ${signal}`, undefined, "followUp");
      expect(result.queued).toBe(true);
      expect(Date.now() - started).toBeLessThan(30_000 + 15_000);
      await senderRestart;
      expect(owner().pid).not.toBe(killed.pid);
      await router.routeMessage(actor.id, "queued successor", undefined, "followUp");
      await waitFor(() => (registry().actors.find((item) => item.id === actor.id)?.messages.length ?? 0) >= before.messages.length + 4);
      const after = registry().actors.find((item) => item.id === actor.id)!;
      expect(after.id).toBe(actor.id);
      expect(after.messages.slice(0, before.messages.length)).toEqual(before.messages);
      expect(after.messages.map((item) => item.data?.message)).toEqual(expect.arrayContaining([`after ${signal}`, "queued successor"]));
      await client.removeActor(actor.id);
    } finally {
      await senderRestart;
      await client.close();
      await stopResident(state.config);
      await control.close();
      await actors.close();
      await agents.close();
      await state.participants.close();
    }
  });

  it("keeps a durable actor responsive after its originating Main closes", { timeout: 45_000 }, async () => {
    const state = await rootHarness("resident-actor");
    const agents = new AgentManager(repo, state.config.agents, {
      workerPath: fakeWorker,
      runRoot: path.join(state.root, "parent-runs"),
      mainAgentId: state.identity.id,
      meshRoot: state.config.meshRoot,
      projectRoot: repo,
      hostId: state.identity.id,
      identityId: state.identity.id,
    });
    const canManage = (id: string): boolean | undefined => {
      const participant = state.participants.get(id);
      return participant ? participant.ownerHostId === state.identity.id : undefined;
    };
    const actors = new ActorManager(
      state.config.sessionId,
      state.identity,
      state.mesh,
      state.meshConfig,
      agents,
      () => {},
      {
        actorRoot: state.config.actorRoot,
        persistent: true,
        canManageActor: canManage,
        claimResidency: "session",
        rootId: state.identity.id,
      },
    );
    state.participants.registerSource(() =>
      actors.listOwned().map((actor) =>
        actorParticipantRecord(
          actor,
          state.identity.id,
          state.identity.id,
          state.identity.id,
          state.identity.id,
        ),
      ),
    );
    actors.subscribe(() => state.participants.scheduleRefresh());
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });
    const actor = await actors.create({
      name: "resident actor",
      instructions: "Persist across Main shutdown.",
      residency: "durable",
      delivery: "mailbox",
    });
    await actors.cede(actor.id);
    await state.participants.refresh();
    await client.ensureActor(actor.id);

    const messageCount = (): number => {
      const registry = JSON.parse(
        fs.readFileSync(path.join(state.config.actorRoot, "actors.json"), "utf8"),
      ) as { actors: Array<{ id: string; messages?: unknown[] }> };
      return registry.actors.find((candidate) => candidate.id === actor.id)?.messages?.length ?? 0;
    };
    const originalControl = new FabricControlPlane(state.mesh, state.identity, {
      enabled: true,
      hostId: state.identity.id,
      pollMs: 20,
      acknowledgementTimeoutMs: 3_000,
    });
    originalControl.start(() => ({ accepted: false }));
    await originalControl.request(
      client.hostId,
      actor.id,
      "followUp",
      { message: "before Main shutdown" },
      client.hostId,
    );
    await waitFor(() => messageCount() >= 2);
    await originalControl.close();

    await actors.close();
    await agents.close();
    await state.participants.close();
    await client.close();

    const peerIdentity: MeshIdentity = {
      id: `session:peer:${randomId()}`,
      name: "peer",
      kind: "main",
      sessionId: "peer",
    };
    const peerControl = new FabricControlPlane(state.mesh, peerIdentity, {
      enabled: true,
      hostId: peerIdentity.id,
      pollMs: 20,
      acknowledgementTimeoutMs: 3_000,
    });
    peerControl.start(() => ({ accepted: false }));
    const before = messageCount();
    await peerControl.request(
      residentHostId(state.identity.id),
      actor.id,
      "followUp",
      { message: "after Main shutdown" },
      residentHostId(state.identity.id),
    );
    await waitFor(() => messageCount() >= before + 2);
    await peerControl.request(
      residentHostId(state.identity.id),
      actor.id,
      "stop",
      {},
      residentHostId(state.identity.id),
    );
    await peerControl.close();

    expect(messageCount()).toBeGreaterThanOrEqual(before + 2);
    const detachedParticipants: FabricParticipantSource = {
      list: () => [],
      get: () => undefined,
      self: () => {
        throw new Error("not used by detached residency client");
      },
      peers: () => [],
      async refresh() {},
      scheduleRefresh() {},
    };
    const reconnect = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: detachedParticipants,
      mainAgent: state.mainAgent,
      hostPath,
    });
    await expect(reconnect.removeActor(actor.id)).resolves.toEqual({ removed: true });
    // review/astra on pi-fabric#136: actor activations leave no saved durable task result.
    const resultsDir = path.join(state.config.residencyRoot, "results");
    expect(fs.existsSync(resultsDir) ? fs.readdirSync(resultsDir) : []).toEqual([]);
    const registry = JSON.parse(
      fs.readFileSync(path.join(state.config.actorRoot, "actors.json"), "utf8"),
    ) as { actors: Array<{ id: string }> };
    expect(registry.actors.some((candidate) => candidate.id === actor.id)).toBe(false);
    await reconnect.close();
  });

  // smarty-dev#2184 item 8: a removal waited for the actor's in-flight run and blocked every
  // later request to the host (clients timed out; a create right after it was dropped).
  it("returns a removal behind an in-flight run at once and keeps serving requests", { timeout: 60_000 }, async () => {
    const testStarted = Date.now();
    const state = await rootHarness("resident-remove-pending");
    state.config.agents = { ...state.config.agents, timeoutMs: 120_000 };
    // The resident host and its workers inherit this and record themselves at launch: the worker
    // to kill is found there, never by a host-wide command-line match (pi-fabric#160 S4).
    const launches = launchLog(state.root);
    const savedEnv = Object.fromEntries(Object.keys(launches.env).map((key) => [key, process.env[key]]));
    Object.assign(process.env, launches.env);
    const client = new ResidencyClient({
      config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent, hostPath,
    });
    const control = new FabricControlPlane(state.mesh, state.identity, {
      enabled: true, hostId: state.identity.id, pollMs: 20, acknowledgementTimeoutMs: 5_000,
    });
    control.start(() => ({ accepted: false }));
    try {
      const request = { name: "hung reviewer", instructions: "Review.", residency: "durable" as const, delivery: "mailbox" as const };
      const actor = await client.createActor(request);
      await control.request(client.hostId, actor.id, "followUp", { message: "HANG_WITH_PROGRESS" }, client.hostId);
      await waitFor(() => state.participants.get(actor.id, undefined, { fresh: true })?.actorRun !== undefined, 30_000);
      const runId = state.participants.get(actor.id, undefined, { fresh: true })!.actorRun!.id;

      const started = Date.now();
      const removed = await client.removeActor(actor.id);
      expect(Date.now() - started).toBeLessThan(2_000);
      expect(removed.pending).toContain(`pending behind its in-flight run ${runId}`);
      expect(client.hostStateNote()).toContain(`removal of hung reviewer (${actor.id}) is pending behind its in-flight run ${runId}`);

      // The queue is free: a same-name create right after the removal succeeds.
      const successor = await client.createActor(request);
      expect(successor.id).not.toBe(actor.id);

      // The old run ends (its worker is killed); the removal then finishes and cleans up.
      // Of the processes this fixture launched, the one started as the worker of this run.
      const workers = launches.owned().filter(({ argv }) =>
        argv[0] === fakeWorker && argv[argv.indexOf("--id") + 1] === runId);
      expect(workers).toHaveLength(1);
      const worker = workers[0]!;
      expect(startedAtMs(worker.started)).toBeGreaterThanOrEqual(testStarted - 1_000);
      // Revalidated just before the signal: the pid still names the recorded process.
      expect(same(worker)).toBe(true);
      process.kill(worker.pid, "SIGKILL");
      const registryIds = (): string[] => [state.config.actorRoot, state.config.sessionActorRoot!].flatMap((root) => {
        try {
          return (JSON.parse(fs.readFileSync(path.join(root, "actors.json"), "utf8")) as { actors: Array<{ id: string }> })
            .actors.map((entry) => entry.id);
        } catch { return []; }
      });
      expect(registryIds()).toContain(actor.id);
      await waitFor(() => !registryIds().includes(actor.id), 30_000);
      expect(registryIds()).toContain(successor.id);
      await waitFor(() => client.hostStateNote() === "", 5_000);
      await client.removeActor(successor.id);
    } finally {
      await control.close();
      await client.close();
      await stopResident(state.config);
      await state.participants.close();
      for (const [key, value] of Object.entries(savedEnv)) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
  });

  it("queues passive actor delivery until Main resumes", { timeout: 45_000 }, async () => {
    const state = await rootHarness("resident-delivery");
    const agents = new AgentManager(repo, state.config.agents, {
      workerPath: fakeWorker,
      runRoot: path.join(state.root, "parent-delivery-runs"),
      mainAgentId: state.identity.id,
      meshRoot: state.config.meshRoot,
      projectRoot: repo,
      hostId: state.identity.id,
      identityId: state.identity.id,
    });
    const actors = new ActorManager(
      state.config.sessionId,
      state.identity,
      state.mesh,
      state.meshConfig,
      agents,
      () => {},
      {
        actorRoot: state.config.actorRoot,
        persistent: true,
        claimResidency: "session",
        rootId: state.identity.id,
      },
    );
    state.participants.registerSource(() =>
      actors.listOwned().map((actor) =>
        actorParticipantRecord(
          actor,
          state.identity.id,
          state.identity.id,
          state.identity.id,
          state.identity.id,
        ),
      ),
    );
    actors.subscribe(() => state.participants.scheduleRefresh());
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });
    const actor = await actors.create({
      name: "resident delivery",
      instructions: "Reply to every message.",
      residency: "durable",
      delivery: "followUp",
      triggerTurn: false,
    });
    await actors.cede(actor.id);
    await state.participants.refresh();
    await client.ensureActor(actor.id);
    const control = new FabricControlPlane(state.mesh, state.identity, {
      enabled: true,
      hostId: state.identity.id,
      pollMs: 20,
      acknowledgementTimeoutMs: 3_000,
    });
    control.start(() => ({ accepted: false }));
    await control.request(client.hostId, actor.id, "followUp", { message: "respond" }, client.hostId);
    const prefix = residentDeliveryPrefix(state.identity.id);
    await waitFor(() => state.mesh.listAll(prefix).length === 1);
    expect(state.deliveries).toEqual([]);

    client.start();
    await waitFor(() => state.deliveries.length === 1);
    expect(state.deliveries[0]).toMatchObject({
      from: { id: actor.id, kind: "actor" },
      delivery: "followUp",
      triggerTurn: false,
      message: "fake worker complete",
    });
    // Delivery lands before MeshStore.delete completes its locked write, so
    // poll the queue drain instead of asserting the removal synchronously.
    await waitFor(() => state.mesh.listAll(prefix).length === 0);
    expect(state.mesh.listAll(prefix)).toEqual([]);

    await client.removeActor(actor.id);
    await control.close();
    await client.close();
    await actors.close();
    await agents.close();
    await state.participants.close();
  });

  it("creates durable actors through root and nested owner channels while Main owns the registry", { timeout: 45_000 }, async () => {
    const state = await rootHarness("resident-recruitment");
    const agents = new AgentManager(repo, state.config.agents, {
      workerPath: fakeWorker,
      runRoot: path.join(state.root, "recruitment-runs"),
      mainAgentId: state.identity.id,
      meshRoot: state.config.meshRoot,
      projectRoot: repo,
      hostId: state.identity.id,
      identityId: state.identity.id,
    });
    const canManage = (id: string): boolean | undefined => {
      const participant = state.participants.get(id);
      return participant ? participant.ownerHostId === state.identity.id : undefined;
    };
    const actors = new ActorManager(
      state.config.sessionId,
      state.identity,
      state.mesh,
      state.meshConfig,
      agents,
      () => {},
      {
        actorRoot: state.config.actorRoot,
        persistent: true,
        canManageActor: canManage,
        claimResidency: "session",
        rootId: state.identity.id,
      },
    );
    state.participants.registerSource(() =>
      actors.listOwned().map((actor) =>
        actorParticipantRecord(
          actor,
          state.identity.id,
          state.identity.id,
          state.identity.id,
          state.identity.id,
        ),
      ),
    );
    actors.subscribe(() => state.participants.scheduleRefresh());
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });

    try {
      const mainOwned = await actors.create({
        name: "main registry owner",
        instructions: "Keep the local registry guarded.",
        residency: "session",
      });
      await state.participants.refresh();
      expect(state.participants.get(mainOwned.id)?.ownerHostId).toBe(state.identity.id);

      const first = await client.createActor({
        name: "recruited architect",
        instructions: "Design the bounded change.",
        residency: "durable",
      });
      expect(state.participants.get(first.id)).toMatchObject({
        ownerHostId: client.hostId,
        residency: "durable",
      });

      const recruitment = new ResidentActorClient(
        state.config.meshRoot,
        state.identity.id,
      );
      const second = await recruitment.createActor({
        scope: "session",
        name: "recruited advisor",
        instructions: "Challenge the design.",
        residency: "durable",
      });
      await waitFor(() => state.participants.get(second.id)?.ownerHostId === client.hostId);

      const residentActors = [first, second];
      expect(residentActors.map((actor) => actor.name).sort()).toEqual([
        "recruited advisor",
        "recruited architect",
      ]);
      expect(residentActors.every((actor) => actor.residency === "durable")).toBe(true);
      expect(first.scope).toBe("project");
      expect(second.scope).toBe("session");
      // smarty-dev#878: resident-side creation records the creating project too.
      expect(residentActors.map((actor) => actor.project)).toEqual([projectOf(state.config.cwd), projectOf(state.config.cwd)]);
      expect(second.sessionFile).toContain(path.join(state.config.sessionActorRoot!, second.id));
      expect(new Set(residentActors.map((actor) => actor.sessionFile)).size).toBe(2);

      for (const actor of residentActors) await client.removeActor(actor.id);
      await actors.remove(mainOwned.id);
    } finally {
      await client.close();
      await actors.close();
      await agents.close();
      await state.participants.close();
    }
  });

  it("rejects a durable actor model that became hidden at the resident owner", { timeout: 45_000 }, async () => {
    const state = await rootHarness("resident-hidden-model");
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });
    const control = new FabricControlPlane(state.mesh, state.identity, {
      enabled: true,
      hostId: state.identity.id,
      pollMs: 20,
      acknowledgementTimeoutMs: 3_000,
    });
    control.start(() => ({ accepted: false }));

    try {
      const actor = await client.createActor({
        name: "visibility witness",
        instructions: "Run only while the bound model remains visible.",
        residency: "durable",
        runner: "pi",
        model: "provider/visible",
      });
      state.config.piModels = {
        available: [],
        aliases: {},
        defaultModel: "provider/visible",
      };
      await client.ensureHost();

      await expect(
        control.request(
          client.hostId,
          actor.id,
          "followUp",
          { message: "Do not launch the hidden binding" },
          client.hostId,
        ),
      ).rejects.toThrow(/not available to this Pi session/);
      await client.removeActor(actor.id);
    } finally {
      await control.close();
      await client.close();
      await state.participants.close();
      await stopResident(state.config);
    }
  });

  it("resolves a model added to models.json after the resident host started, with no restart", { timeout: 60_000 }, async () => {
    // pi-fabric#138: the resident host runs in its own Pi process; its registry, not the
    // session's snapshot from host start, must discover the new exact id on ask and tell.
    const agentDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-resident-models-"));
    roots.push(agentDir);
    const modelsPath = path.join(agentDir, "models.json");
    const writeModels = (ids: string[]) => fs.writeFileSync(modelsPath, JSON.stringify({
      providers: {
        "probe-late": {
          baseUrl: "http://127.0.0.1:9/v1",
          api: "openai-completions",
          apiKey: "probe",
          models: ids.map((id) => ({ id })),
        },
      },
    }));
    writeModels(["claude-late-5-5"]);
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    const state = await rootHarness("resident-late-model");
    state.config.piModels = {
      available: [{ provider: "probe-late", id: "claude-late-5-5" }],
      aliases: {},
      defaultModel: "probe-late/claude-late-5-5",
    };
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });
    const control = new FabricControlPlane(state.mesh, state.identity, {
      enabled: true,
      hostId: state.identity.id,
      pollMs: 20,
      acknowledgementTimeoutMs: 5_000,
    });
    control.start(() => ({ accepted: false }));
    const ownerPid = () =>
      (JSON.parse(fs.readFileSync(path.join(state.config.residencyRoot, "owner.json"), "utf8")) as ResidentHostOwner).pid;
    try {
      const actor = await client.createActor({
        name: "late model witness",
        instructions: "Reply with the model of each run.",
        residency: "durable",
        runner: "pi",
        model: "probe-late/claude-late-5-5",
      });
      const pid = ownerPid();
      writeModels(["claude-late-5-5", "claude-late-5-6"]);

      const asked = await control.requestResult<FabricActorMessage>(
        client.hostId,
        actor.id,
        "ask",
        { message: "ECHO_MODEL ping", binding: { model: "probe-late/claude-late-5-6" } },
        client.hostId,
        { timeoutMs: 20_000 },
      );
      expect(asked).toMatchObject({ text: "model probe-late/claude-late-5-6" });

      writeModels(["claude-late-5-5", "claude-late-5-6", "claude-late-5-7"]);
      await new Promise((resolve) => setTimeout(resolve, 10_500)); // past the shared refresh throttle
      await control.request(
        client.hostId,
        actor.id,
        "followUp",
        { message: "ECHO_MODEL pong", binding: { model: "probe-late/claude-late-5-7" } },
        client.hostId,
      );
      await waitFor(() => state.mesh.read({ topic: "fabric.actor.output", limit: 50 })
        .some((event) => event.text?.includes("model probe-late/claude-late-5-7")), 15_000);

      // A model that is still not configured fails after the refresh, not with a fuzzy stand-in.
      await expect(control.request(
        client.hostId,
        actor.id,
        "followUp",
        { message: "nope", binding: { model: "probe-missing/claude-late-5-9" } },
        client.hostId,
      )).rejects.toThrow(/not available to this Pi session/);
      expect(ownerPid()).toBe(pid);
      await client.removeActor(actor.id);
    } finally {
      await control.close();
      await client.close();
      await state.participants.close();
      await stopResident(state.config);
    }
  });

  it("applies live model guidance snapshots to durable participants", { timeout: 45_000 }, async () => {
    const state = await rootHarness("resident-guidance");
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });
    const guidance = (content: string) => [{
      componentId: "deepseek-guidance",
      component: "deepseek-guidance",
      revision: 1,
      label: "deepseek",
      models: ["deepseek/*"],
      targets: ["participant" as const],
      placement: "append" as const,
      content,
    }];

    client.updateModelGuidance(guidance("First durable guidance"));
    const first = await client.spawnAgent({
      task: "First guided durable agent",
      transport: "process",
      residency: "durable",
      model: "deepseek/deepseek-chat",
    });
    const firstResult = await client.waitAgent(first.id);
    expect((firstResult as typeof firstResult & { systemPrompt?: string }).systemPrompt).toBe(
      "First durable guidance",
    );

    client.updateModelGuidance(guidance("Revised durable guidance"));
    const second = await client.spawnAgent({
      task: "Second guided durable agent",
      transport: "process",
      residency: "durable",
      model: "deepseek/deepseek-chat",
    });
    const secondResult = await client.waitAgent(second.id);
    expect((secondResult as typeof secondResult & { systemPrompt?: string }).systemPrompt).toBe(
      "Revised durable guidance",
    );

    await client.cleanupAgent(first.id);
    await client.cleanupAgent(second.id);
    await client.close();
    await state.participants.close();
  });

  it("completes and cleans a durable agent after its originating Main closes", { timeout: 45_000 }, async () => {
    const state = await rootHarness("resident-agent");
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });
    const handle = await client.spawnAgent({
      task: "STREAM_PREVIEW",
      transport: "process",
      residency: "durable",
    });
    expect(handle.residency).toBe("durable");

    await client.close();
    await state.participants.close();

    const detachedParticipants: FabricParticipantSource = {
      list: () => [],
      get: () => undefined,
      self: () => {
        throw new Error("not used by detached residency client");
      },
      peers: () => [],
      async refresh() {},
      scheduleRefresh() {},
    };
    const reconnect = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: detachedParticipants,
      mainAgent: state.mainAgent,
      hostPath,
    });
    const result = await reconnect.waitAgent(handle.id);
    expect(result).toMatchObject({
      id: handle.id,
      status: "completed",
      residency: "durable",
      text: "stream preview complete",
    });
    await expect(reconnect.cleanupAgent(handle.id)).resolves.toEqual({ cleaned: true });
    expect(reconnect.hasAgent(handle.id)).toBe(false);
    await reconnect.close();
  });

  // smarty-dev#883: on SIGTERM, Pi exited before the host's close stopped its
  // durable workers, which ran on as orphans and kept writing.
  it("stops its durable workers before a SIGTERM'd resident host exits", { timeout: 60_000 }, async () => {
    const state = await rootHarness("resident-sigterm");
    const client = new ResidencyClient({
      config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent, hostPath,
    });
    try {
      const handle = await client.spawnAgent({ task: "HANG", transport: "process", residency: "durable" });
      const workerPid = (): number | undefined => execFileSync("ps", ["-eo", "pid=,args="], { encoding: "utf8" })
        .split("\n").filter((line) => line.includes(handle.id) && line.includes(fakeWorker))
        .map((line) => Number(line.trim().split(/\s+/)[0]))[0];
      await waitFor(() => workerPid() !== undefined, 30_000);
      const worker = workerPid()!;
      const host = (JSON.parse(fs.readFileSync(path.join(state.config.residencyRoot, "owner.json"), "utf8")) as ResidentHostOwner).pid;
      const alive = (pid: number) => { try { process.kill(pid, 0); return true; } catch { return false; } };
      process.kill(host, "SIGTERM");
      await waitFor(() => !alive(host), 30_000);
      // The worker's exit is part of the host's close, so it is already gone.
      expect(alive(worker)).toBe(false);
    } finally {
      await client.close();
      await state.participants.close();
    }
  });

  // smarty-dev#1882: the host's exit removes runs/; a later session still reads the result.
  it("returns a completed durable agent's result after its resident host exits", { timeout: 90_000 }, async () => {
    const state = await rootHarness("resident-exit-status");
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });
    const handle = await client.spawnAgent({ task: "STREAM_PREVIEW", transport: "process", residency: "durable" });
    expect((await client.waitAgent(handle.id)).status).toBe("completed");
    await client.close();
    await state.participants.close();
    // The real path: the host exits after 30 s idle and its close removes runs/.
    const runsDir = path.join(state.config.residencyRoot, "runs");
    await waitFor(() => !fs.existsSync(path.join(state.config.residencyRoot, "owner.json")), 60_000);
    // owner.json goes last: nothing may write under mesh/ once it is gone, or a new host
    // started in that window, and every cleanup that trusts the marker, races the old one.
    const snapshot = (): Map<string, string> => {
      const files = new Map<string, string>();
      const walk = (directory: string): void => {
        for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
          const file = path.join(directory, entry.name);
          if (entry.isDirectory()) walk(file);
          else {
            try {
              const stat = fs.statSync(file);
              files.set(file, `${stat.size}:${stat.mtimeMs}`);
            } catch { /* removed meanwhile */ }
          }
        }
      };
      walk(path.join(state.root, "mesh"));
      return files;
    };
    const released = snapshot();
    await delay(2_000);
    const late = [...snapshot()].filter(([file, stamp]) => released.get(file) !== stamp)
      .map(([file]) => path.relative(state.root, file));
    expect(late).toEqual([]);
    expect(fs.existsSync(path.join(runsDir, handle.id))).toBe(false);

    const reconnect = new ResidencyClient({ ...client.options, hostPath });
    expect(reconnect.statusAgent(handle.id)).toMatchObject({
      id: handle.id,
      status: "completed",
      residency: "durable",
      text: "stream preview complete",
    });
    expect(await reconnect.waitAgent(handle.id)).toMatchObject({ status: "completed", text: "stream preview complete" });
    await expect(reconnect.cleanupAgent(handle.id)).resolves.toEqual({ cleaned: true });
    expect(fs.existsSync(residentResultPath(state.config.residencyRoot, handle.id))).toBe(false);
    await reconnect.close();
  });

  it("rejects tampered durable worktree metadata before destructive cleanup", { timeout: 45_000 }, async () => {
    const state = await rootHarness("resident-worktree-tamper");
    const source = path.join(state.root, "source");
    const unrelated = path.join(state.root, "unrelated");
    initRepository(source);
    initRepository(unrelated);
    const id = randomId().padEnd(32, "0").slice(0, 32);
    const branch = `pi-fabric/tampered-${id.slice(0, 8)}`;
    const worktree = path.join(source, ".pi", "fabric", "worktrees", id);
    fs.mkdirSync(path.dirname(worktree), { recursive: true });
    git(source, "worktree", "add", "-q", "-b", branch, worktree, "HEAD");
    const runDirectory = path.join(state.config.residencyRoot, "runs", id);
    fs.mkdirSync(runDirectory, { recursive: true });
    fs.writeFileSync(path.join(runDirectory, "status.json"), JSON.stringify({
      id,
      name: "tampered worktree",
      task: "test",
      status: "completed",
      runner: "pi",
      transport: "process",
      cwd: worktree,
      startedAt: 1,
      updatedAt: 1,
      finishedAt: 1,
      turns: 0,
      toolCalls: 0,
      text: "complete",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    }));
    const metadataPath = path.join(state.config.residencyRoot, "agents", `${id}.json`);
    const metadata = {
      format: RESIDENT_HOST_FORMAT,
      rootId: state.identity.id,
      id,
      runDirectory,
      handle: {
        id,
        name: "tampered worktree",
        status: "completed",
        runner: "pi",
        transport: "process",
        cwd: worktree,
        residency: "durable",
        branch,
        worktree,
      },
      worktreeGitRoot: unrelated,
      createdAt: 1,
      updatedAt: 1,
    };
    fs.mkdirSync(path.dirname(metadataPath), { recursive: true });
    fs.writeFileSync(metadataPath, JSON.stringify(metadata));
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });

    try {
      await expect(client.cleanupAgent(id, true)).rejects.toThrow(/not registered/);
      expect(worktreeBranches(source)).toContain(branch);
      expect(git(source, "branch", "--list", branch)).toContain(branch);
      expect(fs.existsSync(runDirectory)).toBe(true);

      fs.writeFileSync(metadataPath, JSON.stringify({ ...metadata, worktreeGitRoot: source }));
      await expect(client.cleanupAgent(id, true)).resolves.toEqual({ cleaned: true });
      expect(worktreeBranches(source)).not.toContain(branch);
      expect(git(source, "branch", "--list", branch)).toBe("");
      expect(fs.existsSync(runDirectory)).toBe(false);
    } finally {
      try {
        git(source, "worktree", "remove", "--force", worktree);
      } catch {
        // A failed assertion may follow an implementation that already removed it.
      }
      try {
        git(source, "branch", "-D", branch);
      } catch {
        // The worktree removal may already have removed its branch.
      }
      await client.close();
      await state.participants.close();
    }
  });

  it.each([false, true])("forwards and reports a canonical durable agent cwd (recursive=%s)", { timeout: 45_000 }, async (recursive) => {
    const state = await rootHarness("resident-agent-cwd");
    state.config.cwd = state.root;
    state.config.projectRoot = state.root;
    const target = path.join(state.root, "child");
    fs.mkdirSync(target);
    if (recursive) vi.stubEnv("PI_FABRIC_TOOL_ALLOWLIST", '["read","fabric_exec"]');
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });

    const handle = await client.spawnAgent({
      task: "REPORT_RECURSIVE_CWD",
      kernel: "python",
      cwd: "child",
      tools: ["read", "bash", "write"],
      recursive,
      transport: "process",
      residency: "durable",
    });
    const canonical = fs.realpathSync(target);
    expect(handle.cwd).toBe(canonical);
    expect(handle.kernel).toBe("python");

    const result = await client.waitAgent(handle.id);
    expect(result.cwd).toBe(canonical);
    if (recursive) {
      expect(result).toMatchObject({ recursive: true, tools: ["read", "fabric_exec"], grantedRisks: ["agent"] });
    }
    expect(client.statusAgent(handle.id)).toMatchObject({ cwd: canonical });
    expect(client.readAgentLog(handle.id).status?.cwd).toBe(canonical);
    const reopened = new ResidencyClient(client.options);
    expect(reopened.statusAgent(handle.id)).toMatchObject({ cwd: canonical, kernel: "python", ...(recursive ? { recursive: true } : {}) });
    await reopened.close();
    await expect(client.cleanupAgent(handle.id)).resolves.toEqual({ cleaned: true });
    await client.close();
    await state.participants.close();
  });
});
