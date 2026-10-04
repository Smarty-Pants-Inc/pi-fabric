import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";

const advisoryMessage = "followUp to a running task waits until its current run finishes; use agents.steer for a correction needed before completion.";

// Only the Pi UI/model boundary and task worker are fixtures. Both Mains, participant
// discovery, agents.followUp routing, owner admission, claims and ACKs are real.
const main = (cwd: string, sessionId: string) => {
  const pi = {
    on: vi.fn(() => () => {}), events: { emit: vi.fn() },
    getThinkingLevel: () => "off", getSessionName: () => sessionId, sendMessage: vi.fn(),
  } as unknown as ExtensionAPI;
  const context = {
    cwd, hasUI: false, mode: "rpc", isProjectTrusted: () => true,
    isIdle: () => true, hasPendingMessages: () => false,
    modelRegistry: { getAvailable: () => [{ provider: "fixture", id: "visible" }], find: () => undefined,
      getApiKeyAndHeaders: async () => ({ ok: true }) },
    sessionManager: { getSessionId: () => sessionId, getSessionFile: () => undefined,
      getBranch: () => [], getLeafId: () => null, getEntries: () => [] },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  } as unknown as ExtensionContext;
  const worker = path.resolve("tests/fixtures/fake-worker.mjs");
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
    extension: path.join(cwd, "unused-extension.mjs"), worker,
    residentHost: path.join(cwd, "unused-resident.mjs"), skills: cwd,
  } });
  const invoke = (ref: string, args: Record<string, unknown>) => runtime.registry.invoke(ref, args, {
    cwd, signal: undefined, parentToolCallId: "followup-budget", nestedToolCallId: ref,
    extensionContext: context, update() {}, approve: async () => {}, audits: [], maxResultChars: 10_000,
  });
  return { runtime, context, invoke, worker };
};

describe("remote followUp advisory ACK budget (#3005)", () => {
  it.each((["ordinary", "resident"] as const).flatMap(owner =>
    [2300, 64 * 1024].map(maxEventBytes => ({ owner, maxEventBytes }))))(
    "$owner owner preserves a successful single enqueue at maxEventBytes=$maxEventBytes", async ({ owner, maxEventBytes }) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-followup-budget-"));
      const ownerCwd = path.join(root, "owner"), senderCwd = path.join(root, "sender");
      fs.mkdirSync(ownerCwd); fs.mkdirSync(senderCwd);
      for (const name of Object.keys(process.env)) if (name.startsWith("PI_FABRIC_")) vi.stubEnv(name, undefined);
      vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
      const config = normalizeFabricConfig({ fullCodeMode: false,
        mesh: { enabled: true, root: path.join(root, "mesh"), maxEventBytes, actorPollMs: 20 },
        agents: { budgetUsd: 0 }, residency: { enabled: false },
        mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false },
        prewalk: { enabled: false, alwaysRearm: false },
      });
      const ownerSession = "aaaaaaaa-0000-4000-8000-000000000001";
      const senderSession = "bbbbbbbb-0000-4000-8000-000000000002";
      const receiving = main(ownerCwd, ownerSession), sending = main(senderCwd, senderSession);
      let resident: ResidentHost | undefined;
      let childId: string | undefined;
      try {
        await receiving.runtime.initialize(receiving.context, config);
        await sending.runtime.initialize(sending.context, config);
        if (owner === "resident") {
          const residentConfig: ResidentHostConfig = {
            format: RESIDENT_HOST_FORMAT, rootId: `session:${ownerSession}`, sessionId: ownerSession,
            cwd: ownerCwd, projectRoot: ownerCwd, meshRoot: config.mesh.root!,
            actorRoot: path.join(ownerCwd, "resident-actors"),
            residencyRoot: residentRoot(config.mesh.root!, `session:${ownerSession}`),
            fullCodeMode: false, agents: config.agents, mesh: config.mesh, retention: config.retention,
            workerPath: receiving.worker, fabricExtensionPath: path.join(ownerCwd, "unused-extension.mjs"),
            piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
            piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
          };
          // Only this isolated same-process host uses the test fence on unsupported platforms;
          // Linux continues to exercise the real kernel fence.
          installInProcessResidentFence();
          resident = new ResidentHost(residentConfig);
          await resident.start();
        }
        const manager = resident?.agents ?? receiving.runtime.agents;
        const child = await manager.spawn({ task: "HANG", model: "fixture/visible", transport: "process" });
        childId = child.id;
        const run = manager.runDirectory(child.id)!;
        await vi.waitFor(() => expect(fs.existsSync(path.join(run, "status.json"))).toBe(true));
        // Discovery must come from the real owner; do not inject a remote participant.
        await vi.waitFor(async () => {
          const status = await sending.invoke("agents.status", { id: child.id });
          expect(status).toMatchObject({ id: child.id, local: false, status: "running" });
        });
        const data = { unchanged: "payload" };
        const receipt = await sending.invoke("agents.followUp", { id: child.id, message: "later", data }).catch(error => error);
        const entries = () => fs.readFileSync(path.join(run, "steer.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
        // Assert the commit even if the sender received a rejection (the original bug).
        expect(entries()).toHaveLength(1);
        expect(entries()[0]).toMatchObject({ type: "follow_up", message: "later", data });
        expect(entries()[0]).not.toHaveProperty("warning");
        expect(receipt).toEqual({ queued: true, messageId: expect.any(String), routed: "mesh", acknowledged: true,
          ...(maxEventBytes === 2300 ? {} : { warning: {
            code: "FABRIC_FOLLOW_UP_RUNNING_TASK", targetId: child.id, kind: "agent", status: "running", message: advisoryMessage,
          } }),
        });
        expect(entries()).toEqual([{ type: "follow_up", message: "later", data, provenance: expect.any(Object),
          id: (receipt as { messageId: string }).messageId, ts: expect.any(Number) }]);
        const mesh = new MeshStore(config.mesh.root!, maxEventBytes, 1000);
        const commands = mesh.read({ topic: "fabric.control.command", limit: 10 });
        expect(commands).toHaveLength(1); // No sender retry for accepted work.
        const command = commands[0]!;
        const ack = mesh.read({ topic: "fabric.control.ack", limit: 10 })[0]!;
        expect(ack.data).toMatchObject({ accepted: true, messageId: (receipt as { messageId: string }).messageId });
        expect(JSON.stringify(ack.data)).not.toContain("exceeds");
        // Replay the original command: its persisted bounded success must not re-enqueue.
        await mesh.publish({ topic: command.topic, kind: command.kind, from: command.from, to: command.to!, data: command.data });
        await vi.waitFor(() => expect(mesh.read({ topic: "fabric.control.ack", limit: 10 })).toHaveLength(2));
        expect(mesh.read({ topic: "fabric.control.ack", limit: 10 })[1]!.data).toEqual(ack.data);
        expect(entries()).toHaveLength(1);
      } finally {
        // Explicitly stop our fake worker, including on the pre-fix rejection path.
        if (childId) await (resident?.agents ?? receiving.runtime.agents).stop(childId);
        await resident?.close();
        await sending.runtime.shutdown();
        await receiving.runtime.shutdown();
        vi.unstubAllEnvs();
        fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
      }
    }, 30_000,
  );
});
