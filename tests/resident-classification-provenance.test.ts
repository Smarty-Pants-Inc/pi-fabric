import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { ACTOR_FAILURE_NOTICE_AFTER } from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MainAgentController } from "../src/main-agent.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentDeliveryPrefix, type ResidentDeliveryRecord, type ResidentHostConfig } from "../src/residency/protocol.js";

const waitFor = async (predicate: () => boolean): Promise<void> => {
  const deadline = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Timed out waiting for resident classification delivery");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};

const cases = [
  "surviving older producer alarm",
  "retained unclassified record after upgrade",
  "new classified actor output",
  "new classified host alarm",
] as const;

describe("resident producer classification (smarty-dev#2775 F3 mixed releases)", () => {
  it.each(cases)("%s", async (scenario) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-resident-classification-"));
    const legacy = scenario === cases[0] || scenario === cases[1];
    const retained = scenario === cases[1];
    const config: ResidentHostConfig = {
      format: RESIDENT_HOST_FORMAT, rootId: "session:classification-main", sessionId: "classification-main",
      cwd: process.cwd(), projectRoot: process.cwd(), meshRoot: path.join(root, "mesh"),
      actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
      fullCodeMode: true,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 5_000, budgetUsd: 0 },
      mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      fabricExtensionPath: path.join(root, "older-release/dist/index.js"),
      piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
      piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
    };
    const host = new ResidentHost(config);
    const sendMessage = vi.fn();
    const pi = { hostCapabilities: { turnProvenance: 1 }, sendMessage, on: () => () => {} } as unknown as ExtensionAPI;
    const main = new MainAgentController(pi, config.rootId, true, config.cwd, config.sessionId);
    const context = { isIdle: () => true, hasPendingMessages: () => false,
      sessionManager: { getEntries: () => [], getSessionFile: () => undefined },
    } as unknown as ExtensionContext;
    const journal = path.join(root, "main-followups.json");
    main.attachFollowUpDrain(context, 0, journal);
    const prefix = residentDeliveryPrefix(config.rootId);
    let oldWriter: ReturnType<typeof vi.spyOn> | undefined;
    let client: ResidencyClient | undefined;
    let beforeUpgrade: ResidencyClient | undefined;
    try {
      await host.start();
      // Wire-format adapter for a surviving older producer: retain the real host callback,
      // authenticated mesh writer and receipt path, omitting only its unsupported field.
      const put = host.mesh.put.bind(host.mesh);
      if (legacy) oldWriter = vi.spyOn(host.mesh, "put").mockImplementation((request) => {
        if (request.key.startsWith(prefix)) {
          const { source: _source, ...value } = request.value as ResidentDeliveryRecord;
          return put({ ...request, value });
        }
        return put(request);
      });
      const actor = await host.actors.create({ name: "resident advisor", instructions: "Advise.", residency: "durable",
        transport: "process", responseMode: "directive", delivery: "followUp", triggerTurn: true });
      const ownerPath = path.join(config.residencyRoot, "owner.json");
      const originalOwner = fs.readFileSync(ownerPath, "utf8");
      const upgradedConfig = { ...config, fabricExtensionPath: path.join(root, "new-release/dist/index.js") };
      const attachUpgradedClient = async () => {
        client = new ResidencyClient({ config: upgradedConfig, mesh: host.mesh, participants: host.participants, mainAgent: main });
        expect(await client.ensureHost()).toMatchObject({ hostId: host.hostId, pid: process.pid });
        expect(fs.readFileSync(ownerPath, "utf8")).toBe(originalOwner);
      };
      if (retained) {
        beforeUpgrade = new ResidencyClient({ config, mesh: host.mesh, participants: host.participants, mainAgent: main });
        await beforeUpgrade.ensureHost();
        await beforeUpgrade.close();
      } else {
        // A live old writer emits AFTER Main has attached with the upgraded release config.
        await attachUpgradedClient();
      }
      const output = await host.actors.ask(actor.id, "healthy output");
      expect(output).toMatchObject({ action: "message", text: "fake actor advice" });
      // Alarms still wake Main even if genuine actor output is now mailbox-only.
      await host.actors.setDeliveryPolicy(actor.id, "mailbox", false);
      for (let n = 0; n < ACTOR_FAILURE_NOTICE_AFTER; n++) await host.actors.ask(actor.id, "FAIL_DIRECTIVE");
      await waitFor(() => host.mesh.listAll(prefix).length === 2);
      const records = host.mesh.listAll(prefix);
      const actorRecord = records.find((entry) => (entry.value as ResidentDeliveryRecord).message === "fake actor advice")!.value as ResidentDeliveryRecord;
      const alarmRecord = records.find((entry) => (entry.value as ResidentDeliveryRecord).message.startsWith("Fabric host notice:"))!.value as ResidentDeliveryRecord;
      for (const entry of records) expect(entry.updatedBy.id).toBe(host.hostId);
      expect(alarmRecord).toMatchObject({ from: { id: actor.id, name: actor.name, kind: "actor" }, delivery: "followUp", triggerTurn: true });
      if (legacy) {
        expect(actorRecord).not.toHaveProperty("source");
        expect(alarmRecord).not.toHaveProperty("source");
      }
      if (retained) {
        oldWriter?.mockRestore();
        await attachUpgradedClient();
        // Neither a release change nor a consumer reconnect rewrites the retained records.
        expect(host.mesh.listAll(prefix).map((entry) => entry.value)).toEqual(records.map((entry) => entry.value));
      }
      expect(sendMessage).not.toHaveBeenCalled();
      client!.start();
      await waitFor(() => sendMessage.mock.calls.length === 2);
      const actorCall = sendMessage.mock.calls.find(([message]) => message.content.includes("fake actor advice"))!;
      const alarmCall = sendMessage.mock.calls.find(([message]) => message.content.includes("Fabric host notice:"))!;
      const admitted = JSON.parse(fs.readFileSync(journal, "utf8")) as {
        items: Array<{ deliveryId: string; provenance?: unknown }>;
      };
      for (const [record, call, claimed] of [
        [alarmRecord, alarmCall, false], [actorRecord, actorCall, !legacy],
      ] as const) {
        expect(call[0].content).toContain(actor.name); // Display/routing label is not attribution.
        expect(call[1]).toMatchObject({ deliverAs: record.delivery, triggerTurn: record.triggerTurn });
        const receipt = admitted.items.find((item) => item.deliveryId === `resident:${config.rootId}:${record.id}`);
        expect(receipt).toBeDefined();
        if (claimed) {
          expect(call[1]).toMatchObject({ provenance: { v: 1, channel: "fabric", sender: {
            id: actor.id, name: actor.name, kind: "actor", verified: "mesh",
          }, via: "followUp" } });
          expect(receipt).toHaveProperty("provenance");
        } else {
          expect(call[1]).toEqual({ deliverAs: record.delivery, triggerTurn: record.triggerTurn });
          expect(call[0].details).not.toHaveProperty("provenance");
          expect(receipt).not.toHaveProperty("provenance");
        }
      }
      if (!legacy) {
        // Both new-producer cases require a positive actor-output control. An alarm-only
        // negative check cannot prove that absence no longer grants actor attribution.
        expect(actorRecord).toHaveProperty("source", "actor-output");
        expect(alarmRecord).toHaveProperty("source", "fabric-host");
      }
      await waitFor(() => host.mesh.listAll(prefix).length === 0);
      // Reconnecting to the same owner does not deliver either acknowledged record again.
      await client!.close();
      await attachUpgradedClient();
      client!.start();
      await new Promise((resolve) => setTimeout(resolve, 80));
      expect(sendMessage).toHaveBeenCalledTimes(2);
    } finally {
      oldWriter?.mockRestore();
      await beforeUpgrade?.close();
      await client?.close();
      main.closeFollowUpDrain();
      await host.close();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  }, 20_000);
});
