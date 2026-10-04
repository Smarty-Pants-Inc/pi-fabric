import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";
import { ACTOR_FAILURE_NOTICE_AFTER } from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MainAgentController } from "../src/main-agent.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentDeliveryPrefix, type ResidentHostConfig } from "../src/residency/protocol.js";

// Only isolated same-process fixtures use this adapter on unsupported platforms.
// Linux continues to exercise the real kernel fence.
beforeEach(() => installInProcessResidentFence());

const waitFor = async (predicate: () => boolean): Promise<void> => {
  const deadline = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Timed out waiting for resident delivery");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};

describe("resident host alarm provenance (smarty-dev#2775 F3)", () => {
  it.each(["consecutive-failure", "session-repaired", "session-repair-error"] as const)(
    "%s is unclaimed through the durable route while genuine actor output keeps its sender",
    async (alarm) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-resident-alarm-"));
      const config: ResidentHostConfig = {
        format: RESIDENT_HOST_FORMAT, rootId: "session:alarm-main", sessionId: "alarm-main",
        cwd: process.cwd(), projectRoot: process.cwd(), meshRoot: path.join(root, "mesh"),
        actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
        fullCodeMode: true,
        agents: { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 5_000, budgetUsd: 0 },
        mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 },
        retention: DEFAULT_FABRIC_CONFIG.retention,
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
        fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
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
      const deliveryErrors: string[] = [];
      const deliver = main.deliverAgent.bind(main);
      vi.spyOn(main, "deliverAgent").mockImplementation((request) => {
        try { return deliver(request); } catch (error) { deliveryErrors.push(String(error)); throw error; }
      });
      let client: ResidencyClient | undefined;
      let renameFault: ReturnType<typeof vi.spyOn> | undefined;
      try {
        await host.start();
        client = new ResidencyClient({ config, mesh: host.mesh, participants: host.participants, mainAgent: main });
        const actor = await host.actors.create({ name: "resident advisor", instructions: "Advise.", residency: "durable",
          transport: "process", responseMode: "directive", delivery: "followUp", triggerTurn: true });
        const output = await host.actors.ask(actor.id, "healthy output");
        expect(output).toMatchObject({ action: "message", text: "fake actor advice" });
        // Host alarms must still wake Main even when the actor itself is mailbox-only.
        await host.actors.setDeliveryPolicy(actor.id, "mailbox", false);
        if (alarm === "consecutive-failure") {
          for (let n = 0; n < ACTOR_FAILURE_NOTICE_AFTER; n++) await host.actors.ask(actor.id, "FAIL_DIRECTIVE");
        } else {
          const orphan = '{"type":"message","id":"orphan"}\n';
          fs.writeFileSync(actor.sessionFile!, orphan);
          if (alarm === "session-repair-error") {
            const rename = fs.renameSync;
            renameFault = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
              if (String(from) === actor.sessionFile) throw new Error("injected session repair failure");
              return rename(from, to);
            });
            await expect(host.actors.ask(actor.id, "repair")).rejects.toThrow("injected session repair failure");
            renameFault.mockRestore();
          } else {
            await host.actors.ask(actor.id, "repair");
          }
        }
        const prefix = residentDeliveryPrefix(config.rootId);
        await waitFor(() => host.mesh.listAll(prefix).length === 2);
        const records = host.mesh.listAll(prefix);
        const notice = records.find((entry) => (entry.value as { message: string }).message.startsWith("Fabric host notice:"))!;
        expect(notice.updatedBy.id).toBe(host.hostId);
        const noticeRecord = notice.value as { id: string; message: string; delivery: string; triggerTurn: boolean };
        expect(noticeRecord).toMatchObject({ delivery: "followUp", triggerTurn: true });
        expect(noticeRecord.message).toContain(alarm === "consecutive-failure" ? `failed its last ${ACTOR_FAILURE_NOTICE_AFTER}` : alarm === "session-repaired" ? "session repaired" : "session repair failed");
        // Nothing called the direct deliverActorToMain adapter: the producer persisted both records.
        expect(sendMessage).not.toHaveBeenCalled();
        client.start();
        try { await waitFor(() => sendMessage.mock.calls.length === 2); } catch (error) {
          throw new Error(`${String(error)}; sent=${sendMessage.mock.calls.length}; errors=${JSON.stringify([...new Set(deliveryErrors)])}`);
        }
        const actorCall = sendMessage.mock.calls.find(([message]) => message.content.includes("fake actor advice"))!;
        expect(actorCall[1]).toMatchObject({ deliverAs: "followUp", triggerTurn: true, provenance: {
          v: 1, channel: "fabric", sender: { id: actor.id, name: actor.name, kind: "actor", verified: "mesh" }, via: "followUp",
        } });
        const alarmCall = sendMessage.mock.calls.find(([message]) => message.content.includes("Fabric host notice:"))!;
        expect(alarmCall[1]).toEqual({ deliverAs: "followUp", triggerTurn: true });
        expect(alarmCall[0].details).not.toHaveProperty("provenance");
        // The no-claim form survives durable admission, not just transient Pi API options.
        const admitted = JSON.parse(fs.readFileSync(journal, "utf8")) as { items: Array<{ deliveryId: string; provenance?: unknown }> };
        expect(admitted.items.find((item) => item.deliveryId.endsWith(noticeRecord.id))).not.toHaveProperty("provenance");
        await waitFor(() => host.mesh.listAll(prefix).length === 0);
      } finally {
        renameFault?.mockRestore();
        await client?.close();
        main.closeFollowUpDrain();
        await host.close();
        fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
      }
    }, 20_000,
  );
});
