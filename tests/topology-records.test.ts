import { describe, expect, it } from "vitest";
import type { FabricActorInfo } from "../src/actors/types.js";
import type { AgentRunRecord } from "../src/agents/types.js";
import { SETTLED_AGENT_PUBLISH_MS, actorParticipantRecord, agentParticipantRecords } from "../src/topology/records.js";

const run = (id: string, status: AgentRunRecord["status"], finishedAt?: number): AgentRunRecord =>
  ({
    id, name: id, status, runner: "pi", transport: "process", cwd: "/work",
    startedAt: 0, updatedAt: finishedAt ?? 0, ...(finishedAt !== undefined ? { finishedAt } : {}),
    turns: 1, toolCalls: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
  }) as AgentRunRecord;

describe("actorParticipantRecord", () => {
  it.each(["preparing", "waiting"] as const)("#3307 projects %s preparation diagnostics", status => {
    const preparing = {
      phase: status === "waiting" ? "waiting" : "binding", startedAt: 1_000, ageS: 2, attempts: 1,
      ...(status === "waiting" ? { runId: "admission-receipt", queuePosition: 0 } : {}),
    };
    const actor = {
      id: "actor:test", name: "test", status, runner: "pi", createdAt: 500, updatedAt: 3_000,
      queued: 2, messages: 7, preparing,
    } as FabricActorInfo;
    const record = actorParticipantRecord(actor, "root:test", "host:test", "identity:test", "root:test");
    expect(record).toMatchObject({ status, actorQueued: 2, actorMessages: 7, actorPreparing: preparing });
    expect(record).not.toHaveProperty("actorRun");
  });
});

describe("agentParticipantRecords", () => {
  // smarty-dev#2004: finished agents were a third of the shared state rewritten under the mesh lock.
  it("publishes a finished agent for an hour after it finished, then drops it", () => {
    const now = 10 * SETTLED_AGENT_PUBLISH_MS;
    const records = agentParticipantRecords(
      [
        run("running-long", "running"),
        run("done-recent", "completed", now - SETTLED_AGENT_PUBLISH_MS + 1_000),
        run("done-old", "completed", now - SETTLED_AGENT_PUBLISH_MS - 1_000),
        run("failed-old", "failed", now - SETTLED_AGENT_PUBLISH_MS - 1_000),
      ],
      "root:test", "host:test", "identity:test", "root:test", new Map(), now,
    );
    expect(records.map((record) => record.id)).toEqual(["running-long", "done-recent"]);
  });
});
