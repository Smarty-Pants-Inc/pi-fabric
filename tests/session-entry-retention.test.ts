import { setImmediate } from "node:timers/promises";
import { describe, expect, it } from "vitest";
import { restoreStoppedRuns, STOPPED_AGENTS_ENTRY, type StoppedAgentsEntryData } from "../src/agents/stopped-runs.js";
import type { AgentRunResult } from "../src/agents/types.js";
import { recordsInboxSession, RECORDS_INBOX_CUSTOM_TYPE } from "../src/records/inbox.js";
import { rootInboxSession, ROOT_INBOX_CUSTOM_TYPE } from "../src/topology/root-inbox.js";

// About 50 MiB of distinct heap payload, not repeated strings/ropes. Each fixture
// returns only WeakRefs to history so the test itself cannot be its strong holder.
const largeHistory = (): unknown[] => Array.from({ length: 6_400 }, (_, index) => ({
  type: "message", message: { role: "user", content: Array<number>(1_024).fill(index) },
}));

const collected = async (references: readonly WeakRef<object>[]): Promise<boolean> => {
  expect(globalThis.gc, "Vitest workers must expose GC for retention regressions").toBeTypeOf("function");
  for (let attempt = 0; attempt < 10; attempt++) {
    // WeakRef.deref() keeps its target alive until the end of the current job.
    await setImmediate();
    globalThis.gc!();
    if (references.every((reference) => reference.deref() === undefined)) return true;
  }
  return false;
};

const deliveryFixture = (notifyOnComplete: boolean) => {
  const entries = largeHistory();
  const stopped = { id: "pending", status: "stopped" } as AgentRunResult;
  entries.push({ type: "custom", customType: STOPPED_AGENTS_ENTRY, data: { stopped: [stopped] } });
  const appended: StoppedAgentsEntryData[] = [];
  const restoredIds: string[] = [];
  const notices: (() => void)[] = [];
  const options = {
    entries, notifyOnComplete,
    restore: (runs: AgentRunResult[]) => { restoredIds.push(...runs.map((run) => run.id)); },
    enqueue: (_run: AgentRunResult, delivered: () => void) => { notices.push(delivered); },
    appendEntry: (data: StoppedAgentsEntryData) => { appended.push(data); },
  };
  const markDelivered = restoreStoppedRuns(options);
  return {
    markDelivered, notices, appended, restoredIds,
    references: [new WeakRef(entries), new WeakRef(entries[0] as object), new WeakRef(options), new WeakRef(stopped)],
  };
};

const inboxFixture = (kind: "root" | "records") => {
  const entries = largeHistory();
  const ids = ["a", "b"];
  entries.push({
    type: "custom_message", customType: kind === "root" ? ROOT_INBOX_CUSTOM_TYPE : RECORDS_INBOX_CUSTOM_TYPE,
    details: { ids },
  });
  const carried = { from: { id: "peer" }, data: { key: " work " } };
  entries.push({ type: "custom_message", customType: "pi-fabric-agent-message", details: { items: [carried] } });
  const rootSession = kind === "root" ? rootInboxSession(entries) : undefined;
  const session = rootSession ?? recordsInboxSession(entries);
  return { session, holdsSteer: rootSession?.holdsSteer, references: [new WeakRef(entries), new WeakRef(entries[0] as object), new WeakRef(ids), new WeakRef(carried)] };
};

describe("session-entry consumers release canonical history (smarty-dev#2177)", () => {
  it.each([false, true])("collects history while stopped-run delivery stays usable (notices=%s)", async (notifyOnComplete) => {
    const fixture = deliveryFixture(notifyOnComplete);
    expect(fixture.restoredIds).toEqual(["pending"]);
    expect(fixture.notices).toHaveLength(notifyOnComplete ? 1 : 0);
    expect(await collected(fixture.references)).toBe(true);
    fixture.markDelivered("unknown");
    if (notifyOnComplete) fixture.notices[0]!();
    else fixture.markDelivered("pending");
    fixture.markDelivered("pending");
    fixture.notices[0]?.();
    expect(fixture.appended).toEqual([{ delivered: ["pending"] }]);
  });

  it.each(["root", "records"] as const)("collects history while the %s inbox view stays usable", async (kind) => {
    const { session, holdsSteer, references } = inboxFixture(kind);
    expect(await collected(references)).toBe(true);
    expect(session.holdsBatch(["a", "b"])).toBe(true);
    expect(session.holdsBatch(["missing"])).toBe(false);
    if (holdsSteer) {
      expect(holdsSteer("peer", "work")).toBe(true);
      expect(holdsSteer("other", "work")).toBe(false);
      expect(holdsSteer("peer", "missing")).toBe(false);
    }
  });

  it.each(["root", "records"] as const)("preserves single-batch and lookback boundaries in the %s view", (kind) => {
    const customType = kind === "root" ? ROOT_INBOX_CUSTOM_TYPE : RECORDS_INBOX_CUSTOM_TYPE;
    const batch = (ids: string[]) => ({ type: "custom_message", customType, details: { ids } });
    const entries = [batch(["old"]), ...Array<unknown>(500).fill({ type: "message" }), batch(["a"]), batch(["b"])];
    const session = kind === "root" ? rootInboxSession(entries) : recordsInboxSession(entries);
    expect(session.holdsBatch(["old"])).toBe(false);
    expect(session.holdsBatch(["a"])).toBe(true);
    expect(session.holdsBatch(["b"])).toBe(true);
    expect(session.holdsBatch(["a", "b"])).toBe(false);
    expect(session.holdsBatch([])).toBe(true);
    expect((kind === "root" ? rootInboxSession([]) : recordsInboxSession([])).holdsBatch([])).toBe(false);
  });
});
