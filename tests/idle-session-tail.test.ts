import { describe, expect, it, vi } from "vitest";
import { getSessionBranchFallbackCount, SessionEntryCursor } from "../src/core/preview.js";
import { confirmedSessionEntries } from "../src/core/session-receipts.js";
import { RecordsInboxCursor, recordsInboxSession } from "../src/records/inbox.js";
import type { MeshEvent } from "../src/mesh/store.js";
import { confirmedRootInboxSession, rootInboxMessage, rootInboxSession } from "./root-inbox-issued-fixture.js";

type Entry = { id: string; parentId: string | null; type: string; [key: string]: unknown };
const linked = (rows: Array<Record<string, unknown>>): Entry[] => {
  let parentId: string | null = null;
  return rows.map((row, index) => {
    const entry = { ...row, id: `entry-${index}`, parentId } as Entry;
    parentId = entry.id;
    return entry;
  });
};
const managerFor = (initial: Entry[]) => {
  let entries = initial;
  let byId = new Map(entries.map((entry) => [entry.id, entry]));
  const getLeafEntry = vi.fn(() => entries.at(-1));
  const getEntry = vi.fn((id: string) => byId.get(id));
  const getEntries = vi.fn(() => [...entries]);
  const getBranch = vi.fn(() => [...entries]);
  const manager = {
    getLeafId: () => entries.at(-1)?.id ?? null,
    getLeafEntry, getEntry, getEntries, getBranch,
    getSessionId: () => "synthetic-session",
    getSessionFile: () => undefined,
    isPersisted: () => false,
  };
  return {
    manager,
    get entries() { return entries; },
    append(rows: Array<Record<string, unknown>>) {
      // Preserve the real ancestry: each appended row follows the preceding leaf.
      let parentId = entries.at(-1)?.id ?? null;
      for (const row of rows) {
        const id = `entry-${entries.length}`;
        const entry = { ...row, id, parentId } as Entry;
        entries = [...entries, entry]; byId.set(id, entry); parentId = id;
      }
    },
    replace(rows: Array<Record<string, unknown>>) {
      entries = linked(rows);
      byId = new Map(entries.map((entry) => [entry.id, entry]));
    },
  };
};
const plainRows = (count: number): Array<Record<string, unknown>> => Array.from({ length: count }, (_, index) => ({
  type: "message", timestamp: new Date(1_700_000_000_000 + index).toISOString(),
  message: { role: "user", content: `${index}:` + "x".repeat(4096) },
}));

const meshEvent = (id: string): MeshEvent => ({
  id, sequence: 1, topic: "fleet.work", kind: "work",
  from: { id: "session:peer", name: "peer", kind: "main" },
  text: id, data: { key: `work-${id}` }, createdAt: Date.now() - 1000,
});

describe("bounded active-session readers", () => {
  it("walks a large history once, then reads only entries added across idle wakes", () => {
    const state = managerFor(linked(plainRows(5_000)));
    const cursor = new SessionEntryCursor();
    expect(cursor.reset(state.manager).entries).toHaveLength(5_000);
    state.manager.getLeafEntry.mockClear(); state.manager.getEntry.mockClear(); state.manager.getEntries.mockClear(); state.manager.getBranch.mockClear();
    const appended: unknown[] = [];
    for (let wake = 0; wake < 25; wake++) {
      const count = wake % 3 + 1;
      const rows = plainRows(count);
      state.append(rows);
      const result = cursor.read(state.manager);
      expect(result.reset).toBe(false);
      expect(result.entries).toHaveLength(count);
      appended.push(...result.entries);
    }
    expect(appended).toHaveLength(49);
    expect(state.manager.getEntries).not.toHaveBeenCalled();
    expect(state.manager.getBranch).not.toHaveBeenCalled();
    expect(state.manager.getEntry.mock.calls.length).toBeLessThanOrEqual(appended.length);
    expect(state.manager.getLeafEntry.mock.calls.length).toBe(25);
  });

  it("preserves records cursor decisions across compaction and branch replacement", () => {
    const rows = plainRows(700);
    rows[430] = { type: "custom_message", customType: "pi-fabric-records", details: { ids: ["batch-a"] } };
    rows[620] = { type: "compaction", firstKeptEntryId: "entry-300" };
    const state = managerFor(linked(rows));
    const cursor = new RecordsInboxCursor();
    cursor.reset(state.manager);
    expect(cursor.session(state.manager).holdsBatch(["batch-a"])).toBe(recordsInboxSession(state.entries).holdsBatch(["batch-a"]));
    expect(cursor.session(state.manager).holdsBatch(["batch-a"])).toBe(true);
    state.append(plainRows(501));
    expect(cursor.session(state.manager).holdsBatch(["batch-a"])).toBe(recordsInboxSession(state.entries).holdsBatch(["batch-a"]));
    expect(cursor.session(state.manager).holdsBatch(["batch-a"])).toBe(false);

    const alternate = plainRows(80);
    alternate[20] = { type: "custom_message", customType: "pi-fabric-records", details: { ids: ["branch-b"] } };
    state.replace(alternate);
    expect(cursor.session(state.manager).holdsBatch(["branch-b"])).toBe(recordsInboxSession(state.entries).holdsBatch(["branch-b"]));
    expect(cursor.session(state.manager).holdsBatch(["batch-a"])).toBe(false);
    expect(state.manager.getEntries).not.toHaveBeenCalled();
    expect(state.manager.getBranch).not.toHaveBeenCalled();
  });

  it("recovers missing point ancestors from one full read without redelivering inbox or records receipts", () => {
    const rows = plainRows(700);
    rows[320] = { type: "custom_message", ...rootInboxMessage([meshEvent("ancestor-inbox")]) };
    rows[330] = { type: "custom_message", customType: "pi-fabric-records", details: { ids: ["ancestor-record"] } };
    const state = managerFor(linked(rows));
    const missingAncestorId = state.entries[500]!.id;
    state.manager.getEntry.mockImplementation((id: string) => id === missingAncestorId
      ? undefined : state.entries.find((entry) => entry.id === id));
    const before = getSessionBranchFallbackCount();

    const inbox = confirmedRootInboxSession(state.manager);
    expect(inbox.holdsBatch(["ancestor-inbox"])).toBe(true); // No idle inbox redelivery.
    expect(state.manager.getEntries).toHaveBeenCalledTimes(1);

    const records = new RecordsInboxCursor();
    records.reset(state.manager);
    expect(records.session(state.manager).holdsBatch(["ancestor-record"])).toBe(true); // No record redelivery.
    expect(state.manager.getEntries).toHaveBeenCalledTimes(2);
    expect(getSessionBranchFallbackCount()).toBe(before + 2);
    expect(state.manager.getBranch).not.toHaveBeenCalled();
  });

  it("detects cyclic ancestry in both branch walks and cursor advances with a bounded lookup guard", () => {
    let entries = linked(plainRows(3));
    let byId = new Map(entries.map((entry) => [entry.id, entry]));
    let lookupCount = 0;
    const getEntry = vi.fn((id: string) => {
      lookupCount += 1;
      if (lookupCount > 12) throw new Error("bounded cycle lookup guard");
      return byId.get(id);
    });
    const getEntries = vi.fn(() => [...entries]);
    const manager = {
      getLeafId: () => entries.at(-1)?.id ?? null,
      getLeafEntry: () => entries.at(-1),
      getEntry,
      getEntries,
      getSessionId: () => "cyclic-session",
    };
    const cursor = new SessionEntryCursor();
    cursor.reset(manager);
    getEntry.mockClear(); getEntries.mockClear(); lookupCount = 0;

    entries = [
      { id: "cycle-a", parentId: "cycle-b", type: "message" },
      { id: "cycle-b", parentId: "cycle-a", type: "message" },
    ];
    byId = new Map(entries.map((entry) => [entry.id, entry]));
    const before = getSessionBranchFallbackCount();
    expect(() => cursor.read(manager)).toThrow("Full session read contains cyclic or invalid active ancestry");
    expect(getEntry.mock.calls.length).toBeLessThanOrEqual(4);
    expect(getEntries).toHaveBeenCalledTimes(1);
    expect(getSessionBranchFallbackCount()).toBe(before + 1);
  }, 2_000);

  it("retains the full-read compatibility path only for managers without point lookups", () => {
    const legacyEntry = { type: "custom_message", customType: "pi-fabric-records", details: { ids: ["legacy-batch"] } };
    const getEntries = vi.fn(() => [legacyEntry]);
    const legacy = { getEntries, getSessionId: () => "legacy-session" };
    const cursor = new RecordsInboxCursor();
    cursor.reset(legacy);
    expect(getEntries).not.toHaveBeenCalled();
    expect(cursor.session(legacy).holdsBatch(["legacy-batch"])).toBe(true);
    expect(getEntries).toHaveBeenCalledTimes(1);
  });

  it("keeps old inbox receipts equivalent while incrementally advancing the in-memory point cursor", () => {
    const rows = plainRows(1_200);
    rows[8] = { type: "custom_message", customType: "pi-fabric-agent-message", timestamp: new Date().toISOString(),
      details: { id: "old-delivery", from: { id: "session:peer" }, data: { key: "old-key", topic: "fleet.work" } } };
    rows[950] = { type: "custom_message", timestamp: new Date().toISOString(),
      ...rootInboxMessage([meshEvent("received")]) };
    rows[1_000] = { type: "compaction", firstKeptEntryId: "entry-700" };
    const state = managerFor(linked(rows));
    const expectedEntries = state.entries;
    const expected = rootInboxSession(expectedEntries);
    const actual = confirmedRootInboxSession(state.manager);
    expect([...(actual.delivered ?? [])]).toEqual([...(expected.delivered ?? [])]);
    expect([...(actual.deliveredAt ?? [])]).toEqual([...(expected.deliveredAt ?? [])]);
    expect(actual.holdsBatch(["received"])).toBe(expected.holdsBatch(["received"]));
    expect(actual.holdsSteer("session:peer", "old-key", "fleet.work")).toBe(expected.holdsSteer("session:peer", "old-key", "fleet.work"));
    expect(actual.holdsSteer("session:peer", "old-key", "other.topic")).toBe(false);
    const readsAtBootstrap = state.manager.getEntry.mock.calls.length;
    confirmedRootInboxSession(state.manager);
    expect(state.manager.getEntry).toHaveBeenCalledTimes(readsAtBootstrap);

    state.append(plainRows(4));
    const newActual = confirmedRootInboxSession(state.manager);
    const newExpected = rootInboxSession(state.entries);
    expect([...(newActual.delivered ?? [])]).toEqual([...(newExpected.delivered ?? [])]);
    expect(newActual.holdsBatch(["received"])).toBe(newExpected.holdsBatch(["received"]));
    expect(state.manager.getEntry.mock.calls.length - readsAtBootstrap).toBeLessThanOrEqual(4);
    expect(state.manager.getEntries).not.toHaveBeenCalled();
    expect(state.manager.getBranch).not.toHaveBeenCalled();
  });

  it("builds the compatibility receipt array without getEntries when point lookups exist", () => {
    const state = managerFor(linked(plainRows(1_000)));
    const values = confirmedSessionEntries(state.manager, () => true);
    expect(values).toHaveLength(1_000);
    expect(state.manager.getEntries).not.toHaveBeenCalled();
    expect(state.manager.getBranch).not.toHaveBeenCalled();
  });
});
