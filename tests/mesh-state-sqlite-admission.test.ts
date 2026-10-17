import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SqliteStateStore } from "../src/mesh/state-sqlite.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const identity = { id: "admission-test", name: "admission-test", kind: "agent" as const };
async function fixture(options: Parameters<typeof SqliteStateStore.open>[3] = {}) {
  const parent = fs.mkdtempSync(path.join(os.tmpdir(), "sqlite-admission-")); roots.push(parent);
  const root = path.join(parent, "mesh"); fs.mkdirSync(root);
  const store = await SqliteStateStore.open(root, 65536, 1000, { initialize: "create", ...options });
  return { store, root, file: path.join(root, "state.db") };
}

describe("SQLite native admission", () => {
  it("a native SQLite holder bounds acquisition and leaves no write or waiter", async () => {
    const { store, file } = await fixture({ lockTimeoutMs: 100 });
    const holder = new DatabaseSync(file, { timeout: 0 });
    holder.exec("BEGIN IMMEDIATE");
    const began = performance.now();
    try {
      await expect(store.put({ key: "deadline", value: 3, identity })).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT" });
      expect(performance.now() - began).toBeLessThan(1000);
      expect(store.get("deadline")).toBeUndefined();
      expect(store.stats()).toMatchObject({ writesWaiting: 0, fifoJoins: 0 });
    } finally { if (holder.isTransaction) holder.exec("ROLLBACK"); holder.close(); store.close(); }
  });

  it("abort interrupts native SQLite contention and leaves no write or waiter", async () => {
    const controller = new AbortController();
    const { store, file } = await fixture({ writeSignal: controller.signal });
    const holder = new DatabaseSync(file, { timeout: 0 });
    holder.exec("BEGIN IMMEDIATE");
    const abort = setTimeout(() => controller.abort(new Error("admission cancelled")), 40);
    const began = performance.now();
    try {
      await expect(store.put({ key: "abort", value: 4, identity })).rejects.toThrow("admission cancelled");
      expect(performance.now() - began).toBeLessThan(1000);
      expect(store.get("abort")).toBeUndefined();
      expect(store.stats()).toMatchObject({ writesWaiting: 0, fifoJoins: 0 });
    } finally { clearTimeout(abort); if (holder.isTransaction) holder.exec("ROLLBACK"); holder.close(); store.close(); }
  });
});
