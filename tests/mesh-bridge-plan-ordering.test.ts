import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { StoreBridgeSide, type BridgePresence } from "../src/mesh/bridge.js";
import { MeshStore } from "../src/mesh/store.js";
import { writeHostLease } from "../src/topology/host-leases.js";

// Refs Smarty-Pants-Inc/smarty-dev#7397: real SQLite transactions with the ordering
// exposed by Windows' leaf-inclusive lease stamps. No timing or slower-runner assumption.
const roots: string[] = [];
const stores: MeshStore[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("bridge mirror plan ordering", () => {
  it.each((["none", "unrelated", "renewal", "same-root-history"] as const)
    .flatMap(change => Array.from({ length: 5 }, (_, run) => [change, run + 1] as const)))(
    "lease effects between planning and commit, %s, run %s: no topology scan under the write lock", async (change, run) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-plan-ordering-"));
    roots.push(root);
    const hub = new MeshStore(root, 65536, 1000, { stateBackend: "sqlite" });
    stores.push(hub);
    const now = Date.now();
    const identity = { id: "session:plan-source", name: "source", kind: "main" as const };
    const presence: Pick<BridgePresence, "hosts" | "participants"> = {
      hosts: [{ record: { format: 1, id: identity.id, rootId: identity.id, identity,
        startedAt: now, updatedAt: now, expiresAt: now + 120000, incarnationToken: "source-A" }, expiresAt: now + 120000 }],
      participants: [],
    };
    const side = new StoreBridgeSide(hub, "far", () => now);
    // The fleet-scale lease count from mesh-bridge-lock-hold, so the five hold
    // samples include the real cached validation cost rather than an empty hub.
    for (let n = 0; n < 200; n++) {
      const id = `session:fleet-lease-${n}`;
      writeHostLease(root, { id, rootId: id, identityId: id, startedAt: now, updatedAt: now, expiresAt: now + 120000 });
    }
    await side.mirror(presence); // complete recovery before observing the drain's plans
    const before = hub.listAll("", { fresh: true });
    const sqlite = process.getBuiltinModule("node:sqlite") as typeof import("node:sqlite");
    const exec = sqlite.DatabaseSync.prototype.exec;
    type All = (this: { sourceSQL: string }, ...args: unknown[]) => unknown[];
    const statementProto = sqlite.StatementSync.prototype as unknown as { all: All };
    const all = statementProto.all;
    let writes = 0, scans = 0, batches = 0, holdMs = 0;
    const open = new WeakMap<object, number>();
    vi.spyOn(sqlite.DatabaseSync.prototype, "exec").mockImplementation(function (this: InstanceType<typeof sqlite.DatabaseSync>, sql: string) {
      const result = exec.call(this, sql);
      if (sql === "BEGIN IMMEDIATE") { open.set(this, performance.now()); writes += 1; }
      const began = open.get(this);
      if ((sql === "COMMIT" || sql === "ROLLBACK") && began !== undefined) {
        holdMs += performance.now() - began;
        open.delete(this);
        writes -= 1;
      }
      return result;
    });
    vi.spyOn(statementProto, "all").mockImplementation(function (this: { sourceSQL: string }, ...args: unknown[]) {
      if (writes > 0 && this.sourceSQL.includes("key >= ?") && typeof args[0] === "string" &&
          /^topology\/(hosts|participants|handoffs)\//.test(args[0])) scans += 1;
      return all.apply(this, args);
    });
    const writeBatch = hub.writeBatch.bind(hub);
    vi.spyOn(hub, "writeBatch").mockImplementation(input => {
      batches += 1;
      // Another bridge's post-commit lease effect runs after this plan, before it
      // acquires the shared hub's writer. Two such effects exhaust the old early path.
      if (change !== "none" && batches <= 2) {
        const id = change === "renewal" ? identity.id : `session:other-link-${batches}`;
        writeHostLease(root, { id, rootId: change === "same-root-history" || change === "renewal" ? identity.id : id,
          identityId: id, startedAt: now, updatedAt: now + batches, expiresAt: now + 120000 + batches,
          ...(change === "same-root-history" ? { incarnationToken: "source-B", bridgeSupersededTokens: ["source-A"] }
            : change === "renewal" ? { incarnationToken: "source-A" } : {}) });
        // Make the dependency change deterministic even on a coarse directory clock.
        fs.utimesSync(path.join(root, "host-leases"), now / 1000 + batches, now / 1000 + batches);
      }
      return writeBatch(input);
    });
    if (change === "same-root-history") {
      await expect(side.mirror(presence)).rejects.toMatchObject({ code: "FABRIC_BRIDGE_STALE_INCARNATION" });
      expect(hub.listAll("", { fresh: true })).toEqual(before);
    } else {
      await side.mirror(presence);
      expect(side.holds(identity.id, identity)).toBe(true);
    }
    console.log(JSON.stringify({ change, run, batches, inFenceScans: scans, holdMs: +holdMs.toFixed(3) }));
    expect(scans).toBe(0);
    expect(batches).toBe(1);
  });
});
