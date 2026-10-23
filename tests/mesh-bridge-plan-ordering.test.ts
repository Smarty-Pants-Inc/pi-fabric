import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { StoreBridgeSide, type BridgePresence } from "../src/mesh/bridge.js";
import { MeshStore } from "../src/mesh/store.js";
import * as hostLeases from "../src/topology/host-leases.js";
import { hostLeasePath, readHostLease, writeHostLease } from "../src/topology/host-leases.js";

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
  it.each((["empty", "none", "unrelated", "renewal", "same-root-history"] as const)
    .flatMap(change => Array.from({ length: 5 }, (_, run) => [change, run + 1] as const)))(
    "lease effects between planning and custody, %s, run %s: no in-fence scan; validate only consulted leases", async (change, run) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-plan-ordering-"));
    roots.push(root);
    const hub = new MeshStore(root, 65536, 1000, { stateBackend: "sqlite" });
    stores.push(hub);
    const now = Date.now();
    const identity = { id: "session:plan-source", name: "source", kind: "main" as const };
    const presence: Pick<BridgePresence, "hosts" | "participants"> = change === "empty" ? { hosts: [], participants: [] } : {
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
    let writes = 0, scans = 0, leaseScans = 0, batches = 0, custodies = 0, injected = 0, outsideLeaseReads = 0, holdMs = 0, leaseScanMs = 0;
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
    const readLeases = hostLeases.readHostLeases;
    vi.spyOn(hostLeases, "readHostLeases").mockImplementation((...args) => {
      const started = performance.now();
      const result = readLeases(...args);
      if (writes > 0) { leaseScans += 1; leaseScanMs += performance.now() - started; }
      else outsideLeaseReads += 1;
      return result;
    });
    const writeBatch = hub.writeBatch.bind(hub);
    vi.spyOn(hub, "writeBatch").mockImplementation(input => { batches += 1; return writeBatch(input); });
    const leaseBefore = change === "empty" ? undefined : fs.readFileSync(hostLeasePath(root, identity.id));
    const custody = hub.leaseCustody.bind(hub);
    vi.spyOn(hub, "leaseCustody").mockImplementation((file, operation, timeout, options) => {
      custodies += 1;
      // No-op mirrors now execute lease-only work without writeBatch. Inject both
      // competing effects at the actual plan→custody boundary, and prove they ran.
      if (custodies === 1 && change !== "none" && change !== "empty") {
        for (let effect = 1; effect <= 2; effect++) {
          const id = change === "renewal" ? identity.id : `session:other-link-${effect}`;
          writeHostLease(root, { id, rootId: change === "same-root-history" || change === "renewal" ? identity.id : id,
            identityId: id, startedAt: now, updatedAt: now + effect, expiresAt: now + 120000 + effect,
            ...(change === "same-root-history" ? { incarnationToken: "source-B", bridgeSupersededTokens: ["source-A"] }
              : change === "renewal" ? { incarnationToken: "source-A" } : {}) });
          fs.utimesSync(path.join(root, "host-leases"), now / 1000 + effect, now / 1000 + effect);
          injected += 1;
        }
      }
      return custody(file, operation, timeout, options);
    });
    if (change === "same-root-history") {
      await expect(side.mirror(presence)).rejects.toMatchObject({ code: "FABRIC_BRIDGE_STALE_INCARNATION" });
      expect(hub.listAll("", { fresh: true })).toEqual(before);
      expect(fs.readFileSync(hostLeasePath(root, identity.id))).toEqual(leaseBefore);
    } else {
      await side.mirror(presence);
      expect(side.holds(identity.id, identity)).toBe(change !== "empty");
      expect(hub.listAll("", { fresh: true })).toEqual(before);
      if (change === "renewal") expect(readHostLease(root, identity.id)).toMatchObject({ incarnationToken: "source-A",
        updatedAt: now + 2, expiresAt: now + 120000 + 2 });
      else if (change !== "empty") expect(fs.readFileSync(hostLeasePath(root, identity.id))).toEqual(leaseBefore);
    }
    console.log(JSON.stringify({ change, run, batches, custodies, injected, outsideLeaseReads,
      inFenceTopologyScans: scans, consultedLeaseScans: leaseScans,
      leaseScanMs: +leaseScanMs.toFixed(3), holdMs: +holdMs.toFixed(3) }));
    expect(injected).toBe(change === "none" || change === "empty" ? 0 : 2);
    // Complete fresh admission consumes a valid same-token renewal while its
    // selected gate is ALREADY held; releasing/reacquiring it is no longer needed.
    // Keep exact counts for every shape, not a generalized >=1 allowance.
    expect(custodies).toBe({ empty: 0, none: 1, unrelated: 1, renewal: 1, "same-root-history": 1 }[change]);
    expect(outsideLeaseReads).toBeGreaterThan(0);
    expect(scans).toBe(0);
    expect(leaseScans).toBe(0);
    expect(writes).toBe(0);
    expect(batches).toBe(0);
  });
});
