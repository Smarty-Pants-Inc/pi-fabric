import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
  NatsRootsUnavailableError,
  natsRootsSourceFromEnv,
  readNatsRoots,
  type NatsPresenceBuckets,
  type NatsRootsSource,
} from "../src/mesh/bridge-nats-roots.js";
import { KV_LEASE_TTL_MS, KV_PRESENCE_TTL_MS } from "../src/topology/participant-view.js";
import type { TransportKv, TransportKvEntry, TransportKvEvent } from "../src/transport/types.js";

const token = (id: string): string => createHash("sha256").update(id).digest("hex").slice(0, 32);
// Fictional ids only: pi-fabric is public.
const KATE = "session:00000000-0000-7000-8000-00000000000a";
const PAUL = "session:00000000-0000-7000-8000-00000000000b";
const TWIN = "session:00000000-0000-7000-8000-00000000000f";

/** A read-only snapshot watch: puts, then `ready` (unless held), then nothing until aborted. No writes accepted. */
class SnapshotKv implements TransportKv {
  readonly rows: TransportKvEntry<unknown>[] = [];
  writes = 0;
  constructor(readonly ttlMs: number, readonly holdReady = false) {}
  add(key: string, owner: string, value: unknown): void {
    this.rows.push({ key, machine: key.split(".")[0]!, owner, value, revision: this.rows.length + 1 });
  }
  async get(): Promise<undefined> { return undefined; }
  async put(): Promise<number> { this.writes += 1; throw new Error("read-only fixture"); }
  async delete(): Promise<boolean> { this.writes += 1; throw new Error("read-only fixture"); }
  async *watch<V>(_prefix: string, signal: AbortSignal): AsyncIterable<TransportKvEvent<V>> {
    for (const entry of this.rows) yield { type: "put", entry: entry as TransportKvEntry<V> };
    if (!this.holdReady) yield { type: "ready" };
    await new Promise<void>((resolve) => {
      if (signal.aborted) resolve();
      else signal.addEventListener("abort", () => resolve(), { once: true });
    });
  }
}

const root = (id: string, incarnation: string, change: Record<string, unknown> = {}): Record<string, unknown> => ({
  format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, ownerIncarnation: incarnation,
  sessionId: id.slice("session:".length), name: "main", status: "idle", runner: "pi", transport: "host",
  capabilities: ["steer", "followUp"], controlProtocol: "v1", startedAt: 1_000, updatedAt: 2_000, ...change,
});
const lease = (id: string, incarnation: string): Record<string, unknown> => ({
  id, identityId: id, rootId: id, incarnation, pid: 4242, pidStartTime: "1", bootId: "boot", updatedAt: 2_000, expiresAt: 17_000,
});

const SOURCE: NatsRootsSource = {
  servers: ["tls://broker.example.test:4222"], machine: "sandbox1", credentials: "/etc/fabric-test/nats/machine.creds",
  caFile: "/etc/fabric-test/nats/ca.pem", custodyStream: "FABRIC_CUSTODY", livenessStream: "FABRIC_LIVENESS",
  pathBMachines: ["ryzen1-bridge", "sandbox1"],
};

const fixture = (options: { holdReady?: boolean } = {}) => {
  const presence = new SnapshotKv(KV_PRESENCE_TTL_MS, options.holdReady);
  const leases = new SnapshotKv(KV_LEASE_TTL_MS);
  const put = (machine: string, id: string, incarnation: string, leaseIncarnation = incarnation, change: Record<string, unknown> = {}): void => {
    presence.add(`${machine}.p.${token(id)}`, `${id}#${incarnation}`, root(id, incarnation, change));
    leases.add(`${machine}.h.${token(id)}`, `${id}#${leaseIncarnation}`, lease(id, leaseIncarnation));
  };
  let closed = 0;
  const opened: NatsRootsSource[] = [];
  const open = async (source: NatsRootsSource): Promise<NatsPresenceBuckets> => {
    opened.push(source);
    return { machine: source.machine, presence, leases, close: async () => { closed += 1; } };
  };
  return { presence, leases, put, open, opened, closed: () => closed };
};

describe("nats-link roots reader", () => {
  it("lists live roots by the routing rule, with the key's machine, and never writes", async () => {
    const kv = fixture();
    kv.put("sandbox1", KATE, "inc-k");
    // Paul's v1 root, mirrored into KV by the link under the bridge machine.
    kv.put("ryzen1-bridge", PAUL, "inc-p");
    // A record whose lease names another incarnation is not live.
    kv.put("sandbox1", TWIN, "inc-new", "inc-old");
    const live = await readNatsRoots(SOURCE, {}, kv.open);
    expect(live.map((entry) => [entry.id, entry.machine, entry.live, entry.ownerIncarnation])).toEqual([
      [KATE, "sandbox1", true, "inc-k"], [PAUL, "ryzen1-bridge", true, "inc-p"],
    ]);
    expect(live[0]).toMatchObject({ kind: "root", sessionId: KATE.slice(8), capabilities: ["steer", "followUp"] });
    const all = await readNatsRoots(SOURCE, { includeStale: true }, kv.open);
    expect(all.find((entry) => entry.id === TWIN)).toMatchObject({ live: false });
    expect(kv.presence.writes + kv.leases.writes).toBe(0);
    expect(kv.closed()).toBe(2);
  });

  it("drops an id live on two machines (ambiguous, unroutable) and filters kinds", async () => {
    const kv = fixture();
    kv.put("sandbox1", KATE, "inc-a");
    kv.put("ryzen1-bridge", KATE, "inc-b");
    kv.put("sandbox1", PAUL, "inc-p", "inc-p", { kind: "actor", id: PAUL, runner: "pi" });
    expect(await readNatsRoots(SOURCE, { includeStale: true }, kv.open)).toEqual([]);
    expect((await readNatsRoots(SOURCE, { kinds: ["actor"] }, kv.open)).map((entry) => entry.id)).toEqual([PAUL]);
  });

  it("never answers [] without a complete snapshot: no ready, or no connection, is unavailable", async () => {
    const held = fixture({ holdReady: true });
    held.put("sandbox1", KATE, "inc-k");
    await expect(readNatsRoots(SOURCE, { timeoutMs: 200 }, held.open)).rejects.toBeInstanceOf(NatsRootsUnavailableError);
    expect(held.closed()).toBe(1);
    const refused = async (): Promise<NatsPresenceBuckets> => { throw new Error("NATS USER.INFO machine tag mismatch"); };
    await expect(readNatsRoots(SOURCE, {}, refused)).rejects.toThrow(/presence unreadable: NATS USER.INFO/);
  });

  it("takes its source from a v2 Main's own environment, flags winning field by field", () => {
    const env = { PI_FABRIC_MESH_NATS: JSON.stringify({
      servers: ["tls://intel1.example.test:4222"], machine: "sandbox1", credentials: "/etc/fabric-test/nats/machine.creds",
      custodyStream: "FABRIC_CUSTODY", livenessStream: "FABRIC_LIVENESS", pathBMachines: ["ryzen1-bridge", "sandbox1"],
      operatorMachine: "fabric-ops", tls: { caFile: "/etc/fabric-test/nats/ca.pem" } }) };
    expect(natsRootsSourceFromEnv(env)).toEqual({ ...SOURCE, servers: ["tls://intel1.example.test:4222"] });
    expect(natsRootsSourceFromEnv(env, { machine: "sandbox2", caFile: "/other/ca.pem" })).toMatchObject({ machine: "sandbox2", caFile: "/other/ca.pem" });
    expect(natsRootsSourceFromEnv({ PI_FABRIC_NATS_SERVERS: "tls://a.example.test:4222", PI_FABRIC_MACHINE: "m1",
      PI_FABRIC_NATS_CREDENTIALS: "/c", PI_FABRIC_NATS_CUSTODY_STREAM: "C", PI_FABRIC_NATS_LIVENESS_STREAM: "L",
      PI_FABRIC_NATS_MACHINES: "m1, m2" }, { caFile: "/ca" })).toEqual({ servers: ["tls://a.example.test:4222"], machine: "m1",
      credentials: "/c", caFile: "/ca", custodyStream: "C", livenessStream: "L", pathBMachines: ["m1", "m2"] });
    expect(() => natsRootsSourceFromEnv({})).toThrow(/missing: servers, machine, credentials, tls.caFile or tls.ca/);
    expect(() => natsRootsSourceFromEnv({ PI_FABRIC_MESH_NATS: "[1]" })).toThrow(/JSON object/);
  });
});
