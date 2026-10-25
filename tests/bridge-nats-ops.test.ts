import { spawn, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { parseNatsLinkConfig, type NatsLinkConfig } from "../src/mesh/bridge-nats-config.js";
import {
  acquireNatsLinkLock,
  NatsLinkBusyError,
  natsLinkLockHolder,
  natsLinkLockPath,
  withdrawNatsLink,
} from "../src/mesh/bridge-nats-ops.js";
import type { NatsPresenceBuckets, NatsRootsSource } from "../src/mesh/bridge-nats-roots.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { KV_LEASE_TTL_MS, KV_PRESENCE_TTL_MS } from "../src/topology/participant-view.js";
import type { TransportKv, TransportKvEntry, TransportKvEvent } from "../src/transport/types.js";

// Fictional ids only: pi-fabric is public.
const KATE = "session:00000000-0000-7000-8000-00000000000a";
const PAUL = "session:00000000-0000-7000-8000-00000000000b";
const OTHER = "session:00000000-0000-7000-8000-00000000000c";
const hash = (id: string): string => createHash("sha256").update(id).digest("hex");

const roots: string[] = [];
const children: ChildProcess[] = [];
afterEach(() => {
  for (const child of children.splice(0)) child.kill("SIGKILL");
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

/** CAS rows plus a snapshot watch; deletes are recorded and applied. */
class CasKv implements TransportKv {
  readonly rows = new Map<string, TransportKvEntry<unknown>>();
  readonly deleted: string[] = [];
  #revision = 0;
  constructor(readonly ttlMs: number) {}
  add(key: string): void { this.rows.set(key, { key, machine: key.split(".")[0]!, owner: "o#i", value: {}, revision: ++this.#revision }); }
  bump(key: string): void { this.rows.set(key, { ...this.rows.get(key)!, revision: ++this.#revision }); }
  async get(): Promise<undefined> { return undefined; }
  async put(): Promise<number> { throw new Error("not used"); }
  async delete(key: string, ifRevision: number): Promise<boolean> {
    if (this.rows.get(key)?.revision !== ifRevision) return false;
    this.rows.delete(key);
    this.deleted.push(key);
    return true;
  }
  async *watch<V>(_prefix: string, signal: AbortSignal): AsyncIterable<TransportKvEvent<V>> {
    for (const entry of [...this.rows.values()]) yield { type: "put", entry: entry as TransportKvEntry<V> };
    yield { type: "ready" };
    await new Promise<void>((resolve) => signal.aborted ? resolve() : signal.addEventListener("abort", () => resolve(), { once: true }));
  }
}

const addRoot = async (store: MeshStore, id: string, remoteHost?: string): Promise<void> => {
  const now = Date.now();
  const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: id.slice(8) };
  const remote = remoteHost ? { remoteHost } : {};
  await store.put({ key: `topology/hosts/${hash(id)}`, identity, value: {
    format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + 60_000, ...remote } });
  await store.put({ key: `topology/participants/${hash(id)}`, identity, value: {
    format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, name: "main", status: "idle", runner: "pi",
    transport: "host", capabilities: ["followUp"], sessionId: id.slice(8), startedAt: now, updatedAt: now, controlProtocol: "v1",
    ownerIncarnation: "inc", ...remote } });
};
const participants = (store: MeshStore): string[] =>
  store.listAll("topology/participants/", { fresh: true }).map((entry) => (entry.value as { id: string }).id).sort();

const setup = async (): Promise<{ config: NatsLinkConfig; store: MeshStore; dir: string }> => {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "nats-link-ops-")));
  roots.push(dir);
  const mesh = path.join(dir, "mesh");
  const store = new MeshStore(mesh, 64 * 1024, 1_000);
  await store.publish({ topic: "test.setup", kind: "setup", from: { id: "test", name: "test", kind: "main" } });
  const stateDir = path.join(dir, "state");
  fs.mkdirSync(stateDir, { mode: 0o700 });
  const config = parseNatsLinkConfig({
    version: 1, name: "ryzen1", link: "v2", file: { mesh }, stateDir, orgJson: path.join(dir, "org.json"),
    nats: { servers: ["tls://broker.example.test:4222"], machine: "ryzen1-bridge", credentials: path.join(dir, "bridge.creds"),
      caFile: path.join(dir, "ca.pem"), custodyStream: "FABRIC_CUSTODY", livenessStream: "FABRIC_LIVENESS",
      pathBMachines: ["ryzen1-bridge", "sandbox1"], operatorMachine: "fabric-ops" },
    allow: { file: [{ kind: "root", id: PAUL }], nats: [{ kind: "root", id: KATE, machine: "sandbox1" }] },
  });
  return { config, store, dir };
};

/** A live process that is not this one, to stand for a running link. */
const sleeper = (): ChildProcess => {
  const child = spawn(process.execPath, ["-e", "setTimeout(() => {}, 60_000)"], { stdio: "ignore" });
  children.push(child);
  return child;
};
const holdLock = (config: NatsLinkConfig, pid: number, purpose: "start" | "withdraw" = "start"): void => {
  const startTime = processStartTime(pid);
  fs.writeFileSync(natsLinkLockPath(config), `${JSON.stringify({ v: 1, link: "v2", purpose, pid,
    ...(startTime ? { startTime } : {}), at: new Date().toISOString() })}\n`, { mode: 0o600 });
};
const until = async (check: () => boolean): Promise<void> => {
  for (let index = 0; index < 100 && !check(); index += 1) await new Promise((resolve) => setTimeout(resolve, 20));
};

describe("nats-link lock", () => {
  // Stated skip: nats-link is Linux/macOS only (POSIX file modes, fsync/rename and lock semantics); start refuses on Windows.
  it.skipIf(process.platform === "win32")("lets one process own a state directory, replaces a stale lock and releases only its own", async () => {
    const { config } = await setup();
    const release = acquireNatsLinkLock(config, "start");
    expect(natsLinkLockHolder(config)).toMatchObject({ link: "v2", purpose: "start", pid: process.pid });
    expect(fs.statSync(natsLinkLockPath(config)).mode & 0o777).toBe(0o600);
    release();
    expect(fs.existsSync(natsLinkLockPath(config))).toBe(false);

    const child = sleeper();
    await until(() => processStartTime(child.pid!) !== undefined || process.platform !== "linux");
    holdLock(config, child.pid!);
    expect(() => acquireNatsLinkLock(config, "start")).toThrow(NatsLinkBusyError);
    child.kill("SIGKILL");
    await until(() => natsLinkLockHolder(config) === undefined);
    // The holder is gone: its lock is stale and replaced.
    const again = acquireNatsLinkLock(config, "withdraw");
    expect(natsLinkLockHolder(config)).toMatchObject({ purpose: "withdraw", pid: process.pid });
    // A release never removes a lock another process now holds.
    holdLock(config, sleeper().pid!);
    again();
    expect(fs.existsSync(natsLinkLockPath(config))).toBe(true);
  });
});

describe("nats-link withdraw", () => {
  const buckets = () => {
    const presence = new CasKv(KV_PRESENCE_TTL_MS), leases = new CasKv(KV_LEASE_TTL_MS);
    for (const id of [PAUL, OTHER]) {
      presence.add(`ryzen1-bridge.p.${hash(id).slice(0, 32)}`);
      leases.add(`ryzen1-bridge.h.${hash(id).slice(0, 32)}`);
    }
    // Kate's own records live under her machine: never the link's to delete.
    presence.add(`sandbox1.p.${hash(KATE).slice(0, 32)}`);
    leases.add(`sandbox1.h.${hash(KATE).slice(0, 32)}`);
    let opened: NatsRootsSource | undefined;
    let closed = 0;
    const open = async (source: NatsRootsSource): Promise<NatsPresenceBuckets> => {
      opened = source;
      return { machine: source.machine, presence, leases, close: async () => { closed += 1; } };
    };
    return { presence, leases, open, opened: () => opened, closed: () => closed };
  };

  it("stops the running link, then deletes only its own NATS keys (leases first) and its file mirrors", async () => {
    const { config, store } = await setup();
    await addRoot(store, PAUL);
    await addRoot(store, KATE, "v2");
    await addRoot(store, OTHER, "forge");
    const child = sleeper();
    await until(() => processStartTime(child.pid!) !== undefined || process.platform !== "linux");
    holdLock(config, child.pid!);
    const kv = buckets();
    const report = await withdrawNatsLink(config, { open: kv.open, stopTimeoutMs: 5_000 });
    expect(report.problems).toEqual([]);
    expect(report).toMatchObject({ ok: true, link: "v2", stopped: { pid: child.pid },
      file: { removed: [KATE], remaining: [] }, nats: { machine: "ryzen1-bridge", moved: [] } });
    const deleted = (report.nats as { readonly deleted: readonly string[] }).deleted;
    expect(deleted).toHaveLength(4);
    expect(deleted.slice(0, 2).every((key) => key.startsWith("ryzen1-bridge.h."))).toBe(true);
    expect([...kv.presence.rows.keys(), ...kv.leases.rows.keys()].every((key) => key.startsWith("sandbox1."))).toBe(true);
    expect(kv.opened()).toMatchObject({ machine: "ryzen1-bridge", credentials: config.nats.credentials });
    expect(kv.closed()).toBe(1);
    // The hub's native root and another link's mirror stay.
    expect(participants(store)).toEqual([PAUL, OTHER].sort());
    expect(fs.existsSync(natsLinkLockPath(config))).toBe(false);
  });

  it("withdraws nothing while a link it could not stop still runs, and reports a key that moved", async () => {
    const { config, store } = await setup();
    await addRoot(store, KATE, "v2");
    const child = sleeper();
    await until(() => processStartTime(child.pid!) !== undefined || process.platform !== "linux");
    holdLock(config, child.pid!);
    const kv = buckets();
    const stuck = await withdrawNatsLink(config, { open: kv.open, stopTimeoutMs: 200, kill: () => undefined });
    expect(stuck).toMatchObject({ ok: false, stopped: null, problems: [`the running link (pid ${child.pid}) did not stop; nothing withdrawn`] });
    expect(kv.presence.deleted).toEqual([]);
    expect(participants(store)).toEqual([KATE]);

    child.kill("SIGKILL");
    await until(() => natsLinkLockHolder(config) === undefined);
    // A writer still active on NATS: its key changes between the snapshot and the delete.
    const racing = buckets();
    const original = racing.leases.delete.bind(racing.leases);
    racing.leases.delete = async (key, revision) => { racing.leases.bump(key); return original(key, revision); };
    const moved = await withdrawNatsLink(config, { open: racing.open });
    expect(moved.ok).toBe(false);
    expect(moved.problems).toEqual(["NATS keys changed during the withdraw (a writer is still active): 2"]);
    expect(moved.file).toEqual({ removed: [KATE], remaining: [] });

    const down = await withdrawNatsLink(config, { open: async () => { throw new Error("NATS USER.INFO machine tag mismatch"); } });
    expect(down).toMatchObject({ ok: false, nats: { error: "NATS USER.INFO machine tag mismatch" }, problems: ["NATS withdraw failed"] });
  });

  it("refuses while another withdraw runs", async () => {
    const { config } = await setup();
    const child = sleeper();
    await until(() => processStartTime(child.pid!) !== undefined || process.platform !== "linux");
    holdLock(config, child.pid!, "withdraw");
    await expect(withdrawNatsLink(config, { open: buckets().open })).rejects.toBeInstanceOf(NatsLinkBusyError);
  });
});
