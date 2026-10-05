import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity, type MeshStateEntry } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { readHostLeases } from "../src/topology/host-leases.js";

const roots: string[] = [];
const identity: MeshIdentity = { id: "session:receipt-main", name: "receipt-main", kind: "main", sessionId: "receipt-main" };
const hash = (id: string) => createHash("sha256").update(id).digest("hex");
const deliveryKey = (id: string) => `residency/deliveries/${hash("recipient").slice(0, 32)}/${id}`;
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
const setup = (count: number) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-receipt-pass-"));
  roots.push(root);
  const old = Date.now() - 7 * 60 * 60 * 1000;
  const dir = path.join(root, "agent-completions", "receipts");
  fs.mkdirSync(dir, { recursive: true });
  const entries: Record<string, MeshStateEntry> = {};
  for (let n = 0; n < count; n++) {
    const id = String(n).padStart(4, "0");
    const key = deliveryKey(id);
    entries[key] = { key, version: n + 1, updatedAt: old, updatedBy: identity, value: {
      format: 1, id, rootId: "recipient", createdAt: old, from: { id, kind: "agent" }, agentCompletionId: id,
      message: "backlog".repeat(640),
    } };
    fs.writeFileSync(path.join(dir, `${hash(id)}.json`), JSON.stringify({ id, sessionId: "original", consumedAt: old }));
  }
  fs.writeFileSync(path.join(root, "state.json"), JSON.stringify({ format: 1, entries }));
  return { root, dir, mesh: new MeshStore(root, 64 * 1024, 1000), receipt: path.join(dir, `${hash("0000")}.json`) };
};
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("Astra F4 unlocked bounded receipt passes", () => {
  it("keeps Main's heartbeat, foreground and a competing real mutation responsive with a 3 MB backlog", async () => {
    const { root, dir, mesh } = setup(600);
    const competitor = new MeshStore(root, 64 * 1024, 1000, { lockTimeoutMs: 500 });
    const directory = new ParticipantDirectory(mesh, {
      enabled: true, identity, rootId: identity.id, hostId: identity.id, heartbeatMs: 100, reapDeadHosts: false,
    });
    const main = new MainAgentController({ getThinkingLevel: () => "off" } as ExtensionAPI, identity.id, true, root, identity.sessionId!);
    directory.registerSource(() => [directory.root(main.info())]);
    const passSpy = vi.spyOn(mesh, "compactReceipts");
    const started = deferred(), release = deferred();
    const open = fs.promises.open.bind(fs.promises);
    let receiptAttempts = 0;
    vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      const handle = await open(...args);
      if (String(args[0]).startsWith(dir + path.sep)) {
        expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
        receiptAttempts++;
        const sync = handle.sync.bind(handle);
        handle.sync = async () => { started.resolve(); await release.promise; await sync(); };
      }
      return handle;
    });
    const holds: number[] = [];
    let acquired: number | undefined;
    const mkdir = fs.mkdirSync.bind(fs), rename = fs.renameSync.bind(fs);
    vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
      const result = (mkdir as (...args: unknown[]) => unknown)(file, ...args);
      if (String(file) === path.join(root, ".lock")) acquired = performance.now();
      return result;
    }) as typeof fs.mkdirSync);
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      const result = rename(from, to);
      if (String(from) === path.join(root, ".lock") && acquired !== undefined) {
        holds.push(performance.now() - acquired); acquired = undefined;
      }
      return result;
    });
    let foregroundTicks = 0;
    const timer = setInterval(() => { foregroundTicks++; }, 10);
    try {
      await directory.start();
      expect(passSpy).not.toHaveBeenCalled(); // startup/foreground refresh does not run receipt work
      const initialLease = readHostLeases(root).get(identity.id)!.updatedAt;
      await started.promise; // the real heartbeat timer launches background compaction
      await sleep(200); // a slow asynchronous file barrier spans multiple heartbeats
      await expect(mesh.put({ key: "probe/foreground-reply", value: true, identity })).resolves.toBeDefined();
      await expect(competitor.put({ key: "probe/competing-mutation", value: true, identity })).resolves.toBeDefined();
      await expect(competitor.writeBatch({ identity, ops: [{ kind: "put", key: "probe/batch", value: true }] })).resolves.toHaveLength(1);
      await expect(competitor.delete({ key: "probe/batch" })).resolves.toMatchObject({ deleted: true });
      expect(foregroundTicks).toBeGreaterThanOrEqual(5);
      expect(readHostLeases(root).get(identity.id)!.updatedAt).toBeGreaterThan(initialLease);
      expect(directory.canConsumeMesh()).toBe(true);
      expect(receiptAttempts).toBe(1);
      release.resolve();
      await mesh.compactReceipts(); // coalesces with the still-running timer pass
    } finally {
      clearInterval(timer); release.resolve(); await directory.close();
    }
    expect(mesh.get(deliveryKey("0000"))).toBeUndefined();
    expect(mesh.listAll("residency/deliveries/")).toHaveLength(599);
    expect(holds.length).toBeGreaterThan(0);
    expect(Math.max(...holds)).toBeLessThan(1000); // comfortably below the normal 10 s acquisition deadline
  });

  it("retains a delivery changed by a competing writer while unlocked confirmation is in flight", async () => {
    const { root, dir, mesh } = setup(1);
    const competitor = new MeshStore(root, 64 * 1024, 1000);
    const started = deferred(), release = deferred();
    const open = fs.promises.open.bind(fs.promises);
    vi.spyOn(fs.promises, "open").mockImplementation(async (...args) => {
      const handle = await open(...args);
      if (String(args[0]).startsWith(dir + path.sep)) {
        const sync = handle.sync.bind(handle);
        handle.sync = async () => { started.resolve(); await release.promise; await sync(); };
      }
      return handle;
    });
    const pass = mesh.compactReceipts();
    try {
      await started.promise;
      await competitor.put({ key: deliveryKey("0000"), value: { custody: "changed" }, identity });
    } finally { release.resolve(); await pass; }
    expect(mesh.get(deliveryKey("0000"))!.value).toEqual({ custody: "changed" });
  });

  it("retains a receipt replaced between unlocked confirmation and cheap locked revalidation", async () => {
    const { root, mesh, receipt } = setup(1);
    const mkdir = fs.mkdirSync.bind(fs);
    vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
      const result = (mkdir as (...args: unknown[]) => unknown)(file, ...args);
      if (String(file) === path.join(root, ".lock")) fs.writeFileSync(receipt, JSON.stringify({ id: "0000", sessionId: "other", consumedAt: Date.now() }));
      return result;
    }) as typeof fs.mkdirSync);
    expect(await mesh.compactReceipts()).toBe(0);
    expect(mesh.get(deliveryKey("0000"))).toBeDefined();
  });

  it("makes just an immediate background lock attempt and reconfirms on a later pass", async () => {
    const { root, mesh } = setup(1);
    const lock = path.join(root, ".lock");
    fs.mkdirSync(lock);
    fs.writeFileSync(path.join(lock, "owner"), `held\n${process.pid}\n${Date.now()}\n`);
    await expect(mesh.compactReceipts()).rejects.toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT", attempts: 1 });
    expect(mesh.get(deliveryKey("0000"))).toBeDefined();
    fs.rmSync(lock, { recursive: true });
    expect(await mesh.compactReceipts()).toBe(1);
  });
});
