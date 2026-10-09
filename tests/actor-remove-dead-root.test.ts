import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { main } from "../src/actors-cli.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { meshProcessStartedAt } from "../src/mesh/store.js";
import * as fileLock from "../src/residency/file-lock.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// smarty-dev#7817: `fabric-actors remove --confirm-dead-root` for the two dead-root shapes.
const fixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-dead-root-"));
  const meshRoot = path.join(root, "mesh");
  const config: ResidentHostConfig = {
    format: 1, rootId: "session:dead-root", sessionId: "dead-root", cwd: process.cwd(), projectRoot: process.cwd(),
    meshRoot, actorRoot: path.join(root, "actors"), residencyRoot: residentRoot(meshRoot, "session:dead-root"),
    fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 4, budgetUsd: 0 },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 30 }, retention: { ...DEFAULT_FABRIC_CONFIG.retention },
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  fs.mkdirSync(config.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
  const host = new ResidentHost(config, () => {});
  try { await host.start(); } catch (error) { await host.close(); throw error; }
  let closed = false;
  const cli = async (actor: string, flags: string[] = ["--confirm-dead-root", config.rootId]) => {
    let out = "", err = "";
    const code = await main(["remove", "--resident", config.residencyRoot, "--actor", actor, "--mesh-root", meshRoot, ...flags],
      { out: text => { out += text; }, err: text => { err += text; } });
    return { code, out, err };
  };
  const create = (name: string) => host.actors.create({ name, instructions: "Run", model: "fixture/visible",
    residency: "durable", transport: "process", extensions: false });
  const registered = (id: string) => new ActorRegistryStore(config.actorRoot).records().some(actor => actor.id === id);
  const archives = () => {
    const dir = path.join(config.residencyRoot, "archives");
    return fs.existsSync(dir) ? fs.readdirSync(dir).flatMap(name => fs.readdirSync(path.join(dir, name)).map(file => path.join(name, file))) : [];
  };
  const mainParticipant = (updatedAt: number) => writeParticipantFile(meshRoot, {
    key: "topology/participants/" + createHash("sha256").update(config.rootId).digest("hex"),
    value: { id: config.rootId, rootId: config.rootId, kind: "root", ownerHostId: config.rootId, ownerIdentityId: config.rootId },
    version: 1, updatedAt, updatedBy: { id: config.rootId, name: "main", kind: "main" },
  });
  const killResident = async () => {
    // A crash leaves owner.json and host.lock behind; their pid no longer exists.
    const owner = JSON.parse(fs.readFileSync(path.join(config.residencyRoot, "owner.json"), "utf8"));
    await host.close(); closed = true;
    for (const name of ["owner.json", "host.lock"]) {
      fs.writeFileSync(path.join(config.residencyRoot, name), JSON.stringify({ ...owner, pid: 2147483647, processStartTime: "1" }));
    }
  };
  return { config, host, cli, create, registered, archives, mainParticipant, killResident, close: async () => {
    if (!closed) { for (const actor of host.actors.listOwned()) await host.actors.stop(actor.id, undefined, true); await host.close(); }
    fs.rmSync(root, { recursive: true, force: true });
  } };
};

describe.skipIf(process.platform !== "linux")("fabric-actors remove on a dead root (smarty-dev#7817)", () => {
  it("case 1: removes under a lease its own resident renews with no Main, and still refuses with a live Main", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("leftover");
      // The root lease, renewed by the resident host itself (this process), with no Main session.
      writeHostLease(f.config.meshRoot, { id: f.config.rootId, rootId: f.config.rootId, identityId: f.config.rootId,
        startedAt: meshProcessStartedAt, updatedAt: Date.now(), expiresAt: Date.now() + 60_000,
        writer: { pid: process.pid, host: os.hostname(), releaseSha: "test", lockProtocol: 1, stateBackend: "file", startedAt: meshProcessStartedAt } });
      // A live Main participant record for the root still refuses.
      f.mainParticipant(Date.now());
      const refused = await f.cli(actor.id);
      expect(refused.code).toBe(1); expect(refused.err).toContain("live root lease");
      expect(f.registered(actor.id)).toBe(true);
      expect(f.archives()).toEqual([]);
      // No Main within the lease window: the resident's own lease does not block.
      f.mainParticipant(Date.now() - 10 * 60_000);
      const removed = await f.cli(actor.id);
      expect(removed).toMatchObject({ code: 0, err: "" });
      await f.host.actors.removalSettled(actor.id);
      expect(f.registered(actor.id)).toBe(false);
      expect(f.archives().map(file => path.basename(file))).toEqual(expect.arrayContaining(["SHA256SUMS", `${actor.id}.registry.json`]));
      // A lease written by another process (a Main) is never set aside.
      const other = await f.create("kept");
      writeHostLease(f.config.meshRoot, { id: f.config.rootId, rootId: f.config.rootId, identityId: f.config.rootId,
        updatedAt: Date.now(), expiresAt: Date.now() + 60_000,
        writer: { pid: 1, host: os.hostname(), releaseSha: "test", lockProtocol: 1, stateBackend: "file", startedAt: 1 } });
      expect((await f.cli(other.id)).err).toContain("live root lease");
      expect(f.registered(other.id)).toBe(true);
    } finally { await f.close(); }
  }, 40_000);

  it("case 2: removes offline under host.lock with a dead resident; refuses while host.lock is held or has a waiter", async () => {
    const f = await fixture();
    try {
      const actor = await f.create("orphan");
      const actorDir = path.join(f.config.actorRoot, actor.id);
      fs.mkdirSync(actorDir, { recursive: true }); fs.writeFileSync(path.join(actorDir, "session.jsonl"), "history\n");
      await f.killResident();
      const lock = path.join(f.config.residencyRoot, "host.lock");

      // Held: a live holder of the kernel fence.
      const held = await fileLock.lockFile(lock, 0, true);
      try {
        const result = await f.cli(actor.id);
        expect(result.code).toBe(1); expect(result.err).toContain("host.lock is held");
      } finally { fs.closeSync(held); }
      expect(f.registered(actor.id)).toBe(true);

      // A waiter: a host blocks on host.lock right after our claim; it wins.
      const real = fileLock.lockFile;
      const spy = vi.spyOn(fileLock, "lockFile").mockImplementation(async (file, wait, parent) => {
        const fd = await real(file, wait, parent);
        if (file === lock) {
          spawn("flock", ["-x", lock, "true"], { stdio: "ignore" }).unref();
          const inode = fs.statSync(lock).ino;
          await vi.waitFor(() => expect(fs.readFileSync("/proc/locks", "utf8").split("\n")
            .some(line => line.includes("->") && line.includes(`:${inode} `))).toBe(true), { timeout: 5_000, interval: 20 });
        }
        return fd;
      });
      try {
        const result = await f.cli(actor.id);
        expect(result.code).toBe(1); expect(result.err).toContain("waiter");
      } finally { spy.mockRestore(); }
      expect(f.registered(actor.id)).toBe(true);
      expect(f.archives()).toEqual([]);
      // The waiter got and released the fence; it left the dead holder records unchanged.
      await vi.waitFor(() => expect(fs.readFileSync("/proc/locks", "utf8").includes(`:${fs.statSync(lock).ino} `)).toBe(false), { timeout: 5_000 });

      const dry = await f.cli(actor.id, ["--dry-run"]);
      expect(dry).toMatchObject({ code: 0, err: "" });
      expect(JSON.parse(dry.out)).toMatchObject({ offline: true, dryRun: true, actor: { id: actor.id } });
      expect(f.registered(actor.id)).toBe(true);

      const removed = await f.cli(actor.id);
      expect(removed).toMatchObject({ code: 0, err: "" });
      const output = JSON.parse(removed.out);
      expect(output).toMatchObject({ offline: true, cleaned: true, actor: { id: actor.id } });
      expect(f.registered(actor.id)).toBe(false);
      expect(fs.existsSync(actorDir)).toBe(false);
      expect(fs.existsSync(path.join(f.config.actorRoot, `removal-${actor.id}.json`))).toBe(false);
      const archived = f.archives();
      expect(archived.map(file => path.basename(file)).sort()).toEqual(["SHA256SUMS", `${actor.id}.registry.json`, `${actor.id}.tar`].sort());
      const tar = path.join(output.archive, `${actor.id}.tar`);
      expect(fs.readFileSync(path.join(output.archive, "SHA256SUMS"), "utf8")).toContain(createHash("sha256").update(fs.readFileSync(tar)).digest("hex"));
    } finally { await f.close(); }
  }, 40_000);
});
