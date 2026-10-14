import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { expect, it, vi } from "vitest";
import { ActorRegistryBusyError, ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";

it("reuses the adoption/publication registry receipt for synchronous nested queue work", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "queue-registry-fence-"));
  const store = new ActorRegistryStore(root), peer = new ActorRegistryStore(root);
  const mkdir = vi.spyOn(fs, "mkdirSync");
  const lock = path.join(root, "actors.json.lock");
  try {
    const entered: string[] = [];
    await store.withLock(async () => {
      expect(fs.existsSync(lock)).toBe(true);
      peer.withTryLockSync(() => {
        entered.push("queue");
        expect(fs.existsSync(lock)).toBe(true);
      });
      await Promise.resolve();
      store.withTryLockSync(() => entered.push("after await"));
    });
    expect(entered).toEqual(["queue", "after await"]);
    expect(mkdir.mock.calls.filter(([file]) => String(file) === lock)).toHaveLength(1);
    expect(fs.existsSync(lock)).toBe(false);
    store.withTryLockSync(() => expect(fs.existsSync(lock)).toBe(true));
    expect(mkdir.mock.calls.filter(([file]) => String(file) === lock)).toHaveLength(2);
  } finally { mkdir.mockRestore(); fs.rmSync(root, { recursive: true, force: true }); }
});

it("releases a synchronous registry save before same-turn queue checkpoint work", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "queue-synchronous-save-"));
  const store = new ActorRegistryStore(root), peer = new ActorRegistryStore(root);
  let checkpointed = false, error: unknown;
  try {
    const save = store.withLock(() => undefined);
    try { peer.withTryLockSync(() => { checkpointed = true; }); }
    catch (failure) { error = failure; }
    await save;
    expect(error).toBeUndefined();
    expect(checkpointed).toBe(true);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});

it("the real orphan adoption path writes the successor queue and removes its predecessor only inside the same registry fence", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "queue-adoption-fence-"));
  const actorRoot = path.join(root, "actors");
  const mesh = new MeshStore(path.join(root, "mesh"), 65536, 100);
  const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "runs") });
  let alive = true;
  const make = (name: string) => new ActorManager(name, { id: `session:${name}`, name, kind: "main" }, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 600000 }, agents, () => {}, { persistent: true, actorRoot,
      rootId: `session:${name}`, claimResidency: "session", adoptionGraceMs: 0, retentionSweepMs: 600000,
      canManageActor: () => name === "predecessor" && alive ? true : undefined,
      lineageAlive: id => id !== "session:predecessor" || alive });
  const owner = make("predecessor"); owner.pauseForRelease();
  let successor: ActorManager | undefined;
  const remove = fs.rmSync, rename = fs.renameSync;
  const removals: string[] = [], writes: string[] = [];
  const recorder = vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
    if (/^queue-[a-f0-9]{16}\.json$/.test(path.basename(String(file)))) {
      expect(fs.existsSync(path.join(actorRoot, "actors.json.lock", "owner"))).toBe(true);
      removals.push(String(file)); return; // record product source cleanup, never execute it
    }
    return remove(file, options);
  });
  try {
    const actor = await owner.create({ name: "adopted-backlog", instructions: "Wait", triggerTurn: false, residency: "session" });
    owner.tell(actor.id, "accepted predecessor backlog");
    await owner.close(); alive = false;
    const before = fs.readdirSync(path.dirname(actor.sessionFile!)).filter(file => file.startsWith("queue-"));
    expect(before).toHaveLength(1);
    const writer = vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      if (/^queue-[a-f0-9]{16}\.json$/.test(path.basename(String(target)))) {
        expect(fs.existsSync(path.join(actorRoot, "actors.json.lock", "owner"))).toBe(true);
        writes.push(String(target));
      }
      return rename(source, target);
    });
    try {
      successor = make("successor"); successor.pauseForRelease();
      successor.listOwned();
      await vi.waitFor(() => expect(successor!.status(actor.id).rootId).toBe("session:successor"), { timeout: 5000 });
      await vi.waitFor(() => expect(writes.length).toBeGreaterThan(0), { timeout: 5000 });
      const copy = JSON.parse(fs.readFileSync(writes[0]!, "utf8"));
      expect(copy.items.some((item: { payload?: { message?: string } }) => item.payload?.message === "accepted predecessor backlog")).toBe(true);
      expect(removals.some(file => path.basename(file) === before[0])).toBe(true);
      expect(fs.existsSync(path.join(path.dirname(actor.sessionFile!), before[0]!))).toBe(true); // recorded no-op
      expect(agents.list()).toEqual([]); // held; no activation/archive/pruning path executes
    } finally { writer.mockRestore(); }
  } finally {
    await successor?.close(); await owner.close(); await agents.close();
    recorder.mockRestore(); fs.rmSync(root, { recursive: true, force: true });
  }
}, 20000);

it("a manager whose first queue read is busy restores its accepted backlog on the pending-work deadline", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "queue-acquisition-busy-"));
  const actorRoot = path.join(root, "actors");
  const mesh = new MeshStore(path.join(root, "mesh"), 65536, 100);
  const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "runs") });
  const make = () => new ActorManager("same", { id: "session:same", name: "owner", kind: "main" }, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 600000 }, agents, () => {}, { persistent: true, actorRoot,
      rootId: "session:same", claimResidency: "session", releasePaused: true, preparationRetryMs: 25, retentionSweepMs: 600000,
      canManageActor: () => true });
  const first = make(); let next: ActorManager | undefined;
  const remove = fs.rmSync;
  const recorder = vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
    if (/^queue-[a-f0-9]{16}\.json$/.test(path.basename(String(file)))) return;
    return remove(file, options);
  });
  let release!: () => void;
  try {
    const actor = await first.create({ name: "read-busy", instructions: "Wait", triggerTurn: false });
    first.tell(actor.id, "accepted before restart"); await first.close();
    let entered!: () => void;
    const ready = new Promise<void>(resolve => { entered = resolve; });
    const gate = new Promise<void>(resolve => { release = resolve; });
    const holder = new ActorRegistryStore(actorRoot).withLock(async () => { entered(); await gate; });
    await ready;
    next = make();
    release(); await holder;
    await vi.waitFor(() => expect(next!.status(actor.id).queued).toBe(1), { timeout: 5000, interval: 10 });
    expect(agents.list()).toEqual([]);
  } finally {
    release?.(); await next?.close(); await first.close(); await agents.close();
    recorder.mockRestore(); fs.rmSync(root, { recursive: true, force: true });
  }
}, 10000);

it.each([false, true])("binds a rightful first publication only if the accepted queue revision is unchanged (replaced: %s)", async replaced => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "queue-first-publication-"));
  const actorRoot = path.join(root, "actors"), mesh = new MeshStore(path.join(root, "mesh"), 65536, 100);
  const identity = { id: "session:first", name: "owner", kind: "main" as const };
  const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "runs") });
  let published = false;
  const manager = new ActorManager("first", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 600000 }, agents, () => {},
    { persistent: true, actorRoot, releasePaused: true, retentionSweepMs: 600000, canManageActor: () => published ? true : undefined });
  const remove = fs.rmSync, rename = fs.renameSync;
  const recorder = vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
    if (/^queue-[a-f0-9]{16}\.json$/.test(path.basename(String(file)))) return;
    return remove(file, options);
  });
  const digest = (value: string) => createHash("sha256").update(value).digest("hex");
  try {
    const actor = await manager.create({ name: "first-publication", instructions: "Wait", triggerTurn: false });
    manager.tell(actor.id, "accepted before first participant publication");
    const file = path.join(path.dirname(actor.sessionFile!), `queue-${digest([identity.id, actor.residency].join("\0")).slice(0, 16)}.json`);
    if (replaced) {
      const staging = `${file}.fixture`; fs.writeFileSync(staging, JSON.stringify({ format: 1, items: [{ id: "external-work" }] }));
      fs.renameSync(staging, file);
    }
    const before = fs.readFileSync(file, "utf8");
    await new ActorRegistryStore(actorRoot).withLock(async () => {
      await mesh.put({ key: `topology/hosts/${digest("host:first")}`, identity,
        value: { format: 1, id: "host:first", rootId: identity.id, identity, startedAt: 1, updatedAt: Date.now(), expiresAt: Date.now() + 120000 } });
      await mesh.put({ key: `topology/participants/${digest(actor.id)}`, identity,
        value: { format: 1, id: actor.id, kind: "actor", rootId: identity.id, ownerHostId: "host:first", ownerIdentityId: identity.id,
          name: actor.name, status: "idle", runner: "pi", transport: "host", capabilities: [], startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1" } });
      published = true;
    });
    const attempts: string[] = [];
    const replace = vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      if (String(target) === file) { attempts.push(file); if (replaced) return; }
      return rename(source, target);
    });
    try {
      manager.tell(actor.id, "accepted after rightful first publication");
      if (replaced) {
        expect(attempts).toEqual([]); expect(fs.readFileSync(file, "utf8")).toBe(before);
        expect(manager.status(actor.id).lastError).toContain("queue custody changed");
      } else {
        expect(attempts).toEqual([file]);
        expect(JSON.parse(fs.readFileSync(file, "utf8")).items).toHaveLength(manager.status(actor.id).queued);
        expect(manager.status(actor.id).queued).toBe(2);
        expect(manager.status(actor.id).lastError).toBeUndefined();
      }
      expect(agents.list()).toEqual([]);
    } finally { await manager.close(); replace.mockRestore(); }
  } finally { await manager.close(); await agents.close(); recorder.mockRestore(); fs.rmSync(root, { recursive: true, force: true }); }
});

it.each([false, true])("keeps a byte-identical queue receipt across an atomic rewrite and admission veto (veto: %s)", async veto => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "queue-content-receipt-"));
  const actorRoot = path.join(root, "actors"), mesh = new MeshStore(path.join(root, "mesh"), 65536, 100);
  const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "runs") });
  let admitted = true;
  const manager = new ActorManager("receipt", { id: "session:receipt", name: "owner", kind: "main" }, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 600000 }, agents, () => {},
    { persistent: true, actorRoot, releasePaused: true, retentionSweepMs: 600000, canManageActor: () => admitted });
  const remove = fs.rmSync;
  const recorder = vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
    if (/^queue-[a-f0-9]{16}\.json$/.test(path.basename(String(file)))) return;
    return remove(file, options);
  });
  try {
    const actor = await manager.create({ name: "same-content", instructions: "Wait", triggerTurn: false });
    manager.tell(actor.id, "first accepted item");
    const file = path.join(path.dirname(actor.sessionFile!), fs.readdirSync(path.dirname(actor.sessionFile!)).find(file => /^queue-/.test(file))!);
    const bytes = fs.readFileSync(file);
    fs.writeFileSync(`${file}.fixture`, bytes); fs.renameSync(`${file}.fixture`, file);
    if (veto) {
      admitted = false;
      expect(manager.listOwned()).toEqual([]);
      expect(await manager.haltAll()).toEqual({ halted: 0 });
      expect(JSON.parse(fs.readFileSync(file, "utf8")).items).toEqual([]);
    } else {
      manager.tell(actor.id, "second accepted item");
      expect(JSON.parse(fs.readFileSync(file, "utf8")).items).toHaveLength(2);
    }
    expect(manager.status(actor.id).lastError).toBeUndefined();
    expect(agents.list()).toEqual([]);
  } finally {
    await manager.close(); await agents.close(); recorder.mockRestore(); fs.rmSync(root, { recursive: true, force: true });
  }
});

it.each(["win32", "linux"] as const)("keeps Windows replacement-open retries bounded without changing Linux (%s)", async platform => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "queue-windows-open-"));
  const mesh = new MeshStore(path.join(root, "mesh"), 65536, 100);
  const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "runs") });
  const manager = new ActorManager("read-retry", { id: "session:read-retry", name: "owner", kind: "main" }, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 600000 }, agents, () => {}, { persistent: true,
      actorRoot: path.join(root, "actors"), releasePaused: true, retentionSweepMs: 600000, canManageActor: () => true });
  const descriptor = Object.getOwnPropertyDescriptor(process, "platform")!;
  const open = fs.openSync;
  let failures = 0;
  let spy: ReturnType<typeof vi.spyOn> | undefined;
  try {
    const actor = await manager.create({ name: "windows-replacement", instructions: "Wait", triggerTurn: false });
    Object.defineProperty(process, "platform", { ...descriptor, value: platform });
    spy = vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, flags: string | number, mode?: fs.Mode) => {
      if (/queue-[a-f0-9]+\.json$/.test(String(file)) && typeof flags === "number" && fs.existsSync(file) && failures === 0) {
        failures++;
        throw Object.assign(new Error("replacement pending deletion"), { code: "EPERM" });
      }
      return open(file, flags, mode);
    }) as typeof fs.openSync);
    manager.tell(actor.id, "first accepted item");
    manager.tell(actor.id, "second accepted item");
    const file = fs.readdirSync(path.dirname(actor.sessionFile!)).find(name => name.startsWith("queue-"))!;
    const saved = JSON.parse(fs.readFileSync(path.join(path.dirname(actor.sessionFile!), file), "utf8"));
    expect(failures).toBe(1);
    expect(saved.items).toHaveLength(platform === "win32" ? 2 : 1);
    if (platform === "win32") expect(manager.status(actor.id).lastError).toBeUndefined();
    else expect(manager.status(actor.id).lastError).toContain("custody changed");
    expect(agents.list()).toEqual([]);
  } finally {
    spy?.mockRestore(); Object.defineProperty(process, "platform", descriptor);
    await manager.close(); await agents.close(); fs.rmSync(root, { recursive: true, force: true });
  }
});

it("a cross-process holder gives one zero-wait busy refusal with no mutation or stale recovery", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "queue-registry-busy-"));
  const store = new ActorRegistryStore(root);
  const script = `
    import { ActorRegistryStore } from ${JSON.stringify(path.resolve("src/actors/registry-store.ts"))};
    const store = new ActorRegistryStore(${JSON.stringify(root)});
    let ran = false;
    const started = performance.now();
    try { store.withTryLockSync(() => { ran = true; }); }
    catch (error) { console.log(JSON.stringify({ ran, code: error.code, retryable: error.retryable, ms: performance.now()-started })); }
  `;
  try {
    await store.withLock(() => {
      const owner = fs.readFileSync(path.join(root, "actors.json.lock", "owner"), "utf8");
      const outcome = JSON.parse(execFileSync("bun", ["--eval", script], { encoding: "utf8", timeout: 10_000 }).trim());
      expect(outcome).toMatchObject({ ran: false, code: "FABRIC_ACTOR_REGISTRY_BUSY", retryable: true });
      expect(outcome.ms).toBeLessThan(100);
      expect(fs.readFileSync(path.join(root, "actors.json.lock", "owner"), "utf8")).toBe(owner);
    });
    expect(() => store.withTryLockSync(() => { throw new ActorRegistryBusyError(); })).toThrow(ActorRegistryBusyError);
    expect(fs.existsSync(path.join(root, "actors.json.lock"))).toBe(false);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});
