// Keep: #17082 residency trust boundary. Mocked native platforms, real owner/config/intent files;
// these tests do NOT claim execution on macOS or Windows.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import * as namespace from "../src/core/pid-namespace.js";
import * as processUtils from "../src/agents/transports/process-utils.js";
import { prepareResidentOwnerObservation, residentHostOwnerAlive, residentProcessAlive } from "../src/residency/process-identity.js";
import { canonicalResidentWakeConfig, residentOwnerLive } from "../src/residency/wake-index.js";
import { requestResidentWake, residentWakeRequestPath, wakeDormantActor, wakeResidentActors } from "../src/residency/wake.js";
import { superviseWake } from "../src/residency/launcher.js";
import * as residentFileLock from "../src/residency/file-lock.js";
import { ResidencyClient } from "../src/residency/client.js";
import { RESIDENT_HOST_FORMAT, residentHostId, type ResidentHostConfig, type ResidentHostOwner } from "../src/residency/protocol.js";
import type { MeshEvent, MeshStore } from "../src/mesh/store.js";

const roots: string[] = [];
const clients: ResidencyClient[] = [];
afterEach(async () => {
  await Promise.all(clients.splice(0).map(client => client.close()));
  vi.restoreAllMocks();
  namespace.pidNamespaceTestHooks.resetReader();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const uuid = "00000000-0000-0000-0000-000000000001";
const machine = "00000000-0000-0000-0000-000000000002";
const native = (platform: "darwin" | "win32") => {
  const pidNamespace = platform === "darwin" ? `darwin:${machine}:${uuid}:host` : `win32:${machine}:123456:host`;
  const birth = platform === "darwin" ? "darwin:Mon Jan  1 00:00:00 2024" : "win32:123457";
  const nextBirth = platform === "darwin" ? "darwin:Tue Jan  2 00:00:00 2024" : "win32:123458";
  vi.spyOn(process, "platform", "get").mockReturnValue(platform);
  namespace.pidNamespaceTestHooks.set(pidNamespace);
  return { pidNamespace, bootId: pidNamespace.split(":")[2]!, birth, nextBirth };
};
const fixture = (identity = native("win32")) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-receiver-"));
  roots.push(root);
  const meshRoot = path.join(root, "mesh"), resident = path.join(meshRoot, "residency", "receiver");
  fs.mkdirSync(resident, { recursive: true });
  const config = { format: RESIDENT_HOST_FORMAT, rootId: "session:receiver", residencyRoot: resident,
    meshRoot, cwd: root, fabricExtensionPath: path.resolve("dist/index.js") } as ResidentHostConfig;
  const actorId = "a".repeat(32);
  const owner: ResidentHostOwner = { format: RESIDENT_HOST_FORMAT, hostId: residentHostId(config.rootId),
    pid: process.pid, processStartTime: identity.birth, bootId: identity.bootId, pidNamespace: identity.pidNamespace,
    token: "old-generation", startedAt: 1, readyAt: 1 };
  const file = (name: string) => path.join(resident, name);
  const saveOwner = (value: ResidentHostOwner) => fs.writeFileSync(file("owner.json"), JSON.stringify(value));
  saveOwner(owner);
  fs.writeFileSync(file("config.json"), JSON.stringify(config));
  fs.writeFileSync(file("wake-routes.json"), JSON.stringify({ format: 1, rootId: config.rootId,
    hostId: owner.hostId, configJson: canonicalResidentWakeConfig(config),
    actors: [{ id: actorId, name: "receiver", topics: ["receiver.work"] }] }));
  let held = false;
  const exclusive = vi.fn(async <T>(operation: () => T | Promise<T>): Promise<T> => {
    expect(held).toBe(false);
    held = true;
    try { return await operation(); } finally { held = false; }
  });
  const mesh = { root: meshRoot, listAll: () => [], exclusive } as unknown as Pick<MeshStore, "root" | "listAll" | "exclusive">;
  const admission = { allowed: () => true, exclusive: mesh.exclusive };
  const event: MeshEvent = { id: "delivery-1", sequence: 1, topic: "receiver.work", kind: "message", createdAt: 1,
    from: { id: "publisher", name: "publisher", kind: "main" } };
  return { ...identity, root, resident, meshRoot, config, actorId, owner, file, saveOwner, mesh, admission, event, exclusive,
    held: () => held };
};
const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
};

describe.each(["darwin", "win32"] as const)("mocked %s native resident evidence", platform => {
  it.each(["stale", "same", "unknown", "malformed", "unavailable"] as const)("uses %s birth only after async observation", async verdict => {
    const f = fixture(native(platform));
    const read = vi.spyOn(atomic, "processIncarnation");
    if (verdict === "unavailable") read.mockRejectedValue(new Error("native identity unavailable"));
    else read.mockResolvedValue(verdict === "stale" ? f.nextBirth : verdict === "same" ? f.birth : verdict === "malformed" ? "untrusted" : undefined);
    expect(residentOwnerLive(f.resident)).toBe(true);
    expect(residentHostOwnerAlive(f.owner, f.file("owner.json"))).toBe(true);
    expect(read).not.toHaveBeenCalled();
    await prepareResidentOwnerObservation(f.resident);
    expect(read).toHaveBeenCalledOnce();
    expect(read).toHaveBeenCalledWith(process.pid);
    expect(residentOwnerLive(f.resident)).toBe(verdict !== "stale");
    // Generic child/supervisor identities retain PID-only non-Linux semantics.
    expect(residentProcessAlive(process.pid, f.birth)).toBe(true);
    expect(read).toHaveBeenCalledOnce();
  });

  it("sync predicates never initiate native namespace or birth commands on a cold reader", () => {
    const f = fixture(native(platform));
    namespace.pidNamespaceTestHooks.resetReader();
    const command = vi.spyOn(atomic, "runIncarnationCommand").mockRejectedValue(new Error("must not execute"));
    const birth = vi.spyOn(atomic, "processIncarnation").mockRejectedValue(new Error("must not execute"));
    expect(residentOwnerLive(f.resident)).toBe(true);
    expect(residentHostOwnerAlive(f.owner, f.file("owner.json"))).toBe(true);
    expect(command).not.toHaveBeenCalled();
    expect(birth).not.toHaveBeenCalled();
  });

  it("reuses the existing native reader and its platform parser", async () => {
    const f = fixture(native(platform));
    const run = vi.fn<atomic.IncarnationCommandRunner>(async () => platform === "darwin" ? f.nextBirth.slice(7) : f.nextBirth.slice(6));
    const reader = atomic.createProcessIncarnationReader({ platform, systemRoot: "C:\\Windows", run });
    vi.spyOn(atomic, "processIncarnation").mockImplementation(pid => reader.read(pid));
    await prepareResidentOwnerObservation(f.resident);
    expect(residentOwnerLive(f.resident)).toBe(false);
    expect(run).toHaveBeenCalledOnce();
    expect(run.mock.calls[0]?.[0]).toBe(platform === "darwin" ? "/bin/ps" : "C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe");
  });

  it.each(["boot", "namespace", "unknown-namespace", "legacy"] as const)("keeps %s namespace/legacy rules", async kind => {
    const f = fixture(native(platform));
    if (kind === "boot") {
      f.owner.bootId = platform === "darwin" ? machine : "654321";
      f.owner.pidNamespace = f.pidNamespace.replace(f.bootId, f.owner.bootId);
    } else if (kind === "namespace") f.owner.pidNamespace = f.pidNamespace.replace(machine, uuid);
    else if (kind === "unknown-namespace") namespace.pidNamespaceTestHooks.set(undefined);
    else delete f.owner.processStartTime;
    f.saveOwner(f.owner);
    const read = vi.spyOn(atomic, "processIncarnation").mockRejectedValue(new Error("must not read native birth"));
    await prepareResidentOwnerObservation(f.resident);
    expect(residentOwnerLive(f.resident)).toBe(kind === "unknown-namespace" || kind === "legacy");
    expect(read).not.toHaveBeenCalled();
  });

  it("separates full owner generations and source paths, including nested generation metadata", async () => {
    const f = fixture(native(platform));
    const read = vi.spyOn(atomic, "processIncarnation").mockResolvedValue(f.nextBirth);
    await prepareResidentOwnerObservation(f.resident);
    expect(residentOwnerLive(f.resident)).toBe(false);
    const variants: Partial<ResidentHostOwner>[] = [
      { token: "replacement" }, { hostId: "another-host" }, { startedAt: 2 }, { readyAt: 2 },
      { launchToken: "another-launch" }, { processStartTime: f.nextBirth }, { pid: process.ppid },
      { attempt: { id: "new-attempt", kind: "target" } }, { configDigest: "new-config" }, { releaseRoot: "/other-release" },
    ];
    for (const change of variants) {
      f.saveOwner({ ...f.owner, ...change });
      expect(residentOwnerLive(f.resident), JSON.stringify(change)).toBe(true);
    }
    const other = path.join(f.root, "another-source");
    fs.mkdirSync(other);
    fs.writeFileSync(path.join(other, "owner.json"), JSON.stringify(f.owner));
    expect(residentOwnerLive(other)).toBe(true);
    f.saveOwner(f.owner);
    expect(residentOwnerLive(f.resident)).toBe(false);
    expect(read).toHaveBeenCalledOnce();
  });

  it("discards a late stale observation rather than condemning a replacement/live owner", async () => {
    const f = fixture(native(platform));
    const oldRead = deferred<string | undefined>(), entered = deferred<void>();
    const read = vi.spyOn(atomic, "processIncarnation").mockImplementationOnce(() => { entered.resolve(); return oldRead.promise; });
    const oldPreparation = prepareResidentOwnerObservation(f.resident);
    await entered.promise;
    const replacement = { ...f.owner, token: "replacement", processStartTime: f.nextBirth };
    f.saveOwner(replacement);
    read.mockResolvedValue(f.nextBirth);
    await prepareResidentOwnerObservation(f.resident);
    oldRead.resolve(f.nextBirth);
    await oldPreparation;
    expect(residentOwnerLive(f.resident)).toBe(true);
    f.saveOwner(f.owner);
    expect(residentOwnerLive(f.resident)).toBe(true); // The superseded result was not cached at all.
  });

  it("rereads the owner after async namespace preparation", async () => {
    const f = fixture(native(platform));
    const own = deferred<string | undefined>();
    vi.spyOn(namespace, "ownPidNamespace").mockReturnValue(own.promise);
    const read = vi.spyOn(atomic, "processIncarnation").mockResolvedValue(f.nextBirth);
    const preparing = prepareResidentOwnerObservation(f.resident);
    f.saveOwner({ ...f.owner, token: "new", processStartTime: f.nextBirth });
    own.resolve(f.pidNamespace);
    await preparing;
    expect(residentOwnerLive(f.resident)).toBe(true);
    expect(read).toHaveBeenCalledOnce();
    f.saveOwner(f.owner);
    expect(residentOwnerLive(f.resident)).toBe(true);
  });
});

describe("cold native receiver async boundaries", () => {
  it.each(["stale", "same", "unknown"] as const)("requestResidentWake observes %s before intent custody without manual priming", async verdict => {
    const f = fixture();
    const read = vi.spyOn(atomic, "processIncarnation").mockImplementation(async () => {
      expect(f.held()).toBe(false);
      expect(fs.existsSync(residentWakeRequestPath(f.resident))).toBe(false);
      return verdict === "stale" ? f.nextBirth : verdict === "same" ? f.birth : undefined;
    });
    const launch = vi.fn(async () => {
      expect(f.held()).toBe(false);
      expect(JSON.parse(fs.readFileSync(residentWakeRequestPath(f.resident), "utf8"))).toMatchObject({ id: f.event.id, sequence: 1 });
    });
    await requestResidentWake(f.resident, f.event, launch, undefined, f.admission);
    expect(read).toHaveBeenCalledOnce();
    expect(launch).toHaveBeenCalledTimes(verdict === "stale" ? 1 : 0);
    expect(f.exclusive).toHaveBeenCalledOnce();
  });

  it.each(["stale", "same", "unknown"] as const)("publication receiver observes %s before the first cold veto, without pending-index priming", async verdict => {
    const f = fixture();
    const read = vi.spyOn(atomic, "processIncarnation").mockImplementation(async () => {
      expect(f.held()).toBe(false);
      return verdict === "stale" ? f.nextBirth : verdict === "same" ? f.birth : undefined;
    });
    const launch = vi.fn(async () => {});
    await wakeResidentActors(f.mesh, [f.event], launch);
    expect(read).toHaveBeenCalled();
    expect(launch).toHaveBeenCalledTimes(verdict === "stale" ? 1 : 0);
    expect(fs.existsSync(residentWakeRequestPath(f.resident))).toBe(verdict === "stale");
  });

  it.each(["stale", "same", "unknown"] as const)("direct actor wake observes %s before its first veto and keeps admission", async verdict => {
    const f = fixture();
    vi.spyOn(atomic, "processIncarnation").mockResolvedValue(verdict === "stale" ? f.nextBirth : verdict === "same" ? f.birth : undefined);
    const runtime = vi.spyOn(processUtils, "scriptSpawnArgs").mockRejectedValue(new Error("injected runtime boundary"));
    if (verdict === "stale") {
      await expect(wakeDormantActor(f.meshRoot, f.actorId)).rejects.toThrow("admission closed");
      expect(fs.existsSync(residentWakeRequestPath(f.resident))).toBe(false);
      await expect(wakeDormantActor(f.meshRoot, f.actorId, f.admission)).rejects.toThrow("injected runtime boundary");
      expect(fs.existsSync(residentWakeRequestPath(f.resident))).toBe(true);
      expect(runtime).toHaveBeenCalledOnce();
    } else {
      await expect(wakeDormantActor(f.meshRoot, f.actorId, f.admission)).resolves.toBe(false);
      expect(runtime).not.toHaveBeenCalled();
    }
  });

  it.each(["request", "publication", "direct"] as const)("%s revocation during native preparation leaves no intent or launch", async entry => {
    const f = fixture();
    let allowed = true;
    vi.spyOn(atomic, "processIncarnation").mockImplementation(async () => { allowed = false; return f.nextBirth; });
    const admission = { ...f.admission, allowed: () => allowed }, launch = vi.fn(async () => {});
    if (entry === "request") await expect(requestResidentWake(f.resident, f.event, launch, undefined, admission)).rejects.toThrow("admission closed");
    else if (entry === "direct") await expect(wakeDormantActor(f.meshRoot, f.actorId, admission)).rejects.toThrow("admission closed");
    else await wakeResidentActors(f.mesh, [f.event], launch, undefined, admission.allowed);
    expect(fs.existsSync(residentWakeRequestPath(f.resident))).toBe(false);
    expect(launch).not.toHaveBeenCalled();
  });

  it("rereads config after preparation, before intent publication", async () => {
    const f = fixture();
    vi.spyOn(atomic, "processIncarnation").mockImplementation(async () => {
      fs.writeFileSync(f.file("config.json"), JSON.stringify({ ...f.config, cwd: "changed" }));
      return f.nextBirth;
    });
    const launch = vi.fn(async () => {});
    await expect(requestResidentWake(f.resident, f.event, launch, undefined, f.admission)).rejects.toThrow("config differs");
    expect(launch).not.toHaveBeenCalled();
    expect(f.exclusive).not.toHaveBeenCalled();
    expect(fs.existsSync(residentWakeRequestPath(f.resident))).toBe(false);
  });

  it("does not launch from a stale verdict when a live replacement appears during mesh custody", async () => {
    const f = fixture();
    vi.spyOn(atomic, "processIncarnation").mockResolvedValue(f.nextBirth);
    const launch = vi.fn(async () => {});
    const admission = { ...f.admission, exclusive: (async (operation: () => unknown) => {
      const result = operation();
      f.saveOwner({ ...f.owner, token: "replacement", processStartTime: f.nextBirth });
      return result;
    }) as typeof f.admission.exclusive };
    await requestResidentWake(f.resident, f.event, launch, undefined, admission);
    expect(launch).not.toHaveBeenCalled();
    expect(residentOwnerLive(f.resident)).toBe(true);
    expect(fs.existsSync(residentWakeRequestPath(f.resident))).toBe(true);
  });

  it("launcher cold observation rejects a reused native birth before its synchronous owner veto", async () => {
    const f = fixture();
    vi.spyOn(atomic, "processIncarnation").mockResolvedValue(f.nextBirth);
    // Only the native POSIX-lock boundary is substituted for this mocked Windows run.
    vi.spyOn(residentFileLock, "lockFile").mockImplementation(async file => fs.openSync(file, "w"));
    const run = vi.fn(async () => {});
    await superviseWake(f.file("config.json"), run);
    expect(run).toHaveBeenCalledOnce();
  });

  it.each(["stale", "same", "unknown"] as const)("client observes %s before adopting a ready owner", async verdict => {
    const f = fixture();
    vi.spyOn(atomic, "processIncarnation").mockResolvedValue(verdict === "stale" ? f.nextBirth : verdict === "same" ? f.birth : undefined);
    const spawn = vi.spyOn(processUtils, "spawnDetached").mockRejectedValue(new Error("injected launch boundary"));
    const client = new ResidencyClient({ config: { ...f.config, agents: { defaultTools: [] } } as unknown as ResidentHostConfig,
      mesh: f.mesh as MeshStore, participants: {} as never, mainAgent: { local: false } as never });
    clients.push(client);
    if (verdict === "stale") await expect(client.ensureHost()).rejects.toThrow("injected launch boundary");
    else await expect(client.ensureHost()).resolves.toMatchObject({ token: f.owner.token });
    expect(spawn).toHaveBeenCalledTimes(verdict === "stale" ? 1 : 0);
  });

  it("client cancellation during command preparation dispatches nothing", async () => {
    const f = fixture();
    const abort = new AbortController();
    vi.spyOn(atomic, "processIncarnation").mockImplementation(async () => { abort.abort(); return f.birth; });
    const client = new ResidencyClient({ config: f.config, mesh: f.mesh as MeshStore, participants: {} as never,
      mainAgent: { local: false } as never });
    clients.push(client);
    await expect(client.actors(abort.signal)).rejects.toThrow();
    expect(fs.existsSync(f.file("requests"))).toBe(false);
  });

  it("never observes a symlink or nonregular owner file", async () => {
    const f = fixture();
    const target = path.join(f.root, "saved-owner.json");
    fs.renameSync(f.file("owner.json"), target);
    fs.symlinkSync(target, f.file("owner.json"));
    const read = vi.spyOn(atomic, "processIncarnation").mockResolvedValue(f.nextBirth);
    await prepareResidentOwnerObservation(f.resident);
    expect(read).not.toHaveBeenCalled();
    fs.unlinkSync(f.file("owner.json"));
    fs.mkdirSync(f.file("owner.json"));
    await prepareResidentOwnerObservation(f.resident);
    expect(read).not.toHaveBeenCalled();
  });
});
