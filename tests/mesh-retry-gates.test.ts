import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { AgentManager } from "../src/agents/manager.js";
import type { MeshStore, MeshStateEntry } from "../src/mesh/store.js";
type MeshBatchInput = Parameters<MeshStore["writeBatch"]>[0];
import type { FabricHostLease } from "../src/topology/host-leases.js";

// Keep reason: #12036's no-write failure paths and data safety. Exercise the public
// consumers, recording custody/native effects instead of touching a real mesh.
const io = vi.hoisted(() => ({
  calls: [] as Array<{ operation: string; args: unknown[] }>,
  activationError: undefined as Error | undefined,
  control: [] as unknown[][],
  leases: new Map<string, FabricHostLease>(),
  files: [] as MeshStateEntry[],
  stop: new Error("recorded control construction boundary"),
}));
vi.mock("node:fs", async importOriginal => {
  const native = await importOriginal<typeof import("node:fs")>();
  const fs = { ...native };
  // Fail closed on unexpected mutating I/O, even if the product catches it.
  for (const operation of ["mkdirSync", "writeFileSync", "appendFileSync", "renameSync", "rmSync", "unlinkSync",
    "rmdirSync", "copyFileSync", "cpSync", "truncateSync", "chmodSync", "utimesSync", "ftruncateSync"] as const) {
    Object.assign(fs, { [operation]: (...args: unknown[]) => {
      io.calls.push({ operation, args });
      if (operation === "mkdirSync") return undefined; // recorded actor directory creation only
      throw new Error(`Unexpected mocked filesystem operation: ${operation}`);
    } });
  }
  Object.assign(fs, { watch: (...args: unknown[]) => {
    io.calls.push({ operation: "recorded-watch", args });
    return { on() {}, close() {} };
  } });
  const open = native.openSync;
  fs.openSync = ((file, flags, mode) => {
    if (typeof flags === "string" ? /[wa+]/.test(flags) : (flags & (fs.constants.O_WRONLY | fs.constants.O_RDWR | fs.constants.O_CREAT)) !== 0) {
      io.calls.push({ operation: "open-write", args: [file, flags] });
      throw new Error("Unexpected mocked writable open");
    }
    return open(file, flags, mode);
  }) as typeof fs.openSync;
  return { ...native, ...fs, default: fs };
});
vi.mock("node:child_process", async importOriginal => {
  const native = await importOriginal<typeof import("node:child_process")>();
  const guarded = { ...native };
  for (const operation of ["spawn", "spawnSync", "exec", "execSync", "execFile", "execFileSync", "fork"] as const) {
    Object.assign(guarded, { [operation]: (...args: unknown[]) => {
      io.calls.push({ operation, args });
      throw new Error(`Unexpected native child launch: ${operation}`);
    } });
  }
  return { ...native, ...guarded, default: guarded };
});
vi.mock("../src/mesh/store.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/mesh/store.js")>();
  return { ...actual, MeshStore: class {
    constructor(readonly root: string) {}
    maxEventBytes = 65_536;
    get = vi.fn(() => undefined);
    listAll = vi.fn(() => []);
    listAllShared = vi.fn(() => []);
    latestOffset = vi.fn(() => 0);
    stateToken = vi.fn(() => ({}));
    writeBatch = vi.fn(async (input: MeshBatchInput) => {
      const view = { get: () => undefined, listAll: () => [], version: () => 0 };
      input.prepare?.(view);
      input.beforeCommit?.();
      const results = input.ops.map(op => ({ key: op.key, applied: true, version: 1 }));
      input.commitOutbox?.({ backend: "file", results, changed: results.map(result => result.key), stamp: undefined, view });
      return results;
    });
    closeState = vi.fn();
    publish = vi.fn(async () => ({}));
  } };
});
vi.mock("../src/mesh/custody-lock.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/mesh/custody-lock.js")>(),
  acquireMeshCustodyLock: vi.fn(async () => { throw io.activationError; }),
}));
vi.mock("../src/topology/host-lease-lock.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/topology/host-lease-lock.js")>(),
  withHostLeaseLock: vi.fn(async (_mesh, _file, operation: () => unknown) => operation()),
}));
vi.mock("../src/topology/host-leases.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/topology/host-leases.js")>(),
  hostLeasePredecessor: (_root: string, id: string) => io.leases.get(id),
  readHostLeaseCurrent: (_root: string, id: string) => io.leases.get(id),
  readHostLeases: () => io.leases,
  renewHostLease: vi.fn(async (_mesh, lease: FabricHostLease) => { io.leases.set(lease.id, lease); }),
  assertHostLeaseOwner: vi.fn(), // the matching recorded lease replaces native file custody
  withOwnedHostLease: vi.fn(async (_mesh, _owner, operation: () => unknown) => operation()),
  removeOwnedHostLease: vi.fn(async (_mesh, owner: FabricHostLease) => io.leases.delete(owner.id)),
  removeHostLeaseIf: vi.fn(() => false),
  removeHostLeaseVersionUnderCustody: vi.fn(() => false),
}));
vi.mock("../src/topology/participant-files.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/topology/participant-files.js")>(),
  readParticipantFiles: () => io.files,
  removeParticipantFileIf: vi.fn(async () => true),
  sweepParticipantLockLeftovers: vi.fn(async () => {}),
}));
vi.mock("../src/mesh/temp-janitor.js", () => ({ sweepAbandonedStateTemporaries: vi.fn() }));
vi.mock("../src/topology/main-publication-fence.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/topology/main-publication-fence.js")>(),
  deleteRootActorTree: vi.fn(async () => { io.calls.push({ operation: "recorded-root-cleanup", args: [] }); }),
}));
vi.mock("../src/actors/child-completions.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/actors/child-completions.js")>(),
  ActorChildCompletionWatch: class { unavailable = false; async close() {} },
}));
vi.mock("../src/runtime-state-builtins.js", () => ({
  RuntimeStateBuiltins: class { async tools() {} async install() {} },
}));
vi.mock("../src/topology/control-plane.js", () => ({
  FabricControlPlane: class {
    constructor(...args: unknown[]) { io.control.push(args); throw io.stop; }
  },
}));

import { MeshIncarnationPendingError, MeshSnapshotRetriesExhaustedError, MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { ActorManager } from "../src/actors/manager.js";
import { reapDeadHostRecords } from "../src/topology/host-reaper.js";
import { MeshStore as RecordedStore } from "../src/mesh/store.js";
import { removeParticipantFileIf } from "../src/topology/participant-files.js";
import { removeHostLeaseVersionUnderCustody } from "../src/topology/host-leases.js";
import { RootInbox } from "../src/topology/root-inbox.js";

const root = path.join(process.cwd(), ".local", "retry-gates-virtual");
const identity = { id: "retry-gates", name: "retry-gates", kind: "main" as const };
const cases = () => [new MeshIncarnationPendingError(), new MeshSnapshotRetriesExhaustedError(3),
  new MeshLockTimeoutError(" held by recorded owner", 1, 0), new Error("permanent storage refusal")];
const permanent = (error: Error) => error.message === "permanent storage refusal";
const cleanups: Array<() => Promise<unknown>> = [];
beforeEach(() => {
  vi.useFakeTimers();
  io.calls.length = 0; io.control.length = 0; io.files.length = 0; io.leases.clear();
  vi.clearAllMocks();
  vi.spyOn(console, "warn").mockImplementation(() => {});
  vi.stubEnv("PI_CODING_AGENT_DIR", root);
  vi.stubEnv("SMARTY_LEAD_SESSION", undefined);
  vi.stubEnv("SMARTY_ROLE", undefined);
});
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) {
    const closing = cleanup();
    await vi.advanceTimersByTimeAsync(0);
    await closing;
  }
  expect(io.calls.filter(call => !["mkdirSync", "recorded-root-cleanup", "recorded-watch"].includes(call.operation))).toEqual([]);
  vi.useRealTimers(); vi.unstubAllEnvs();
});

it("participant start coalesces pending/snapshot retries, commits stable recovery, and preserves message-only legacy timeout", async () => {
  vi.spyOn(Math, "random").mockReturnValue(0);
  const legacy = new Error("Timed out waiting for the Fabric mesh lock (legacy receipt)");
  for (const error of [...cases(), legacy]) {
    const retryable = !permanent(error);
    const mesh = new RecordedStore(root, 65_536, 100);
    const admission = vi.fn(async () => {});
    const beforePublication = vi.fn(async () => {});
    const committed = vi.fn();
    vi.mocked(mesh.writeBatch).mockRejectedValueOnce(error);
    const directory = new ParticipantDirectory(mesh, {
      enabled: true, hostId: identity.id, rootId: identity.id, identity, reapDeadHosts: false,
      beforePublication, waitForPublicationRetry: admission,
      publicationBatch: () => ({ ops: [], committed }),
    });
    cleanups.push(() => directory.close());
    const confirmed = directory.confirmedAt();
    await expect(directory.start()).rejects.toBe(error);
    expect(directory.routingUnavailable()).toBe(error.message);
    expect(directory.confirmedAt()).toBe(confirmed);
    expect(mesh.writeBatch).toHaveBeenCalledOnce();
    expect(committed).not.toHaveBeenCalled();
    // Neither new refusal is a wait-timeout diagnostic; the legacy receipt still is.
    expect(directory.writeStalled() !== undefined).toBe(error === legacy || error instanceof MeshLockTimeoutError);
    if (retryable) {
      directory.scheduleRefresh();
      await directory.refreshPresence();
      expect(directory.canConsumeMesh()).toBe(false);
      expect(beforePublication).toHaveBeenCalledOnce();
    }
    await vi.advanceTimersByTimeAsync(50);
    expect(admission).toHaveBeenCalledTimes(retryable ? 1 : 0);
    expect(beforePublication).toHaveBeenCalledTimes(retryable ? 2 : 1);
    expect(mesh.writeBatch).toHaveBeenCalledTimes(retryable ? 2 : 1);
    expect(committed).toHaveBeenCalledTimes(retryable ? 1 : 0);
    expect(directory.routingUnavailable()).toBe(retryable ? undefined : error.message);
    expect(directory.canConsumeMesh()).toBe(retryable);
    expect(directory.confirmedAt()).toBe(retryable ? confirmed + 50 : confirmed);
    directory.suspend(); // close is a recorded withdrawal, never a native deletion
    await directory.close();
  }
});

it("runtime bootstrap retains pending/snapshot for background activation but propagates permanent errors without opening the inbox", async () => {
  const context = {
    cwd: root, hasUI: false, mode: "json", isProjectTrusted: () => false,
    isIdle: () => true, hasPendingMessages: () => false,
    sessionManager: { getSessionId: () => "retry-gates-bootstrap", getSessionFile: () => undefined },
    ui: { setStatus: vi.fn() }, modelRegistry: { getAvailable: () => [] },
  } as unknown as ExtensionContext;
  const pi = { on: () => () => {}, getThinkingLevel: () => "off", getSessionName: () => "retry-gates" } as unknown as ExtensionAPI;
  const config = normalizeFabricConfig({ fullCodeMode: false, mesh: { enabled: true, root },
    speculation: { enabled: false }, residency: { enabled: false }, agents: { enabled: false } });
  const start = vi.spyOn(RootInbox.prototype, "start");
  const next = vi.spyOn(RootInbox.prototype, "next");
  for (const error of cases()) {
    io.activationError = error;
    const priorControls = io.control.length;
    const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog());
    cleanups.push(() => runtime.shutdown());
    // Stop at the first post-catch constructor: no downstream workers, watchers or writes.
    await expect(runtime.initialize(context, config)).rejects.toBe(permanent(error) ? error : io.stop);
    expect(io.control.length - priorControls).toBe(permanent(error) ? 0 : 1);
    if (!permanent(error)) {
      const options = io.control.at(-1)![2] as { canConsumeMesh(): boolean };
      expect(options.canConsumeMesh()).toBe(false);
    }
    await expect(runtime.nextRootInbox({ holdsBatch: () => false, holdsSteer: () => false })).resolves.toBeUndefined();
    expect(start).not.toHaveBeenCalled();
    expect(next).not.toHaveBeenCalled();
    expect(runtime.initialized).toBe(false);
    await runtime.shutdown();
  }
});

it("actor preparation cause chains keep pending/snapshot work automatically retryable instead of permanently blocked", async () => {
  for (const [index, error] of cases().entries()) {
    const agents = {
      config: DEFAULT_FABRIC_CONFIG.agents, resolveKernel: () => undefined, defaultModel: () => undefined,
      resolveModelReasonPolicy: vi.fn(() => ({ model: undefined, thinking: undefined, fallback: false })),
      actorArchiveSources: () => new Map(), run: vi.fn(),
    } as unknown as AgentManager;
    const prepare = vi.fn(async () => { throw new Error("recorded custody refusal", { cause: error }); });
    const actors = new ActorManager("retry-gates", identity, new RecordedStore(root, 65_536, 100),
      { ...DEFAULT_FABRIC_CONFIG.mesh, enabled: true }, agents, () => {}, {
        actorRoot: path.join(root, "actors"), persistent: false, meshRetentionSweepPath: false,
        canConsumeMesh: () => false, preparationRetryMs: 1, transientBlockCooldownMs: 60_000,
        acquireCapabilityView: prepare,
        presencePublisher: { refresh: async () => {}, schedule() {} },
      });
    cleanups.push(() => actors.close());
    const actor = await actors.create({ name: `retry-gate-${index}`, instructions: "Reply", requires: ["demo.echo"], coalesce: false });
    actors.tell(actor.id, "retain this accepted work");
    await vi.advanceTimersByTimeAsync(20); // bounded four preparation attempts: 1 + 3 + 12 ms backoffs
    expect(prepare).toHaveBeenCalledTimes(4);
    expect(actors.status(actor.id)).toMatchObject({ queued: 1, activationBlocked: {
      code: permanent(error) ? "failing-preparation" : "preparation-timeout", class: permanent(error) ? "hard" : "transient",
    } });
    expect(actors.status(actor.id).activationBlocked?.retryAt !== undefined).toBe(!permanent(error));
    expect(agents.run).not.toHaveBeenCalled();
    await actors.close();
  }
});

it("host reaper defers pending/snapshot without removing selected records and propagates permanent storage refusal", async () => {
  const entry: MeshStateEntry = {
    key: "topology/hosts/" + createHash("sha256").update("dead-target").digest("hex"),
    value: { id: "dead-target", expiresAt: 1 }, version: 7, updatedAt: 1, updatedBy: identity,
  };
  const file: MeshStateEntry = { ...entry, key: "topology/participants/recorded-target",
    value: { ownerHostId: "dead-target" } };
  for (const error of cases()) {
    const entries = [entry];
    io.files = [file];
    const writeBatch = vi.fn(async (input: MeshBatchInput) => {
      input.prepare?.({ get: key => entries.find(item => item.key === key),
        listAll: prefix => entries.filter(item => item.key.startsWith(prefix)),
        version: key => entries.find(item => item.key === key)?.version ?? 0 });
      input.beforeCommit?.();
      throw error; // no COMMIT, so neither outbox nor post-commit file removal may run
    });
    const mesh = { root, listAll: (prefix: string) => entries.filter(item => item.key.startsWith(prefix)),
      writeBatch, custody: vi.fn() } as unknown as Parameters<typeof reapDeadHostRecords>[0];
    const result = reapDeadHostRecords(mesh, identity, { ownHostId: identity.id, now: 100, deadAfterMs: 0 });
    if (permanent(error)) await expect(result).rejects.toBe(error);
    else await expect(result).resolves.toBe(0);
    expect(writeBatch).toHaveBeenCalledOnce();
    expect(writeBatch.mock.calls[0]).toEqual([expect.objectContaining({ ops: [expect.objectContaining({ key: entry.key, ifVersion: 7 })] })]);
    expect(entries).toEqual([entry]);
    expect(io.files).toEqual([file]);
    expect(removeParticipantFileIf).not.toHaveBeenCalled();
    expect(removeHostLeaseVersionUnderCustody).not.toHaveBeenCalled();
    expect(mesh.custody).not.toHaveBeenCalled();
  }
});
