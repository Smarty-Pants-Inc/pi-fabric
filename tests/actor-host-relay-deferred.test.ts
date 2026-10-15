import path from "node:path";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import {
  MeshIncarnationPendingError,
  MeshLockTimeoutError,
  MeshSnapshotRetriesExhaustedError,
} from "../src/core/atomic-write.js";
import { ActorManager } from "../src/actors/manager.js";
import type { MeshIdentity } from "../src/mesh/store.js";
import { MeshStore } from "../src/mesh/store.js";

// This test owns no real actor, mesh, or child-process side effects. The actor manager and
// its MeshBackgroundQueue remain real; only the storage/host boundaries are recorded.
const io = vi.hoisted(() => ({
  calls: [] as Array<{ operation: string; args: unknown[] }>,
  stores: [] as Array<{ publishFailures: Error[]; published: unknown[] }>,
}));

vi.mock("node:fs", async importOriginal => {
  const native = await importOriginal<typeof import("node:fs")>();
  const fs = { ...native };
  for (const operation of [
    "writeFileSync", "appendFileSync", "renameSync", "rmSync", "unlinkSync", "rmdirSync",
    "copyFileSync", "cpSync", "truncateSync", "chmodSync", "utimesSync", "ftruncateSync",
  ] as const) {
    Object.assign(fs, {
      [operation]: (...args: unknown[]) => {
        io.calls.push({ operation, args });
        throw new Error(`Unexpected mocked filesystem operation: ${operation}`);
      },
    });
  }
  Object.assign(fs, {
    mkdirSync: (...args: unknown[]) => {
      io.calls.push({ operation: "mkdirSync", args });
      return undefined;
    },
    watch: (...args: unknown[]) => {
      io.calls.push({ operation: "recorded-watch", args });
      return { on() {}, close() {} };
    },
  });
  const open = native.openSync;
  fs.openSync = ((file, flags, mode) => {
    if (typeof flags === "string" ? /[wa+]/.test(flags) :
      (flags & (fs.constants.O_WRONLY | fs.constants.O_RDWR | fs.constants.O_CREAT)) !== 0) {
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
    Object.assign(guarded, {
      [operation]: (...args: unknown[]) => {
        io.calls.push({ operation, args });
        throw new Error(`Unexpected native child launch: ${operation}`);
      },
    });
  }
  return { ...native, ...guarded, default: guarded };
});

vi.mock("../src/mesh/store.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/mesh/store.js")>();
  return {
    ...actual,
    MeshStore: class {
      readonly root: string;
      readonly maxEventBytes = 65_536;
      readonly publishFailures: Error[] = [];
      readonly published: unknown[] = [];
      constructor(root: string) {
        this.root = root;
        io.stores.push(this);
      }
      get = vi.fn(() => undefined);
      listAll = vi.fn(() => []);
      listAllShared = vi.fn(() => []);
      latestOffset = vi.fn(() => 0);
      latestCursor = vi.fn(() => ({ cursor: 0 }));
      tail = vi.fn((offset: number) => ({ events: [], nextOffset: offset }));
      stateToken = vi.fn(() => ({}));
      writeBatch = vi.fn(async () => []);
      put = vi.fn(async () => undefined);
      delete = vi.fn(async () => undefined);
      closeState = vi.fn();
      publish = vi.fn(async (input: unknown) => {
        const failure = this.publishFailures.shift();
        if (failure) throw failure;
        this.published.push(input);
        return input;
      });
    },
  };
});

vi.mock("../src/actors/child-completions.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/actors/child-completions.js")>(),
  ActorChildCompletionWatch: class { unavailable = false; async close() {} },
}));

vi.mock("../src/topology/main-publication-fence.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/topology/main-publication-fence.js")>(),
  deleteRootActorTree: vi.fn(async () => undefined),
}));

const root = path.join(process.cwd(), ".local", "actor-host-relay-deferred");
const identity: MeshIdentity = { id: "relay-main", name: "relay-main", kind: "main", sessionId: "relay" };
const agents = {
  config: DEFAULT_FABRIC_CONFIG.agents,
  resolveKernel: () => undefined,
  defaultModel: () => undefined,
  resolvePythonRuntime: () => undefined,
  actorArchiveSources: () => new Map(),
  run: vi.fn(),
} as unknown as AgentManager;

const deferredErrors = [
  ["pending incarnation", () => new MeshIncarnationPendingError()],
  ["snapshot retries exhausted", () => new MeshSnapshotRetriesExhaustedError(3)],
  ["lock timeout", () => new MeshLockTimeoutError(" held by recorded owner", 1, 0)],
] as const;

beforeEach(() => {
  vi.useFakeTimers();
  vi.spyOn(Math, "random").mockReturnValue(0); // one millisecond first retry delay
  io.calls.length = 0;
  io.stores.length = 0;
  vi.clearAllMocks();
  vi.spyOn(console, "warn").mockImplementation(() => {});
});

afterEach(async () => {
  expect(io.calls.filter(call => !["mkdirSync", "recorded-watch"].includes(call.operation))).toEqual([]);
  vi.useRealTimers();
  vi.restoreAllMocks();
});

const makeManager = (mesh: MeshStore, options: NonNullable<ConstructorParameters<typeof ActorManager>[6]> = {}) =>
  new ActorManager("relay", identity, mesh, {
    ...DEFAULT_FABRIC_CONFIG.mesh,
    enabled: true,
    actorPollMs: 60_000,
  }, agents, () => {}, {
    actorRoot: path.join(root, "actors"),
    persistent: false,
    meshRetentionSweepPath: false,
    ...options,
  });

it.each(deferredErrors)("retries resident host relay with images after %s and commits the original payload", async (_label, makeError) => {
  const mesh = new MeshStore(root, 65_536, 100);
  const recorded = io.stores.at(-1)!;
  const actors = makeManager(mesh);
  try {
    const actor = await actors.create({
      name: "resident-relay",
      instructions: "Keep the host event.",
      residency: "durable",
      events: ["agent_settled"],
    });
    await actors.cede(actor.id);
    recorded.published.length = 0; // discard the actor-created lifecycle notification
    recorded.publishFailures.push(makeError());

    const payload = { marker: "original-payload", nested: { count: 7 } };
    const images = [{ type: "image", data: "aGVsbG8=", mimeType: "image/png" }] as const;
    expect(actors.dispatchObservedHostEvent("agent_settled", payload, images)).toBe(1);
    await vi.advanceTimersByTimeAsync(0); // first MeshBackgroundQueue attempt
    expect(recorded.published).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(1); // typed deferred write becomes ready

    expect(recorded.published).toHaveLength(1);
    const committed = recorded.published[0] as { topic: string; kind: string; data: Record<string, unknown> };
    expect(committed.topic).toBe("fabric.actor.host-event");
    expect(committed.kind).toBe("agent_settled");
    expect(committed.data.payload).toEqual(payload);
    expect(committed.data.images).toEqual(images);
  } finally {
    await actors.close();
  }
});

it("falls back to a committed message-only relay for a permanent publish error", async () => {
  const mesh = new MeshStore(root, 65_536, 100);
  const recorded = io.stores.at(-1)!;
  const actors = makeManager(mesh);
  try {
    const actor = await actors.create({ name: "permanent-relay", instructions: "Relay safely.", residency: "durable", events: ["agent_settled"] });
    await actors.cede(actor.id);
    recorded.published.length = 0; // discard the actor-created lifecycle notification
    recorded.publishFailures.push(new Error("permanent publish refusal"));

    const payload = { marker: "permanent-failure" };
    const images = [{ type: "image", data: "aGVsbG8=", mimeType: "image/png" }] as const;
    expect(actors.dispatchObservedHostEvent("agent_settled", payload, images)).toBe(1);
    await vi.advanceTimersByTimeAsync(1);

    expect(recorded.published).toHaveLength(1);
    const committed = recorded.published[0] as { data: Record<string, unknown> };
    expect(committed.data.payload).toEqual(payload);
    expect(committed.data).not.toHaveProperty("images");
  } finally {
    await actors.close();
  }
});
