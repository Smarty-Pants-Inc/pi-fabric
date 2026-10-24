import { beforeAll, describe, expect, it, vi } from "vitest";
import { makePlacementTerminalRecord, PlacementTerminalWriteError } from "../src/placement/terminal.js";

type NatsKvModule = typeof import("../src/transport/nats/kv.js");
let NatsKvStore: NatsKvModule["NatsKvStore"];
let PLACEMENT_TERMINAL_BUCKET: NatsKvModule["PLACEMENT_TERMINAL_BUCKET"];
let PLACEMENT_TERMINAL_TTL_MS: NatsKvModule["PLACEMENT_TERMINAL_TTL_MS"];
let KV_MARKER_TTL_MS: NatsKvModule["KV_MARKER_TTL_MS"];

// Only the KV port is exercised; SDK names are test-local and no live broker is used.
vi.mock("@nats-io/jetstream", () => ({
  DeliverPolicy: { LastPerSubject: "last_per_subject" },
  JetStreamApiError: class JetStreamApiError extends Error {},
  StorageType: { File: "file" },
}));
vi.mock("@nats-io/kv", () => ({ Kvm: class Kvm {} }));
vi.mock("@nats-io/transport-node", () => ({
  PermissionViolationError: class PermissionViolationError extends Error {},
  AuthorizationError: class AuthorizationError extends Error {},
}));

beforeAll(async () => {
  const natsKv = await import("../src/transport/nats/kv.js");
  NatsKvStore = natsKv.NatsKvStore;
  PLACEMENT_TERMINAL_BUCKET = natsKv.PLACEMENT_TERMINAL_BUCKET;
  PLACEMENT_TERMINAL_TTL_MS = natsKv.PLACEMENT_TERMINAL_TTL_MS;
  KV_MARKER_TTL_MS = natsKv.KV_MARKER_TTL_MS;
});

interface StoredEntry {
  bytes: Uint8Array;
  revision: number;
}

class FakeKvBucket {
  readonly entries = new Map<string, StoredEntry>();
  readonly creates: Array<{ key: string; value: string }> = [];

  async get(key: string): Promise<unknown | null> {
    const stored = this.entries.get(key);
    if (!stored) return null;
    return {
      key,
      value: new Uint8Array(stored.bytes),
      revision: stored.revision,
      operation: "PUT",
    };
  }

  async create(key: string, value: string): Promise<number> {
    this.creates.push({ key, value });
    if (this.entries.has(key)) throw new Error("key already exists");
    this.entries.set(key, { bytes: new TextEncoder().encode(value), revision: 1 });
    return 1;
  }
}

const storeFor = (bucket: FakeKvBucket): InstanceType<typeof NatsKvStore> => new NatsKvStore(
  bucket as never,
  { bucket: PLACEMENT_TERMINAL_BUCKET, ttlMs: PLACEMENT_TERMINAL_TTL_MS, markerTtlMs: KV_MARKER_TTL_MS },
  {} as never,
);

describe("NATS Placement terminal receipts", () => {
  it("uses the 15-minute TTL and raw run ID key, stores exact JSON, and handles replay safely", async () => {
    expect(PLACEMENT_TERMINAL_TTL_MS).toBe(900_000);
    const bucket = new FakeKvBucket();
    const store = storeFor(bucket);
    expect(store.ttlMs).toBe(900_000);

    const receipt = makePlacementTerminalRecord("placement-run-123", "finished", {
      at: 1_700_000_000_000, exitCode: 0, reason: "completed",
    });
    const exactJson = JSON.stringify(receipt);
    await store.writePlacementTerminal(receipt);

    expect(bucket.creates).toEqual([{ key: "placement-run-123", value: exactJson }]);
    expect([...bucket.entries.keys()]).toEqual([receipt.placementRunId]);
    expect(new TextDecoder().decode(bucket.entries.get(receipt.placementRunId)!.bytes)).toBe(exactJson);

    await expect(store.writePlacementTerminal(receipt)).resolves.toBeUndefined();
    expect(bucket.creates).toHaveLength(1);

    const conflicting = { ...receipt, state: "failed" as const, exitCode: 1 };
    const error = await store.writePlacementTerminal(conflicting).then(
      () => undefined,
      cause => cause,
    );
    expect(error).toBeInstanceOf(PlacementTerminalWriteError);
    expect(error).toMatchObject({ code: "FABRIC_PLACEMENT_TERMINAL_WRITE_FAILED" });
    expect(bucket.creates).toHaveLength(1);
    expect(new TextDecoder().decode(bucket.entries.get(receipt.placementRunId)!.bytes)).toBe(exactJson);
  });
});
