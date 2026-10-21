import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";
import { inspect } from "node:util";
import { ProtocolError } from "@nats-io/nats-core";
import { jserrors } from "@nats-io/jetstream/internal";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Stream replicas: omitted values come only from the agent-dir fabric.json reader (default 1); explicit values still
// win. The real-reader cases use private fixtures and check the agent reader/client path without global config/policy
// loads, warnings, or config rewrites. NATS network operations are pure fakes; SDK error classes stay real.
// Options are passed through wide variables so assertions remain behavioral across the old option types.
const fake = vi.hoisted(() => {
  type StreamInfo = { config: Record<string, unknown> & { name: string; subjects?: string[] }; state: { last_seq: number; first_seq: number }; cluster?: { leader?: string } };
  return {
    FakeJetStreamApiError: undefined as unknown as new (errCode: number, statusCode?: number, options?: ErrorOptions) => import("@nats-io/jetstream").JetStreamApiError,
    streams: new Map<string, StreamInfo>(),
    log: [] as { op: string; args: unknown[] }[],
    managerErrors: [] as unknown[],
    infoErrors: [] as unknown[],
    infoDelays: [] as number[],
    messageErrors: [] as unknown[],
    messageDelays: [] as number[],
    consumerErrors: [] as unknown[],
    consumerOptions: [] as unknown[],
    consumerDelays: [] as number[],
    consumeIteratorError: undefined as unknown,
    publishErrors: [] as unknown[],
    drains: 0,
    addStop: false,
    addErrors: [] as unknown[],
    addDelays: [] as number[],
    addErrorAfterStore: false,
    addStoredConfigOverride: undefined as Record<string, unknown> | undefined,
    createdWithoutLeader: false,
    leaderPolls: 0,
    wrappedTransient: () => {
      const noResponders = Object.assign(new Error("no responders"), { name: "NoResponders" });
      const request = Object.assign(new Error("request failed"), { name: "RequestError", cause: noResponders });
      return Object.assign(new Error("jetstream is not enabled"), { name: "JetStreamNotEnabled", cause: request });
    },
    accountDisabled: () => {
      const api = new fake.FakeJetStreamApiError(10039);
      return Object.assign(new Error("jetstream is not enabled for account"), { name: "JetStreamNotEnabled", cause: api });
    },
    permissionDenied: () => Object.assign(new Error("permissions violation"), { name: "PermissionViolationError" }),
    /** mesh.nats.replicas returned by the fake agent-only reader; reset to 1 before every case. */
    configuredReplicas: 1 as unknown,
    /** Guards that NATS replica lookup never enters the global config / policy path. */
    loadGlobal: vi.fn<(agentDir: string) => unknown>(),
    loadAgent: vi.fn<(agentDir: string) => number>(),
  };
});

// Only the global loader is faked; normalizeFabricConfig (and every other export) stays the real one.
vi.mock("../src/config.js", async importOriginal => ({
  ...(await importOriginal<typeof import("../src/config.js")>()),
  loadGlobalFabricConfig: fake.loadGlobal,
  loadAgentNatsReplicas: fake.loadAgent,
}));

vi.mock("@nats-io/transport-node", () => ({
  connect: async () => (fake.log.push({ op: "connect", args: [] }), { info: { version: "2.14.7", auth_required: false, max_payload: 1024 * 1024 }, drain: async () => { fake.drains++; } }),
}));
vi.mock("@nats-io/jetstream", async importOriginal => {
  const actual = await importOriginal<typeof import("@nats-io/jetstream")>();
  fake.FakeJetStreamApiError = class extends actual.JetStreamApiError {
    constructor(errCode: number, statusCode = 404, options?: ErrorOptions) {
      super({ code: statusCode, err_code: errCode, description: `fake JetStream API error ${errCode}` }, options);
    }
  };
  return {
  DeliverPolicy: { All: "all", StartSequence: "by_start_sequence" },
  JetStreamApiError: actual.JetStreamApiError,
  StorageType: { File: "file" },
  jetstream: (...args: unknown[]) => {
    fake.log.push({ op: "jetstream", args });
    return {
    publish: async (...args: unknown[]) => {
      fake.log.push({ op: "publish", args });
      if (fake.publishErrors.length > 0) throw fake.publishErrors.shift();
      throw new Error("fake: publish is not expected");
    },
    consumers: { get: async (...args: unknown[]) => {
      fake.log.push({ op: "consumer", args });
      const options = structuredClone(args[1]);
      fake.consumerOptions.push(options);
      // Bind the reply to this request before waiting: an abandoned get must not steal a later get's outcome.
      const error = fake.consumerErrors.shift();
      const delay = fake.consumerDelays.shift() ?? 0;
      if (delay > 0) await new Promise<void>(resolve => setTimeout(resolve, delay));
      if (error !== undefined) throw error;
      return { consume: async () => {
        fake.log.push({ op: "consumerConsume", args: [options] });
        return {
          async *[Symbol.asyncIterator]() { if (fake.consumeIteratorError !== undefined) throw fake.consumeIteratorError; },
          close: async () => undefined,
        };
      } };
    } },
    };
  },
  jetstreamManager: async () => {
    fake.log.push({ op: "managerInfo", args: [] });
    if (fake.managerErrors.length > 0) throw fake.managerErrors.shift();
    return {
    streams: {
      info: async (name: string) => {
        fake.log.push({ op: "info", args: [name] });
        const delay = fake.infoDelays.shift() ?? 0;
        if (delay > 0) await new Promise<void>(resolve => setTimeout(resolve, delay));
        if (fake.infoErrors.length > 0) {
          const error = fake.infoErrors.shift();
          if (error !== undefined) throw error;
        }
        const stream = fake.streams.get(name);
        if (!stream) throw new fake.FakeJetStreamApiError(10059);
        const result = structuredClone(stream);
        if (fake.leaderPolls > 0) {
          fake.leaderPolls--;
          delete result.cluster;
        } else if (result.cluster && !result.cluster.leader) {
          result.cluster.leader = "n1";
        }
        return result;
      },
      add: async (config: Record<string, unknown> & { name: string }) => {
        fake.log.push({ op: "add", args: [structuredClone(config)] });
        if (fake.addStop) throw new Error("fake: add recorded, stop");
        const delay = fake.addDelays.shift() ?? 0;
        if (delay > 0) await new Promise<void>(resolve => setTimeout(resolve, delay));
        const error = fake.addErrors.shift();
        if (error !== undefined) throw error;
        const created = { config: { ...config, ...fake.addStoredConfigOverride }, state: { last_seq: 0, first_seq: 0 },
          cluster: fake.createdWithoutLeader ? {} : { leader: "n1" } };
        fake.streams.set(config.name, created);
        if (fake.addErrorAfterStore) throw fake.wrappedTransient();
        return structuredClone(created);
      },
      update: async (name: string, config: Record<string, unknown>) => {
        fake.log.push({ op: "update", args: [name, structuredClone(config)] });
        const stream = fake.streams.get(name);
        if (!stream) throw new fake.FakeJetStreamApiError(10059);
        stream.config = { ...stream.config, ...config } as typeof stream.config;
        return structuredClone(stream);
      },
      getMessage: async (...args: unknown[]) => {
        fake.log.push({ op: "getMessage", args });
        const error = fake.messageErrors.shift();
        const delay = fake.messageDelays.shift() ?? 0;
        if (delay > 0) await new Promise<void>(resolve => setTimeout(resolve, delay));
        if (error !== undefined) throw error;
        return undefined;
      },
      purge: async (...args: unknown[]) => { fake.log.push({ op: "purge", args }); throw new Error("fake: purge is not expected"); },
    },
  };
  },
  };
});

const { connectJetStream } = await import("../src/mesh/state-backend-nats-client.js");
const { MeshStateNatsBusyError, MeshStateNatsRefusedError, MeshStateNatsUnavailableError, connectNatsEndpoint, natsStreamName, openNatsStateBackend } = await import("../src/mesh/state-backend-nats.js");
const { normalizeFabricConfig } = await import("../src/config.js");

const STREAM = "FABRIC_MESH_REPLICAS_TEST";
const SUBJECTS = [`${STREAM}.state`, `${STREAM}.snapshot`];
const SERVERS = ["nats://127.0.0.1:4222"];
const AGENT_DIR = "/nonexistent-fabric-replicas-test/agent";
/** The harness-provided private agent dir (scripts/test-temp.ts), captured before any per-case env stub. */
const FIXTURE_AGENT_DIR = process.env.PI_CODING_AGENT_DIR;
const MUTATIONS = new Set(["add", "update", "purge", "publish"]);
const connects = () => fake.log.filter(entry => entry.op === "connect").length;
const mutations = () => fake.log.filter(entry => MUTATIONS.has(entry.op));

const seedStream = (name: string, numReplicas: number, subjects: string[]) => {
  fake.streams.set(name, {
    config: { name, subjects: [...subjects], storage: "file", num_replicas: numReplicas, max_msg_size: 1024 * 1024 },
    state: { last_seq: 7, first_seq: 1 },
    cluster: { leader: "n1" },
  });
};

// Wide option variables: `monitoringUrl` overlaps the old (all-optional) ConnectJetStreamOptions, so `replicas` is
// carried without a compile error on the old type; the old client simply ignores it.
const withReplicas = (replicas: number): { monitoringUrl?: string; replicas?: number } => ({ replicas });

/** The configured/observed replicas the refusal names: flat number own fields survive redactNatsError (guardNatsPort). */
const replicaFields = (error: unknown) => {
  const record = error as Record<string, unknown>;
  const nested = record.replicas as { configured?: unknown; observed?: unknown } | undefined;
  return {
    configured: nested?.configured ?? record.configuredReplicas,
    observed: nested?.observed ?? record.observedReplicas,
  };
};

const refusal = async (promise: Promise<unknown>): Promise<unknown> => {
  try { await promise; } catch (error) { return error; }
  throw new Error("expected a replicas refusal, but the call resolved");
};

const expectReplicasRefusal = (error: unknown, configured: number, observed: number) => {
  expect(error).toBeInstanceOf(MeshStateNatsRefusedError);
  expect((error as { code?: unknown }).code).toBe("FABRIC_MESH_STATE_NATS_REFUSED");
  expect((error as Error).message).toMatch(/replicas/i);
  expect(replicaFields(error)).toEqual({ configured, observed });
};

beforeEach(() => {
  fake.streams.clear();
  fake.log.length = 0;
  fake.managerErrors.length = 0;
  fake.infoErrors.length = 0;
  fake.infoDelays.length = 0;
  fake.messageErrors.length = 0;
  fake.messageDelays.length = 0;
  fake.consumerErrors.length = 0;
  fake.consumerOptions.length = 0;
  fake.consumerDelays.length = 0;
  fake.consumeIteratorError = undefined;
  fake.publishErrors.length = 0;
  fake.drains = 0;
  fake.addStop = false;
  fake.addErrors.length = 0;
  fake.addDelays.length = 0;
  fake.addErrorAfterStore = false;
  fake.addStoredConfigOverride = undefined;
  fake.createdWithoutLeader = false;
  fake.leaderPolls = 0;
  fake.configuredReplicas = 1;
  fake.loadGlobal.mockReset();
  fake.loadGlobal.mockImplementation(() => normalizeFabricConfig({ mesh: { nats: { replicas: fake.configuredReplicas } } }));
  fake.loadAgent.mockReset();
  fake.loadAgent.mockImplementation(() => fake.configuredReplicas as number);
  // The agent dir the client resolves: a nonexistent path, only ever handed to the fake reader.
  vi.stubEnv("PI_CODING_AGENT_DIR", AGENT_DIR);
  vi.stubGlobal("fetch", async () => { throw new Error("fake: no network"); });
});
afterEach(() => { vi.unstubAllGlobals(); vi.unstubAllEnvs(); });

describe("NATS client read resilience (pure fakes)", () => {
  it("recovers the initial JetStream manager INFO after wrapped NoResponders errors", async () => {
    fake.managerErrors.push(fake.wrappedTransient(), fake.wrappedTransient());
    const port = await connectJetStream(SERVERS, withReplicas(1));
    expect(fake.log.filter(entry => entry.op === "managerInfo")).toHaveLength(3);
    await port.close();
  });

  it.each(["account-disabled", "permission-denied", "cause-less-request"] as const)("does not retry permanent %s manager errors", async kind => {
    fake.managerErrors.push(kind === "account-disabled" ? fake.accountDisabled()
      : kind === "permission-denied" ? fake.permissionDenied() : Object.assign(new Error("request failed"), { name: "RequestError" }));
    await expect(connectJetStream(SERVERS, withReplicas(1))).rejects.toThrow();
    expect(fake.log.filter(entry => entry.op === "managerInfo")).toHaveLength(1);
    expect(fake.drains).toBe(1);
  });

  it("preserves transient JetStream API 503 retry handling", async () => {
    fake.managerErrors.push(new fake.FakeJetStreamApiError(0, 503), new fake.FakeJetStreamApiError(0, 503));
    const port = await connectJetStream(SERVERS, withReplicas(1));
    expect(fake.log.filter(entry => entry.op === "managerInfo")).toHaveLength(3);
    await port.close();
  });

  it("treats non-503 JetStream API errors as permanent even when they wrap NoResponders", async () => {
    fake.managerErrors.push(new fake.FakeJetStreamApiError(0, 500, { cause: fake.wrappedTransient() }));
    await expect(connectJetStream(SERVERS, withReplicas(1))).rejects.toThrow();
    expect(fake.log.filter(entry => entry.op === "managerInfo")).toHaveLength(1);
  });

  it("does not retry a 503 that wraps the installed protocol-error class name", async () => {
    const protocol = new ProtocolError("protocol violation");
    fake.managerErrors.push(new fake.FakeJetStreamApiError(0, 503, { cause: protocol }));
    await expect(connectJetStream(SERVERS, withReplicas(1))).rejects.toThrow();
    expect(fake.log.filter(entry => entry.op === "managerInfo")).toHaveLength(1);
    expect(fake.drains).toBe(1);
  });

  it("fails closed when an authorization cause is beyond the eight-link bound", async () => {
    let cause: Error = Object.assign(new Error("authorization denied"), { name: "AuthorizationError" });
    for (let index = 0; index < 9; index++) cause = Object.assign(new Error("JetStream wrapper"), { name: "JetStreamNotEnabled", cause });
    fake.managerErrors.push(new fake.FakeJetStreamApiError(0, 503, { cause }));
    await expect(connectJetStream(SERVERS, withReplicas(1))).rejects.toThrow();
    expect(fake.log.filter(entry => entry.op === "managerInfo")).toHaveLength(1);
  });

  it("fails closed on a cyclic cause chain", async () => {
    const cycle = Object.assign(new Error("JetStream wrapper"), { name: "JetStreamNotEnabled" }) as Error & { cause?: unknown };
    cycle.cause = cycle;
    fake.managerErrors.push(new fake.FakeJetStreamApiError(0, 503, { cause: cycle }));
    await expect(connectJetStream(SERVERS, withReplicas(1))).rejects.toThrow();
    expect(fake.log.filter(entry => entry.op === "managerInfo")).toHaveLength(1);
  });

  it("retries wrapped NoResponders under API 503 at most three times and recovers", async () => {
    fake.managerErrors.push(new fake.FakeJetStreamApiError(0, 503, { cause: fake.wrappedTransient() }),
      new fake.FakeJetStreamApiError(0, 503, { cause: fake.wrappedTransient() }));
    const port = await connectJetStream(SERVERS, withReplicas(1));
    expect(fake.log.filter(entry => entry.op === "managerInfo")).toHaveLength(3);
    await port.close();
  });

  it("recovers a stream INFO read after readiness without retrying a mutation", async () => {
    seedStream(STREAM, 1, SUBJECTS);
    const port = await connectJetStream(SERVERS, withReplicas(1));
    fake.infoErrors.push(fake.wrappedTransient());
    await expect(port.lastSequence(STREAM)).resolves.toBe(7);
    expect(fake.log.filter(entry => entry.op === "info")).toHaveLength(2);
    expect(mutations()).toEqual([]);
  });

  it("bounds transient read exhaustion at three attempts", async () => {
    seedStream(STREAM, 1, SUBJECTS);
    const port = await connectJetStream(SERVERS, withReplicas(1));
    fake.infoErrors.push(fake.wrappedTransient(), fake.wrappedTransient(), fake.wrappedTransient());
    vi.useFakeTimers();
    try {
      const result = port.lastSequence(STREAM).then(() => undefined, error => error);
      await vi.advanceTimersByTimeAsync(200);
      const error = await result;
      expect(error).toBeInstanceOf(Error);
      expect((error as Error).name).toBe("JetStreamNotEnabled");
      expect(fake.log.filter(entry => entry.op === "info")).toHaveLength(3);
    } finally {
      vi.useRealTimers();
    }
  });

  it("retries consumer creation with one stable name prefix and identical start options", async () => {
    seedStream(STREAM, 1, SUBJECTS);
    const port = await connectJetStream(SERVERS, withReplicas(1));
    fake.consumerErrors.push(fake.wrappedTransient());
    await port.consume(STREAM, 7, () => undefined);
    expect(fake.consumerOptions).toHaveLength(2);
    expect(fake.consumerOptions[0]).toEqual(fake.consumerOptions[1]);
    expect(fake.consumerOptions[0]).toMatchObject({ opt_start_seq: 8, deliver_policy: "by_start_sequence" });
    expect((fake.consumerOptions[0] as { name_prefix: string }).name_prefix).toMatch(/^[0-9a-f-]{36}$/);
  });

  it("bounds transient consumer creation exhaustion at three attempts", async () => {
    seedStream(STREAM, 1, SUBJECTS);
    const port = await connectJetStream(SERVERS, withReplicas(1));
    fake.consumerErrors.push(fake.wrappedTransient(), fake.wrappedTransient(), fake.wrappedTransient());
    vi.useFakeTimers();
    try {
      const result = port.consume(STREAM, 7, () => undefined).then(() => undefined, error => error);
      await vi.advanceTimersByTimeAsync(200);
      const error = await result;
      expect(error).toBeInstanceOf(Error);
      expect((error as Error).name).toBe("JetStreamNotEnabled");
      expect(fake.consumerOptions).toHaveLength(3);
      expect(fake.consumerOptions[0]).toEqual(fake.consumerOptions[1]);
      expect(fake.consumerOptions[1]).toEqual(fake.consumerOptions[2]);
    } finally {
      vi.useRealTimers();
    }
  });

  it("marks a wrapped transient publish for the backend's existing write retry owner", async () => {
    const port = await connectJetStream(SERVERS, withReplicas(1));
    fake.publishErrors.push(fake.wrappedTransient());
    const error = await refusal(port.publish("subject", new Uint8Array(), { expectLastSequence: 0, msgID: "id" }));
    expect((error as { transient?: unknown }).transient).toBe(true);
  });
});

// #11950/#13112: a finite initial-open reader must not leave SDK retries running after its original waiter.
// Keep: data safety / late native operations; ordinary reads retain the existing three-attempt path.
describe("NATS initial history read budget (#11950/#13112)", () => {
  type Budget = { deadline: number; active: () => boolean };
  const read = (port: Awaited<ReturnType<typeof connectJetStream>>, budget: Budget) =>
    (port.getMessage as (stream: string, seq: number, budget?: Budget) => Promise<unknown>)(STREAM, 1, budget);
  const reads = () => fake.log.filter(entry => entry.op === "getMessage");

  it("caps transient retry backoff at D and never starts a follow-on request", async () => {
    const port = await connectJetStream(SERVERS, withReplicas(1));
    fake.messageErrors.push(fake.wrappedTransient());
    vi.useFakeTimers();
    try {
      let settled = false;
      const result = read(port, { deadline: Date.now() + 50, active: () => true }).then(
        () => { settled = true; }, () => { settled = true; });
      await vi.advanceTimersByTimeAsync(49);
      expect(settled).toBe(false);
      expect(reads()).toHaveLength(1);
      await vi.advanceTimersByTimeAsync(1);
      expect(settled).toBe(true);
      await result;
      await vi.advanceTimersByTimeAsync(1_000);
      expect(reads()).toHaveLength(1);
    } finally { vi.useRealTimers(); await port.close(); }
  });

  it.each(["resolve", "reject"] as const)("bounds an in-flight GET and observes its late %s without new requests", async completion => {
    const port = await connectJetStream(SERVERS, withReplicas(1));
    fake.messageDelays.push(1_000);
    if (completion === "reject") fake.messageErrors.push(fake.wrappedTransient());
    vi.useFakeTimers();
    try {
      let settled = false;
      const result = read(port, { deadline: Date.now() + 50, active: () => true }).then(
        () => { settled = true; }, () => { settled = true; });
      await vi.advanceTimersByTimeAsync(49);
      expect(settled).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      expect(settled).toBe(true);
      await result;
      await vi.advanceTimersByTimeAsync(2_000);
      expect(reads()).toHaveLength(1);
    } finally { vi.useRealTimers(); await port.close(); }
  });

  it("checks active again after backoff so a live winner prevents a redundant retry", async () => {
    const port = await connectJetStream(SERVERS, withReplicas(1));
    fake.messageErrors.push(fake.wrappedTransient());
    vi.useFakeTimers();
    try {
      let active = true;
      const result = read(port, { deadline: Date.now() + 5_000, active: () => active }).then(() => undefined, () => undefined);
      await vi.advanceTimersByTimeAsync(0);
      expect(reads()).toHaveLength(1);
      active = false;
      await vi.advanceTimersByTimeAsync(5_001);
      await result;
      expect(reads()).toHaveLength(1);
    } finally { vi.useRealTimers(); await port.close(); }
  });

  it("rechecks active inside the deferred operation closure, before the SDK call", async () => {
    const port = await connectJetStream(SERVERS, withReplicas(1));
    vi.useFakeTimers();
    try {
      let active = true;
      const result = read(port, { deadline: Date.now() + 5_000, active: () => active }).then(() => undefined, () => undefined);
      active = false; // Synchronous retirement before untilDeadline's request microtask runs.
      await vi.advanceTimersByTimeAsync(5_001);
      await result;
      expect(reads()).toHaveLength(0);
    } finally { vi.useRealTimers(); await port.close(); }
  });

  it("does not retry an active permanent GET error", async () => {
    const port = await connectJetStream(SERVERS, withReplicas(1));
    fake.messageErrors.push(fake.permissionDenied());
    await expect(read(port, { deadline: Date.now() + 5_000, active: () => true })).rejects.toMatchObject({ name: "PermissionViolationError" });
    expect(reads()).toHaveLength(1);
    await port.close();
  });

  it("preserves omitted-budget GET retry count/backoff and pending request behavior", async () => {
    const port = await connectJetStream(SERVERS, withReplicas(1));
    fake.messageErrors.push(fake.wrappedTransient(), fake.wrappedTransient());
    vi.useFakeTimers();
    try {
      const result = port.getMessage(STREAM, 1);
      await vi.advanceTimersByTimeAsync(199);
      expect(reads()).toHaveLength(2);
      await vi.advanceTimersByTimeAsync(1);
      await expect(result).resolves.toBeUndefined();
      expect(reads()).toHaveLength(3);
      fake.messageDelays.push(1_500);
      let settled = false;
      const pending = port.getMessage(STREAM, 1).then(() => { settled = true; });
      await vi.advanceTimersByTimeAsync(1_000);
      expect(settled).toBe(false);
      await vi.advanceTimersByTimeAsync(500);
      await pending;
      expect(settled).toBe(true);
      expect(reads()).toHaveLength(4);
    } finally { vi.useRealTimers(); await port.close(); }
  });
});

// Decision C: data-safety regression #12881. The real cluster flow below owns R3 recovery proof;
// deterministic clocks cover deadline/request boundaries and error counterexamples that are costly there.
describe("NATS stream placement budget (smarty-dev#12881)", () => {
  const placementFault = () => new fake.FakeJetStreamApiError(10005, 400);
  const adds = () => fake.log.filter(entry => entry.op === "add");
  const placementOptions = (placementTimeoutMs: number) => ({ replicas: 3, placementTimeoutMs });

  it("waits exactly one configured budget, then preserves the typed unavailable error through the port guard", async () => {
    fake.addErrors.push(...Array.from({ length: 10 }, placementFault));
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, placementOptions(350));
      let settled = false;
      const result = port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 }).then(
        () => { settled = true; return undefined; }, error => { settled = true; return error; });
      await vi.advanceTimersByTimeAsync(349);
      expect(settled).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      const error = await result;
      expect(error).toBeInstanceOf(MeshStateNatsUnavailableError);
      expect(error).toMatchObject({ code: "FABRIC_MESH_STATE_NATS_UNAVAILABLE", reason: "placement-unavailable" });
      expect((error as Error).message).toContain("10005");
      expect((error as Error).message).toMatch(/placement/i);
      expect(adds().length).toBeGreaterThan(1);
      expect(adds().every(entry => (entry.args[0] as { num_replicas: number }).num_replicas === 3)).toBe(true);
      expect(fake.streams.size).toBe(0);
    } finally { vi.useRealTimers(); }
  });

  it("recovers placement before the deadline with identical R3 configuration", async () => {
    fake.addErrors.push(placementFault(), placementFault());
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, placementOptions(1_000));
      const result = port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 }).then(value => value, error => error);
      await vi.advanceTimersByTimeAsync(750);
      expect(await result).toEqual({ lastSeq: 0, maxMsgSize: 1024 * 1024 });
      expect(adds()).toHaveLength(3);
      expect(adds().every(entry => JSON.stringify(entry.args) === JSON.stringify(adds()[0]!.args))).toBe(true);
      expect(fake.streams.get(STREAM)?.config.num_replicas).toBe(3);
    } finally { vi.useRealTimers(); }
  });

  it("does not let a pending CREATE extend the placement deadline", async () => {
    fake.addErrors.push(placementFault());
    fake.addDelays.push(0, 5_000);
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, placementOptions(350));
      const result = port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 }).then(() => undefined, error => error);
      await vi.advanceTimersByTimeAsync(350);
      expect(await result).toBeInstanceOf(MeshStateNatsUnavailableError);
      expect(adds()).toHaveLength(2);
    } finally { vi.useRealTimers(); }
  });

  it("shares the placement deadline with post-create leader readiness instead of granting another 10s", async () => {
    fake.addErrors.push(placementFault());
    fake.createdWithoutLeader = true;
    fake.leaderPolls = Number.MAX_SAFE_INTEGER;
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, placementOptions(350));
      const result = port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 }).then(() => undefined, error => error);
      await vi.advanceTimersByTimeAsync(350);
      expect(await result).toBeInstanceOf(MeshStateNatsUnavailableError);
      expect(adds()).toHaveLength(2);
    } finally { vi.useRealTimers(); }
  });

  it("bounds lost-ack reconciliation INFO by the same placement deadline", async () => {
    fake.addErrors.push(placementFault(), fake.wrappedTransient());
    fake.infoDelays.push(0, 5_000);
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, placementOptions(350));
      const result = port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 }).then(() => undefined, error => error);
      await vi.advanceTimersByTimeAsync(350);
      expect(await result).toBeInstanceOf(MeshStateNatsUnavailableError);
      expect(adds()).toHaveLength(2);
      expect(fake.log.filter(entry => entry.op === "info")).toHaveLength(2);
    } finally { vi.useRealTimers(); }
  });

  it.each([new fake.FakeJetStreamApiError(10058, 400), fake.permissionDenied()])(
    "keeps unrelated $name fatal even after a placement retry", async fatal => {
      fake.addErrors.push(placementFault(), fatal);
      vi.useFakeTimers();
      try {
        const port = await connectJetStream(SERVERS, placementOptions(1_000));
        const result = port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 }).then(() => undefined, error => error);
        await vi.advanceTimersByTimeAsync(250);
        expect(await result).toMatchObject({ name: fatal.name, message: fatal.message });
        expect(adds()).toHaveLength(2);
      } finally { vi.useRealTimers(); }
    },
  );

  it("defaults to 30000ms for a normal peer restart and metadata election", async () => {
    fake.addErrors.push(...Array.from({ length: 400 }, placementFault));
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, withReplicas(3));
      let settled = false;
      const result = port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 }).then(
        () => { settled = true; return undefined; }, error => { settled = true; return error; });
      await vi.advanceTimersByTimeAsync(29_999);
      expect(settled).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      expect(await result).toBeInstanceOf(MeshStateNatsUnavailableError);
    } finally { vi.useRealTimers(); }
  });

  it.each([
    new fake.FakeJetStreamApiError(10058, 400),
    fake.permissionDenied(),
    new Error("no suitable peers for placement, peer offline"),
    Object.assign(new fake.FakeJetStreamApiError(10071, 400), { message: "no suitable peers for placement, peer offline" }),
  ])("does not retry unrelated errors or classify placement by text: $message", async fatal => {
    fake.addErrors.push(fatal);
    const port = await connectJetStream(SERVERS, placementOptions(350));
    await expect(port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 })).rejects.toMatchObject({ name: fatal.name, message: fatal.message });
    expect(adds()).toHaveLength(1);
  });

  it.each([0, -1, 1.5, NaN, Infinity, 2 ** 31])("rejects invalid placement timeout %s before connecting", async placementTimeoutMs => {
    await expect(connectJetStream(SERVERS, placementOptions(placementTimeoutMs))).rejects.toThrow(/placementTimeoutMs/);
    expect(connects()).toBe(0);
  });
});

// smarty-dev#12900 / #12881: fresh-stream 10059 and an R1 ephemeral assignment stuck on a paused stream peer.
// Keep reason: recorded cluster catch and data safety; fake time reaches exact budgets/late orphan replies cheaply.
describe("NATS initial ordered-consumer bootstrap budget (smarty-dev#12900)", () => {
  const notFound = () => jserrors.StreamNotFoundError.fromMessage("stream not found");
  const timeout = () => Object.assign(new Error("consumer create timed out"), { name: "TimeoutError" });
  const prefixes = () => fake.consumerOptions.map(options => (options as { name_prefix: string }).name_prefix);
  const activePrefixes = () => fake.log.filter(entry => entry.op === "consumerConsume")
    .map(entry => (entry.args[0] as { name_prefix: string }).name_prefix);
  const expectOrderedOptions = (afterSeq: number) => {
    for (const options of fake.consumerOptions) {
      expect(options).toEqual({ name_prefix: expect.stringMatching(/^[0-9a-f-]{36}$/), inactive_threshold: 10_000,
        ...(afterSeq === 0 ? { deliver_policy: "all" } : { deliver_policy: "by_start_sequence", opt_start_seq: afterSeq + 1 }) });
    }
  };
  const ensure = (port: Awaited<ReturnType<typeof connectJetStream>>) => port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 });
  const settle = <T>(promise: Promise<T>) => {
    const state = { settled: false };
    const result = promise.then(value => { state.settled = true; return value as unknown; }, error => { state.settled = true; return error as unknown; });
    return { state, result };
  };
  const expectStableOptions = () => {
    expect(fake.consumerOptions.length).toBeGreaterThan(0);
    expect(fake.consumerOptions.every(options => JSON.stringify(options) === JSON.stringify(fake.consumerOptions[0]))).toBe(true);
    expect((fake.consumerOptions[0] as { name_prefix: string }).name_prefix).toMatch(/^[0-9a-f-]{36}$/);
  };

  it.each(["created", "ambiguous created"] as const)("retries consumer 10059 after a %s stream, then succeeds", async source => {
    fake.addErrorAfterStore = source === "ambiguous created";
    fake.createdWithoutLeader = true;
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, withReplicas(3));
      await ensure(port);
      const fault = notFound();
      expect(fault).toBeInstanceOf(jserrors.JetStreamApiError);
      expect(fault).toMatchObject({ name: "StreamNotFoundError", code: 10059, status: 404 });
      fake.consumerErrors.push(fault, notFound());
      const { result } = settle(port.consume(STREAM, 0, () => undefined));
      await vi.advanceTimersByTimeAsync(200);
      expect(await result).toMatchObject({ stop: expect.any(Function) });
      expect(fake.consumerOptions).toHaveLength(3);
      expectStableOptions();
      expect(fake.consumerOptions[0]).toMatchObject({ deliver_policy: "all" });
    } finally { vi.useRealTimers(); }
  });

  it("surfaces persistent 10059 as typed consumer-bootstrap unavailability at exactly the default readiness deadline", async () => {
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, withReplicas(1));
      const startedAt = Date.now();
      await ensure(port);
      const fault = jserrors.StreamNotFoundError.fromMessage("stream not found at nats://bootstrap-user:bootstrap-secret@localhost:4222");
      fake.consumerErrors.push(...Array.from({ length: 500 }, () => fault));
      const { state, result } = settle(port.consume(STREAM, 0, () => undefined));
      await vi.advanceTimersByTimeAsync(9_999);
      expect(state.settled).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      const error = await result;
      expect(Date.now() - startedAt).toBe(10_000);
      expect(error).toBeInstanceOf(MeshStateNatsUnavailableError);
      expect(error).toMatchObject({ code: "FABRIC_MESH_STATE_NATS_UNAVAILABLE", reason: "consumer-bootstrap-unavailable", natsErrCode: 10059 });
      expect((error as Error).message).toContain("10059");
      expect((error as Error).message).not.toMatch(/placement/i);
      expect((error as Error).cause).toBeInstanceOf(jserrors.StreamNotFoundError);
      expect(((error as Error).cause as { apiError?: () => { err_code: number } }).apiError?.().err_code).toBe(10059);
      expect((error as Error).cause).not.toBe(fault);
      expect(inspect(error, { showHidden: true, depth: null })).not.toContain("bootstrap-secret");
      expect(inspect(error, { showHidden: true, depth: null })).not.toContain("bootstrap-user");
      expect(fake.consumerOptions).toHaveLength(100);
      expectStableOptions();
    } finally { vi.useRealTimers(); }
  });

  it("keeps consumer 10059 on an existing stream fatal after one attempt", async () => {
    seedStream(STREAM, 3, SUBJECTS);
    const port = await connectJetStream(SERVERS, withReplicas(3));
    await ensure(port);
    fake.consumerErrors.push(notFound(), notFound());
    await expect(port.consume(STREAM, 7, () => undefined)).rejects.toBeInstanceOf(jserrors.StreamNotFoundError);
    expect(fake.consumerOptions).toHaveLength(1);
  });

  it("clears a fresh budget when a later ensure only observes the existing stream", async () => {
    const port = await connectJetStream(SERVERS, withReplicas(1));
    await ensure(port);
    await ensure(port);
    expect(mutations().map(entry => entry.op)).toEqual(["add"]);
    fake.consumerErrors.push(notFound(), notFound());
    await expect(port.consume(STREAM, 0, () => undefined)).rejects.toBeInstanceOf(jserrors.StreamNotFoundError);
    expect(fake.consumerOptions).toHaveLength(1);
  });

  it("grants no 10059 tolerance to a second consume or a running consumer", async () => {
    const port = await connectJetStream(SERVERS, withReplicas(1));
    await ensure(port);
    const first = await port.consume(STREAM, 0, () => undefined);
    fake.consumerErrors.push(notFound(), notFound());
    await expect(port.consume(STREAM, 0, () => undefined)).rejects.toBeInstanceOf(jserrors.StreamNotFoundError);
    expect(fake.consumerOptions).toHaveLength(2);
    await first.stop();

    fake.consumerErrors.length = 0;
    fake.streams.clear();
    await ensure(port);
    fake.consumeIteratorError = notFound();
    const errors: unknown[] = [];
    await port.consume(STREAM, 0, () => undefined, error => { errors.push(error); });
    await vi.waitFor(() => expect(errors).toHaveLength(1));
    expect((errors[0] as { apiError?: () => { err_code: number } }).apiError?.().err_code).toBe(10059);
    expect(fake.consumerOptions).toHaveLength(3);
  });

  it("rotates pending retries within the shared deadline while preserving the observed fresh 10059 cause", async () => {
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, withReplicas(1));
      await ensure(port);
      fake.consumerErrors.push(notFound());
      fake.consumerDelays.push(0, ...Array.from({ length: 20 }, () => 60_000));
      const { state, result } = settle(port.consume(STREAM, 4, () => undefined));
      await vi.advanceTimersByTimeAsync(9_999);
      expect(state.settled).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      expect(await result).toMatchObject({ reason: "consumer-bootstrap-unavailable", natsErrCode: 10059 });
      expect(fake.consumerOptions).toHaveLength(10);
      expect(prefixes()[0]).toBe(prefixes()[1]); // 10059 means no assignment: keep its name.
      expect(new Set(prefixes())).toHaveProperty("size", 9);
      expect(activePrefixes()).toEqual([]);
      expectOrderedOptions(4);
      await vi.advanceTimersByTimeAsync(60_000); // All abandoned successes arrive after the open has failed.
      expect(fake.consumerOptions).toHaveLength(10); // No request starts at or after the deadline.
      expect(activePrefixes()).toEqual([]);
    } finally { vi.useRealTimers(); }
  });

  it("reports a first pending request that outlives the budget without faking a 10059 cause", async () => {
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, withReplicas(1));
      await ensure(port);
      fake.consumerDelays.push(...Array.from({ length: 20 }, () => 60_000));
      const { result } = settle(port.consume(STREAM, 0, () => undefined));
      await vi.advanceTimersByTimeAsync(10_000);
      const error = await result;
      expect(error).toBeInstanceOf(MeshStateNatsUnavailableError);
      expect(error).toMatchObject({ reason: "consumer-bootstrap-unavailable" });
      expect((error as { natsErrCode?: unknown }).natsErrCode).toBeUndefined();
      expect((error as Error).message).not.toContain("10059");
      expect((error as Error).cause).toBeUndefined();
      expect(fake.consumerOptions).toHaveLength(10);
      expect(new Set(prefixes())).toHaveProperty("size", 10);
      expectOrderedOptions(0);
      expect(activePrefixes()).toEqual([]);
      await vi.advanceTimersByTimeAsync(60_000);
      expect(fake.consumerOptions).toHaveLength(10); // Neither late replies nor the deadline renew the budget.
      expect(activePrefixes()).toEqual([]);
    } finally { vi.useRealTimers(); }
  });

  it.each(["fresh", "existing"] as const)("does not start a consumer request after the %s ensure's shared deadline has elapsed", async source => {
    if (source === "existing") seedStream(STREAM, 1, SUBJECTS);
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, withReplicas(1));
      await ensure(port);
      await vi.advanceTimersByTimeAsync(10_000);
      const error = await port.consume(STREAM, 0, () => undefined).then(() => undefined, error => error);
      expect(error).toBeInstanceOf(MeshStateNatsUnavailableError);
      expect(error).toMatchObject({ reason: "consumer-bootstrap-unavailable" });
      expect(fake.consumerOptions).toHaveLength(0);
    } finally { vi.useRealTimers(); }
  });

  it("shares one budget across placement retries, leader wait and consumer 10059", async () => {
    fake.addErrors.push(new fake.FakeJetStreamApiError(10005, 400));
    fake.createdWithoutLeader = true;
    fake.infoDelays.push(0, 50);
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, { replicas: 3, placementTimeoutMs: 350 });
      const startedAt = Date.now();
      fake.consumerErrors.push(...Array.from({ length: 50 }, notFound));
      const { state, result } = settle(ensure(port).then(() => port.consume(STREAM, 0, () => undefined)));
      await vi.advanceTimersByTimeAsync(349);
      expect(state.settled).toBe(false);
      expect(fake.consumerOptions).toHaveLength(1);
      await vi.advanceTimersByTimeAsync(1);
      const error = await result;
      expect(Date.now() - startedAt).toBe(350);
      expect(error).toMatchObject({ reason: "consumer-bootstrap-unavailable", natsErrCode: 10059 });
      expectStableOptions();
    } finally { vi.useRealTimers(); }
  });

  it.each([new fake.FakeJetStreamApiError(10058, 400), fake.permissionDenied()])(
    "keeps unrelated bootstrap error $name fatal without retry", async fatal => {
      const port = await connectJetStream(SERVERS, withReplicas(1));
      await ensure(port);
      fake.consumerErrors.push(fatal);
      await expect(port.consume(STREAM, 0, () => undefined)).rejects.toMatchObject({ name: fatal.name, message: fatal.message });
      expect(fake.consumerOptions).toHaveLength(1);
    },
  );

  it("keeps the three-attempt transport bound inside a fresh budget", async () => {
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, withReplicas(1));
      await ensure(port);
      fake.consumerErrors.push(fake.wrappedTransient(), notFound(), fake.wrappedTransient(), fake.wrappedTransient(), fake.wrappedTransient());
      const { result } = settle(port.consume(STREAM, 0, () => undefined));
      await vi.advanceTimersByTimeAsync(1_000);
      expect(await result).toMatchObject({ name: "JetStreamNotEnabled" });
      expect(fake.consumerOptions).toHaveLength(4);
      expectStableOptions();
    } finally { vi.useRealTimers(); }
  });

  it.each(["fresh", "existing"] as const)("rotates a first TimeoutError after a %s ensure and succeeds with a new prefix", async source => {
    if (source === "existing") seedStream(STREAM, 3, SUBJECTS);
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, withReplicas(3));
      await ensure(port);
      fake.consumerErrors.push(timeout());
      const { result } = settle(port.consume(STREAM, 7, () => undefined));
      await vi.advanceTimersByTimeAsync(100);
      const consumer = await result as { stop(): Promise<void> };
      expect(consumer).toMatchObject({ stop: expect.any(Function) });
      expect(fake.consumerOptions).toHaveLength(2);
      expect(prefixes()[1]).not.toBe(prefixes()[0]);
      expect(activePrefixes()).toEqual([prefixes()[1]]);
      expectOrderedOptions(7);
      await consumer.stop();
    } finally { vi.useRealTimers(); }
  });

  it.each(["fresh", "existing"] as const)("abandons a slow initial get after a %s ensure and never consumes its late reply", async source => {
    if (source === "existing") seedStream(STREAM, 1, SUBJECTS);
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, withReplicas(1));
      await ensure(port);
      fake.consumerDelays.push(1_500);
      const { state, result } = settle(port.consume(STREAM, 0, () => undefined));
      await vi.advanceTimersByTimeAsync(999);
      expect(state.settled).toBe(false);
      expect(fake.consumerOptions).toHaveLength(1);
      await vi.advanceTimersByTimeAsync(101);
      expect(state.settled).toBe(true); // Old code must fail RED here, not hang awaiting the 1.5s get.
      const consumer = await result as { stop(): Promise<void> };
      expect(consumer).toMatchObject({ stop: expect.any(Function) });
      expect(fake.consumerOptions).toHaveLength(2);
      const [abandoned, accepted] = prefixes();
      expect(accepted).not.toBe(abandoned);
      expect(activePrefixes()).toEqual([accepted]);
      await vi.advanceTimersByTimeAsync(400); // The abandoned get now resolves successfully.
      expect(fake.consumerOptions).toHaveLength(2);
      expect(prefixes().filter(prefix => prefix === abandoned)).toHaveLength(1);
      expect(activePrefixes()).toEqual([accepted]);
      // The actual options objects held by the SDK must also retain their original, distinct names.
      const requests = fake.log.filter(entry => entry.op === "consumer");
      expect((requests[0]!.args[1] as { name_prefix: string }).name_prefix).toBe(abandoned);
      expect(requests[0]!.args[1]).not.toBe(requests[1]!.args[1]);
      expectOrderedOptions(0);
      expect(mutations().map(entry => entry.op)).toEqual(source === "fresh" ? ["add"] : []);
      await consumer.stop();
    } finally { vi.useRealTimers(); }
  });

  it.each(["fresh", "existing"] as const)("bounds persistent slow TimeoutErrors after a %s ensure at exactly 10s, not 15.2s", async source => {
    if (source === "existing") seedStream(STREAM, 1, SUBJECTS);
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, withReplicas(1));
      const startedAt = Date.now();
      await ensure(port);
      fake.consumerErrors.push(...Array.from({ length: 20 }, timeout));
      fake.consumerDelays.push(...Array.from({ length: 20 }, () => 5_000));
      const { state, result } = settle(port.consume(STREAM, 0, () => undefined));
      await vi.advanceTimersByTimeAsync(9_999);
      expect(state.settled).toBe(false);
      expect(fake.consumerOptions).toHaveLength(10);
      await vi.advanceTimersByTimeAsync(1);
      expect(Date.now() - startedAt).toBe(10_000);
      expect(await result).toBeInstanceOf(MeshStateNatsUnavailableError);
      expect(await result).toMatchObject({ reason: "consumer-bootstrap-unavailable", consumerBootstrap: true });
      expect((await result as { natsErrCode?: unknown }).natsErrCode).toBeUndefined();
      expect(new Set(prefixes())).toHaveProperty("size", 10);
      expectOrderedOptions(0);
      expect(activePrefixes()).toEqual([]);
      await vi.advanceTimersByTimeAsync(5_200); // Late failures cannot start another request or consumer.
      expect(fake.consumerOptions).toHaveLength(10);
      expect(activePrefixes()).toEqual([]);
    } finally { vi.useRealTimers(); }
  });

  it("does not apply the three-transport-failure bound to its own immediate TimeoutError rotations", async () => {
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, withReplicas(1));
      await ensure(port);
      fake.consumerErrors.push(...Array.from({ length: 200 }, timeout));
      const { state, result } = settle(port.consume(STREAM, 0, () => undefined));
      await vi.advanceTimersByTimeAsync(9_999);
      expect(state.settled).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      expect(await result).toMatchObject({ reason: "consumer-bootstrap-unavailable" });
      expect(fake.consumerOptions).toHaveLength(100);
      expect(new Set(prefixes())).toHaveProperty("size", 100);
      expectOrderedOptions(0);
      expect(activePrefixes()).toEqual([]);
    } finally { vi.useRealTimers(); }
  });

  it("carries an existing stream's leader-readiness deadline into its first consumer without renewing it", async () => {
    seedStream(STREAM, 3, SUBJECTS);
    fake.leaderPolls = 1;
    fake.infoDelays.push(0, 6_500);
    fake.consumerDelays.push(...Array.from({ length: 20 }, () => 60_000));
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, withReplicas(3));
      const startedAt = Date.now();
      const { state, result } = settle(ensure(port).then(() => port.consume(STREAM, 0, () => undefined)));
      await vi.advanceTimersByTimeAsync(6_499);
      expect(fake.consumerOptions).toHaveLength(0);
      await vi.advanceTimersByTimeAsync(3_500);
      expect(state.settled).toBe(false);
      expect(fake.consumerOptions).toHaveLength(4);
      await vi.advanceTimersByTimeAsync(1);
      expect(Date.now() - startedAt).toBe(10_000);
      expect(await result).toMatchObject({ reason: "consumer-bootstrap-unavailable" });
      expect(new Set(prefixes())).toHaveProperty("size", 4);
      expectOrderedOptions(0);
      expect(activePrefixes()).toEqual([]);
    } finally { vi.useRealTimers(); }
  });

  it("uses exactly the remaining 350ms placement/leader/consumer budget, not a renewed 1s attempt", async () => {
    fake.addErrors.push(new fake.FakeJetStreamApiError(10005, 400));
    fake.createdWithoutLeader = true;
    fake.infoDelays.push(0, 50);
    fake.consumerDelays.push(60_000);
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, { replicas: 3, placementTimeoutMs: 350 });
      const startedAt = Date.now();
      const { state, result } = settle(ensure(port).then(() => port.consume(STREAM, 0, () => undefined)));
      await vi.advanceTimersByTimeAsync(349);
      expect(state.settled).toBe(false);
      expect(fake.consumerOptions).toHaveLength(1);
      await vi.advanceTimersByTimeAsync(1);
      expect(Date.now() - startedAt).toBe(350);
      expect(await result).toBeInstanceOf(MeshStateNatsUnavailableError);
      expect(await result).toMatchObject({ reason: "consumer-bootstrap-unavailable" });
      expect((await result as { natsErrCode?: unknown }).natsErrCode).toBeUndefined();
      await vi.advanceTimersByTimeAsync(1_000);
      expect(fake.consumerOptions).toHaveLength(1);
      expectOrderedOptions(0);
      expect(activePrefixes()).toEqual([]);
    } finally { vi.useRealTimers(); }
  });

  it("creates only its own options-based ordered ephemeral consumer and leaves the SDK/ordinary-read timeout unchanged", async () => {
    seedStream(STREAM, 1, SUBJECTS);
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, withReplicas(1));
      await ensure(port);
      const consumer = await port.consume(STREAM, 0, () => undefined);
      expect(fake.log.filter(entry => entry.op === "jetstream")).toHaveLength(1);
      expect(fake.log.find(entry => entry.op === "jetstream")!.args).toHaveLength(1); // nc only; no timeout override.
      expect(fake.consumerOptions).toEqual([{ name_prefix: expect.stringMatching(/^[0-9a-f-]{36}$/),
        inactive_threshold: 10_000, deliver_policy: "all" }]); // No durable/name string or other timeout field.
      fake.infoDelays.push(1_500);
      const { state, result } = settle(port.lastSequence(STREAM));
      await vi.advanceTimersByTimeAsync(1_000);
      expect(state.settled).toBe(false);
      await vi.advanceTimersByTimeAsync(500);
      expect(await result).toBe(7);
      await consumer.stop();
    } finally { vi.useRealTimers(); }
  });

  it.each([timeout(), notFound(), fake.wrappedTransient(), fake.permissionDenied()])(
    "passes running consumer $name failures through without bootstrap retry or rotation", async fatal => {
      const port = await connectJetStream(SERVERS, withReplicas(1));
      await ensure(port);
      fake.consumeIteratorError = fatal;
      const errors: unknown[] = [];
      const consumer = await port.consume(STREAM, 0, () => undefined, error => { errors.push(error); });
      await vi.waitFor(() => expect(errors).toHaveLength(1));
      expect(errors[0]).toMatchObject({ name: fatal.name, message: fatal.message });
      expect(fake.consumerOptions).toHaveLength(1);
      expect(activePrefixes()).toEqual([prefixes()[0]]);
      await consumer.stop();
    },
  );

  it("keeps a TimeoutError wrapping a permission failure fatal without rotating", async () => {
    const port = await connectJetStream(SERVERS, withReplicas(1));
    await ensure(port);
    const fatal = Object.assign(timeout(), { cause: fake.permissionDenied() });
    fake.consumerErrors.push(fatal);
    await expect(port.consume(STREAM, 0, () => undefined)).rejects.toMatchObject({ name: fatal.name, message: fatal.message });
    expect(fake.consumerOptions).toHaveLength(1);
  });

  it("keeps the placement reason for 10005 and the projector reason without a code", () => {
    expect(new MeshStateNatsUnavailableError("x", { natsErrCode: 10005 }).reason).toBe("placement-unavailable");
    expect(new MeshStateNatsUnavailableError("x").reason).toBe("projector-unavailable");
  });
});

describe("NATS stream replicas: real client ensureStream (pure fakes)", () => {
  it("accepts an ambiguously-created stream only after matching its exact requested config", async () => {
    fake.addErrorAfterStore = true;
    const port = await connectJetStream(SERVERS, withReplicas(1));
    await expect(port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 })).resolves.toEqual({ lastSeq: 0, maxMsgSize: 1024 * 1024 });
    expect(mutations().map(entry => entry.op)).toEqual(["add"]);
    expect(fake.streams.get(STREAM)?.config).toMatchObject({ subjects: SUBJECTS, storage: "file", num_replicas: 1, max_msg_size: 1024 * 1024 });
  });

  it("refuses an ambiguously-created stream whose existing config differs from the requested limits", async () => {
    fake.addErrorAfterStore = true;
    fake.addStoredConfigOverride = { max_msg_size: 1024 * 1024 + 1 };
    const port = await connectJetStream(SERVERS, withReplicas(1));
    const error = await refusal(port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 }));
    expect(error).toBeInstanceOf(MeshStateNatsRefusedError);
    expect((error as Error).message).toMatch(/ambiguous create.*configuration/);
    expect(mutations().map(entry => entry.op)).toEqual(["add"]);
    expect(fake.streams.get(STREAM)?.config.max_msg_size).toBe(1024 * 1024 + 1);
  });

  it("refuses an existing 1-replica stream when 3 are configured, before adding its missing subject", async () => {
    seedStream(STREAM, 1, [SUBJECTS[0]!]);
    const port = await connectJetStream(SERVERS, withReplicas(3));
    const error = await refusal(port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 }));
    expectReplicasRefusal(error, 3, 1);
    expect(mutations()).toEqual([]);
    expect(fake.streams.get(STREAM)?.config).toMatchObject({ num_replicas: 1, subjects: [SUBJECTS[0]] });
  });

  it("refuses an existing 3-replica stream when 1 is configured and never lowers it", async () => {
    seedStream(STREAM, 3, SUBJECTS);
    const port = await connectJetStream(SERVERS, withReplicas(1));
    const error = await refusal(port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 }));
    expectReplicasRefusal(error, 1, 3);
    expect(mutations()).toEqual([]);
    expect(fake.streams.get(STREAM)?.config.num_replicas).toBe(3);
  });

  it("control: a matching 3-replica stream with every subject opens with no mutation", async () => {
    seedStream(STREAM, 3, SUBJECTS);
    const port = await connectJetStream(SERVERS, withReplicas(3));
    await expect(port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 })).resolves.toEqual({ lastSeq: 7, maxMsgSize: 1024 * 1024 });
    expect(mutations()).toEqual([]);
  });

  it("waits for a replicated stream leader before returning from ensureStream", async () => {
    seedStream(STREAM, 3, SUBJECTS);
    fake.leaderPolls = 2;
    const port = await connectJetStream(SERVERS, withReplicas(3));
    await expect(port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 })).resolves.toEqual({ lastSeq: 7, maxMsgSize: 1024 * 1024 });
    expect(fake.log.filter(entry => entry.op === "info")).toHaveLength(3);
  });

  it("rejects a leader that arrives after two slow INFO requests consume the 10-second deadline", async () => {
    seedStream(STREAM, 3, SUBJECTS);
    fake.leaderPolls = 1;
    fake.infoDelays.push(0, 5_000, 5_000);
    fake.infoErrors.push(undefined, Object.assign(new Error("timeout"), { name: "TimeoutError" }));
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, withReplicas(3));
      const result = port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 }).then(() => undefined, error => error);
      await vi.advanceTimersByTimeAsync(5_000);
      await vi.advanceTimersByTimeAsync(100);
      await vi.advanceTimersByTimeAsync(4_900);
      const error = await result;
      expect(error).toBeInstanceOf(MeshStateNatsBusyError);
      expect(fake.log.filter(entry => entry.op === "info")).toHaveLength(3); // initial lookup + two deadline-bounded polls
    } finally {
      vi.useRealTimers();
    }
  });

  it("stops starting leader INFO requests when repeated five-second timeouts exhaust the deadline", async () => {
    seedStream(STREAM, 3, SUBJECTS);
    fake.leaderPolls = 1;
    fake.infoDelays.push(0, 5_000, 5_000);
    fake.infoErrors.push(undefined, Object.assign(new Error("timeout"), { name: "TimeoutError" }),
      Object.assign(new Error("timeout"), { name: "TimeoutError" }));
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, withReplicas(3));
      const result = port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 }).then(() => undefined, error => error);
      await vi.advanceTimersByTimeAsync(5_000);
      await vi.advanceTimersByTimeAsync(100);
      await vi.advanceTimersByTimeAsync(4_900);
      const error = await result;
      expect(error).toBeInstanceOf(MeshStateNatsBusyError);
      expect(fake.log.filter(entry => entry.op === "info")).toHaveLength(3);
    } finally {
      vi.useRealTimers();
    }
  });

  it("waits for a leader after creating a replicated stream", async () => {
    fake.createdWithoutLeader = true;
    fake.leaderPolls = 2;
    const port = await connectJetStream(SERVERS, withReplicas(3));
    await expect(port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 })).resolves.toEqual({ lastSeq: 0, maxMsgSize: 1024 * 1024 });
    expect(fake.log.filter(entry => entry.op === "info")).toHaveLength(4); // absent check + three bounded polls
  });

  it("does not poll a single-node stream for a cluster leader", async () => {
    seedStream(STREAM, 1, SUBJECTS);
    fake.leaderPolls = 2;
    const port = await connectJetStream(SERVERS, withReplicas(1));
    await expect(port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 })).resolves.toEqual({ lastSeq: 7, maxMsgSize: 1024 * 1024 });
    expect(fake.log.filter(entry => entry.op === "info")).toHaveLength(1); // the initial lookup only
  });

  // smarty-dev#12633: deterministic replay of the hosted four-writer child's readiness failure.
  it.each(["successful create", "observed stream", "ambiguous create"] as const)(
    "recovers leader-wait INFO 10059 after %s", async source => {
      if (source === "observed stream") {
        seedStream(STREAM, 3, SUBJECTS);
        fake.leaderPolls = 1; // The initial lookup observes the stream without its leader.
      } else {
        fake.createdWithoutLeader = true;
        fake.addErrorAfterStore = source === "ambiguous create";
        if (fake.addErrorAfterStore) fake.leaderPolls = 1; // Reconciliation observes the stream without its leader.
      }
      fake.infoErrors.push(undefined, ...(source === "ambiguous create" ? [undefined] : []),
        new fake.FakeJetStreamApiError(10059), new fake.FakeJetStreamApiError(10059));
      vi.useFakeTimers();
      try {
        const port = await connectJetStream(SERVERS, withReplicas(3));
        // Attach the rejection handler before advancing timers so RED has no unhandled rejection.
        const result = port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 }).then(value => value, error => error);
        await vi.advanceTimersByTimeAsync(200);
        expect(await result).toEqual({ lastSeq: source === "observed stream" ? 7 : 0, maxMsgSize: 1024 * 1024 });
        expect(mutations().map(entry => entry.op)).toEqual(source === "observed stream" ? [] : ["add"]);
        expect(fake.log.filter(entry => entry.op === "info")).toHaveLength(source === "ambiguous create" ? 5 : 4);
      } finally {
        vi.useRealTimers();
      }
    },
  );

  it("bounds persistent leader-wait INFO 10059 at the unchanged 10000ms busy deadline", async () => {
    seedStream(STREAM, 3, SUBJECTS);
    fake.leaderPolls = 1;
    fake.infoErrors.push(undefined, ...Array.from({ length: 100 }, () => new fake.FakeJetStreamApiError(10059)));
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, withReplicas(3));
      const startedAt = Date.now();
      let settled = false;
      const result = port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 }).then(
        () => { settled = true; return undefined; }, error => { settled = true; return error; });
      await vi.advanceTimersByTimeAsync(9_999);
      expect(settled).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      const error = await result;
      expect(error).toBeInstanceOf(MeshStateNatsBusyError);
      expect((error as Error).message).toContain(`stream ${STREAM} had no elected leader after 10000ms`);
      expect(Date.now() - startedAt).toBe(10_000);
      expect(fake.log.filter(entry => entry.op === "info")).toHaveLength(101);
    } finally {
      vi.useRealTimers();
    }
  });

  it.each(["other API error", "permission denied"] as const)(
    "keeps %s immediately fatal during leader wait", async kind => {
      seedStream(STREAM, 3, SUBJECTS);
      fake.leaderPolls = 1;
      const fatal = kind === "other API error" ? new fake.FakeJetStreamApiError(10071, 400) : fake.permissionDenied();
      fake.infoErrors.push(undefined, fatal);
      const port = await connectJetStream(SERVERS, withReplicas(3));
      await expect(port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 })).rejects.toMatchObject({
        name: fatal.name, message: fatal.message,
      });
      expect(fake.log.filter(entry => entry.op === "info")).toHaveLength(2);
    },
  );

  it("keeps INFO 10059 fatal for ordinary reads and false for streamExists", async () => {
    const port = await connectJetStream(SERVERS, withReplicas(3));
    await expect(port.lastSequence(STREAM)).rejects.toBeInstanceOf(fake.FakeJetStreamApiError);
    expect(fake.log.filter(entry => entry.op === "info")).toHaveLength(1);
    await expect(port.streamExists(STREAM)).resolves.toBe(false);
    expect(fake.log.filter(entry => entry.op === "info")).toHaveLength(2);
    expect(mutations()).toEqual([]);
  });

  it("fails with a bounded busy error when a replicated stream never elects a leader", async () => {
    seedStream(STREAM, 3, SUBJECTS);
    fake.leaderPolls = Number.MAX_SAFE_INTEGER;
    vi.useFakeTimers();
    try {
      const port = await connectJetStream(SERVERS, withReplicas(3));
      const result = port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 }).then(() => undefined, error => error);
      await vi.advanceTimersByTimeAsync(10_000);
      const error = await result;
      expect(error).toBeInstanceOf(MeshStateNatsBusyError);
      expect((error as Error).message).toContain(`stream ${STREAM} had no elected leader after 10000ms`);
      expect((error as Error).message).not.toContain("fake");
    } finally {
      vi.useRealTimers();
    }
  });

  it("control: a matching 3-replica stream missing a subject gets only that subject, never a replicas change", async () => {
    seedStream(STREAM, 3, [SUBJECTS[0]!]);
    const port = await connectJetStream(SERVERS, withReplicas(3));
    await port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 });
    expect(mutations().map(entry => entry.op)).toEqual(["update"]);
    const [, update] = mutations()[0]!.args as [string, Record<string, unknown>];
    expect(update.subjects).toEqual(SUBJECTS);
    expect(update.num_replicas === undefined || update.num_replicas === 3).toBe(true);
    expect(fake.streams.get(STREAM)?.config.num_replicas).toBe(3);
  });

  it("creates an absent stream with num_replicas 3 when 3 are configured", async () => {
    const port = await connectJetStream(SERVERS, withReplicas(3));
    await port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 });
    expect(mutations().map(entry => entry.op)).toEqual(["add"]);
    expect(mutations()[0]!.args[0]).toMatchObject({ name: STREAM, subjects: SUBJECTS, num_replicas: 3, max_msg_size: 1024 * 1024 });
  });

  it("creates every JetStream stream asset with configured replicas 3", async () => {
    fake.configuredReplicas = 3;
    const port = await connectJetStream(SERVERS);
    await port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 });
    const secondStream = `${STREAM}_SECOND`;
    await port.ensureStream(secondStream, [`${secondStream}.state`, `${secondStream}.snapshot`], { maxMsgSize: 1024 * 1024 });
    const adds = mutations().filter(entry => entry.op === "add");
    expect(adds).toHaveLength(2);
    expect(adds.map(entry => (entry.args[0] as { name: string; num_replicas: number }).name)).toEqual([STREAM, secondStream]);
    expect(adds.every(entry => (entry.args[0] as { num_replicas: number }).num_replicas === 3)).toBe(true);
  });

  it("creates an absent stream with num_replicas 1 by default", async () => {
    const port = await connectJetStream(SERVERS);
    await port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 });
    expect(mutations().map(entry => entry.op)).toEqual(["add"]);
    expect(mutations()[0]!.args[0]).toMatchObject({ name: STREAM, num_replicas: 1 });
  });
});

describe("NATS stream replicas: OpenNatsStateBackendOptions.replicas wiring (pure fakes)", () => {
  // A root that does not exist: natsStreamName only resolves it (no write); open never reaches the projector.
  const root = "/nonexistent-fabric-replicas-test/root";
  const openOptions = (replicas: number): { root: string; servers: string[]; acceptPeriodicSync: boolean; replicas: number } =>
    ({ root, servers: [...SERVERS], acceptPeriodicSync: true, replicas });

  it("passes replicas 3 through to the created stream", async () => {
    fake.addStop = true; // record the add, then stop the open before any read of the new stream
    await openNatsStateBackend(openOptions(3)).catch(() => undefined);
    const adds = mutations().filter(entry => entry.op === "add");
    expect(adds).toHaveLength(1);
    expect(adds[0]!.args[0]).toMatchObject({ name: natsStreamName(root), num_replicas: 3 });
  });

  it("refuses an existing 1-replica stream when opened with replicas 3, with no mutation", async () => {
    const stream = natsStreamName(root);
    seedStream(stream, 1, [`${stream}.state`]);
    const error = await refusal(openNatsStateBackend(openOptions(3)));
    expectReplicasRefusal(error, 3, 1);
    expect(mutations()).toEqual([]);
  });
});

describe("NATS stream replicas: agent fabric.json mesh.nats.replicas wiring (fake reader, pure fakes)", () => {
  const root = "/nonexistent-fabric-replicas-test/root";
  const openOmitted = (): { root: string; servers: string[]; acceptPeriodicSync: boolean } =>
    ({ root, servers: [...SERVERS], acceptPeriodicSync: true });
  const openExplicit = (replicas: unknown): { root: string; servers: string[]; acceptPeriodicSync: boolean; replicas: number } =>
    ({ ...openOmitted(), replicas: replicas as number });

  it("omitted replicas: connectJetStream reads the agent-only value at first use and creates 3 when configured", async () => {
    fake.configuredReplicas = 3;
    expect(fake.loadAgent).not.toHaveBeenCalled();
    const port = await connectJetStream(SERVERS);
    expect(fake.loadAgent).toHaveBeenCalledTimes(1);
    expect(fake.loadAgent).toHaveBeenCalledWith(AGENT_DIR);
    expect(fake.loadGlobal).not.toHaveBeenCalled();
    await port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 });
    expect(mutations().map(entry => entry.op)).toEqual(["add"]);
    expect(mutations()[0]!.args[0]).toMatchObject({ name: STREAM, num_replicas: 3 });
  });

  it("omitted replicas: configured 1 refuses an existing 3-replica stream with no mutation", async () => {
    seedStream(STREAM, 3, SUBJECTS);
    const port = await connectJetStream(SERVERS);
    const error = await refusal(port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 }));
    expectReplicasRefusal(error, 1, 3);
    expect(mutations()).toEqual([]);
    expect(fake.streams.get(STREAM)?.config.num_replicas).toBe(3);
  });

  it("explicit replicas 1 wins over configured 3 and never loads the global config", async () => {
    fake.configuredReplicas = 3;
    const port = await connectJetStream(SERVERS, withReplicas(1));
    await port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 });
    expect(mutations()[0]!.args[0]).toMatchObject({ name: STREAM, num_replicas: 1 });
    expect(fake.loadGlobal).not.toHaveBeenCalled();
  });

  it("explicit ensureStream replicas 3 overrides the port's configured 1", async () => {
    const port = await connectJetStream(SERVERS);
    await port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024, replicas: 3 });
    expect(mutations()[0]!.args[0]).toMatchObject({ name: STREAM, num_replicas: 3 });
  });

  it("omitted OpenOptions.replicas: openNatsStateBackend creates the stream with configured 3", async () => {
    fake.configuredReplicas = 3;
    fake.addStop = true;
    await openNatsStateBackend(openOmitted()).catch(() => undefined);
    expect(fake.loadAgent).toHaveBeenCalledWith(AGENT_DIR);
    expect(fake.loadGlobal).not.toHaveBeenCalled();
    const adds = mutations().filter(entry => entry.op === "add");
    expect(adds).toHaveLength(1);
    expect(adds[0]!.args[0]).toMatchObject({ name: natsStreamName(root), num_replicas: 3 });
  });

  it("explicit OpenOptions.replicas 3 is forwarded and wins over configured 1 without loading the global config", async () => {
    fake.addStop = true;
    await openNatsStateBackend(openExplicit(3)).catch(() => undefined);
    const adds = mutations().filter(entry => entry.op === "add");
    expect(adds).toHaveLength(1);
    expect(adds[0]!.args[0]).toMatchObject({ name: natsStreamName(root), num_replicas: 3 });
    expect(fake.loadGlobal).not.toHaveBeenCalled();
  });

  it.each([["zero", 0], ["negative", -1], ["fraction", 1.5], ["string", "3"], ["NaN", Number.NaN]])(
    "explicit invalid replicas (%s) is refused before any connect or config read", async (_label, replicas) => {
      const direct = await refusal(connectJetStream(SERVERS, { replicas: replicas as number }));
      expect(direct).toBeInstanceOf(MeshStateNatsRefusedError);
      const opened = await refusal(openNatsStateBackend(openExplicit(replicas)));
      expect(opened).toBeInstanceOf(MeshStateNatsRefusedError);
      expect(connects()).toBe(0);
      expect(fake.loadAgent).not.toHaveBeenCalled();
      expect(fake.loadGlobal).not.toHaveBeenCalled();
      expect(mutations()).toEqual([]);
    });
});

describe("NATS replicas: real agent-only reader on private fixture", () => {
  const inFleet = FIXTURE_AGENT_DIR !== undefined && /pi-fabric-test-fleet-[^/\\]+[/\\]agent[/\\]?$/.test(FIXTURE_AGENT_DIR);
  const actualReader = async (): Promise<(agentDir: string) => number> => {
    const actualConfig = await vi.importActual<typeof import("../src/config.js")>("../src/config.js");
    const reader = (actualConfig as unknown as { loadAgentNatsReplicas?: (agentDir: string) => number }).loadAgentNatsReplicas;
    expect(reader).toBeTypeOf("function");
    return reader!;
  };
  const readerFixture = (name: string): string => {
    const directory = path.join(FIXTURE_AGENT_DIR!, `reader-${name}`);
    mkdirSync(directory, { recursive: true });
    return directory;
  };

  it.skipIf(!inFleet)("the real reader feeds connectJetStream: creates replicas 3 without policy/global reads, warnings, or writes", async () => {
    const file = path.join(FIXTURE_AGENT_DIR!, "fabric.json");
    const before = existsSync(file) ? readFileSync(file, "utf8") : undefined;
    const document = before === undefined ? {} : JSON.parse(before) as Record<string, unknown>;
    const mesh = (document.mesh ?? {}) as Record<string, unknown>;
    const nats = (mesh.nats ?? {}) as Record<string, unknown>;
    const configured = JSON.stringify({ ...document, mesh: { ...mesh, nats: { ...nats, replicas: 3 } } }, null, 2);
    const reader = await actualReader();
    fake.loadAgent.mockImplementation(reader);
    vi.stubEnv("PI_CODING_AGENT_DIR", FIXTURE_AGENT_DIR!);
    const warnings = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const policy = await import("../src/host-policy.js");
    const policyRead = vi.spyOn(policy, "readHostPolicy");
    try {
      writeFileSync(file, configured);
      const port = await connectJetStream(SERVERS);
      await port.ensureStream(STREAM, SUBJECTS, { maxMsgSize: 1024 * 1024 });
      expect(fake.loadAgent).toHaveBeenCalledTimes(1);
      expect(fake.loadAgent).toHaveBeenCalledWith(FIXTURE_AGENT_DIR);
      expect(mutations().map(entry => entry.op)).toEqual(["add"]);
      expect(mutations()[0]!.args[0]).toMatchObject({ name: STREAM, subjects: SUBJECTS, num_replicas: 3 });
      expect(fake.loadGlobal).not.toHaveBeenCalled();
      expect(policyRead).not.toHaveBeenCalled();
      expect(warnings).not.toHaveBeenCalled();
      expect(readFileSync(file, "utf8")).toBe(configured);
    } finally {
      if (before === undefined) writeFileSync(file, "{}");
      else writeFileSync(file, before);
      policyRead.mockRestore();
      warnings.mockRestore();
    }
  });

  it.skipIf(!inFleet)("defaults a missing agent fabric.json to one replica", async () => {
    const directory = readerFixture("missing-file");
    expect((await actualReader())(directory)).toBe(1);
    expect(existsSync(path.join(directory, "fabric.json"))).toBe(false);
  });

  it.skipIf(!inFleet)("defaults an absent mesh.nats.replicas key to one without rewriting the file", async () => {
    const directory = readerFixture("absent-key");
    const file = path.join(directory, "fabric.json");
    const raw = JSON.stringify({ mesh: { nats: {} } });
    writeFileSync(file, raw);
    expect((await actualReader())(directory)).toBe(1);
    expect(readFileSync(file, "utf8")).toBe(raw);
  });

  it.skipIf(!inFleet)("refuses invalid replicas without rewriting the file", async () => {
    const directory = readerFixture("invalid-replicas");
    const file = path.join(directory, "fabric.json");
    const raw = JSON.stringify({ mesh: { nats: { replicas: 0 } } });
    writeFileSync(file, raw);
    await expect(actualReader().then(reader => reader(directory))).rejects.toThrow(/mesh\.nats\.replicas/);
    expect(readFileSync(file, "utf8")).toBe(raw);
  });

  it.skipIf(!inFleet)("refuses malformed and unreadable config without rewriting or quarantining it", async () => {
    const malformedDir = readerFixture("malformed");
    const malformedFile = path.join(malformedDir, "fabric.json");
    const malformed = "{not-json";
    writeFileSync(malformedFile, malformed);
    await expect(actualReader().then(reader => reader(malformedDir))).rejects.toThrow(/mesh\.nats\.replicas/);
    expect(readFileSync(malformedFile, "utf8")).toBe(malformed);
    expect(existsSync(`${malformedFile}.corrupt`)).toBe(false);

    const unreadableDir = readerFixture("unreadable");
    const unreadableFile = path.join(unreadableDir, "fabric.json");
    mkdirSync(unreadableFile);
    await expect(actualReader().then(reader => reader(unreadableDir))).rejects.toThrow(/mesh\.nats\.replicas/);
    expect(existsSync(unreadableFile)).toBe(true);
    expect(existsSync(`${unreadableFile}.corrupt`)).toBe(false);
  });

  it.skipIf(!inFleet)("keeps malformed parser details out of direct and NATS entry-point refusals", async () => {
    const directory = readerFixture("malformed-canary");
    const file = path.join(directory, "fabric.json");
    const root = path.join(directory, "mesh-root");
    const canary = "__FABRIC_JSON_PARSER_CANARY__";
    const malformed = `{"mesh":{"nats":{"replicas":3}},"broken":"${canary}`;
    writeFileSync(file, malformed);

    const reader = await actualReader();
    fake.loadAgent.mockImplementation(reader);
    vi.stubEnv("PI_CODING_AGENT_DIR", directory);
    const originalParse = JSON.parse;
    const parseSpy = vi.spyOn(JSON, "parse").mockImplementation((text, reviver) => {
      if (text === malformed) throw new SyntaxError(`synthetic JSON parser excerpt: ${canary}`);
      return originalParse(text, reviver);
    });
    const assertSafeRefusal = async (receiver: string, promise: Promise<unknown>) => {
      const error = await refusal(promise);
      expect(error, receiver).toBeInstanceOf(Error);
      expect((error as Error).message, receiver).toBe(`mesh.nats.replicas: ${file} is not valid JSON`);
      expect.soft((error as Error).cause, receiver).toBeUndefined();
      const inspections = [
        inspect(error, { showHidden: true, depth: null }),
        inspect((error as Error).message),
        inspect((error as Error).cause),
      ].join("\n");
      expect.soft(inspections, receiver).not.toContain(canary);
      expect(readFileSync(file, "utf8"), receiver).toBe(malformed);
      expect(existsSync(`${file}.corrupt`), receiver).toBe(false);
    };

    try {
      expect(JSON.parse('{"delegated":true}')).toEqual({ delegated: true });
      await assertSafeRefusal("loadAgentNatsReplicas", Promise.resolve().then(() => reader(directory)));
      await assertSafeRefusal("connectJetStream", connectJetStream(SERVERS));
      await assertSafeRefusal("connectNatsEndpoint", connectNatsEndpoint({ servers: [...SERVERS] }));
      await assertSafeRefusal("openNatsStateBackend", openNatsStateBackend({
        root, servers: [...SERVERS], acceptPeriodicSync: true,
      }));
      expect(parseSpy.mock.calls.filter(([text]) => text === malformed)).toHaveLength(4);
      expect(fake.loadAgent).toHaveBeenCalledTimes(3);
      expect(fake.loadGlobal).not.toHaveBeenCalled();
      expect(connects()).toBe(0);
      expect(mutations()).toEqual([]);
      expect(fake.log).toEqual([]);
    } finally {
      parseSpy.mockRestore();
    }
  });
});

describe("config mesh.nats.replicas (pure)", () => {
  // Read structurally: the old FabricMeshConfig has no `nats`, so the old code fails on the value, not on types.
  const replicasOf = (mesh: Record<string, unknown> | undefined): unknown =>
    (normalizeFabricConfig(mesh === undefined ? {} : { mesh }).mesh as unknown as { nats?: { replicas?: unknown } }).nats?.replicas;

  it("defaults to 1", () => {
    expect(replicasOf(undefined)).toBe(1);
    expect(replicasOf({ nats: {} })).toBe(1);
  });

  it("accepts 3", () => {
    expect(replicasOf({ nats: { replicas: 3 } })).toBe(3);
  });

  it.each([
    ["zero", 0], ["negative", -1], ["fraction", 1.5], ["string", "3"], ["infinity", Number.POSITIVE_INFINITY], ["NaN", Number.NaN],
  ])("refuses %s", (_label, replicas) => {
    expect(() => replicasOf({ nats: { replicas } })).toThrow(/mesh\.nats\.replicas/);
  });
});

describe("NATS_STATE_PROFILE harness helper (private fixture config only, no server)", () => {
  // scripts/test-temp.ts gives this file a fresh private agent dir; the helper writes only there. Outside that
  // harness the helper (correctly) refuses, so these cases skip rather than touch a real profile.
  const fixtureDir = FIXTURE_AGENT_DIR;
  const inFleet = fixtureDir !== undefined && /pi-fabric-test-fleet-[^/\\]+[/\\]agent[/\\]?$/.test(fixtureDir);
  const fabricJson = () => path.join(fixtureDir!, "fabric.json");
  const readRaw = (): string | undefined => existsSync(fabricJson()) ? readFileSync(fabricJson(), "utf8") : undefined;
  const readConfig = (): Record<string, unknown> | undefined => {
    const raw = readRaw();
    return raw === undefined ? undefined : JSON.parse(raw) as Record<string, unknown>;
  };
  const importHelper = async (profile: string, agentDir = fixtureDir!) => {
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    vi.stubEnv("NATS_STATE_PROFILE", profile);
    vi.stubEnv("NATS_SERVER_REQUIRED", "");
    vi.resetModules();
    return import("./helpers/nats-state-server.js");
  };

  it.skipIf(!inFleet).each([["cluster", 3], ["single", 1]] as const)(
    "%s exports its profile and writes mesh.nats.replicas %i into the private fabric.json, preserving the rest",
    async (profile, replicas) => {
      const before = readConfig() ?? {};
      const helper = await importHelper(profile);
      expect(helper.natsStateProfile).toBe(profile);
      expect(helper.natsStateReplicas).toBe(replicas);
      const beforeMesh = (before.mesh ?? {}) as Record<string, unknown>;
      expect(readConfig()).toEqual({
        ...before, mesh: { ...beforeMesh, nats: { ...(beforeMesh.nats as Record<string, unknown> | undefined), replicas } },
      });
      expect(normalizeFabricConfig(readConfig()!).mesh.nats.replicas).toBe(replicas);
    });

  it.skipIf(!inFleet)("an unknown profile is refused at import with no config write", async () => {
    const before = readRaw();
    await expect(importHelper("triple")).rejects.toThrow(/NATS_STATE_PROFILE/);
    expect(readRaw()).toBe(before);
  });

  it.skipIf(!inFleet)("refuses the old unmarked fleet location directly under process TMPDIR", async () => {
    const processTemp = process.env.TMPDIR;
    if (!processTemp) throw new Error("TMPDIR is required for the private fixture");
    const fleet = mkdtempSync(path.join(processTemp, "pi-fabric-test-fleet-"));
    const agent = path.join(fleet, "agent");
    mkdirSync(agent);
    try {
      await expect(importHelper("single", agent)).rejects.toThrow(/refusing to write config outside the private test fleet/);
      expect(existsSync(path.join(agent, "fabric.json"))).toBe(false);
    } finally { rmSync(fleet, { recursive: true, force: true }); }
  });
});
