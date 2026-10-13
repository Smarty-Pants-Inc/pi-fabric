import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { inspect } from "node:util";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Stream replicas: omitted values come only from the agent-dir fabric.json reader (default 1); explicit values still
// win. The real-reader cases use private fixtures and check the agent reader/client path without global config/policy
// loads, warnings, or config rewrites. Both @nats-io modules are pure fakes: no server/network.
// Options are passed through wide variables so assertions remain behavioral across the old option types.
const fake = vi.hoisted(() => {
  class FakeJetStreamApiError extends Error {
    constructor(readonly errCode: number) { super(`fake JetStream API error ${errCode}`); this.name = "JetStreamApiError"; }
    apiError() { return { code: 404, err_code: this.errCode, description: "stream not found" }; }
  }
  type StreamInfo = { config: Record<string, unknown> & { name: string; subjects?: string[] }; state: { last_seq: number; first_seq: number }; cluster?: { leader?: string } };
  return {
    FakeJetStreamApiError,
    streams: new Map<string, StreamInfo>(),
    log: [] as { op: string; args: unknown[] }[],
    addStop: false,
    createdWithoutLeader: false,
    leaderPolls: 0,
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
  connect: async () => (fake.log.push({ op: "connect", args: [] }), { info: { version: "2.14.7", auth_required: false, max_payload: 1024 * 1024 }, drain: async () => undefined }),
}));
vi.mock("@nats-io/jetstream", () => ({
  DeliverPolicy: { All: "all", StartSequence: "by_start_sequence" },
  JetStreamApiError: fake.FakeJetStreamApiError,
  StorageType: { File: "file" },
  jetstream: () => ({
    publish: async (...args: unknown[]) => { fake.log.push({ op: "publish", args }); throw new Error("fake: publish is not expected"); },
    consumers: { get: async (...args: unknown[]) => { fake.log.push({ op: "consumer", args }); throw new Error("fake: no consumers"); } },
  }),
  jetstreamManager: async () => ({
    streams: {
      info: async (name: string) => {
        fake.log.push({ op: "info", args: [name] });
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
        const created = { config: { ...config }, state: { last_seq: 0, first_seq: 0 },
          cluster: fake.createdWithoutLeader ? {} : { leader: "n1" } };
        fake.streams.set(config.name, created);
        return structuredClone(created);
      },
      update: async (name: string, config: Record<string, unknown>) => {
        fake.log.push({ op: "update", args: [name, structuredClone(config)] });
        const stream = fake.streams.get(name);
        if (!stream) throw new fake.FakeJetStreamApiError(10059);
        stream.config = { ...stream.config, ...config } as typeof stream.config;
        return structuredClone(stream);
      },
      getMessage: async (...args: unknown[]) => { fake.log.push({ op: "getMessage", args }); return undefined; },
      purge: async (...args: unknown[]) => { fake.log.push({ op: "purge", args }); throw new Error("fake: purge is not expected"); },
    },
  }),
}));

const { connectJetStream } = await import("../src/mesh/state-backend-nats-client.js");
const { MeshStateNatsBusyError, MeshStateNatsRefusedError, connectNatsEndpoint, natsStreamName, openNatsStateBackend } = await import("../src/mesh/state-backend-nats.js");
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
  fake.addStop = false;
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

describe("NATS stream replicas: real client ensureStream (pure fakes)", () => {
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
  const importHelper = async (profile: string) => {
    vi.stubEnv("PI_CODING_AGENT_DIR", fixtureDir!);
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
});
