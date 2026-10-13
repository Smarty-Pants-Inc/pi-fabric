import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Stream replicas (proposed contract): ConnectJetStreamOptions.replicas (default 1) is the stream's num_replicas on
// create; an existing stream whose num_replicas differs is refused with MeshStateNatsRefusedError
// (FABRIC_MESH_STATE_NATS_REFUSED, configured/observed replicas named) BEFORE any update, add or publish, and a stream
// is never lowered (3 -> 1). Both @nats-io modules are pure fakes: the real connectJetStream/ensureStream path runs,
// with no server, network or filesystem write. Options are passed through wide variables so the file compiles against
// the old option types and the old code fails on BEHAVIOR, not on types.
const fake = vi.hoisted(() => {
  class FakeJetStreamApiError extends Error {
    constructor(readonly errCode: number) { super(`fake JetStream API error ${errCode}`); this.name = "JetStreamApiError"; }
    apiError() { return { code: 404, err_code: this.errCode, description: "stream not found" }; }
  }
  type StreamInfo = { config: Record<string, unknown> & { name: string; subjects?: string[] }; state: { last_seq: number; first_seq: number } };
  return {
    FakeJetStreamApiError,
    streams: new Map<string, StreamInfo>(),
    log: [] as { op: string; args: unknown[] }[],
    addStop: false,
    /** mesh.nats.replicas the fake global config holds; reset to 1 before every case. */
    configuredReplicas: 1 as unknown,
    /** Stands in for config.loadGlobalFabricConfig: no real config file, migration or cleanup path runs. */
    loadGlobal: vi.fn<(agentDir: string) => unknown>(),
  };
});

// Only the global loader is faked; normalizeFabricConfig (and every other export) stays the real one.
vi.mock("../src/config.js", async importOriginal => ({
  ...(await importOriginal<typeof import("../src/config.js")>()),
  loadGlobalFabricConfig: fake.loadGlobal,
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
        return structuredClone(stream);
      },
      add: async (config: Record<string, unknown> & { name: string }) => {
        fake.log.push({ op: "add", args: [structuredClone(config)] });
        if (fake.addStop) throw new Error("fake: add recorded, stop");
        const created = { config: { ...config }, state: { last_seq: 0, first_seq: 0 } };
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
const { MeshStateNatsRefusedError, natsStreamName, openNatsStateBackend } = await import("../src/mesh/state-backend-nats.js");
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
  fake.configuredReplicas = 1;
  fake.loadGlobal.mockReset();
  fake.loadGlobal.mockImplementation(() => normalizeFabricConfig({ mesh: { nats: { replicas: fake.configuredReplicas } } }));
  // The agent dir the client resolves: a nonexistent path, only ever handed to the fake loader.
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

describe("NATS stream replicas: global config mesh.nats.replicas wiring (fake loader, pure fakes)", () => {
  const root = "/nonexistent-fabric-replicas-test/root";
  const openOmitted = (): { root: string; servers: string[]; acceptPeriodicSync: boolean } =>
    ({ root, servers: [...SERVERS], acceptPeriodicSync: true });
  const openExplicit = (replicas: unknown): { root: string; servers: string[]; acceptPeriodicSync: boolean; replicas: number } =>
    ({ ...openOmitted(), replicas: replicas as number });

  it("omitted replicas: connectJetStream reads the global config at first use and creates 3 when 3 are configured", async () => {
    fake.configuredReplicas = 3;
    expect(fake.loadGlobal).not.toHaveBeenCalled();
    const port = await connectJetStream(SERVERS);
    expect(fake.loadGlobal).toHaveBeenCalledTimes(1);
    expect(fake.loadGlobal).toHaveBeenCalledWith(AGENT_DIR);
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
    expect(fake.loadGlobal).toHaveBeenCalledWith(AGENT_DIR);
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
      expect(fake.loadGlobal).not.toHaveBeenCalled();
      expect(mutations()).toEqual([]);
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
