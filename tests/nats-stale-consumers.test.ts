import { randomUUID } from "node:crypto";
import { AckPolicy, DeliverPolicy, JetStreamApiError, type ConsumerInfo, type JetStreamClient, type JetStreamManager } from "@nats-io/jetstream";
import { Kvm } from "@nats-io/kv";
import { headers, type NatsConnection } from "@nats-io/transport-node";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import { parseStaleConsumerAge, reportStaleConsumers, staleConsumerActivity } from "../src/transport/nats/stale-consumers.js";
import { hashToken, transportDurableName, transportInboxFilter, transportInboxStream, transportInboxSubject } from "../src/transport/nats/subjects.js";
import { createPathBHarness, openJetStream, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";

// Keep reasons: data safety (#16203), real broker counter preservation, and the bootstrap trust boundary.
// No broker mocks. Only fixture setup provisions/publishes/pulls; the report is observed on the real wire.
// This suite calls runTransportAdmin, the route used by bin/fabric-transport-admin. It does NOT claim to
// exercise the compiled executable. The parent runs that exact bin against dist after its build.
type Report = Awaited<ReturnType<typeof reportStaleConsumers>>;
const activity = (created: string, delivered?: unknown, ackFloor?: unknown) => ({
  created, delivered: { last_active: delivered }, ack_floor: { last_active: ackFloor },
}) as unknown as Pick<ConsumerInfo, "created" | "delivered" | "ack_floor">;

describe("stale-consumers cheap input/activity failures", () => {
  it.each(["0h", "-1h", "+1h", "1.5h", "24", "1w", "01h", "1H", "Infinityh", "NaNh", "9007199254740992ms", "999999999999999999d", " 24h", "1h\n", ""])("rejects invalid --older-than %j", value => {
    expect(() => parseStaleConsumerAge(value)).toThrow("--older-than");
  });
  it("defaults to 24 hours and supports explicit positive integer units", () => {
    expect(parseStaleConsumerAge()).toBe(86_400_000);
    for (const [text, ms] of [["1ms", 1], ["2s", 2_000], ["3m", 180_000], ["24h", 86_400_000], ["7d", 604_800_000]] as const) {
      expect(parseStaleConsumerAge(text)).toBe(ms);
    }
  });
  it("uses the latest delivered/ACK-floor RFC3339 timestamp, not creation or request telemetry", () => {
    const created = "2026-01-01T00:00:00Z", delivered = "2026-01-02T00:00:00.123456789Z", ack = "2026-01-03T00:00:00Z";
    expect(staleConsumerActivity(activity(created))).toEqual({ timestamp: created, ms: Date.parse(created), source: "created" });
    expect(staleConsumerActivity(activity(created, delivered))).toMatchObject({ timestamp: delivered, source: "delivered" });
    expect(staleConsumerActivity(activity(created, delivered, ack))).toMatchObject({ timestamp: ack, source: "ack_floor" });
    expect(staleConsumerActivity(activity(created, ack, delivered))).toMatchObject({ timestamp: ack, source: "delivered" });
  });
  it("does not fall back to creation when an activity timestamp is malformed", () => {
    for (const invalid of [42, "", "yesterday", "2026-13-01T00:00:00Z", "2026-02-31T00:00:00Z", "2026-01-01T24:00:00Z", {}, "2026"]) {
      expect(staleConsumerActivity(activity("2026-01-01T00:00:00Z", invalid))).toBeUndefined();
    }
    expect(staleConsumerActivity(activity("not-a-date"))).toBeUndefined();
  });
  it.each([
    new JetStreamApiError({ code: 403, err_code: 10039, description: "permission denied" }),
    new JetStreamApiError({ code: 503, err_code: 10008, description: "server unavailable" }),
    new JetStreamApiError({ code: 404, err_code: 10059, description: "stream disappeared" }),
    new Error("request timed out"),
  ])("does not mistake permission/server/request failures for end-of-filter: %s", async failure => {
    // Cheap failure-path probe only. All successful report paths below use the real TLS broker.
    const manager = {
      consumers: { list: () => ({ async *[Symbol.asyncIterator]() {} }) },
      streams: {
        async info() { return { created: "2026-01-01T00:00:00Z", state: { first_seq: 1, last_seq: 1 },
          config: { max_age: 600_000_000_000, max_msgs_per_subject: 1, subjects: ["$KV.fabric_presence.>"] } }; },
        async getMessage() { throw failure; },
      },
    } as unknown as Parameters<typeof reportStaleConsumers>[0];
    await expect(reportStaleConsumers(manager, "machine-a")).rejects.toBe(failure);
  });
  it.each([
    [], ["--machine"], ["--machine", "machine-a", "--older-than"],
    ["--machine", "machine-a", "--machine", "machine-b"],
    ["--machine", "machine-a", "--older-than", "1h", "--older-than", "2h"],
    ["--machine", "machine-a", "--older-than", "0h"],
    ["--machine", "machine-a", "--delete"], ["--machine", "machine.*"], ["--machine", "machine-a\n"],
    ["--machine", "machine-a", "--ca-file", "relative"],
  ])("refuses invalid CLI arguments before a connection: %j", async (...args: string[]) => {
    // Missing credentials are a cheap failure sentinel: argument validation must win before dialing.
    vi.stubEnv("PI_FABRIC_NATS_SERVERS", "tls://localhost:1");
    vi.stubEnv("PI_FABRIC_NATS_CREDENTIALS", "not-a-credential-file");
    try { await expect(runTransportAdmin(["stale-consumers", ...args])).rejects.not.toThrow(/ENOENT|connection failed/); }
    finally { vi.unstubAllEnvs(); }
  });
});

const binary = resolveNatsBinary() ?? "";
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");

describe.skipIf(!binary || process.platform === "win32")("stale-consumers on the real TLS Path B bootstrap admin path", { concurrent: false, shuffle: false }, () => {
  let harness: PathBHarness;
  let seed: { manager: JetStreamManager; js: JetStreamClient };
  let seedNc: NatsConnection;
  const machine = "machine-a";
  const stream = transportInboxStream(machine);
  const rowFor = (report: Report, host: string) => report.consumers.find(row => row.fabricHostId === host);

  beforeEach(async () => {
    harness = await createPathBHarness({ binary, tls: { dnsNames: ["localhost"], ipAddresses: [] } });
    seedNc = await harness.connect(harness.users.bootstrap);
    seed = await openJetStream(seedNc);
    for (const target of [machine, "machine-b"]) {
      await seed.manager.streams.add({ name: transportInboxStream(target), subjects: [`fabric.in.${target}.>`], allow_direct: false });
    }
    await new Kvm(seedNc).create("fabric_presence", { history: 1, ttl: 600_000, allow_direct: false });
    await new Kvm(seedNc).create("fabric_leases", { history: 1, ttl: 15_000, allow_direct: false });
    for (const [key, value] of Object.entries(harness.adminEnv(harness.users.bootstrap))) vi.stubEnv(key, value);
  }, 60_000);

  afterEach(async () => {
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
    await harness?.stop();
  });

  async function consumer(host = `host:${randomUUID()}`, options: { target?: string; description?: string; filter?: string; durable?: string } = {}) {
    const target = options.target ?? machine;
    const name = options.durable ?? transportDurableName(host);
    await seed.manager.consumers.add(transportInboxStream(target), {
      durable_name: name, ack_policy: AckPolicy.Explicit, deliver_policy: DeliverPolicy.All,
      filter_subject: options.filter ?? transportInboxFilter(target, host),
      description: options.description ?? JSON.stringify({ fabricHostId: host, machine: target }),
      ack_wait: 120_000_000_000, inactive_threshold: 0,
    });
    return host;
  }

  function participant(host: string, incarnation = "incarnation-1") {
    const id = `participant:${host}`;
    return { format: 1, id, kind: "root", rootId: id, ownerHostId: host, ownerIdentityId: id,
      ownerIncarnation: incarnation, name: "fixture", status: "running", controlProtocol: "v1", transport: "host",
      runner: "pi", capabilities: ["steer"], startedAt: Date.now(), updatedAt: Date.now() };
  }

  async function putPresence(host: string, options: { incarnation?: string; owner?: string; status?: string; target?: string; keyId?: string } = {}) {
    const value = { ...participant(host, options.incarnation), ...(options.status ? { status: options.status } : {}) };
    await seed.js.publish(`$KV.fabric_presence.${options.target ?? machine}.p.${hashToken(options.keyId ?? value.id)}`,
      JSON.stringify({ owner: options.owner ?? `${host}#${value.ownerIncarnation}`, value }));
    return value;
  }

  async function putLease(host: string, options: { incarnation?: string; expiresAt?: number; target?: string; identityId?: string; owner?: string; keyId?: string } = {}) {
    const record = participant(host, options.incarnation);
    const value = { id: host, rootId: record.rootId, identityId: options.identityId ?? record.ownerIdentityId,
      incarnation: record.ownerIncarnation, pid: 123, pidStartTime: "123", bootId: "fixture-boot",
      updatedAt: Date.now() - 1_000, expiresAt: options.expiresAt ?? Date.now() + 10_000 };
    await seed.js.publish(`$KV.fabric_leases.${options.target ?? machine}.h.${hashToken(options.keyId ?? host)}`,
      JSON.stringify({ owner: options.owner ?? `${host}#${value.incarnation}`, value }));
  }

  async function cli(extra: readonly string[] = ["--older-than", "1ms"]): Promise<Report> {
    let output = "";
    const stdout = vi.spyOn(process.stdout, "write").mockImplementation(chunk => {
      output += typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8");
      return true;
    });
    try { expect(await runTransportAdmin(["stale-consumers", "--machine", machine, "--ca-file", harness.tls!.caFile, ...extra])).toBe(0); }
    finally { stdout.mockRestore(); }
    expect(output.trim().split("\n")).toHaveLength(1);
    return JSON.parse(output) as Report;
  }

  async function retainedState() {
    const streams = [];
    for await (const info of seed.manager.streams.list()) {
      const consumers = [];
      for await (const item of seed.manager.consumers.list(info.config.name)) {
        consumers.push({ name: item.name, created: item.created, config: item.config, delivered: item.delivered,
          ack_floor: item.ack_floor, num_pending: item.num_pending, num_ack_pending: item.num_ack_pending,
          num_redelivered: item.num_redelivered, num_waiting: item.num_waiting });
      }
      streams.push({ name: info.config.name, created: info.created, config: info.config,
        state: info.state, consumers: consumers.sort((a, b) => a.name.localeCompare(b.name)) });
    }
    return streams.sort((a, b) => a.name.localeCompare(b.name));
  }

  it("prints aged rows and candidacy; preserves every consumer/config/counter/floor and uses read-only wire APIs", async () => {
    const absent = await consumer();
    const pending = await consumer();
    const ackPending = await consumer();
    const live = await consumer();
    await putPresence(live);
    await putLease(live);
    for (const host of [pending, ackPending]) {
      await seed.js.publish(transportInboxSubject(machine, host, "cmd", "fixture-target", "machine-b"), "PRIVATE-INBOX-MUST-NOT-BE-READ");
    }
    // Fixture setup only: establish the ACK-pending counter before observing the report's wire traffic.
    const fixtureConsumer = await seed.js.consumers.get(stream, transportDurableName(ackPending));
    expect(await fixtureConsumer.next({ expires: 2_500 })).not.toBeNull();
    await consumer("not-a-host", { durable: "some_other_consumer" });
    await consumer("other-machine", { target: "machine-b" });
    const before = await retainedState();

    // Spies forward the real native implementation; there is no mocked connection, manager or broker.
    const prototype = Object.getPrototypeOf(seedNc) as NatsConnection;
    const requests = vi.spyOn(prototype, "request");
    const publishes = vi.spyOn(prototype, "publish");
    const report = await cli();
    const allowed = new Set([
      "$JS.API.INFO", `$JS.API.CONSUMER.LIST.${stream}`,
      "$JS.API.STREAM.INFO.KV_fabric_presence", "$JS.API.STREAM.INFO.KV_fabric_leases",
      "$JS.API.STREAM.MSG.GET.KV_fabric_presence", "$JS.API.STREAM.MSG.GET.KV_fabric_leases",
    ]);
    expect(requests.mock.calls.length).toBeGreaterThan(5);
    for (const [subject] of [...requests.mock.calls, ...publishes.mock.calls]) expect(allowed.has(subject), subject).toBe(true);
    const gets = requests.mock.calls.filter(([subject]) => subject.includes("STREAM.MSG.GET"));
    expect(gets).toHaveLength(2);
    for (const [subject, payload] of gets) {
      const query = JSON.parse(new TextDecoder().decode(payload as Uint8Array));
      expect(query).toEqual({ seq: 1, next_by_subj: subject.endsWith("presence") ? "$KV.fabric_presence.machine-a.p.>" : "$KV.fabric_leases.machine-a.h.>" });
    }
    requests.mockRestore();
    publishes.mockRestore();

    expect(report).toMatchObject({ ok: true, mode: "report-only", disposition: "none", machine, stream,
      older_than_ms: 1, selection: "all-aged-validated-per-host-durables", eligible_count: 1, ignored_consumers: 1, diagnostics: [] });
    expect(report.consumers).toHaveLength(4);
    expect(rowFor(report, absent)).toMatchObject({ durable_name: transportDurableName(absent), fabricHostId: absent,
      last_active_source: "created", num_pending: 0, num_ack_pending: 0,
      presence: { row_exists: false, live: false, host_lease: "not-observed" }, eligible: true, reasons: [] });
    expect(rowFor(report, pending)).toMatchObject({ num_pending: 1, num_ack_pending: 0, eligible: false, reasons: ["pending"] });
    expect(rowFor(report, ackPending)).toMatchObject({ last_active_source: "delivered", num_pending: 0, num_ack_pending: 1,
      eligible: false, reasons: ["ack-pending"] });
    expect(rowFor(report, live)).toMatchObject({ presence: { row_exists: true, live: true, host_lease: "live" }, eligible: false });
    expect(report.last_active_semantics).toContain("not pull/request telemetry");
    expect(report.limitation).toContain("not proof of host death");
    expect(JSON.stringify(report)).not.toContain("PRIVATE-INBOX-MUST-NOT-BE-READ");
    expect(await retainedState()).toEqual(before);
  });

  it("distinguishes presence existence from matching live leases, including expiry, incarnation and machine", async () => {
    const presenceOnly = await consumer(), expiredLease = await consumer(), wrongIncarnation = await consumer();
    const wrongIdentity = await consumer(), liveLeaseOnly = await consumer(), foreignMachine = await consumer();
    const terminal = await consumer();
    for (const host of [presenceOnly, expiredLease, wrongIncarnation, wrongIdentity]) await putPresence(host);
    await putLease(expiredLease, { expiresAt: Date.now() - 1 });
    await putLease(wrongIncarnation, { incarnation: "successor-incarnation" });
    await putLease(wrongIdentity, { identityId: "someone-else" });
    await putLease(liveLeaseOnly);
    await putPresence(foreignMachine, { target: "machine-b" });
    await putLease(foreignMachine, { target: "machine-b" });
    await putPresence(terminal, { status: "completed" });
    const report = await cli();
    for (const host of [presenceOnly, expiredLease, wrongIncarnation, wrongIdentity]) {
      expect(rowFor(report, host)).toMatchObject({ presence: { row_exists: true, live: null }, eligible: false });
    }
    expect(rowFor(report, expiredLease)?.presence.host_lease).toBe("expired");
    expect(rowFor(report, liveLeaseOnly)).toMatchObject({ presence: { row_exists: false, live: false, host_lease: "live" }, eligible: false });
    expect(rowFor(report, foreignMachine)).toMatchObject({ presence: { row_exists: false, live: false }, eligible: true });
    expect(rowFor(report, terminal)).toMatchObject({ presence: { row_exists: true, live: false, nonterminal_rows: 0 }, eligible: true });
  });

  it("validates descriptions against the full durable/filter/machine and retains exclusion diagnostics", async () => {
    const good = await consumer();
    const invalid: string[] = [];
    for (const description of ["not-json", JSON.stringify({ hostId: "old-field", machine }), JSON.stringify({ fabricHostId: "different-host", machine }),
      JSON.stringify({ fabricHostId: "wrong-machine", machine: "machine-b" })]) {
      const host = await consumer(undefined, { description });
      invalid.push(transportDurableName(host));
    }
    const broad = await consumer(undefined, { filter: `fabric.in.${machine}.>` });
    invalid.push(transportDurableName(broad));
    const report = await cli();
    expect(report.consumers.map(row => row.fabricHostId)).toEqual([good]);
    expect(report.diagnostics.map(item => item.consumer).sort()).toEqual(invalid.sort());
    expect(report.diagnostics.every(item => item.reason === "invalid-host-description-durable-or-filter")).toBe(true);
  });

  it.each(["bad-json", "owner-mismatch", "key-mismatch", "lease-owner-mismatch", "lease-key-mismatch"])("makes malformed %s evidence unknown, never proof of absence", async fault => {
    const host = await consumer(), unrelated = await consumer();
    if (fault === "bad-json") await seed.js.publish(`$KV.fabric_presence.${machine}.p.${hashToken("malformed")}`, "{");
    if (fault === "owner-mismatch") await putPresence(host, { owner: "someone-else#incarnation-1" });
    if (fault === "key-mismatch") await putPresence(host, { keyId: "not-this-participant" });
    if (fault === "lease-owner-mismatch") await putLease(host, { owner: "someone-else#incarnation-1" });
    if (fault === "lease-key-mismatch") await putLease(host, { keyId: "not-this-host" });
    const report = await cli();
    for (const id of [host, unrelated]) expect(rowFor(report, id)).toMatchObject({ presence: { live: null }, eligible: false });
    expect([...report.snapshots.presence.diagnostics, ...report.snapshots.leases.diagnostics]).toHaveLength(1);
    expect(report.eligible_count).toBe(0);
  });

  it("uses the older-than boundary and creation fallback, while default 24h omits new consumers", async () => {
    const host = await consumer();
    expect(rowFor(await cli([]), host)).toBeUndefined();
    const info = await seed.manager.consumers.info(stream, transportDurableName(host));
    const created = Date.parse(info.created);
    expect(rowFor(await reportStaleConsumers(seed.manager, machine, 3_600_000, created + 3_600_000 - 1), host)).toBeUndefined();
    const report = await reportStaleConsumers(seed.manager, machine, 3_600_000, created + 3_600_000);
    expect(rowFor(report, host)).toMatchObject({ age_ms: 3_600_000, last_active: info.created, last_active_source: "created", eligible: true });
    expect(rowFor(await reportStaleConsumers(seed.manager, machine, 1, created - 1), host)).toBeUndefined();
  });

  it("completes absence when both KV streams contain ONLY other-machine rows, including overwritten sequence gaps", async () => {
    const host = await consumer();
    const foreign = "foreign-host";
    for (let index = 0; index < 2; index++) {
      await putPresence(foreign, { target: "machine-b" });
      await putLease(foreign, { target: "machine-b" });
    }
    expect((await seed.manager.streams.info("KV_fabric_presence")).state.first_seq).toBe(2);
    expect((await seed.manager.streams.info("KV_fabric_leases")).state.first_seq).toBe(2);
    const before = await retainedState();
    const report = await cli();
    expect(report.snapshots.presence).toMatchObject({ last_seq: 2, complete: true, diagnostics: [] });
    expect(report.snapshots.leases).toMatchObject({ last_seq: 2, complete: true, diagnostics: [] });
    expect(rowFor(report, host)).toMatchObject({ presence: { row_exists: false, live: false, host_lease: "not-observed" }, eligible: true });
    expect(await retainedState()).toEqual(before);
  });

  it("skips raw tombstones and treats a trailing nonmatching subject as end-of-filter, not a read failure", async () => {
    const host = await consumer();
    const tombstone = headers();
    tombstone.set("KV-Operation", "DEL");
    // Append tombstones for never-existing fixture keys; no DELETE/PURGE API or live value is touched.
    await seed.js.publish(`$KV.fabric_presence.${machine}.p.${hashToken("no-participant")}`, "", { headers: tombstone });
    tombstone.set("KV-Operation", "PURGE");
    await seed.js.publish(`$KV.fabric_leases.${machine}.h.${hashToken("no-host")}`, "", { headers: tombstone });
    await putPresence("foreign-host", { target: "machine-b" });
    await putLease("foreign-host", { target: "machine-b" });
    const before = await retainedState();
    const report = await cli();
    expect(report.snapshots.presence).toMatchObject({ last_seq: 2, complete: true, diagnostics: [] });
    expect(report.snapshots.leases).toMatchObject({ last_seq: 2, complete: true, diagnostics: [] });
    expect(rowFor(report, host)).toMatchObject({ presence: { row_exists: false, live: false }, eligible: true });
    expect(await retainedState()).toEqual(before);
  });

  it("does not provision missing presence/lease streams or claim their absence is known", async () => {
    const missing = await createPathBHarness({ binary, tls: { dnsNames: ["localhost"], ipAddresses: [] } });
    try {
      const raw = await openJetStream(await missing.connect(missing.users.bootstrap));
      await raw.manager.streams.add({ name: stream, subjects: [`fabric.in.${machine}.>`] });
      const host = "host-without-presence-infrastructure";
      const info = await raw.manager.consumers.add(stream, { durable_name: transportDurableName(host),
        ack_policy: AckPolicy.Explicit, filter_subject: transportInboxFilter(machine, host),
        description: JSON.stringify({ fabricHostId: host, machine }) });
      const before = await raw.manager.streams.list().next();
      const report = await reportStaleConsumers(raw.manager, machine, 1, Date.parse(info.created) + 10);
      expect(rowFor(report, host)).toMatchObject({ presence: { row_exists: null, live: null, host_lease: "unknown" }, eligible: false });
      for (const snapshot of Object.values(report.snapshots)) {
        expect(snapshot).toMatchObject({ last_seq: null, complete: false, diagnostics: [{ reason: "stream-not-found; absence is unknown" }] });
      }
      expect((await raw.manager.streams.list().next()).map(item => item.config)).toEqual(before.map(item => item.config));
    } finally { await missing.stop(); }
  });

  it("captures the raw upper sequence once and makes concurrent writes unknown instead of chasing the tail", async () => {
    const host = await consumer();
    await putPresence("unrelated-existing-host");
    let appended = false;
    const queries: number[] = [];
    // Event injection around real API calls, not a broker mock: append just after INFO captured last_seq=1.
    const observed: Parameters<typeof reportStaleConsumers>[0] = {
      consumers: { list: seed.manager.consumers.list.bind(seed.manager.consumers) },
      streams: {
        info: seed.manager.streams.info.bind(seed.manager.streams),
        async getMessage(name, query) {
          if (name === "KV_fabric_presence") {
            queries.push((query as { seq: number }).seq);
            if (!appended) { appended = true; await putPresence(host); }
          }
          return seed.manager.streams.getMessage(name, query);
        },
      },
    };
    const report = await reportStaleConsumers(observed, machine, 1);
    expect(appended).toBe(true);
    expect(queries).toEqual([1]);
    expect(report.snapshots.presence).toMatchObject({ last_seq: 1, complete: false });
    expect(rowFor(report, host)).toMatchObject({ presence: { live: null }, eligible: false });
  });

  it("honors broker row TTL independently of lease expiresAt, without waiting for broker expiry", async () => {
    const host = await consumer();
    await putPresence(host);
    await putLease(host, { expiresAt: Date.now() + 24 * 3_600_000 });
    // Explicit observation clock tests TTL logic on retained real rows without sleeps or stream mutation.
    const report = await reportStaleConsumers(seed.manager, machine, 1, Date.now() + 601_000);
    expect(rowFor(report, host)).toMatchObject({ presence: { row_exists: false, live: false, host_lease: "not-observed" }, eligible: true });
    expect(report.snapshots.presence.complete).toBe(true);
    expect(report.snapshots.leases.complete).toBe(true);
  });
});
