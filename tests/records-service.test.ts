import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import pg from "pg";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { RemoteRecords } from "../src/records/client.js";
import { recordsInboxMessage, recordsInboxSession, RecordsInbox } from "../src/records/inbox.js";
import { PublicationRelay, type NudgePublisher } from "../src/records/relay.js";
import { migrate, SERVICE_ROLE } from "../src/records/schema.js";
import { issueCredentialFile, issuePrincipal, normalizeServiceConfig, RecordsServer, writeStatusFile, type OperatorRole, type RecordsServiceConfig } from "../src/records/server.js";
import type { ClientPool } from "../src/records/store.js";
import { postgresBin, startPostgres, type TestPostgres } from "./helpers/postgres.js";

/**
 * C10: the records service owns database authority; Fabric reaches it over a unix socket with a
 * per-principal token. The cluster here runs with the install's own pg_hba and pg_ident
 * (scripts/records-paul-steps.sh --print), with this test's OS user in the records user's place.
 */
const REF = "Smarty-Pants-Inc/smarty-dev#754";
const ALICE = "session:0a1b2c3d-0000-4000-8000-00000000a11c";
const BOB = "session:0a1b2c3d-0000-4000-8000-000000000b0b";
const osUser = os.userInfo().username;
const script = path.resolve(__dirname, "../scripts/records-paul-steps.sh");
const rendered = (what: string): string => {
  const result = spawnSync("bash", [script, "--org", "test-org", "--org-user", "nobodyuser", "--print", what], { encoding: "utf8" });
  if (result.status !== 0) throw new Error(result.stderr);
  // The install maps the OS user test-org-records; this test's OS user stands in for it.
  return result.stdout.replaceAll("test-org-records", osUser);
};
const canRun = Boolean(postgresBin) && process.platform !== "win32";

describe.skipIf(!canRun)("the records service (C10)", () => {
  let server: TestPostgres;
  let dir: string;
  let databases = 0;
  const cleanups: (() => Promise<unknown>)[] = [];

  beforeAll(async () => {
    server = await startPostgres({ hba: rendered("hba"), ident: rendered("ident") });
    const admin = new pg.Client({ ...server.connection, user: "postgres" });
    await admin.connect();
    // As the install does: the service's login role, then its grants come from the migration.
    await admin.query(`CREATE ROLE ${SERVICE_ROLE} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT`);
    await admin.query("CREATE ROLE agent LOGIN");
    await admin.end();
    dir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-records-service-"));
  }, 60_000);
  afterEach(async () => {
    for (const cleanup of cleanups.reverse()) await cleanup().catch(() => undefined);
    cleanups.length = 0;
  });
  afterAll(async () => {
    await server?.stop();
    fs.rmSync(dir, { recursive: true, force: true });
  }, 30_000);

  /** A fresh `records` database (the name the install's hba admits), migrated as the owner. */
  const freshService = async (overrides: Partial<RecordsServiceConfig> = {}) => {
    const admin = new pg.Client({ ...server.connection, user: "postgres" });
    await admin.connect();
    await admin.query("DROP DATABASE IF EXISTS records WITH (FORCE)");
    await admin.query("CREATE DATABASE records");
    await admin.end();
    const n = ++databases;
    const config = { ...normalizeServiceConfig({
      org: "smarty-pants", origin: "dev1", socket: path.join(dir, `records-${n}.sock`),
      database: { host: server.socketDir, port: server.port, database: "records", user: SERVICE_ROLE },
      migration: { host: server.socketDir, port: server.port, database: "records", user: "postgres" },
      roles: { importer: ["importer:github"], mirror: [], relay: ["relay:fabric", "relay:other"] },
    }), ...overrides };
    const owner = new pg.Pool({ ...config.migration!, max: 2 });
    const client = await owner.connect();
    try { await migrate(client); } finally { client.release(); }
    const pool = new pg.Pool({ ...config.database, max: 6 });
    pool.on("error", () => undefined);
    const service = await RecordsServer.open(config, { pool: pool as unknown as ClientPool });
    await service.listen();
    cleanups.push(() => service.close(), () => owner.end());
    return { config, service, owner };
  };
  /** An operator-issued principal's client (the relay, the importer), as the installer issues it. */
  const operator = async (config: RecordsServiceConfig, owner: pg.Pool, id: string) => {
    const issued = await issuePrincipal(config, id, id.split(":")[0] as OperatorRole, id, owner as unknown as ClientPool);
    const file = path.join(dir, `${id.replaceAll(":", "_")}-${databases}.json`);
    fs.writeFileSync(file, JSON.stringify(issued), { mode: 0o600 });
    return connect(config, "unused", file);
  };
  const connect = async (config: RecordsServiceConfig, id: string, credentialFile?: string) => {
    const client = new RemoteRecords({ socket: config.socket, identity: { id, name: id.slice(-4) }, credentialDir: path.join(dir, "credentials", id.replaceAll(":", "_")), ...(credentialFile ? { credentialFile } : {}) });
    await client.open();
    cleanups.push(async () => client.close());
    return client;
  };

  it("appends and reads over the socket, with a same-key retry returning the same receipt", async () => {
    const { config } = await freshService();
    const alice = await connect(config, ALICE);
    expect([alice.org, alice.origin]).toEqual(["smarty-pants", "dev1"]);
    const receipt = await alice.append({ id: "ignored" }, { ref: REF, kind: "status", key: "s1", text: "on it", data: { state: "in progress" } });
    expect(await alice.append({ id: "ignored" }, { ref: REF, kind: "status", key: "s1", text: "on it", data: { state: "in progress" } })).toEqual(receipt);
    await expect(alice.append({ id: "ignored" }, { ref: REF, kind: "status", key: "s1", text: "changed" })).rejects.toThrow(/already used for a different payload/);
    const page = await alice.read({ id: "ignored" }, { after: 0 });
    // The author is the token's principal, whatever the call says.
    expect(page.records.map((record) => [record.from, record.key])).toEqual([[ALICE, "s1"]]);
    // The credential is kept 0600 and reused by a new connection of the same participant.
    const again = await connect(config, ALICE);
    expect((await again.read({ id: "x" }, { after: 0 })).frontier).toBe(1);
    const file = fs.readdirSync(path.join(dir, "credentials", ALICE.replaceAll(":", "_")))[0]!;
    expect(fs.statSync(path.join(dir, "credentials", ALICE.replaceAll(":", "_"), file)).mode & 0o777).toBe(0o600);
  });

  it("authenticates every call: no token, an unknown token, a second claim of an id and an operator-shaped id are refused", async () => {
    const { config } = await freshService();
    await connect(config, ALICE);
    const raw = async (request: object) => new Promise<{ ok: boolean; error?: { code?: string; message: string } }>((resolve, reject) => {
      const socket = require("node:net").connect(config.socket);
      socket.setEncoding("utf8");
      let buffer = "";
      socket.on("data", (chunk: string) => { buffer += chunk; if (buffer.includes("\n")) { socket.destroy(); resolve(JSON.parse(buffer.split("\n")[0]!)); } });
      socket.on("error", reject);
      socket.write(`${JSON.stringify(request)}\n`);
    });
    expect((await raw({ id: 1, method: "append", args: { args: { ref: REF, kind: "comment", key: "k", text: "x" } } })).error?.code).toBe("RECORD_UNAUTHENTICATED");
    expect((await raw({ id: 1, method: "read", token: "made-up", args: { args: {} } })).error?.code).toBe("RECORD_UNAUTHENTICATED");
    // Alice's id is taken: another process cannot claim it and write as her.
    // Alice's id is taken: another process, with its own nonce or none, cannot claim it.
    expect((await raw({ id: 1, method: "register", args: { id: ALICE, nonce: "x".repeat(43) } })).error?.code).toBe("RECORD_PRINCIPAL_TAKEN");
    expect((await raw({ id: 1, method: "register", args: { id: ALICE } })).error?.code).toBe("RECORD_INVALID");
    for (const id of ["importer:github", "github:paul", "fabric-v2", "session:not-a-uuid"]) {
      expect((await raw({ id: 1, method: "register", args: { id, nonce: "y".repeat(43) } })).error?.code).toBe("RECORD_PRINCIPAL_INVALID");
    }
    expect((await raw({ id: 1, method: "drop tables", token: "x", args: {} })).error?.code).toBe("RECORD_UNKNOWN_METHOD");
  });

  it("takes roles only from the service's configuration: a session cannot import; an issued importer can", async () => {
    const { config, owner } = await freshService();
    const alice = await connect(config, ALICE);
    await expect(alice.append({ id: ALICE, importer: true, mirror: true }, { ref: REF, kind: "comment", key: "k", text: "x", author: "github:paul", data: { via: "github:bot" } }))
      .rejects.toThrow(/importer role/);
    await expect(alice.append({ id: ALICE }, { ref: REF, kind: "mirror", key: "m", data: { mirrorOf: "00000000-0000-4000-8000-000000000000", target: "github", state: "mirrored" } }))
      .rejects.toThrow(/mirror role/);
    const issued = await issuePrincipal(config, "importer:github", "importer", "github importer", owner as unknown as ClientPool);
    const file = path.join(dir, "importer.json");
    fs.writeFileSync(file, JSON.stringify(issued), { mode: 0o600 });
    const importer = await connect(config, "whatever", file);
    const imported = await importer.append({ id: "x" }, { ref: REF, kind: "comment", key: "gh-1", text: "from GitHub", author: "github:paul", data: { via: "github:smarty-fleet-write[bot]" } });
    expect((await alice.get({ id: ALICE }, { ref: REF })).history.find((record) => record.id === imported.id)?.from).toBe("github:paul");
  });

  it("serves the inbox, relay and watchdog paths, with each consumer's cursor its own", async () => {
    const { config, owner } = await freshService();
    const alice = await connect(config, ALICE);
    const bob = await connect(config, BOB);
    const relay = await operator(config, owner, "relay:fabric");
    const inbox = new RecordsInbox(bob, BOB, () => ["bob"]);
    await inbox.next(recordsInboxSession([]));
    const ask = await alice.append({ id: ALICE }, { ref: REF, kind: "ask", key: "a", text: "host?", data: { to: "bob" } });
    const batch = await inbox.next(recordsInboxSession([]));
    expect(batch.records.map((record) => record.id)).toEqual([ask.id]);
    // Alice cannot move Bob's cursor: the service applies her calls to her own consumer.
    await alice.saveConsumer(BOB, 99, null);
    expect((await inbox.next(recordsInboxSession([]))).records.map((record) => record.id)).toEqual([ask.id]);
    await inbox.next(recordsInboxSession([{ type: "custom_message", ...recordsInboxMessage(batch.records) }]));
    expect((await inbox.next(recordsInboxSession([]))).records).toEqual([]);
    const events: { topic: string; kind: string; to?: string }[] = [];
    const publisher: NudgePublisher = { publish: async (input) => { events.push(input); return { sequence: events.length }; } };
    expect(await new PublicationRelay(relay, publisher).flush()).toEqual({ published: 1, failed: 0 });
    expect(events).toMatchObject([{ topic: "record/Smarty-Pants-Inc/smarty-dev/754", kind: "record.ask", to: "bob" }]);
    expect(await alice.unpublished()).toBe(0);
  });

  it("closing the service cancels a blocked append: no late commit after the lock frees", async () => {
    const { config, service, owner } = await freshService();
    const alice = await connect(config, ALICE);
    const blocker = await owner.connect();
    await blocker.query("BEGIN");
    await blocker.query("SELECT pg_advisory_xact_lock(hashtextextended('fabric-records:smarty-pants', 0))");
    const blocked = alice.append({ id: ALICE }, { ref: REF, kind: "status", key: "late", text: "must not land" });
    await new Promise((resolve) => setTimeout(resolve, 300));
    await service.close(3_000);
    await expect(blocked).rejects.toThrow();
    await blocker.query("COMMIT");
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect((await blocker.query("SELECT count(*)::int AS n FROM records")).rows[0].n).toBe(0);
    blocker.release();
  });

  it("a client that disconnects cancels its blocked append on the service", async () => {
    const { config, owner, service } = await freshService();
    const alice = await connect(config, ALICE);
    const blocker = await owner.connect();
    await blocker.query("BEGIN");
    await blocker.query("SELECT pg_advisory_xact_lock(hashtextextended('fabric-records:smarty-pants', 0))");
    const blocked = alice.append({ id: ALICE }, { ref: REF, kind: "status", key: "late", text: "must not land" });
    await new Promise((resolve) => setTimeout(resolve, 300));
    alice.close();
    await expect(blocked).rejects.toThrow(/outcome is unknown: retry with the same key|closed/);
    await disconnected(service, 0);
    await blocker.query("COMMIT");
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect((await blocker.query("SELECT count(*)::int AS n FROM records")).rows[0].n).toBe(0);
    blocker.release();
    // The counterexample: a live client's append lands.
    const again = await connect(config, ALICE);
    expect((await again.append({ id: ALICE }, { ref: REF, kind: "status", key: "late", text: "must not land" })).sequence).toBe(1);
  });

  it("closing Fabric's side cancels a blocked append through the provider: nothing commits later", async () => {
    const { config, owner } = await freshService();
    const { RecordsService } = await import("../src/records/service.js");
    const { RecordsProvider } = await import("../src/providers/records-provider.js");
    const fabric = await RecordsService.open({
      config: { enabled: true, socket: config.socket, watchdogMs: 60_000, consumerLagSeconds: 120 },
      publisher: { publish: async () => ({ sequence: 1 }) }, identity: { id: ALICE }, credentialDir: path.join(dir, "fabric-credentials"),
    });
    const provider = new RecordsProvider(async () => fabric);
    const blocker = await owner.connect();
    await blocker.query("BEGIN");
    await blocker.query("SELECT pg_advisory_xact_lock(hashtextextended('fabric-records:smarty-pants', 0))");
    const caller = new AbortController(); // the caller stays alive: only Fabric's side closes
    const blocked = provider.invoke("append", { ref: REF, kind: "status", key: "late", text: "must not land" }, { signal: caller.signal } as never);
    await new Promise((resolve) => setTimeout(resolve, 300));
    await fabric.close(3_000);
    await expect(blocked).rejects.toThrow(/outcome is unknown: retry with the same key|closed/);
    await blocker.query("COMMIT");
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect((await blocker.query("SELECT count(*)::int AS n FROM records")).rows[0].n).toBe(0);
    blocker.release();
  });

  /** Wait until the service has seen a disconnect (its open connections drop to `count`). */
  const disconnected = async (service: RecordsServer, count: number) => {
    for (let i = 0; i < 250 && service.connectionCount > count; i++) await new Promise((resolve) => setTimeout(resolve, 10));
    expect(service.connectionCount).toBe(count);
  };

  /** A raw protocol connection: send frames, read responses by id. */
  const rawClient = async (socketPath: string) => {
    const net = await import("node:net");
    const socket = net.connect(socketPath);
    socket.setEncoding("utf8");
    await new Promise<void>((resolve, reject) => { socket.once("connect", () => resolve()); socket.once("error", reject); });
    const responses = new Map<number, { ok: boolean; result?: unknown; error?: { code?: string; message: string } }>();
    let buffer = "";
    socket.on("data", (chunk: string) => {
      buffer += chunk;
      let index: number;
      while ((index = buffer.indexOf("\n")) >= 0) {
        const response = JSON.parse(buffer.slice(0, index));
        buffer = buffer.slice(index + 1);
        responses.set(response.id, response);
      }
    });
    socket.on("error", () => undefined);
    cleanups.push(async () => socket.destroy());
    const closed = new Promise<void>((resolve) => socket.once("close", () => resolve()));
    return {
      socket, closed, responses,
      send: (frame: unknown) => socket.write(`${typeof frame === "string" ? frame : JSON.stringify(frame)}\n`),
      response: async (id: number) => {
        for (let i = 0; i < 100 && !responses.has(id); i++) await new Promise((resolve) => setTimeout(resolve, 20));
        return responses.get(id);
      },
    };
  };

  it("survives malformed frames: only the bad connection ends, and another client keeps working (F8)", async () => {
    const { config } = await freshService();
    const alice = await connect(config, ALICE);
    for (const frame of ["null", "[]", "42", "\"text\"", "{\"id\":\"1\",\"method\":\"read\"}", "{\"id\":1}", "{\"id\":1,\"method\":\"cancel\",\"args\":null}",
      "{\"id\":1,\"method\":\"cancel\",\"args\":{\"target\":\"x\"}}", "{\"id\":1,\"method\":\"read\",\"args\":[1]}", "{\"id\":1,\"method\":\"saveConsumer\",\"token\":\"t\",\"args\":{\"after\":-1}}", "not json"]) {
      const bad = await rawClient(config.socket);
      bad.send(frame);
      await Promise.race([bad.closed, new Promise((resolve) => setTimeout(resolve, 300))]);
    }
    // The service is alive, and a healthy client's call still works.
    expect((await alice.append({ id: ALICE }, { ref: REF, kind: "comment", key: "after-garbage", text: "still here" })).sequence).toBe(1);
  });

  it("refuses a duplicate in-flight request id, so a disconnect still cancels the first call (F3)", async () => {
    const { config, owner, service } = await freshService();
    const alice = await connect(config, ALICE);
    const token = JSON.parse(fs.readFileSync(path.join(dir, "credentials", ALICE.replaceAll(":", "_"), fs.readdirSync(path.join(dir, "credentials", ALICE.replaceAll(":", "_")))[0]!), "utf8")).token as string;
    void alice;
    const blocker = await owner.connect();
    await blocker.query("BEGIN");
    await blocker.query("SELECT pg_advisory_xact_lock(hashtextextended('fabric-records:smarty-pants', 0))");
    const raw = await rawClient(config.socket);
    raw.send({ id: 7, method: "append", token, args: { args: { ref: REF, kind: "status", key: "late", text: "must not land" } } });
    await new Promise((resolve) => setTimeout(resolve, 200));
    // The reported sequence: the same id again, finishing first.
    raw.send({ id: 7, method: "read", token, args: { args: {} } });
    expect((await raw.response(7))?.error?.code).toBe("RECORD_DUPLICATE_REQUEST");
    raw.socket.destroy();
    await disconnected(service, 1); // Alice's own connection stays
    await blocker.query("COMMIT");
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect((await blocker.query("SELECT count(*)::int AS n FROM records")).rows[0].n).toBe(0);
    blocker.release();
    // Counterexample: distinct ids on one connection both run.
    const ok = await rawClient(config.socket);
    ok.send({ id: 1, method: "read", token, args: { args: {} } });
    ok.send({ id: 2, method: "status", token, args: {} });
    expect((await ok.response(1))?.ok).toBe(true);
    expect((await ok.response(2))?.ok).toBe(true);
  });

  it("cancels a registration whose client disconnects before it commits (F3)", async () => {
    const { config, owner, service } = await freshService();
    const blocker = await owner.connect();
    await blocker.query("BEGIN");
    await blocker.query("LOCK TABLE principals IN ACCESS EXCLUSIVE MODE");
    const raw = await rawClient(config.socket);
    raw.send({ id: 1, method: "register", args: { id: BOB, nonce: "z".repeat(43) } });
    // Disconnect only once the registration's INSERT is waiting on the lock (no timing guess).
    for (let i = 0; i < 250; i++) {
      const { rows } = await owner.query("SELECT count(*)::int AS n FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE 'INSERT INTO principals%'");
      if (rows[0].n > 0) break;
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    expect((await owner.query("SELECT count(*)::int AS n FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE 'INSERT INTO principals%'")).rows[0].n).toBeGreaterThanOrEqual(1);
    raw.socket.destroy();
    // The lock is released only once the service has seen the disconnect (else it is a lost response, not a cancel).
    await disconnected(service, 0);
    await blocker.query("COMMIT");
    blocker.release();
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect((await owner.query("SELECT count(*)::int AS n FROM principals WHERE id = $1", [BOB])).rows[0].n).toBe(0);
    // Counterexample: Bob can still register himself afterwards.
    await connect(config, BOB);
  });

  it("gives publication and alarm authority only to the reserved relay role (F9)", async () => {
    const { config, owner } = await freshService();
    const alice = await connect(config, ALICE);
    const bob = await connect(config, BOB);
    const record = await alice.append({ id: ALICE }, { ref: REF, kind: "ask", key: "a", text: "host?", data: { to: "bob" } });
    const published = async () => (await owner.query("SELECT published_at IS NOT NULL AS p FROM publication WHERE record_id = $1", [record.id])).rows[0].p as boolean;
    // The reported sequence: an ordinary token claims, then falsely acks. Every step is refused.
    await expect(bob.claimPublications(10)).rejects.toThrow(/only the records relay/);
    const fake = { claimId: "00000000-0000-4000-8000-000000000000", recordId: record.id };
    await expect(bob.ackPublication(fake, 999)).rejects.toThrow(/only the records relay/);
    await expect(bob.failPublication(fake, "x")).rejects.toThrow(/only the records relay/);
    await expect(bob.releasePublications(fake.claimId, [record.id])).rejects.toThrow(/only the records relay/);
    await expect(bob.claimAlarm(`consumer-lag:${ALICE}`, Date.now(), 600_000)).rejects.toThrow(/only the records relay/);
    expect(await published()).toBe(false);
    // Between relays, a claim stays its claimant's.
    const relay = await operator(config, owner, "relay:fabric");
    const other = await operator(config, owner, "relay:other");
    const { claims: [claim] } = await relay.claimPublications(10);
    expect(claim!.recordId).toBe(record.id);
    expect(await other.ackPublication(claim!, 999)).toBe(false);
    await other.failPublication(claim!, "suppressed");
    expect((await owner.query("SELECT claimed_by, error FROM publication WHERE record_id = $1", [record.id])).rows[0]).toEqual({ claimed_by: "relay:fabric", error: null });
    // Counterexample: the relay acks its own live claim after publishing.
    expect(await relay.ackPublication(claim!, 5)).toBe(true);
    expect(await published()).toBe(true);
  });

  it("delivers by cursor even when a publication was acked with no mesh event (F9 is bounded)", async () => {
    const { config, owner } = await freshService();
    const alice = await connect(config, ALICE);
    const bob = await connect(config, BOB);
    const relay = await operator(config, owner, "relay:fabric");
    const inbox = new RecordsInbox(bob, BOB, () => ["bob"]);
    await inbox.next(recordsInboxSession([]));
    const ask = await alice.append({ id: ALICE }, { ref: REF, kind: "ask", key: "a", text: "host?", data: { to: "bob" } });
    // A relay (the only one allowed) marks the nudge published but never publishes it.
    const { claims: [claim] } = await relay.claimPublications(10);
    expect(await relay.ackPublication(claim!, 1)).toBe(true);
    // The receiver's next reconcile still delivers the record.
    expect((await inbox.next(recordsInboxSession([]))).records.map((record) => record.id)).toEqual([ask.id]);
  });

  it("honors a relay's alarm claim only while its condition holds (F9)", async () => {
    const { config, owner } = await freshService();
    const relay = await operator(config, owner, "relay:fabric");
    expect(await relay.claimAlarm(`consumer-lag:${BOB}`, Date.now(), 10 * 60_000)).toBe(false);
    expect(await relay.claimAlarm("archive-lag:refuse", Date.now(), 10 * 60_000)).toBe(false);
    await expect(relay.claimAlarm("anything", Date.now(), 1)).rejects.toThrow(/unknown alarm key/);
  });

  it("recovers an enrollment whose response was lost, with its saved nonce only (F3)", async () => {
    const { config, service, owner } = await freshService();
    const credentialDir = path.join(dir, "enroll");
    // The service commits the registration but the client never gets the credential.
    const client = new RemoteRecords({ socket: config.socket, identity: { id: BOB }, credentialDir });
    const original = service.register.bind(service);
    let lose = true;
    (service as unknown as { register: typeof service.register }).register = async (...args) => {
      const issued = await original(...args);
      if (lose) { lose = false; service.disconnectAll(); throw new Error("lost"); }
      return issued;
    };
    await expect(client.open()).rejects.toThrow();
    client.close();
    expect((await owner.query("SELECT count(*)::int AS n FROM principals WHERE id = $1", [BOB])).rows[0].n).toBe(1);
    // An attacker with no nonce, or another nonce, cannot take the id.
    const raw = await rawClient(config.socket);
    raw.send({ id: 1, method: "register", args: { id: BOB, nonce: "a".repeat(43) } });
    expect((await raw.response(1))?.error?.code).toBe("RECORD_PRINCIPAL_TAKEN");
    raw.send({ id: 2, method: "register", args: { id: BOB } });
    expect((await raw.response(2))?.error?.code).toBe("RECORD_INVALID");
    // The owner's retry, with the nonce it saved before sending, gets a working credential.
    // Another directory (no saved nonce) is refused too: only the saved nonce recovers.
    await expect(connect(config, BOB)).rejects.toThrow(/already registered/);
    const again = new RemoteRecords({ socket: config.socket, identity: { id: BOB }, credentialDir });
    await again.open();
    cleanups.push(async () => again.close());
    expect((await again.append({ id: BOB }, { ref: REF, kind: "comment", key: "c", text: "recovered" })).sequence).toBe(1);
  });

  it("audits each append's peer process and raises the token-reuse alarm (B)", async () => {
    const { config, service, owner } = await freshService({ statusFile: path.join(dir, "status.json") } as Partial<RecordsServiceConfig>);
    const alice = await connect(config, ALICE);
    const record = await alice.append({ id: ALICE }, { ref: REF, kind: "comment", key: "c", text: "audited" });
    const peer = (await owner.query("SELECT principal, pid, uid, cmdline FROM record_peers WHERE record_id = $1", [record.id])).rows[0];
    expect(peer).toMatchObject({ principal: ALICE, pid: process.pid, uid: process.getuid!() });
    expect(peer.cmdline).toContain("node");
    // The same token used from another live process (a stolen credential).
    const file = path.join(dir, "credentials", ALICE.replaceAll(":", "_"), fs.readdirSync(path.join(dir, "credentials", ALICE.replaceAll(":", "_")))[0]!);
    const { token } = JSON.parse(fs.readFileSync(file, "utf8")) as { token: string };
    const { spawn } = await import("node:child_process");
    const thief = spawn(process.execPath, ["-e", `
      const s = require("node:net").connect(${JSON.stringify(config.socket)});
      s.on("connect", () => s.write(JSON.stringify({ id: 1, method: "whoami", token: ${JSON.stringify(token)}, args: {} }) + String.fromCharCode(10)));
      s.on("data", () => { s.destroy(); });`], { stdio: "ignore" });
    expect(await new Promise((resolve) => thief.on("exit", resolve))).toBe(0);
    for (let i = 0; i < 50 && service.alerts().length === 0; i++) await new Promise((resolve) => setTimeout(resolve, 20));
    expect(service.alerts().at(-1)).toMatchObject({ principal: ALICE });
    expect(service.alerts().at(-1)!.pids).toContain(process.pid);
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(JSON.parse(fs.readFileSync(path.join(dir, "status.json"), "utf8")).alarm).toMatch(new RegExp(`token of ${ALICE} used by live processes`));
    // The factory check (another user) reads it: 0644, no secret in it.
    expect(fs.statSync(path.join(dir, "status.json")).mode & 0o777).toBe(0o644);
    // Counterexample: one process calling many times raises nothing new.
    const count = service.alerts().length;
    await alice.read({ id: ALICE }, {});
    await alice.read({ id: ALICE }, {});
    expect(service.alerts().length).toBe(count);
  });

  it("reloads the role policy without a restart (SIGHUP)", async () => {
    const { config, service, owner } = await freshService();
    const newcomer = await operator(config, owner, "importer:new");
    await expect(newcomer.append({ id: "x" }, { ref: REF, kind: "comment", key: "k", text: "t", author: "github:paul", data: { via: "github:bot" } })).rejects.toThrow(/importer role/);
    service.reloadRoles({ ...config.roles, importer: [...config.roles.importer, "importer:new"] });
    expect((await newcomer.append({ id: "x" }, { ref: REF, kind: "comment", key: "k", text: "t", author: "github:paul", data: { via: "github:bot" } })).sequence).toBe(1);
  });

  it("delivers near-limit records through the inbox, relay and read in bounded responses (F10)", async () => {
    const { config, owner } = await freshService();
    const alice = await connect(config, ALICE);
    const bob = await connect(config, BOB);
    const relay = await operator(config, owner, "relay:fabric");
    const inbox = new RecordsInbox(bob, BOB, () => ["bob"]);
    await inbox.next(recordsInboxSession([]));
    // 20 asks at the text limit, with quotes that JSON escapes: one page of them is several MiB.
    const text = `"${"x".repeat(60 * 1024)}"`;
    const ids: string[] = [];
    for (let i = 0; i < 20; i++) ids.push((await alice.append({ id: ALICE }, { ref: REF, kind: "ask", key: `big-${i}`, text, data: { to: "bob" } })).id);
    // The inbox: every record arrives, over several bounded batches.
    const delivered: string[] = [];
    let entries: unknown[] = [];
    for (let round = 0; round < 40 && delivered.length < 20; round++) {
      const batch = await inbox.next(recordsInboxSession(entries));
      expect(batch.records.length).toBeGreaterThan(0);
      expect(batch.records.length).toBeLessThan(20);
      delivered.push(...batch.records.map((record) => record.id));
      entries = [{ type: "custom_message", ...recordsInboxMessage(batch.records) }];
    }
    expect(delivered).toEqual(ids);
    // read by cursor pages through them too.
    const seen: string[] = [];
    for (let after = 0, round = 0; round < 40 && seen.length < 20; round++) {
      const page = await bob.read({ id: BOB }, { after, limit: 500 });
      seen.push(...page.records.map((record) => record.id));
      after = page.next;
    }
    expect(seen).toEqual(ids);
    // byIds (any client may ask for up to 500 ids) answers within the budget too.
    expect((await bob.byIds(ids)).length).toBeLessThan(20);
    // get bounds its history too.
    const got = await bob.get({ id: BOB }, { ref: REF, limit: 500 });
    expect(got.history.length).toBeLessThan(20);
    expect(got.next).toBe(got.history.at(-1)!.sequence);
    // The relay publishes all of them.
    const events: unknown[] = [];
    expect(await new PublicationRelay(relay, { publish: async (input) => { events.push(input); return { sequence: events.length }; } }).flush()).toEqual({ published: 20, failed: 0 });
  }, 60_000);

  it("refuses oversized or malformed recipients at append, so claims stay small (F10)", async () => {
    const { config, owner } = await freshService();
    const alice = await connect(config, ALICE);
    for (const to of ["x".repeat(257), "a b", "bob\n", "ü"]) {
      await expect(alice.append({ id: ALICE }, { ref: REF, kind: "ask", key: `bad-${to.length}`, text: "t", data: { to } })).rejects.toThrow(/participant id or name/);
    }
    // The reported sequence: 20 asks with ~60 KiB recipients. All are refused, nothing is written.
    for (let i = 0; i < 20; i++) {
      await expect(alice.append({ id: ALICE }, { ref: REF, kind: "ask", key: `big-to-${i}`, text: "t", data: { to: "r".repeat(60 * 1024) } })).rejects.toThrow(/participant id or name/);
    }
    expect((await owner.query("SELECT count(*)::int AS n FROM records")).rows[0].n).toBe(0);
    // Counterexample: a 256-character recipient is accepted and published.
    await alice.append({ id: ALICE }, { ref: REF, kind: "ask", key: "ok", text: "t", data: { to: "r".repeat(256) } });
    const relay = await operator(config, owner, "relay:fabric");
    const events: { to?: string }[] = [];
    expect(await new PublicationRelay(relay, { publish: async (input) => { events.push(input); return { sequence: events.length }; } }).flush()).toEqual({ published: 1, failed: 0 });
    expect(events[0]!.to).toBe("r".repeat(256));
  });

  it("pages a fold with many large statuses to completion (F10)", async () => {
    const { config, owner } = await freshService();
    const importer = await operator(config, owner, "importer:github");
    const text = `"${"s".repeat(62 * 1024)}"`;
    // 70 authors: their fold statuses (2 KiB each) outgrow the statuses share, so the fold continues.
    const authors = Array.from({ length: 70 }, (_, i) => `github:author${String(i).padStart(2, "0")}`);
    for (const author of authors) {
      await importer.append({ id: "x" }, { ref: REF, kind: "status", key: `st-${author}`, text, author, data: { state: "in progress", via: "github:bot" } });
    }
    await importer.append({ id: "x" }, { ref: REF, kind: "issue", key: "issue", author: "github:paul", data: { title: "T".repeat(60 * 1024), via: "github:bot" } });
    const got = await importer.get({ id: "x" }, { ref: REF, limit: 500 });
    // Bounded: the state and the history each fit, and the fold says what continues.
    expect(Buffer.byteLength(JSON.stringify(got))).toBeLessThan(1024 * 1024);
    expect(got.state!.truncated).toEqual(["title"]);
    expect(Object.values(got.state!.statuses).every((status) => Buffer.byteLength(status.text ?? "") <= 2048)).toBe(true);
    expect(Object.keys(got.state!.statuses).length).toBeLessThan(70);
    expect(got.state!.more?.statuses).toBeDefined();
    const seen = new Set(Object.keys(got.state!.statuses));
    let cursor = got.state!.more?.statuses;
    for (let round = 0; cursor !== undefined && round < 20; round++) {
      const page = await importer.fold({ id: "x" }, { ref: REF, part: "statuses", after: cursor });
      for (const item of page.items as { author: string }[]) seen.add(item.author);
      cursor = page.next;
    }
    expect([...seen].sort()).toEqual(authors);
    // The history continues to every record too.
    const history: number[] = [];
    for (let after = 0, round = 0; round < 40; round++) {
      const page = await importer.get({ id: "x" }, { ref: REF, after, limit: 500 });
      history.push(...page.history.map((record) => record.sequence));
      if (page.next === undefined) break;
      after = page.next;
    }
    expect(history).toEqual(Array.from({ length: 71 }, (_, i) => i + 1));
  }, 60_000);

  it("traverses all history past a populated fold, with escape-heavy near-limit records (F10)", async () => {
    const { config, owner } = await freshService();
    const importer = await operator(config, owner, "importer:github");
    // Escapes grow JSON: quotes, backslashes and control characters (\u0001 is 6 bytes encoded).
    const heavy = (n: number) => '"\\\u0001'.repeat(n);
    // The encoded-size limit refuses a record that raw limits would let through.
    await expect(importer.append({ id: "x" }, { ref: REF, kind: "comment", key: "too-big", text: heavy(20 * 1024), author: "github:paul", data: { via: "github:bot" } }))
      .rejects.toThrow(/as JSON \(escapes count\)/);
    // A populated fold: 70 statuses and a large title, then near-limit escape-heavy records.
    for (let i = 0; i < 70; i++) {
      await importer.append({ id: "x" }, { ref: REF, kind: "status", key: `st-${i}`, text: "s".repeat(2048), author: `github:a${i}`, data: { state: "waiting", via: "github:bot" } });
    }
    await importer.append({ id: "x" }, { ref: REF, kind: "issue", key: "issue", author: "github:paul", data: { title: "T".repeat(60 * 1024), via: "github:bot" } });
    const big = heavy(7 * 1024); // ~84 KiB encoded text
    for (let i = 0; i < 6; i++) {
      await importer.append({ id: "x" }, { ref: REF, kind: "comment", key: `big-${i}`, text: big, author: "github:paul", data: { via: `github:${"q".repeat(40)}`, githubId: heavy(4 * 1024) } });
    }
    const first = await importer.get({ id: "x" }, { ref: REF, limit: 500 });
    expect(first.state).toBeDefined();
    expect(Buffer.byteLength(JSON.stringify(first))).toBeLessThan(1024 * 1024);
    // Every later page: history only, never the state again, at least one record, advancing.
    const seqs = first.history.map((record) => record.sequence);
    let next = first.next;
    for (let round = 0; next !== undefined && round < 200; round++) {
      const page = await importer.get({ id: "x" }, { ref: REF, after: next, limit: 500 });
      expect(page.state).toBeUndefined();
      expect(Buffer.byteLength(JSON.stringify(page))).toBeLessThan(1024 * 1024);
      if (page.next !== undefined) {
        expect(page.history.length).toBeGreaterThan(0);
        expect(page.next).toBeGreaterThan(next);
      }
      seqs.push(...page.history.map((record) => record.sequence));
      next = page.next;
    }
    expect(seqs).toEqual(Array.from({ length: 77 }, (_, i) => i + 1));
  }, 120_000);

  it("writes the status file 0644 under the unit's umask 0007, fresh and replaced (F16)", async () => {
    const file = path.join(dir, "status-umask", "o.status.json");
    const previous = process.umask(0o007);
    try {
      await writeStatusFile(file, "{\"a\":1}\n");
      expect(fs.statSync(file).mode & 0o777).toBe(0o644);
      await writeStatusFile(file, "{\"a\":2}\n");
      expect(fs.statSync(file).mode & 0o777).toBe(0o644);
      expect(fs.readFileSync(file, "utf8")).toBe("{\"a\":2}\n");
      expect(fs.readdirSync(path.dirname(file))).toEqual(["o.status.json"]);
    } finally {
      process.umask(previous);
    }
  });

  it("pages a list of issues with large titles to completion (F10)", async () => {
    const { config, owner } = await freshService();
    const alice = await connect(config, ALICE);
    void owner;
    const refs: string[] = [];
    for (let i = 1; i <= 30; i++) {
      const ref = `Smarty-Pants-Inc/smarty-dev#${i}`;
      refs.push(ref);
      await alice.append({ id: ALICE }, { ref, kind: "issue", key: `i-${i}`, data: { title: `${i}:${"t".repeat(60 * 1024)}` } });
    }
    const seen: string[] = [];
    let after: string | undefined;
    for (let round = 0; round < 40; round++) {
      const page = await alice.list({ id: ALICE }, { limit: 500, ...(after ? { after } : {}) });
      expect(page.items.length).toBeLessThan(30);
      seen.push(...page.items.map((item) => item.ref));
      if (!page.next) break;
      after = page.next;
    }
    expect(seen.sort()).toEqual(refs.sort());
  }, 60_000);

  it("keeps operator roles to principals the installer issued for that role", async () => {
    const { config, owner } = await freshService();
    // The config grants relay to importer:github, but it was issued as an importer: no relay authority.
    const confused = normalizeServiceConfig({ ...config, roles: { importer: ["importer:github"], mirror: [], relay: ["importer:github"] } });
    const { service } = { service: await RecordsServer.open(confused, { pool: new pg.Pool({ ...config.database, max: 2 }) as unknown as ClientPool }) };
    cleanups.push(() => service.close());
    await service.listen(`${config.socket}.confused`);
    const importer = await operator({ ...confused, socket: `${config.socket}.confused` }, owner, "importer:github");
    await expect(importer.claimPublications(10)).rejects.toThrow(/only the records relay/);
    // An operator id cannot be registered, and a session id cannot be issued.
    await expect(issuePrincipal(config, ALICE, "relay", "x", owner as unknown as ClientPool)).rejects.toThrow(/registers itself/);
    const raw = await rawClient(config.socket);
    raw.send({ id: 1, method: "register", args: { id: "relay:sneaky", nonce: "n".repeat(43) } });
    expect((await raw.response(1))?.error?.code).toBe("RECORD_PRINCIPAL_INVALID");
    // Counterexample: issued as relay and granted relay, it claims.
    const relay = await operator(config, owner, "relay:fabric");
    expect((await relay.claimPublications(10)).claims).toEqual([]);
  });

  it("reissue recovers an interrupted operator issuance, and touches nothing else", async () => {
    const { config, owner } = await freshService();
    const pool = owner as unknown as ClientPool;
    const first = await issuePrincipal(config, "relay:fabric", "relay", "relay", pool);
    // The credential file was never written (interrupted): a plain issue is refused...
    await expect(issuePrincipal(config, "relay:fabric", "relay", "relay", pool)).rejects.toThrow(/pass --reissue/);
    // ...and a reissue rotates the token: the new one works, the lost one no longer does.
    const second = await issuePrincipal(config, "relay:fabric", "relay", "relay", pool, true);
    expect(second.token).not.toBe(first.token);
    const file = path.join(dir, `reissued-${databases}.json`);
    fs.writeFileSync(file, JSON.stringify(second), { mode: 0o600 });
    const relay = await connect(config, "unused", file);
    expect((await relay.claimPublications(10)).claims).toEqual([]);
    const lost = path.join(dir, `lost-${databases}.json`);
    fs.writeFileSync(lost, JSON.stringify(first), { mode: 0o600 });
    const stale = await connect(config, "unused", lost);
    await expect(stale.claimPublications(10)).rejects.toThrow(/not known to this service/);
    // A reissue never changes the role of an existing operator.
    await expect(issuePrincipal(config, "relay:fabric", "importer", "x", pool, true)).rejects.toThrow(/another kind or role/);
    expect((await owner.query("SELECT role FROM principals WHERE id = 'relay:fabric'")).rows[0].role).toBe("relay");
  });

  it("a reissue invalidates the old token in the RUNNING service at once (Astra F1 on #117)", async () => {
    const { config, owner } = await freshService();
    const pool = owner as unknown as ClientPool;
    const out = path.join(dir, `f1-${databases}`, "relay.json");
    await issueCredentialFile(config, "relay:fabric", "relay", out, { pool });
    const old = JSON.parse(fs.readFileSync(out, "utf8")) as { token: string };
    const before = await connect(config, "unused", out);
    // The old token has been used, so a cache would now hold it.
    expect((await before.claimPublications(10)).claims).toEqual([]);
    const kept = path.join(dir, `f1-old-${databases}.json`);
    fs.writeFileSync(kept, JSON.stringify({ id: "relay:fabric", token: old.token }), { mode: 0o600 });
    await issueCredentialFile(config, "relay:fabric", "relay", out, { pool, reissue: true });
    const stale = await connect(config, "unused", kept);
    await expect(stale.claimPublications(10)).rejects.toThrow(/not known to this service/);
    await expect(before.claimPublications(10)).rejects.toThrow(/not known to this service/);
    const fresh = await connect(config, "unused", out);
    expect((await fresh.claimPublications(10)).claims).toEqual([]);
  });

  it("issues a credential file without ever losing a token (Astra F2 on #117)", async () => {
    const { config, owner } = await freshService();
    const pool = owner as unknown as ClientPool;
    const dirOut = path.join(dir, `f2-${databases}`);
    const out = path.join(dirOut, "relay.json");
    await issueCredentialFile(config, "relay:fabric", "relay", out, { pool });
    const first = fs.readFileSync(out, "utf8");
    // Without --reissue an existing file is refused before the database is touched: the token still works.
    await expect(issueCredentialFile(config, "relay:fabric", "relay", out, { pool })).rejects.toThrow(/exists; pass --reissue/);
    expect(fs.readFileSync(out, "utf8")).toBe(first);
    expect((await (await connect(config, "unused", out)).claimPublications(10)).claims).toEqual([]);
    // A reissue whose database step fails (another role) leaves the file, the old token and no temp file.
    await expect(issueCredentialFile(config, "relay:fabric", "importer", out, { pool, reissue: true })).rejects.toThrow(/another kind or role/);
    expect(fs.readFileSync(out, "utf8")).toBe(first);
    expect(fs.readdirSync(dirOut)).toEqual(["relay.json"]);
    expect((await (await connect(config, "unused", out)).claimPublications(10)).claims).toEqual([]);
    // A successful reissue replaces the file atomically with the new, working token, 0600.
    await issueCredentialFile(config, "relay:fabric", "relay", out, { pool, reissue: true });
    expect(fs.readFileSync(out, "utf8")).not.toBe(first);
    expect(fs.statSync(out).mode & 0o777).toBe(0o600);
    expect(fs.readdirSync(dirOut)).toEqual(["relay.json"]);
    expect((await (await connect(config, "unused", out)).claimPublications(10)).claims).toEqual([]);
  });

  it("a client closed while it reconnects starts no call (F3)", async () => {
    const { config, service, owner } = await freshService();
    const alice = await connect(config, ALICE);
    // The connection drops; the next append starts reconnecting; close() comes before connect completes.
    service.disconnectAll();
    await new Promise((resolve) => setTimeout(resolve, 100));
    const caller = new AbortController(); // still live
    const append = alice.append({ id: ALICE }, { ref: REF, kind: "status", key: "after-close", text: "must not land" }, { signal: caller.signal });
    alice.close();
    await expect(append).rejects.toThrow(/closed/);
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect((await owner.query("SELECT count(*)::int AS n FROM records")).rows[0].n).toBe(0);
    await expect(alice.read({ id: ALICE }, {})).rejects.toThrow(/closed/);
    // Counterexample: a client that is not closed reconnects and appends.
    const again = await connect(config, ALICE);
    service.disconnectAll();
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect((await again.append({ id: ALICE }, { ref: REF, kind: "status", key: "after-close", text: "must not land" })).sequence).toBe(1);
  });

  it("closes promptly while an archive check is running", async () => {
    const { service } = await freshService({ admission: { targets: [{ name: "stuck", command: [process.execPath, "-e", "setTimeout(() => {}, 60000)"] }], alarmSeconds: 120, refuseSeconds: 300, refreshMs: 30_000 } });
    const tick = service.watchdog.tick().catch((error: unknown) => error);
    await new Promise((resolve) => setTimeout(resolve, 500));
    const started = Date.now();
    await service.close(3_000);
    expect(Date.now() - started).toBeLessThan(2_000);
    expect(await tick).toBeDefined();
  });

  describe("database authority under the install's pg_hba", () => {
    const as = async (user: string) => {
      const client = new pg.Client({ host: server.socketDir, port: server.port, database: "records", user });
      await client.connect();
      cleanups.push(() => client.end());
      return client;
    };

    it("admits only the records user's mapped roles; any other role and TCP are rejected", async () => {
      await freshService();
      await expect(as("agent")).rejects.toThrow(/pg_hba\.conf rejects connection|no pg_hba\.conf entry/);
      await expect(new pg.Client({ host: server.socketDir, port: server.port, database: "postgres", user: SERVICE_ROLE }).connect()).rejects.toThrow(/pg_hba\.conf rejects connection|no pg_hba\.conf entry/);
      await expect(new pg.Client({ host: "127.0.0.1", port: server.port, database: "records", user: "postgres", connectionTimeoutMillis: 2_000 }).connect()).rejects.toThrow();
      expect(fs.statSync(server.socketDir).mode & 0o777).toBe(0o700);
      expect(rendered("conf")).toMatch(/unix_socket_permissions = 0700/);
    });

    it("gives the service role no UPDATE, DELETE, TRUNCATE or DDL, and no way to become the owner", async () => {
      await freshService();
      const service = await as(SERVICE_ROLE);
      const refused: [string, RegExp][] = [
        ["UPDATE records SET text = 'x'", /permission denied/],
        ["DELETE FROM records", /permission denied/],
        ["TRUNCATE records", /permission denied/],
        ["DROP TRIGGER records_no_update_delete ON records", /must be owner/],
        ["ALTER TABLE records DISABLE TRIGGER ALL", /must be owner/],
        ["ALTER TABLE records DROP CONSTRAINT records_data_check", /must be owner/],
        ["CREATE TABLE sneaky (id int)", /permission denied for schema public/],
        ["UPDATE consumers SET after = 0", /permission denied/],
        ["UPDATE publication SET published_at = now()", /permission denied/],
        ["UPDATE archive_checks SET frontier = 'FFFFFFFF/0'", /permission denied/],
        ["DELETE FROM principals", /permission denied/],
        ["SET ROLE postgres", /permission denied/],
        ["GRANT fabric_records_writer TO agent", /permission denied|must have admin option/],
        ["CREATE OR REPLACE FUNCTION consumer_save(text, bigint, jsonb) RETURNS void LANGUAGE sql AS 'SELECT 1'", /must be owner|permission denied for schema public/],
      ];
      for (const [statement, error] of refused) await expect(service.query(statement), statement).rejects.toThrow(error);
      // Its own grants: exactly what the store needs.
      const grants = await service.query("SELECT table_name, string_agg(privilege_type, ',' ORDER BY privilege_type) AS p FROM information_schema.role_table_grants WHERE grantee = 'fabric_records_writer' AND table_name IN ('records','consumers','publication','principals') GROUP BY table_name ORDER BY table_name");
      expect(grants.rows).toEqual([
        { table_name: "consumers", p: "SELECT" }, { table_name: "principals", p: "INSERT,SELECT" },
        { table_name: "publication", p: "INSERT,SELECT" }, { table_name: "records", p: "INSERT,SELECT" },
      ]);
      // The data invariant holds for any writer: record ids in data are lowercase.
      await expect(service.query("INSERT INTO records (org, origin, seq, ref, kind, author, data, key, payload_hash) VALUES ('o','x',1,'A/b#1','answer','a','{\"ask\":\"ABC\"}','k','h')")).rejects.toThrow(/check constraint/);
    });
  });
});
