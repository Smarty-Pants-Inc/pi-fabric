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
import { issuePrincipal, normalizeServiceConfig, RecordsServer, type RecordsServiceConfig } from "../src/records/server.js";
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
      roles: { importer: ["importer:github"], mirror: [] },
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
    expect((await raw({ id: 1, method: "register", args: { id: ALICE } })).error?.code).toBe("RECORD_PRINCIPAL_TAKEN");
    for (const id of ["importer:github", "github:paul", "fabric-v2", "session:not-a-uuid"]) {
      expect((await raw({ id: 1, method: "register", args: { id } })).error?.code).toBe("RECORD_PRINCIPAL_INVALID");
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
    const issued = await issuePrincipal(config, "importer:github", "github importer", owner as unknown as ClientPool);
    const file = path.join(dir, "importer.json");
    fs.writeFileSync(file, JSON.stringify(issued), { mode: 0o600 });
    const importer = await connect(config, "whatever", file);
    const imported = await importer.append({ id: "x" }, { ref: REF, kind: "comment", key: "gh-1", text: "from GitHub", author: "github:paul", data: { via: "github:smarty-fleet-write[bot]" } });
    expect((await alice.get({ id: ALICE }, { ref: REF })).history.find((record) => record.id === imported.id)?.from).toBe("github:paul");
  });

  it("serves the inbox, relay and watchdog paths, with each consumer's cursor its own", async () => {
    const { config } = await freshService();
    const alice = await connect(config, ALICE);
    const bob = await connect(config, BOB);
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
    expect(await new PublicationRelay(bob, publisher).flush()).toEqual({ published: 1, failed: 0 });
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
    const { config, owner } = await freshService();
    const alice = await connect(config, ALICE);
    const blocker = await owner.connect();
    await blocker.query("BEGIN");
    await blocker.query("SELECT pg_advisory_xact_lock(hashtextextended('fabric-records:smarty-pants', 0))");
    const blocked = alice.append({ id: ALICE }, { ref: REF, kind: "status", key: "late", text: "must not land" });
    await new Promise((resolve) => setTimeout(resolve, 300));
    alice.close();
    await expect(blocked).rejects.toThrow(/outcome is unknown: retry with the same key/);
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
    await expect(blocked).rejects.toThrow(/outcome is unknown: retry with the same key/);
    await blocker.query("COMMIT");
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect((await blocker.query("SELECT count(*)::int AS n FROM records")).rows[0].n).toBe(0);
    blocker.release();
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
