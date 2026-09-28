import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { MeshStore } from "../src/mesh/store.js";
import { recordsInboxSession } from "../src/records/inbox.js";
import pg from "pg";
import { migrate } from "../src/records/schema.js";
import { normalizeServiceConfig, RecordsServer } from "../src/records/server.js";
import type { ClientPool } from "../src/records/store.js";
import { postgresBin, startPostgres, type TestPostgres } from "./helpers/postgres.js";

/** records.* end to end: guest code, type checks, provider, service, real PostgreSQL, mesh. */
describe.skipIf(!postgresBin)("records in a Fabric runtime", () => {
  let server: TestPostgres;
  let service: RecordsServer;
  let socket: string;
  beforeAll(async () => {
    server = await startPostgres();
    socket = path.join(server.dir, "records.sock");
    const pool = new pg.Pool({ ...server.connection, max: 6 });
    pool.on("error", () => undefined);
    const client = await pool.connect();
    try { await migrate(client); } finally { client.release(); }
    service = await RecordsServer.open(normalizeServiceConfig({ org: "smarty-pants", origin: "test-node", socket, database: server.connection }), { pool: pool as unknown as ClientPool });
    await service.listen();
  }, 60_000);
  afterAll(async () => { await service?.close(); await server?.stop(); }, 30_000);

  it("appends from guest code, reads by cursor, retries by key, nudges the mesh and reconciles the inbox", async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-records-runtime-"));
    fs.mkdirSync(path.join(cwd, ".pi"), { recursive: true });
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(cwd, "agent"));
    vi.stubEnv("PI_FABRIC_PROJECT_ROOT", cwd);
    const sent: unknown[] = [];
    const pi = {
      events: { emit: vi.fn() },
      getThinkingLevel: vi.fn(() => "off"),
      getSessionName: () => "records-main",
      sendMessage: vi.fn((message: unknown) => { sent.push(message); }),
    } as unknown as ExtensionAPI;
    const entries: unknown[] = [];
    const context = {
      cwd, hasUI: false, isProjectTrusted: () => true, isIdle: () => true, hasPendingMessages: () => false,
      modelRegistry: { find: vi.fn(), getApiKeyAndHeaders: vi.fn(), getAvailable: () => [] },
      sessionManager: { getSessionId: () => "01a0e500-0000-7000-8000-00000000cafe", getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => undefined, getEntries: () => entries },
      ui: { setStatus: vi.fn(), notify: vi.fn() },
    } as unknown as ExtensionContext;
    const config = normalizeFabricConfig({
      mcp: { enabled: false, cache: { enabled: false } }, mesh: { enabled: true }, memory: { enabled: false },
      agents: { enabled: false }, jev: { enabled: false },
      records: { enabled: true, socket },
    });
    const fixture = path.join(cwd, "unused.mjs");
    fs.writeFileSync(fixture, "export default {};");
    const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: { extension: fixture, worker: fixture, residentHost: fixture, skills: cwd } });
    try {
      await runtime.initialize(context, config);
      expect(runtime.registry.has("records")).toBe(true);
      // The first turn start makes this root a consumer at the present.
      expect((await runtime.nextRecordsInbox(recordsInboxSession(entries)))?.records).toEqual([]);
      const result = await runtime.execution.execute({
        code: `
          const ref = "Smarty-Pants-Inc/smarty-dev#754";
          const status = await records.append({ ref, kind: "status", key: "proof-status", text: "building", data: { state: "in progress", eta: "~08:00Z PR" } });
          const ask = await records.append({ ref, kind: "ask", key: "proof-ask", text: "Approve the host?", data: { to: "records-main", class: "decision" } });
          const retry = await records.append({ ref, kind: "status", key: "proof-status", text: "building", data: { state: "in progress", eta: "~08:00Z PR" } });
          let conflict = "";
          try { await records.append({ ref, kind: "status", key: "proof-status", text: "changed" }); } catch (error) { conflict = String(error); }
          const page = await records.read({ after: 0, limit: 10 });
          const fold = await records.get({ ref });
          return { status, ask, retry, conflict, kinds: page.records.map((r) => r.kind), next: page.next, frontier: page.frontier,
            state: fold.state.statuses[Object.keys(fold.state.statuses)[0]!]?.state, openAsks: fold.state.openAsks.length };`,
        context, signal: undefined, parentToolCallId: "records-probe", onPartial() {},
      });
      expect(result.success, result.error ?? JSON.stringify(result.typeErrors)).toBe(true);
      const value = result.value as Record<string, any>;
      expect(value.retry).toEqual(value.status);
      expect(value.conflict).toMatch(/already used for a different payload/);
      expect(value).toMatchObject({ kinds: ["status", "ask"], next: 2, frontier: 2, state: "in progress", openAsks: 1 });
      expect(value.status).toMatchObject({ sequence: 1, origin: "test-node", topic: "record/Smarty-Pants-Inc/smarty-dev/754" });

      // Commit, then nudge: both records reach the mesh on the ref's topic.
      const mesh = new MeshStore(path.join(cwd, ".pi", "fabric", "mesh"), 256 * 1024, 500);
      const deadline = Date.now() + 5_000;
      let nudges = mesh.read({ after: 0, topic: "record/Smarty-Pants-Inc/smarty-dev/754", limit: 10 });
      while (nudges.length < 2 && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 50));
        nudges = mesh.read({ after: 0, topic: "record/Smarty-Pants-Inc/smarty-dev/754", limit: 10 });
      }
      expect(nudges.map((event) => [event.kind, event.to])).toEqual([["record.status", undefined], ["record.ask", "records-main"]]);

      const probe = await runtime.execution.execute({ code: "return await records.status();", context, signal: undefined, parentToolCallId: "records-status", onPartial() {} });
      expect(probe.value).toMatchObject({ org: "smarty-pants", origin: "test-node", frontier: 2, unpublished: 0, admission: { state: "disabled" } });
      // The session registered its own participant id; the credential sits in its agent directory.
      expect(fs.readdirSync(path.join(cwd, "agent", "fabric", "records-credentials"))).toHaveLength(1);
      // The runtime's own ask is not delivered back to it (a consumer skips its own records).
      expect((await runtime.nextRecordsInbox(recordsInboxSession(entries)))?.records).toEqual([]);
    } finally {
      await runtime.shutdown();
      vi.unstubAllEnvs();
      fs.rmSync(cwd, { recursive: true, force: true });
    }
  }, 60_000);
});
