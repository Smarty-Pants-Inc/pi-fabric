import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { describe, expect, it, onTestFinished } from "vitest";
import { MeshStore } from "../src/mesh/store.js";
import { readHostLeases, type FabricHostLease } from "../src/topology/host-leases.js";
import { LIFECYCLE_LOST_TICKS } from "../src/topology/participant-directory.js";

// smarty-dev#8382 (follow-up to pi-fabric#660): the compiled Fabric in a real Pi process across two
// back-to-back in-process /reloads. Pi's RPC mode has no native reload command, so the test-only
// adapter extension tests/fixtures/two-reload-adapter-8382.ts registers /reload (ctx.reload(),
// the same AgentSession.reload() as an interactive /reload) and a participant heartbeat census.
// No model or provider is called: every prompt is an extension command.
//
// Run explicitly against a fresh build:
//   bun run build && FABRIC_8382_REAL_PI=1 bunx vitest run tests/main-two-reload-e2e.test.ts
// FABRIC_8382_PI_CLI=<installed pi cli.js> selects another host; FABRIC_8382_EVIDENCE_DIR keeps evidence.

const repo = fileURLToPath(new URL("../", import.meta.url));
const entry = path.join(repo, "dist/index.js");
const adapter = path.join(repo, "tests/fixtures/two-reload-adapter-8382.ts");
const cli = process.env.FABRIC_8382_PI_CLI || path.join(repo, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
const STALE = /ctx is stale|stale ctx/i;
// pi-fabric#660's lost-lifecycle report: its console warning and its fleet ops event.
const DEGRADED = /no live session ctx|is degraded|fabric\.presence\.degraded/;

type Json = Record<string, any>;
interface Exit { code: number | null; signal: NodeJS.Signals | null }
interface CensusTimer { generation: number; createdAt: number; delay: number; live: boolean }
interface Census { generation: number; at: number; timers: CensusTimer[] }

/** A minimal JSONL client for `pi --mode rpc`. RpcClient.stop() sends SIGTERM (exit 143); ending stdin is Pi's clean exit 0. */
class RpcProcess {
  readonly records: Json[] = [];
  stderr = "";
  readonly exited: Promise<Exit>;
  #buffer = "";
  #next = 0;
  readonly #pending = new Map<string, (record: Json) => void>();

  constructor(readonly child: ChildProcessWithoutNullStreams) {
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => {
      this.#buffer += chunk;
      for (let end = this.#buffer.indexOf("\n"); end >= 0; end = this.#buffer.indexOf("\n")) {
        const line = this.#buffer.slice(0, end).trim();
        this.#buffer = this.#buffer.slice(end + 1);
        if (!line) continue;
        let record: Json;
        try { record = JSON.parse(line) as Json; } catch { record = { type: "unparsed", line }; }
        this.records.push(record);
        if (record.type === "response" && typeof record.id === "string") this.#pending.get(record.id)?.(record);
      }
    });
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk: string) => { this.stderr += chunk; });
    child.stdin.on("error", () => undefined);
    this.exited = new Promise(resolve => child.once("close", (code, signal) => resolve({ code, signal })));
  }

  async prompt(message: string, timeoutMs = 60_000): Promise<Json> {
    const id = `e2e-${++this.#next}`;
    let timer: NodeJS.Timeout | undefined;
    const response = new Promise<Json>((resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`No RPC response to ${message} in ${timeoutMs} ms. Stderr: ${this.stderr}`)), timeoutMs);
      this.#pending.set(id, resolve);
    });
    const exited = this.exited.then(exit => {
      throw new Error(`Pi exited (${exit.code}/${exit.signal}) before answering ${message}. Stderr: ${this.stderr}`);
    });
    this.child.stdin.write(`${JSON.stringify({ type: "prompt", id, message })}\n`);
    try { return await Promise.race([response, exited]); }
    finally { clearTimeout(timer); this.#pending.delete(id); }
  }
}

const until = async <T>(read: () => T | undefined, what: string, timeoutMs: number, pi: RpcProcess): Promise<T> => {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = read();
    if (value !== undefined) return value;
    if (pi.child.exitCode !== null) throw new Error(`Pi exited (${pi.child.exitCode}) while waiting for ${what}. Stderr: ${pi.stderr}`);
    if (Date.now() >= deadline) throw new Error(`Timed out after ${timeoutMs} ms waiting for ${what}. Stderr: ${pi.stderr}`);
    await delay(50);
  }
};

const adapterEvents = (report: string): Json[] => {
  const file = path.join(report, "adapter.jsonl");
  if (!fs.existsSync(file)) return [];
  return fs.readFileSync(file, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line) as Json);
};

describe.runIf(process.platform !== "win32" && process.env.FABRIC_8382_REAL_PI === "1")(
  "real Pi: two back-to-back in-process /reloads keep one live Main heartbeat (smarty-dev#8382)", () => {
    it.each([
      // Default mesh.announce=false: first use publishes the Main; #660's re-arm hint carries it across reloads.
      ["first-use", false],
      // mesh.announce=true: every generation's session_start activates eagerly.
      ["announce", true],
    ] as const)("%s activation: one live heartbeat, lease renewed after the second reload, no stale ctx, no degraded report", async (mode, announce) => {
      expect(fs.existsSync(entry), "run `bun run build` first: this proves the compiled bundle").toBe(true);
      const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-two-reload-")));
      onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
      const home = path.join(root, "home");
      const agentDir = path.join(home, ".pi", "agent");
      const cwd = path.join(root, "project");
      const meshRoot = path.join(root, "mesh");
      const report = path.join(root, "report");
      const tmp = path.join(root, "tmp");
      for (const dir of [agentDir, cwd, report, tmp]) fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ compaction: { enabled: false }, retry: { enabled: false } }));
      fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({
        executor: { kernel: "typescript" }, fullCodeMode: false, mesh: { enabled: true, announce },
        mcp: { enabled: false }, jev: { enabled: false }, memory: { enabled: false },
        compaction: { engine: "pi" }, entropy: { compile: false }, speculation: { enabled: false },
      }));
      // A private HOME, agent dir, mesh and tmp; nothing inherited from the caller's Pi or fleet session.
      const child = spawn(process.execPath, [cli, "--mode", "rpc", "--no-extensions", "--no-skills", "--no-prompt-templates",
        "--no-themes", "--no-context-files", "--approve", "--no-builtin-tools", "--offline", "--thinking", "off",
        "--session-dir", path.join(root, "sessions"), "-e", entry, "-e", adapter], {
        cwd, stdio: ["pipe", "pipe", "pipe"],
        env: {
          PATH: process.env.PATH ?? "", LANG: "C.UTF-8", HOME: home, TMPDIR: tmp, PI_OFFLINE: "1",
          PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_AGENT_DIR: path.join(root, "exports"), PI_FABRIC_MESH_ROOT: meshRoot,
          PI_FABRIC_PROJECT_ROOT: cwd, PI_FABRIC_RUN_ROOT: path.join(root, "runs"), FABRIC_8382_REPORT_DIR: report,
        },
      });
      const pi = new RpcProcess(child);
      const summary: Json = { mode, cli: fs.realpathSync(cli), pid: child.pid };
      const samples: Array<FabricHostLease & { observedAt: number }> = [];
      const censuses: Census[] = [];
      const census = async (): Promise<Census> => {
        expect(await pi.prompt("/e2e-census")).toMatchObject({ success: true });
        const value = JSON.parse(fs.readFileSync(path.join(report, "census.json"), "utf8")) as Census;
        censuses.push(value);
        return value;
      };
      try {
        const started = await until(() => adapterEvents(report).find(event => event.type === "session_start"),
          "generation 1 session_start", 60_000, pi);
        const sessionId = String(started.sessionId);
        const hostId = `session:${sessionId}`;
        const lease = () => readHostLeases(meshRoot).get(hostId);
        if (!announce) {
          expect(lease(), "announce=false publishes the Main only on first use").toBeUndefined();
          expect(await pi.prompt("/fabric status")).toMatchObject({ success: true }); // first Fabric use
        }
        await until(lease, "the Main's first participant lease", 30_000, pi);

        // Two in-process reloads, the second sent as soon as the first one's command returns.
        const reloadsAt = Date.now();
        for (let index = 0; index < 2; index++) expect(await pi.prompt("/reload")).toMatchObject({ success: true });
        summary.reloadsMs = Date.now() - reloadsAt;
        const events = adapterEvents(report);
        expect(events.filter(event => event.type === "factory").map(event => event.generation)).toEqual([1, 2, 3]);
        expect(events.filter(event => event.type === "session_start").map(event => [event.generation, event.reason, event.sessionId]))
          .toEqual([[1, "startup", sessionId], [2, "reload", sessionId], [3, "reload", sessionId]]);
        expect(events.filter(event => event.type === "session_shutdown").map(event => [event.generation, event.reason]))
          .toEqual([[1, "reload"], [2, "reload"]]);
        const thirdStartedAt = Number(events.find(event => event.type === "factory" && event.generation === 3)!.at);

        const early = await census();
        const armed = early.timers.find(timer => timer.generation === 3);
        expect(armed, "the third generation armed a participant heartbeat").toBeDefined();
        const heartbeatMs = armed!.delay;
        expect(heartbeatMs).toBeGreaterThan(0);
        summary.heartbeatMs = heartbeatMs;

        // Observe past LIFECYCLE_LOST_TICKS + 1 heartbeats of the third generation: long enough for
        // #660's lost-lifecycle report to have fired had any heartbeat found no live session ctx.
        const windowEnd = thirdStartedAt + (LIFECYCLE_LOST_TICKS + 1) * heartbeatMs + 1_500;
        while (Date.now() < windowEnd) {
          if (child.exitCode !== null) throw new Error(`Pi exited (${child.exitCode}) mid-observation. Stderr: ${pi.stderr}`);
          const current = lease();
          const last = samples.at(-1);
          if (current && (last?.updatedAt !== current.updatedAt || last.expiresAt !== current.expiresAt)) {
            samples.push({ ...current, observedAt: Date.now() });
          }
          await delay(100);
        }

        // (1) Exactly one live participant heartbeat timer: the third generation's.
        const final = await census();
        const live = final.timers.filter(timer => timer.live);
        expect(live).toHaveLength(1);
        expect(live[0]).toMatchObject({ generation: 3, delay: heartbeatMs });
        expect([...new Set(final.timers.map(timer => timer.generation))].sort()).toEqual([1, 2, 3]);

        // (2) The third incarnation renews its own ordinary lease, at least one interval after it started.
        const renewals = samples.filter(sample => sample.startedAt !== undefined && sample.startedAt >= thirdStartedAt &&
          sample.reloadUntil === undefined);
        expect(renewals.length).toBeGreaterThanOrEqual(2);
        expect(new Set(renewals.map(sample => sample.startedAt)).size).toBe(1);
        const [first, last] = [renewals[0]!, renewals.at(-1)!];
        expect(last.updatedAt).toBeGreaterThanOrEqual(thirdStartedAt + heartbeatMs);
        expect(last.expiresAt - first.expiresAt).toBeGreaterThanOrEqual(heartbeatMs);
        expect(last.expiresAt).toBeGreaterThan(Date.now());
        Object.assign(summary, {
          sessionId, thirdStartedAt,
          renewalsAfterThirdStartMs: renewals.map(sample => sample.updatedAt - thirdStartedAt),
          expiryAdvanceMs: last.expiresAt - first.expiresAt,
          timersCreated: final.timers.length, timersLive: live.length, earlyLive: early.timers.filter(timer => timer.live).length,
        });
      } finally {
        child.stdin.end(); // RPC input end: Pi's own clean shutdown, exit 0.
        const exit = await Promise.race([pi.exited, delay(30_000).then(() => undefined)]);
        if (!exit) child.kill("SIGKILL");
        summary.exit = exit ?? "killed after 30 s";
        const evidence = process.env.FABRIC_8382_EVIDENCE_DIR;
        if (evidence) {
          const dest = path.join(evidence, mode);
          fs.mkdirSync(dest, { recursive: true });
          fs.writeFileSync(path.join(dest, "stderr.log"), pi.stderr);
          fs.writeFileSync(path.join(dest, "rpc-records.json"), JSON.stringify(pi.records, null, 2));
          fs.writeFileSync(path.join(dest, "lease-samples.json"), JSON.stringify(samples, null, 2));
          fs.writeFileSync(path.join(dest, "census.json"), JSON.stringify(censuses, null, 2));
          if (fs.existsSync(path.join(report, "adapter.jsonl"))) fs.copyFileSync(path.join(report, "adapter.jsonl"), path.join(dest, "adapter.jsonl"));
        }
      }

      // Both the Pi process and its shutdown are clean.
      expect(summary.exit).toEqual({ code: 0, signal: null });
      // (3) No stale-ctx use anywhere: stderr, extension errors or any RPC record.
      const stale = [...pi.stderr.split("\n"), ...pi.records.map(record => JSON.stringify(record))].filter(line => STALE.test(line));
      expect(stale).toEqual([]);
      // (4) #660's degraded report is not raised for a healthy current binding: no warning, no ops event.
      expect(pi.stderr.split("\n").filter(line => DEGRADED.test(line))).toEqual([]);
      const presence = new MeshStore(meshRoot, 64 * 1024, 1_000).read({ topic: "ops.fabric.presence" });
      expect(presence.filter(event => event.kind === "fabric.presence.degraded")).toEqual([]);
      // No extension error, no refused command, and no model turn (no provider call).
      expect(pi.records.filter(record => record.type === "extension_error")).toEqual([]);
      expect(pi.records.filter(record => record.type === "response" && record.success !== true)).toEqual([]);
      expect(pi.records.filter(record => record.type === "agent_start" || record.type === "message_start")).toEqual([]);
      Object.assign(summary, { staleLines: stale.length, presenceEvents: presence.length, rpcRecords: pi.records.length });
      console.log(`[smarty-dev#8382] ${JSON.stringify(summary)}`);
      const evidence = process.env.FABRIC_8382_EVIDENCE_DIR;
      if (evidence) fs.writeFileSync(path.join(evidence, mode, "summary.json"), JSON.stringify(summary, null, 2));
    }, 150_000);
  });
