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

// smarty-dev#8382 (follow-up to pi-fabric#660) and smarty-dev#8588: the compiled Fabric in a real Pi
// process across in-process /reloads and a session replacement. Three lifecycles:
//   awaited      two /reloads, the second sent once the first one's command answered;
//   overlapping  both /reload prompts in ONE stdin write, so the second reload starts while the first
//                one's session_shutdown teardown is still running (#8588's reload-during-reload race);
//   replacement  a /reload, then Pi's own RPC `new_session` (AgentSessionRuntime.newSession():
//                session_shutdown "new" -> a fresh AgentSession and extension generation -> session_start "new").
// After each transition: exactly one live participant heartbeat timer (the live generation's), the live
// Main's lease renews after it, no stale-ctx line and no #660 degraded report.
//
// Pi's RPC mode has no native reload command, so the test-only adapter extension
// tests/fixtures/two-reload-adapter-8382.ts registers /reload (ctx.reload(), the same
// AgentSession.reload() as an interactive /reload) and a participant heartbeat census. The session
// replacement needs no adapter: `new_session` is in Pi 0.87's RPC protocol. No model or provider is
// called: every prompt is an extension command.
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
type LeaseSample = FabricHostLease & { observedAt: number };

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

  /** Writes every command in ONE stdin write: each one is sent before Pi answers any earlier one. */
  send(commands: Json[], timeoutMs = 60_000): Array<Promise<Json>> {
    const sent = commands.map(command => {
      const record = { ...command, id: `e2e-${++this.#next}` };
      let timer: NodeJS.Timeout | undefined;
      const response = new Promise<Json>((resolve, reject) => {
        timer = setTimeout(() => reject(new Error(`No RPC response to ${JSON.stringify(command)} in ${timeoutMs} ms. Stderr: ${this.stderr}`)), timeoutMs);
        this.#pending.set(record.id, resolve);
      });
      const exited = this.exited.then(exit => {
        throw new Error(`Pi exited (${exit.code}/${exit.signal}) before answering ${JSON.stringify(command)}. Stderr: ${this.stderr}`);
      });
      const answered = Promise.race([response, exited]).finally(() => { clearTimeout(timer); this.#pending.delete(record.id); });
      return { record, answered };
    });
    this.child.stdin.write(sent.map(({ record }) => `${JSON.stringify(record)}\n`).join(""));
    return sent.map(({ answered }) => answered);
  }

  request(command: Json, timeoutMs?: number): Promise<Json> { return this.send([command], timeoutMs)[0]!; }

  prompt(message: string, timeoutMs?: number): Promise<Json> { return this.request({ type: "prompt", message }, timeoutMs); }
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

const ofType = (events: Json[], type: string): Json[] => events.filter(event => event.type === type);

/** One real Pi RPC process with Fabric and the adapter, in a private HOME, agent dir, mesh and tmp. */
class RealPi {
  readonly pi: RpcProcess;
  readonly summary: Json;
  readonly samples: Record<string, LeaseSample[]> = {};
  readonly censuses: Census[] = [];
  readonly #report: string;

  constructor(readonly scenario: string, readonly mode: string, readonly announce: boolean, readonly root: string) {
    const home = path.join(root, "home");
    const agentDir = path.join(home, ".pi", "agent");
    const cwd = path.join(root, "project");
    const tmp = path.join(root, "tmp");
    this.#report = path.join(root, "report");
    for (const dir of [agentDir, cwd, this.#report, tmp]) fs.mkdirSync(dir, { recursive: true });
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
        PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_AGENT_DIR: path.join(root, "exports"), PI_FABRIC_MESH_ROOT: this.meshRoot,
        PI_FABRIC_PROJECT_ROOT: cwd, PI_FABRIC_RUN_ROOT: path.join(root, "runs"), FABRIC_8382_REPORT_DIR: this.#report,
      },
    });
    this.pi = new RpcProcess(child);
    this.summary = { scenario, mode, cli: fs.realpathSync(cli), pid: child.pid, transitions: [] };
  }

  get meshRoot(): string { return path.join(this.root, "mesh"); }

  events(): Json[] { return adapterEvents(this.#report); }

  lease(sessionId: string): FabricHostLease | undefined { return readHostLeases(this.meshRoot).get(`session:${sessionId}`); }

  /** The census as the LIVE generation's /e2e-census command writes it: its `generation` is the live one. */
  async census(): Promise<Census> {
    expect(await this.pi.prompt("/e2e-census")).toMatchObject({ success: true });
    const value = JSON.parse(fs.readFileSync(path.join(this.#report, "census.json"), "utf8")) as Census;
    this.censuses.push(value);
    return value;
  }

  /** Generation 1 started; under first use (announce=false) Fabric is used once. Its Main lease is up. */
  async boot(): Promise<string> {
    const started = await until(() => ofType(this.events(), "session_start")[0], "generation 1 session_start", 60_000, this.pi);
    const sessionId = String(started.sessionId);
    if (!this.announce) {
      expect(this.lease(sessionId), "announce=false publishes the Main only on first use").toBeUndefined();
      expect(await this.pi.prompt("/fabric status")).toMatchObject({ success: true }); // first Fabric use
    }
    await until(() => this.lease(sessionId), "the Main's first participant lease", 30_000, this.pi);
    return sessionId;
  }

  stale(): string[] {
    return [...this.pi.stderr.split("\n"), ...this.pi.records.map(record => JSON.stringify(record))].filter(line => STALE.test(line));
  }

  degradedEvents(): Json[] {
    return new MeshStore(this.meshRoot, 64 * 1024, 1_000).read({ topic: "ops.fabric.presence" })
      .filter(event => event.kind === "fabric.presence.degraded");
  }

  /** (3) no stale-ctx line and (4) no #660 degraded report so far, and no extension error or refused command. */
  expectHealthy(label: string): void {
    expect(this.stale(), `${label}: no stale-ctx line`).toEqual([]);
    expect(this.pi.stderr.split("\n").filter(line => DEGRADED.test(line)), `${label}: no degraded warning`).toEqual([]);
    expect(this.degradedEvents(), `${label}: no fabric.presence.degraded event`).toEqual([]);
    expect(this.pi.records.filter(record => record.type === "extension_error"), `${label}: no extension error`).toEqual([]);
    expect(this.pi.records.filter(record => record.type === "response" && record.success !== true), `${label}: no refused command`).toEqual([]);
  }

  /**
   * After a transition to `generation` (its factory ran at `since`, before its session_start activated
   * Fabric) on the Main `sessionId`: observe past LIFECYCLE_LOST_TICKS + 1 of its heartbeats, long enough
   * for #660's lost-lifecycle report to have fired had any heartbeat found no live session ctx, then
   * assert (1) one live heartbeat timer, the live generation's, (2) its own lease renewed at least one
   * interval after it started, and (3)/(4) via expectHealthy.
   */
  async settled(label: string, generation: number, sessionId: string): Promise<Json> {
    const since = Number(ofType(this.events(), "factory").find(event => event.generation === generation)!.at);
    const early = await this.census();
    expect(early.generation, `${label}: the live command runner is generation ${generation}`).toBe(generation);
    const armed = early.timers.find(timer => timer.generation === generation);
    expect(armed, `${label}: generation ${generation} armed a participant heartbeat`).toBeDefined();
    const heartbeatMs = armed!.delay;
    expect(heartbeatMs).toBeGreaterThan(0);
    const samples: LeaseSample[] = (this.samples[label] = []);
    const windowEnd = since + (LIFECYCLE_LOST_TICKS + 1) * heartbeatMs + 1_500;
    while (Date.now() < windowEnd) {
      if (this.pi.child.exitCode !== null) throw new Error(`Pi exited (${this.pi.child.exitCode}) mid-observation. Stderr: ${this.pi.stderr}`);
      const current = this.lease(sessionId);
      const last = samples.at(-1);
      if (current && (last?.updatedAt !== current.updatedAt || last.expiresAt !== current.expiresAt)) {
        samples.push({ ...current, observedAt: Date.now() });
      }
      await delay(100);
    }

    // (1) Exactly one live participant heartbeat timer: the live generation's.
    const final = await this.census();
    expect(final.generation).toBe(generation);
    const live = final.timers.filter(timer => timer.live);
    expect(live, `${label}: exactly one live participant heartbeat timer`).toHaveLength(1);
    expect(live[0]).toMatchObject({ generation, delay: heartbeatMs });

    // (2) The live generation renews its own ordinary lease, at least one interval after it started.
    const renewals = samples.filter(sample => sample.startedAt !== undefined && sample.startedAt >= since &&
      sample.reloadUntil === undefined);
    expect(renewals.length, `${label}: lease renewals after generation ${generation} started`).toBeGreaterThanOrEqual(2);
    expect(new Set(renewals.map(sample => sample.startedAt)).size).toBe(1);
    const [first, last] = [renewals[0]!, renewals.at(-1)!];
    expect(last.updatedAt).toBeGreaterThanOrEqual(since + heartbeatMs);
    expect(last.expiresAt - first.expiresAt).toBeGreaterThanOrEqual(heartbeatMs);
    expect(last.expiresAt).toBeGreaterThan(Date.now());

    // (3) and (4), so far.
    this.expectHealthy(label);
    const result = {
      label, generation, sessionId, since, heartbeatMs,
      renewalsAfterStartMs: renewals.map(sample => sample.updatedAt - since),
      expiryAdvanceMs: last.expiresAt - first.expiresAt,
      timersCreated: final.timers.length, timersLive: live.length, earlyLive: early.timers.filter(timer => timer.live).length,
    };
    (this.summary.transitions as Json[]).push(result);
    return result;
  }

  /** RPC input end is Pi's own clean shutdown, exit 0. Writes the evidence when FABRIC_8382_EVIDENCE_DIR is set. */
  async stop(): Promise<void> {
    this.pi.child.stdin.end();
    const exit = await Promise.race([this.pi.exited, delay(30_000).then(() => undefined)]);
    if (!exit) this.pi.child.kill("SIGKILL");
    this.summary.exit = exit ?? "killed after 30 s";
    const evidence = this.evidenceDir();
    if (!evidence) return;
    fs.writeFileSync(path.join(evidence, "stderr.log"), this.pi.stderr);
    fs.writeFileSync(path.join(evidence, "rpc-records.json"), JSON.stringify(this.pi.records, null, 2));
    fs.writeFileSync(path.join(evidence, "lease-samples.json"), JSON.stringify(this.samples, null, 2));
    fs.writeFileSync(path.join(evidence, "census.json"), JSON.stringify(this.censuses, null, 2));
    const events = path.join(this.#report, "adapter.jsonl");
    if (fs.existsSync(events)) fs.copyFileSync(events, path.join(evidence, "adapter.jsonl"));
  }

  /** Final: clean exit, no stale ctx, no degraded report, no refused command and no model turn. */
  finish(): void {
    expect(this.summary.exit).toEqual({ code: 0, signal: null });
    this.expectHealthy("after exit");
    expect(this.pi.records.filter(record => record.type === "agent_start" || record.type === "message_start")).toEqual([]);
    Object.assign(this.summary, {
      staleLines: this.stale().length,
      presenceEvents: new MeshStore(this.meshRoot, 64 * 1024, 1_000).read({ topic: "ops.fabric.presence" }).length,
      rpcRecords: this.pi.records.length,
    });
    console.log(`[smarty-dev#8382] ${JSON.stringify(this.summary)}`);
    const evidence = this.evidenceDir();
    if (evidence) fs.writeFileSync(path.join(evidence, "summary.json"), JSON.stringify(this.summary, null, 2));
  }

  evidenceDir(): string | undefined {
    const evidence = process.env.FABRIC_8382_EVIDENCE_DIR;
    if (!evidence) return undefined;
    const dest = path.join(evidence, `${this.scenario}-${this.mode}`);
    fs.mkdirSync(dest, { recursive: true });
    return dest;
  }
}

type Drive = (main: RealPi, sessionId: string) => Promise<void>;

const scenarios: Record<string, Drive> = {
  // Two in-process reloads, the second sent as soon as the first one's command answered.
  awaited: async (main, sessionId) => {
    const reloadsAt = Date.now();
    for (let index = 0; index < 2; index++) expect(await main.pi.prompt("/reload")).toMatchObject({ success: true });
    main.summary.reloadsMs = Date.now() - reloadsAt;
    const events = main.events();
    expect(ofType(events, "factory").map(event => event.generation)).toEqual([1, 2, 3]);
    expect(ofType(events, "session_start").map(event => [event.generation, event.reason, event.sessionId]))
      .toEqual([[1, "startup", sessionId], [2, "reload", sessionId], [3, "reload", sessionId]]);
    expect(ofType(events, "session_shutdown").map(event => [event.generation, event.reason]))
      .toEqual([[1, "reload"], [2, "reload"]]);
    await main.settled("after the second reload", 3, sessionId);
  },

  // smarty-dev#8588: both /reload prompts in one stdin write. Pi dispatches RPC input lines without
  // waiting for the previous command, so the second /reload runs while the first reload's
  // session_shutdown (Fabric's teardown) is still pending, before either command answers.
  overlapping: async (main, sessionId) => {
    const reloadsAt = Date.now();
    const answers = await Promise.all(main.pi.send([{ type: "prompt", message: "/reload" }, { type: "prompt", message: "/reload" }]));
    for (const answer of answers) expect(answer).toMatchObject({ success: true });
    main.summary.reloadsMs = Date.now() - reloadsAt;
    const events = main.events();
    const seq = (type: string, match: (event: Json) => boolean): number => {
      const found = events.find(event => event.type === type && match(event));
      expect(found, `${type} recorded`).toBeDefined();
      return Number(found!.seq);
    };
    // Both reloads ran generation 1's command: the second arrived before the first one invalidated it.
    expect(ofType(events, "reload").map(event => [event.generation, event.call])).toEqual([[1, 1], [1, 2]]);
    // The overlap itself: the second reload started before the first one finished and before generation
    // 1's teardown finished (the adapter's session_shutdown handler runs after Fabric's, which it awaits).
    const secondStarted = seq("reload", event => event.call === 2);
    expect(secondStarted).toBeLessThan(seq("reload_done", event => event.call === 1));
    expect(secondStarted).toBeLessThan(seq("session_shutdown", event => event.generation === 1));
    // Whatever generations the race built, the live runner is the one that answers commands now.
    const { generation } = await main.census();
    const liveStarts = ofType(events, "session_start").filter(event => event.generation === generation);
    expect(liveStarts.length, `live generation ${generation} started`).toBeGreaterThanOrEqual(1);
    for (const start of liveStarts) expect([start.reason, start.sessionId]).toEqual(["reload", sessionId]);
    // A generation that received session_start but never session_shutdown keeps whatever it armed.
    const started = new Set(ofType(events, "session_start").map(event => Number(event.generation)));
    const shut = new Set(ofType(events, "session_shutdown").map(event => Number(event.generation)));
    const orphaned = [...started].filter(owner => owner !== generation && !shut.has(owner));
    Object.assign(main.summary, {
      liveGeneration: generation, orphanedGenerations: orphaned,
      lifecycle: events.map(event =>
        `${event.seq}:${event.type}${event.reason ? `(${event.reason})` : ""}${event.call ? `#${event.call}` : ""}@g${event.generation}`),
    });
    await main.settled("after the overlapping reloads", generation, sessionId);
    // Pi 0.87's AgentSession.reload() is not reentrant: both overlapping calls tear down generation 1,
    // and the generation built in between is replaced without session_shutdown (a Pi bug Fabric
    // cannot repair). Fabric's own guarantee is that such an orphan keeps nothing alive: the census
    // above found exactly one live heartbeat timer, and the orphan's was retired by the newer
    // instance's runtime-epoch claim (pi-fabric#808), not cleared by a teardown that never came.
    main.summary.heartbeatRetirements = main.pi.stderr.split("\n")
      .filter(line => line.includes("participant heartbeat/change refresh: retired obsolete background timer")).length;
    if (orphaned.length > 0) {
      expect(main.summary.heartbeatRetirements, "an orphaned generation's heartbeat was retired").toBeGreaterThanOrEqual(orphaned.length);
    }
    expect(shut.has(generation)).toBe(false);
  },

  // smarty-dev#8382: a /reload, then a real session replacement through Pi's own RPC `new_session`.
  replacement: async (main, sessionId) => {
    expect(await main.pi.prompt("/reload")).toMatchObject({ success: true });
    await main.settled("after the reload", 2, sessionId);
    const replaced = await main.pi.request({ type: "new_session" });
    expect(replaced).toMatchObject({ command: "new_session", success: true, data: { cancelled: false } });
    const events = main.events();
    expect(ofType(events, "factory").map(event => event.generation)).toEqual([1, 2, 3]);
    const starts = ofType(events, "session_start").map(event => [event.generation, event.reason, event.sessionId]);
    const next = String(starts.at(-1)?.[2]);
    expect(next).not.toBe(sessionId);
    expect(starts.slice(0, 2)).toEqual([[1, "startup", sessionId], [2, "reload", sessionId]]);
    // Pi 0.87's RPC new_session binds the new session twice: AgentSessionRuntime.newSession() rebinds
    // through the host's hook (finishSessionReplacement), then the RPC handler rebinds again, and each
    // bindExtensions() emits session_start. Fabric must stay correct under that repeat.
    const replacementStarts = starts.slice(2);
    expect(replacementStarts.length).toBeGreaterThanOrEqual(1);
    for (const start of replacementStarts) expect(start).toEqual([3, "new", next]);
    main.summary.replacementSessionStarts = replacementStarts.length;
    expect(ofType(events, "session_shutdown").map(event => [event.generation, event.reason]))
      .toEqual([[1, "reload"], [2, "new"]]);
    // Pi's own view agrees: the runtime now runs the new session.
    expect(await main.pi.request({ type: "get_state" })).toMatchObject({ success: true, data: { sessionId: next } });
    main.summary.replacedSessionId = next;
    await main.settled("after the session replacement", 3, next);
    // The replaced Main withdrew its lease instead of leaving it to lapse.
    expect(main.lease(sessionId), "the replaced Main's lease is withdrawn").toBeUndefined();
  },
};

describe.runIf(process.platform !== "win32" && process.env.FABRIC_8382_REAL_PI === "1")(
  "real Pi: in-process /reloads and a session replacement keep one live Main heartbeat (smarty-dev#8382, #8588)", () => {
    const cases = Object.keys(scenarios).flatMap(scenario => [
      // Default mesh.announce=false: first use publishes the Main; #660's re-arm hint carries it across
      // reloads and session replacement.
      [scenario, "first-use", false],
      // mesh.announce=true: every generation's session_start activates eagerly.
      [scenario, "announce", true],
    ] as const);
    it.each(cases)("%s, %s activation: one live heartbeat, lease renewed after each transition, no stale ctx, no degraded report",
      async (scenario, mode, announce) => {
        expect(fs.existsSync(entry), "run `bun run build` first: this proves the compiled bundle").toBe(true);
        const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-two-reload-")));
        onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
        const main = new RealPi(scenario, mode, announce, root);
        try {
          await scenarios[scenario]!(main, await main.boot());
        } finally {
          await main.stop();
        }
        main.finish();
      }, 150_000);
  });
