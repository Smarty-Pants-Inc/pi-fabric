import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { describe, expect, it, onTestFinished } from "vitest";

// smarty-dev#8467 (follow-up to pi-fabric#606, smarty-dev#6622): the compiled Fabric in a real Pi
// process leaves no mesh state temp or staging entry behind when it ends, while a large state write
// loop is in flight and the mesh lock is busy (held by another live process, here this test). Three
// ways to end it:
//   (1) a normal exit: RPC input end, Pi's own clean shutdown (exit 0). Every platform.
//   (2) SIGTERM: Pi's handled shutdown (exit 143). Not on Windows.
//   (3) SIGKILL during a write: the loop's next write is staged and waiting for the lock. It leaves
//       its staged temp; the next Pi start on the same mesh root must reap it. Not on Windows.
// The model is an offline faux provider (tests/fixtures/exit-cleanup-8467.ts): each prompt is one
// fabric_exec call, so every write goes through the shipped mesh store in dist/.
//
// In Pi 0.87 a graceful exit (1, 2) waits for Fabric's shutdown, which outlasts a staged write's
// 10 s lock timeout, so that write's own cleanup normally removes its temp first; #606's exit hook
// is the backstop when it has not. A kill inside the synchronous staging write itself (not covered
// here) leaves `...prepared.tmp.<pid>.<uuid>.tmp`, which only the host reaper's janitor removes.
//
// Run explicitly against a fresh build:
//   bun run build && FABRIC_8467_REAL_PI=1 bunx vitest run tests/mesh-exit-cleanup-real-pi.e2e.test.ts
// FABRIC_8467_PI_CLI=<installed pi cli.js> selects another host; FABRIC_8467_EVIDENCE_DIR keeps evidence.

const repo = fileURLToPath(new URL("../", import.meta.url));
const entry = path.join(repo, "dist/index.js");
const fixture = path.join(repo, "tests/fixtures/exit-cleanup-8467.ts");
const cli = process.env.FABRIC_8467_PI_CLI || path.join(repo, "node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
const evidenceDir = process.env.FABRIC_8467_EVIDENCE_DIR;

type Json = Record<string, any>;
interface Exit { code: number | null; signal: NodeJS.Signals | null }

// The RPC client and wait helper of tests/main-two-reload-e2e.test.ts (smarty-dev#8382).
/** A minimal JSONL client for `pi --mode rpc`. Ending stdin is Pi's clean exit 0; SIGTERM its handled exit 143. */
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
    const what = message.slice(0, 60);
    let timer: NodeJS.Timeout | undefined;
    const response = new Promise<Json>((resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`No RPC response to ${what} in ${timeoutMs} ms. Stderr: ${this.stderr}`)), timeoutMs);
      this.#pending.set(id, resolve);
    });
    const exited = this.exited.then(exit => {
      throw new Error(`Pi exited (${exit.code}/${exit.signal}) before answering ${what}. Stderr: ${this.stderr}`);
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
    await delay(20);
  }
};

// Any temp (`*.tmp`, `state.json.<pid>.<uuid>.prepared.tmp` and its inner `...prepared.tmp.<pid>.<uuid>.tmp`,
// `state.json.<pid>.<uuid>.tmp`, `state.read-signal.json.<pid>.<uuid>.tmp`) and any lock staging
// directory (mesh/fence `.lock.pending.<token>.XXXXXX`, host lease `.pending-XXXXXX`, custody
// `.p.<token>.XXXXXX`), at any depth under the mesh root.
const TEMP = /\.tmp(?:\.|$)/;
const STAGING = /\.pending[.-]|\.p\.[^/\\]+\.[A-Za-z0-9]{6}$/;
const leftovers = (meshRoot: string): string[] => {
  const found: string[] = [];
  const visit = (dir: string): void => {
    let entries: fs.Dirent[];
    try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch { return; }
    for (const item of entries) {
      const full = path.join(dir, item.name);
      if (TEMP.test(item.name) || STAGING.test(item.name)) found.push(path.relative(meshRoot, full).split(path.sep).join("/"));
      if (item.isDirectory()) visit(full);
    }
  };
  visit(meshRoot);
  return found.sort();
};
/** The staged state temps of one writer pid: `state.json.<pid>.<uuid>.prepared.tmp`. */
const stagedBy = (meshRoot: string, pid: number): string[] => {
  try {
    return fs.readdirSync(meshRoot).filter(name => name.startsWith(`state.json.${pid}.`) && name.endsWith(".prepared.tmp")).sort();
  } catch { return []; }
};

// The mesh lock held by another live process (this test runner). Its owner record carries no
// process incarnation, so Fabric's stale-lock recovery fails closed and never takes it over
// (src/mesh/mesh-lock.ts #clearStaleLock); Fabric's writers wait for it as for a busy mesh.
const holdMeshLock = async (meshRoot: string): Promise<() => void> => {
  const lock = path.join(meshRoot, ".lock");
  const deadline = Date.now() + 30_000;
  for (;;) {
    try { fs.mkdirSync(lock, { mode: 0o700 }); break; } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST" || Date.now() > deadline) throw error;
      await delay(2); // Pi holds it for a commit right now.
    }
  }
  fs.writeFileSync(path.join(lock, "owner"), `e2e-8467\n${process.pid}\n${Date.now()}\n`, { mode: 0o600 });
  let held = true;
  const release = (): void => { if (held) fs.rmSync(lock, { recursive: true, force: true }); held = false; };
  onTestFinished(release);
  return release;
};

interface Scratch { root: string; home: string; agentDir: string; cwd: string; meshRoot: string; tmp: string }
/** A private HOME, agent dir, mesh root and tmp: nothing is inherited from the caller's Pi or fleet session. */
const scratch = (): Scratch => {
  expect(fs.existsSync(entry), "run `bun run build` first: this proves the compiled bundle").toBe(true);
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-exit-cleanup-")));
  onTestFinished(() => fs.rmSync(root, { recursive: true, force: true }));
  const home = path.join(root, "home");
  const s: Scratch = { root, home, agentDir: path.join(home, ".pi", "agent"), cwd: path.join(root, "project"),
    meshRoot: path.join(root, "mesh"), tmp: path.join(root, "tmp") };
  for (const dir of [s.agentDir, s.cwd, s.tmp]) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(s.agentDir, "settings.json"), JSON.stringify({ compaction: { enabled: false }, retry: { enabled: false } }));
  // mesh.announce: every session_start opens the mesh store at once, so a next start is a real reaper pass.
  fs.writeFileSync(path.join(s.agentDir, "fabric.json"), JSON.stringify({
    executor: { kernel: "typescript" }, fullCodeMode: false, mesh: { enabled: true, announce: true },
    mcp: { enabled: false }, jev: { enabled: false }, memory: { enabled: false },
    compaction: { engine: "pi" }, entropy: { compile: false }, speculation: { enabled: false },
  }));
  return s;
};

interface Launched { pi: RpcProcess; pid: number }
const launch = async (s: Scratch, run: string): Promise<Launched> => {
  const report = path.join(s.root, `report-${run}`);
  fs.mkdirSync(report, { recursive: true });
  const child = spawn(process.execPath, [cli, "--mode", "rpc", "--no-extensions", "--no-skills", "--no-prompt-templates",
    "--no-themes", "--no-context-files", "--approve", "--no-builtin-tools", "--offline", "--thinking", "off",
    "--provider", "exit-8467", "--model", "faux-1", "--session-dir", path.join(s.root, "sessions"), "-e", entry, "-e", fixture], {
    cwd: s.cwd, stdio: ["pipe", "pipe", "pipe"],
    env: {
      PATH: process.env.PATH ?? "", LANG: "C.UTF-8", HOME: s.home, USERPROFILE: s.home, TMPDIR: s.tmp, TEMP: s.tmp, TMP: s.tmp,
      ...(process.env.SYSTEMROOT ? { SYSTEMROOT: process.env.SYSTEMROOT } : {}), PI_OFFLINE: "1",
      PI_CODING_AGENT_DIR: s.agentDir, PI_FABRIC_AGENT_DIR: path.join(s.root, "exports"), PI_FABRIC_MESH_ROOT: s.meshRoot,
      PI_FABRIC_PROJECT_ROOT: s.cwd, PI_FABRIC_RUN_ROOT: path.join(s.root, "runs"), FABRIC_8467_REPORT_DIR: report,
    },
  });
  const pi = new RpcProcess(child);
  onTestFinished(() => { if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL"); });
  const started = await until(() => {
    try { return JSON.parse(fs.readFileSync(path.join(report, "ready.json"), "utf8")) as { pid: number }; } catch { return undefined; }
  }, `${run} session_start`, 60_000, pi);
  return { pi, pid: started.pid };
};

const startExec = async (pi: RpcProcess, code: string): Promise<number> => {
  const from = pi.records.length;
  expect(await pi.prompt(`EXEC ${code}`)).toMatchObject({ success: true });
  return from;
};
const exec = async (pi: RpcProcess, code: string): Promise<string> => {
  const from = await startExec(pi, code);
  const ended = await until(() => pi.records.slice(from).find(record => record.type === "tool_execution_end" && record.toolName === "fabric_exec"),
    "fabric_exec result", 60_000, pi);
  const text = JSON.stringify(ended.result?.content ?? null);
  expect(ended.isError, text).toBe(false);
  return text;
};

const exitOf = async (pi: RpcProcess, timeoutMs = 120_000): Promise<Exit & { ms: number }> => {
  const at = Date.now();
  const exit = await Promise.race([pi.exited, delay(timeoutMs).then(() => undefined)]);
  if (!exit) { pi.child.kill("SIGKILL"); throw new Error(`Pi did not exit within ${timeoutMs} ms. Stderr: ${pi.stderr}`); }
  return { ...exit, ms: Date.now() - at };
};

const SEED = `const put = await mesh.put({ key: "e2e/8467/seed", value: { seeded: true } });
const event = await mesh.publish({ topic: "e2e.8467", kind: "seed", text: "mesh state written before the exit" });
return { version: put.version, sequence: event.sequence };`;
// ~9.6 MB of state (48 keys x 200 KiB; one value may hold at most 256 KiB): every put rewrites all
// of it, staged off-lock as state.json.<pid>.<uuid>.prepared.tmp. It runs until the exit ends it.
const WRITE_LOOP = `const pad = "x".repeat(200 * 1024);
for (let i = 0; i < 100000; i++) await mesh.put({ key: "e2e/8467/loop/" + (i % 48), value: { i, pad } });
return "loop ended";`;
const READ_BACK = `const seed = await mesh.get({ key: "e2e/8467/seed" });
const loop = await mesh.get({ key: "e2e/8467/loop/0" });
await mesh.put({ key: "e2e/8467/after-restart", value: true });
return { seeded: seed?.value?.seeded === true, loopWritten: typeof loop?.value?.i === "number" };`;

/** Mesh state is written, a large write loop runs, the lock goes busy and the loop's next write waits staged. */
const parkWriteLoop = async (s: Scratch, { pi, pid }: Launched): Promise<{ seed: string; staged: string[]; release: () => void }> => {
  const seed = await exec(pi, SEED);
  expect(fs.readFileSync(path.join(s.meshRoot, "state.json"), "utf8")).toContain("e2e/8467/seed");
  await startExec(pi, WRITE_LOOP);
  const statePath = path.join(s.meshRoot, "state.json");
  await until(() => { try { return fs.statSync(statePath).size > 2 * 1024 * 1024 ? true : undefined; } catch { return undefined; } },
    "the write loop past 2 MiB of state", 90_000, pi);
  const release = await holdMeshLock(s.meshRoot);
  // Staged and waiting: the same prepared temp on several polls (a put waits up to the 10 s lock timeout).
  let staged: string[] = [];
  let seen = 0;
  await until(() => {
    const now = stagedBy(s.meshRoot, pid);
    seen = now.length > 0 && now.join() === staged.join() ? seen + 1 : 0;
    staged = now;
    return seen >= 5 ? true : undefined;
  }, "a write staged and waiting for the busy mesh lock", 8_000, pi);
  return { seed, staged, release };
};

const keepEvidence = (name: string, summary: Json, ...runs: RpcProcess[]): void => {
  console.log(`[smarty-dev#8467] ${name} ${JSON.stringify(summary)}`);
  if (!evidenceDir) return;
  const dest = path.join(evidenceDir, name);
  fs.mkdirSync(dest, { recursive: true });
  fs.writeFileSync(path.join(dest, "summary.json"), JSON.stringify(summary, null, 2));
  runs.forEach((run, index) => {
    fs.writeFileSync(path.join(dest, `stderr-${index + 1}.log`), run.stderr);
    fs.writeFileSync(path.join(dest, `rpc-records-${index + 1}.json`), JSON.stringify(run.records, null, 2));
  });
};

const noHostErrors = (pi: RpcProcess): void => {
  expect(pi.records.filter(record => record.type === "extension_error")).toEqual([]);
  expect(pi.records.filter(record => record.type === "response" && record.success !== true)).toEqual([]);
};

describe.runIf(process.env.FABRIC_8467_REAL_PI === "1")(
  "real Pi: exits leave no mesh state temp or staging entry (smarty-dev#8467)", () => {
    const endsCleanly = async (name: string, end: (pi: RpcProcess) => void, expected: Exit): Promise<void> => {
      const s = scratch();
      const first = await launch(s, "first");
      const { seed, staged, release } = await parkWriteLoop(s, first);
      expect(staged.length).toBeGreaterThan(0);
      end(first.pi);
      const exit = await exitOf(first.pi);
      // Checked while the lock is still busy: the exiting process itself removed every temp.
      const left = leftovers(s.meshRoot);
      release();
      keepEvidence(name, { cli: fs.realpathSync(cli), pid: first.pid, seed, stagedBeforeExit: staged, exit, leftovers: left }, first.pi);
      expect({ code: exit.code, signal: exit.signal }).toEqual(expected);
      expect(left).toEqual([]);
      expect(leftovers(s.meshRoot)).toEqual([]);
      // The committed state is intact: the seed survives and the file still parses.
      const state = fs.readFileSync(path.join(s.meshRoot, "state.json"), "utf8");
      expect(() => JSON.parse(state)).not.toThrow();
      expect(state).toContain("e2e/8467/seed");
      noHostErrors(first.pi);
    };

    it("(1) a normal exit (RPC input end) while a staged write waits for the busy lock leaves no temp", async () => {
      await endsCleanly("normal-exit", pi => pi.child.stdin.end(), { code: 0, signal: null });
    }, 240_000);

    it.skipIf(process.platform === "win32")("(2) SIGTERM while a staged write waits for the busy lock leaves no temp", async () => {
      await endsCleanly("sigterm", pi => pi.child.kill("SIGTERM"), { code: 143, signal: null });
    }, 240_000);

    it.skipIf(process.platform === "win32")("(3) SIGKILL during a write loop: its staged temp is reaped by the next start", async () => {
      const s = scratch();
      const first = await launch(s, "first");
      const { seed, staged, release } = await parkWriteLoop(s, first);
      first.pi.child.kill("SIGKILL");
      const exit = await exitOf(first.pi);
      const killed = leftovers(s.meshRoot);
      release();
      expect(exit.signal).toBe("SIGKILL");
      // The kill landed during a write: its staged temp is left, and every leftover is the dead pid's.
      expect(killed).toEqual(expect.arrayContaining(staged));
      expect(killed.filter(name => !name.includes(`.${first.pid}.`))).toEqual([]);

      const second = await launch(s, "second");
      expect(second.pid).not.toBe(first.pid);
      // While the second Pi runs, its own in-flight writes may show their temps; none may be another pid's.
      const notLive = (): string[] => leftovers(s.meshRoot).filter(name => !name.includes(`.${second.pid}.`));
      const reapAt = Date.now();
      await until(() => notLive().length === 0 ? true : undefined, "the next start to reap the leftover", 30_000, second.pi);
      const reapedMs = Date.now() - reapAt;
      const readBack = await exec(second.pi, READ_BACK);
      expect(readBack).toContain("seeded: true");
      expect(readBack).toContain("loopWritten: true");
      const afterStart = notLive();
      second.pi.child.stdin.end();
      const secondExit = await exitOf(second.pi);
      const afterExit = leftovers(s.meshRoot);
      keepEvidence("sigkill", { cli: fs.realpathSync(cli), pid: first.pid, seed, stagedBeforeKill: staged, exit,
        leftAfterKill: killed, nextPid: second.pid, reapedWithinMsOfReady: reapedMs, readBack, afterStart, secondExit, afterExit },
        first.pi, second.pi);
      expect(afterStart).toEqual([]);
      expect({ code: secondExit.code, signal: secondExit.signal }).toEqual({ code: 0, signal: null });
      expect(afterExit).toEqual([]);
      noHostErrors(second.pi);
    }, 240_000);
  });
