import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { canRemoveTerminalRun } from "../src/storage/retention.js";

// Trust-boundary regressions for smarty-dev#10544 / pi-fabric#1192: native
// os.tmpdir()/mkdtemp and >50KiB spills must work without widening the grant.
// Each private fixture root is removed after its manager closes.
const managers: AgentManager[] = [];
const roots: string[] = [];
const scratchPaths: string[] = [];
const taskTmpPaths: string[] = [];
const outputPaths: string[] = [];
let taskTmpRoot = "";
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  for (const output of outputPaths.splice(0)) fs.rmSync(output, { recursive: true, force: true });
  for (const temporary of taskTmpPaths.splice(0)) fs.rmSync(temporary, { recursive: true, force: true });
  if (taskTmpRoot) {
    for (const directory of scratchPaths.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
  }
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  vi.unstubAllEnvs();
});

const fixture = (processSlice?: string, longRunRoot = false, retainRuns = true,
  taskTmpRootOverride?: (runRoot: string, shortRoot: string) => string) => {
  taskTmpRoot = fs.realpathSync(os.tmpdir());
  const root = fs.realpathSync(fs.mkdtempSync(path.join(taskTmpRoot, "fpt-")));
  roots.push(root);
  const cwd = path.join(taskTmpRoot, "c");
  const scratch = path.join(taskTmpRoot, "s");
  const scratchPath = (suffix: string): string => path.join(scratch, suffix);
  scratchPaths.push(cwd, scratch);
  const parentTemp = path.join(root, "parent-temp");
  const runRoot = path.join(root, longRunRoot ? `runs-${"x".repeat(64)}` : "runs");
  for (const directory of [cwd, scratch, parentTemp]) fs.mkdirSync(directory);
  if (processSlice && process.platform === "linux") {
    fs.writeFileSync(path.join(root, "systemd-run"), '#!/bin/sh\nprintf \'%s\\n\' "$@" > scope-args\nwhile [ "$1" != "--" ]; do shift; done; shift; exec "$@"\n', { mode: 0o700 });
    vi.stubEnv("PATH", `${root}${path.delimiter}${process.env.PATH ?? ""}`);
    vi.stubEnv("XDG_RUNTIME_DIR", root);
    vi.stubEnv("DBUS_SESSION_BUS_ADDRESS", undefined);
  }
  vi.stubEnv("TMPDIR", parentTemp);
  vi.stubEnv("TMP", parentTemp);
  vi.stubEnv("TEMP", parentTemp);

  const worker = path.join(root, "worker.mjs");
  fs.writeFileSync(worker, `import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i], process.argv[i + 1]);
// Mirror worker.ts exporting --tmpdir to its execution child's environment.
if (args.has('--tmpdir')) {
  for (const key of ['TMPDIR', 'TMP', 'TEMP']) process.env[key] = args.get('--tmpdir');
}
const tmp = os.tmpdir();
const output = fs.mkdtempSync(path.join(tmp, 'pi-output-'));
const payload = path.join(output, 'payload.bin');
fs.writeFileSync(payload, Buffer.alloc(64 * 1024, 0x5a));
const now = Date.now();
fs.writeFileSync(args.get('--status-file'), JSON.stringify({
  id: args.get('--id'), name: args.get('--name'), task: 'filesystem probe', status: 'completed',
  runner: args.get('--runner'), transport: 'process', sessionId: String(process.pid),
  cwd: args.get('--cwd'), startedAt: now, updatedAt: now, finishedAt: now, turns: 1, toolCalls: 0,
  text: JSON.stringify({ tmp, env: { TMPDIR: process.env.TMPDIR, TMP: process.env.TMP, TEMP: process.env.TEMP },
    argTmpdir: args.get('--tmpdir') ?? null, policy: JSON.parse(args.get('--write-policy') ?? 'null'), output, payload,
    bytes: fs.statSync(payload).size }),
  exitCode: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 }
}));`);
  const manager = new AgentManager(cwd, {
    ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [], timeoutMs: 8_000, retainRuns,
    ...(processSlice ? { processSlice } : {}),
  }, { workerPath: worker, fullCodeMode: false, runRoot,
    taskTmpRoot: taskTmpRootOverride?.(root, taskTmpRoot) ?? taskTmpRoot });
  managers.push(manager);
  return { root, cwd, scratch, scratchPath, parentTemp, runRoot, taskTmpRoot, manager, worker };
};

const report = async (f: ReturnType<typeof fixture>, request: Partial<Parameters<AgentManager["spawn"]>[0]>) => {
  const handle = await f.manager.spawn({ task: "filesystem probe", transport: "process", ...request });
  const result = await f.manager.wait(handle.id);
  expect(result.status, result.error).toBe("completed");
  const value = JSON.parse(result.text) as {
    tmp: string; env: Record<string, string | undefined>; argTmpdir: string | null;
    policy: { readOnly: boolean; writableRoots: string[]; shell: string } | null;
    output: string; payload: string; bytes: number;
  };
  const temporary = path.isAbsolute(value.tmp) ? value.tmp : path.resolve(f.cwd, value.tmp);
  if ([f.taskTmpRoot, f.cwd, f.scratch].includes(path.dirname(temporary)) &&
      /^(?:r|t)\.[A-Za-z0-9]{6}$/.test(path.basename(temporary))) taskTmpPaths.push(temporary);
  const output = path.isAbsolute(value.output) ? value.output : path.resolve(f.cwd, value.output);
  if (path.dirname(output) === temporary && /^pi-output-[A-Za-z0-9]{6}$/.test(path.basename(output))) outputPaths.push(output);
  return { result, value };
};

const inside = (root: string, candidate: string) => {
  const relative = path.relative(root, candidate);
  return relative === "" || (!relative.startsWith(`..${path.sep}`) && relative !== ".." && !path.isAbsolute(relative));
};

describe("native process task temporary directory containment", () => {
  it("moves an outside parent TMPDIR under granted scratch without widening policy", async () => {
    const f = fixture();
    const requestedRoots = [f.cwd, f.scratch];
    const { value } = await report(f, { writableRoots: requestedRoots, shell: "unconfined" });

    expect(value.policy).toEqual({ readOnly: false, writableRoots: [...requestedRoots].sort(), shell: "unconfined" });
    expect(value.tmp).not.toBe(f.parentTemp);
    expect(inside(f.scratch, value.tmp)).toBe(true);
    expect(value.tmp).not.toBe(f.scratch);
    if (process.platform !== "win32") expect(fs.statSync(value.tmp).mode & 0o777).toBe(0o700);
    expect(value.env).toEqual({ TMPDIR: value.tmp, TMP: value.tmp, TEMP: value.tmp });
    expect(value.output).toBe(path.dirname(value.payload));
    expect(inside(f.scratch, value.output)).toBe(true);
    expect(value.bytes).toBeGreaterThan(50 * 1024);
    expect(fs.statSync(value.payload).size).toBe(value.bytes);
    expect(fs.readdirSync(f.parentTemp)).toEqual([]);
  });

  it.each(["missing", "regular-file"] as const)("replaces %s in-root TMPDIR with usable private scratch", async kind => {
    const f = fixture("batch.slice");
    const callerTemp = f.scratchPath("i");
    scratchPaths.push(callerTemp);
    if (kind === "regular-file") fs.writeFileSync(callerTemp, "not a directory");
    vi.stubEnv("TMPDIR", callerTemp);
    const requestedRoots = [f.cwd, f.scratch];
    const { value } = await report(f, { writableRoots: requestedRoots, shell: "unconfined" });

    expect(value.tmp).not.toBe(callerTemp);
    expect(value.tmp).not.toBe(f.scratch);
    expect(inside(f.scratch, value.tmp)).toBe(true);
    expect(fs.statSync(value.tmp).isDirectory()).toBe(true);
    if (process.platform !== "win32") expect(fs.statSync(value.tmp).mode & 0o777).toBe(0o700);
    expect(value.env).toEqual({ TMPDIR: value.tmp, TMP: value.tmp, TEMP: value.tmp });
    expect(value.policy).toEqual({ readOnly: false, writableRoots: [...requestedRoots].sort(), shell: "unconfined" });
    expect(inside(value.tmp, value.output)).toBe(true);
    expect(value.bytes).toBeGreaterThan(50 * 1024);
    expect(fs.statSync(value.payload).size).toBe(value.bytes);
    expect(fs.readdirSync(f.parentTemp)).toEqual([]);
  });

  it.each(["missing", "regular-file"] as const)("replaces %s in-root --tmpdir before worker bootstrap", async kind => {
    const f = fixture();
    const optionTmpdir = f.scratchPath("o");
    scratchPaths.push(optionTmpdir);
    if (kind === "regular-file") fs.writeFileSync(optionTmpdir, "not a directory");
    const nativeLaunch = ProcessTransport.prototype.launch;
    // Inject at the receiver boundary, including retries, while retaining the real native transport.
    const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(function (this: ProcessTransport, request) {
      return nativeLaunch.call(this, { ...request, workerArguments: [...request.workerArguments, "--tmpdir", optionTmpdir] });
    });
    try {
      const requestedRoots = [f.cwd, f.scratch];
      const { value } = await report(f, { writableRoots: requestedRoots, shell: "unconfined" });
      expect(value.argTmpdir).toBe(value.tmp);
      expect(value.tmp).not.toBe(optionTmpdir);
      expect(value.tmp).not.toBe(f.scratch);
      expect(inside(f.scratch, value.tmp)).toBe(true);
      if (process.platform !== "win32") expect(fs.statSync(value.tmp).mode & 0o777).toBe(0o700);
      expect(value.env).toEqual({ TMPDIR: value.tmp, TMP: value.tmp, TEMP: value.tmp });
      expect(value.policy).toEqual({ readOnly: false, writableRoots: [...requestedRoots].sort(), shell: "unconfined" });
      expect(inside(value.tmp, value.output)).toBe(true);
      expect(value.bytes).toBeGreaterThan(50 * 1024);
      expect(fs.statSync(value.payload).size).toBe(value.bytes);
      expect(fs.readdirSync(f.parentTemp)).toEqual([]);
    } finally { launch.mockRestore(); }
  });

  it.each(["equal", "inside"] as const)("preserves caller TMPDIR when it is %s an authorized root", async placement => {
    const f = fixture("batch.slice");
    const callerTemp = placement === "equal" ? f.scratch : f.scratchPath("t");
    if (placement === "inside") {
      fs.mkdirSync(callerTemp);
      scratchPaths.push(callerTemp);
    }
    vi.stubEnv("TMPDIR", callerTemp);
    // Other native temp aliases must follow the accepted TMPDIR, not leak outside.
    const requestedRoots = [f.cwd, f.scratch];
    const { value } = await report(f, { writableRoots: requestedRoots, shell: "unconfined" });

    expect(value.tmp).toBe(callerTemp);
    expect(value.env).toEqual({ TMPDIR: callerTemp, TMP: callerTemp, TEMP: callerTemp });
    expect(value.policy).toEqual({ readOnly: false, writableRoots: [...requestedRoots].sort(), shell: "unconfined" });
    expect(inside(f.scratch, value.output)).toBe(true);
    expect(value.bytes).toBeGreaterThan(50 * 1024);
  });

  it.each(["relative", "native-symlink-parent"] as const)("preserves valid %s TMPDIR for scoped state", async kind => {
    const f = fixture("batch.slice");
    const candidate = f.scratchPath(kind === "native-symlink-parent" ? "l" : "t");
    let callerTemp: string;
    let expected: string;
    if (kind === "relative") {
      fs.mkdirSync(candidate);
      scratchPaths.push(candidate);
      callerTemp = path.relative(f.cwd, candidate);
      expected = candidate;
    } else {
      const nested = path.join(f.cwd, "nested");
      fs.mkdirSync(nested);
      fs.symlinkSync(nested, candidate, "junction");
      scratchPaths.push(candidate);
      callerTemp = `${candidate}${path.sep}..`;
      expected = process.platform === "win32" ? f.scratch : f.cwd;
    }
    vi.stubEnv("TMPDIR", callerTemp);
    const { value } = await report(f, { writableRoots: [f.cwd, f.scratch], shell: "unconfined" });
    expect(value.tmp).toBe(callerTemp);
    expect(value.env).toEqual({ TMPDIR: callerTemp, TMP: callerTemp, TEMP: callerTemp });
    if (process.platform === "linux") {
      const scopeArgs = fs.readFileSync(path.join(f.cwd, "scope-args"), "utf8").split("\n");
      const marker = scopeArgs.find(argument => path.basename(argument) === "admitted")!;
      expect(inside(expected, marker)).toBe(true);
    }
    expect(value.bytes).toBeGreaterThan(50 * 1024);
  });

  it("preserves a temp symlink when its lexical and real paths are both granted", async () => {
    const f = fixture();
    const callerTemp = f.scratchPath("l");
    fs.symlinkSync(f.cwd, callerTemp, "junction");
    scratchPaths.push(callerTemp);
    vi.stubEnv("TMPDIR", callerTemp);
    const { value } = await report(f, { writableRoots: [f.cwd, f.scratch], shell: "unconfined" });
    expect(value.tmp).toBe(callerTemp);
    expect(value.env).toEqual({ TMPDIR: callerTemp, TMP: callerTemp, TEMP: callerTemp });
    expect(inside(f.cwd, fs.realpathSync(value.output))).toBe(true);
    expect(value.policy?.writableRoots).toEqual([f.cwd, f.scratch].sort());
  });

  it.each([{ writePolicy: "unrestricted" as const }, { writableRoots: [] }, { readOnly: true }])(
    "keeps caller temp environment unchanged without writable roots (%j)", async request => {
      const f = fixture();
      const { value } = await report(f, request);
      expect(value.policy).toEqual("writePolicy" in request ? null
        : { readOnly: "readOnly" in request, writableRoots: [], shell: "deny" });
      expect(value.argTmpdir).toBeNull();
      expect(value.tmp).toBe(f.parentTemp);
      expect(value.env).toEqual({ TMPDIR: f.parentTemp, TMP: f.parentTemp, TEMP: f.parentTemp });
      expect(value.bytes).toBeGreaterThan(50 * 1024);
    },
  );

  it("uses the last root when only the worktree is writable", async () => {
    const f = fixture();
    const { value } = await report(f, { writableRoots: [f.cwd], shell: "unconfined" });
    expect(value.policy?.writableRoots).toEqual([f.cwd]);
    expect(inside(f.cwd, value.tmp)).toBe(true);
    expect(value.env).toEqual({ TMPDIR: value.tmp, TMP: value.tmp, TEMP: value.tmp });
    expect(inside(f.cwd, value.output)).toBe(true);
  });

  it.each([undefined, "batch.slice"])("uses a short associated --tmpdir for a long run directory (slice %s)", async processSlice => {
    const f = fixture(processSlice, true);
    const { result, value } = await report(f, { actorId: "tmpdir-test-actor" });
    const runDirectory = f.manager.runDirectory(result.id)!;
    const association = JSON.parse(fs.readFileSync(path.join(runDirectory, "tmp", "task-tmp.json"), "utf8")) as {
      runId: string; path: string; dev: string; ino: string;
    };
    const stat = fs.statSync(value.tmp);
    expect(Buffer.byteLength(runDirectory, "utf8")).toBeGreaterThan(60);
    expect(value.argTmpdir).toBe(value.tmp);
    expect(Buffer.byteLength(value.tmp, "utf8")).toBeLessThanOrEqual(51);
    expect(value.tmp).not.toBe(path.join(runDirectory, "tmp"));
    expect(association).toMatchObject({ runId: result.id, path: value.tmp, dev: String(stat.dev), ino: String(stat.ino) });
    expect(canRemoveTerminalRun(runDirectory)).toBe(true);
    expect(path.dirname(value.tmp)).toBe(f.taskTmpRoot);
    expect(stat.isDirectory()).toBe(true);
    if (process.platform !== "win32") expect(stat.mode & 0o777).toBe(0o700);
    if (process.getuid) expect(stat.uid).toBe(process.getuid());
    expect(value.env).toEqual({ TMPDIR: value.tmp, TMP: value.tmp, TEMP: value.tmp });
    expect(value.policy).toEqual({ readOnly: false, writableRoots: [value.tmp], shell: "unconfined" });
    expect(inside(value.tmp, value.output)).toBe(true);
    expect(value.bytes).toBeGreaterThan(50 * 1024);
    if (processSlice && process.platform === "linux") {
      expect(fs.readFileSync(path.join(f.cwd, "scope-args"), "utf8")).toContain("--slice=batch.slice\n");
    }
  });

  it("removes the associated scratch only after a confirmed run cleanup", async () => {
    const f = fixture(undefined, true, false);
    const { result, value } = await report(f, { actorId: "tmpdir-cleanup-test" });
    const runDirectory = f.manager.runDirectory(result.id)!;
    expect(fs.existsSync(value.tmp)).toBe(true);
    expect(fs.existsSync(path.join(runDirectory, "tmp", "task-tmp.json"))).toBe(true);
    await f.manager.cleanup(result.id);
    expect(fs.existsSync(value.tmp)).toBe(false);
    expect(fs.existsSync(runDirectory)).toBe(false);
  });

  it("retains the run and scratch if its association disappears while the manager owns it", async () => {
    const f = fixture(undefined, true, false);
    const { result, value } = await report(f, { actorId: "tmpdir-missing-association" });
    const runDirectory = f.manager.runDirectory(result.id)!;
    fs.unlinkSync(path.join(runDirectory, "tmp", "task-tmp.json"));
    await expect(f.manager.cleanup(result.id)).rejects.toThrow("task temporary directory custody is unknown");
    expect(fs.existsSync(runDirectory)).toBe(true);
    expect(fs.existsSync(value.tmp)).toBe(true);
  });

  it("retains the run and replacement scratch when its recorded inode is unknown", async () => {
    const f = fixture(undefined, true, false);
    const { result, value } = await report(f, { actorId: "tmpdir-unknown-test" });
    const runDirectory = f.manager.runDirectory(result.id)!;
    const moved = path.join(f.root, "moved-original-task-tmp");
    fs.renameSync(value.tmp, moved);
    taskTmpPaths.push(moved);
    fs.mkdirSync(value.tmp, { mode: 0o700 });
    await expect(f.manager.cleanup(result.id)).rejects.toThrow("task temporary directory custody is unknown");
    expect(fs.existsSync(runDirectory)).toBe(true);
    expect(fs.existsSync(path.join(runDirectory, "tmp", "task-tmp.json"))).toBe(true);
    expect(fs.existsSync(value.tmp)).toBe(true);
    expect(fs.existsSync(moved)).toBe(true);
  });

  it.each(["missing", "regular-file", "escaping-symlink"] as const)(
    "places explicit processSlice scope state under validated scratch when parent TMPDIR is %s",
    async kind => {
      if (process.platform !== "linux") return;
      const f = fixture("batch.slice");
      const movedParent = path.join(f.root, "parent-temp-original");
      fs.renameSync(f.parentTemp, movedParent);
      if (kind === "regular-file") fs.writeFileSync(f.parentTemp, "not a directory");
      else if (kind === "escaping-symlink") fs.symlinkSync(movedParent, f.parentTemp, "junction");
      const { value } = await report(f, { writableRoots: [f.cwd, f.scratch], shell: "unconfined" });

      expect(inside(f.scratch, value.tmp)).toBe(true);
      const scopeArgs = fs.readFileSync(path.join(f.cwd, "scope-args"), "utf8").split("\n");
      const marker = scopeArgs.find(argument => path.basename(argument) === "admitted")!;
      expect(path.basename(marker)).toBe("admitted");
      expect(inside(value.tmp, marker)).toBe(true);
      expect(fs.readdirSync(movedParent)).toEqual([]);
      expect(value.bytes).toBeGreaterThan(50 * 1024);
    },
  );

  it("does not preserve a lexically contained TMPDIR symlink that escapes the root", async () => {
    const f = fixture();
    const escape = f.scratchPath("e");
    fs.symlinkSync(f.parentTemp, escape, "junction");
    scratchPaths.push(escape);
    vi.stubEnv("TMPDIR", escape);
    vi.stubEnv("TMP", escape);
    vi.stubEnv("TEMP", escape);
    const { value } = await report(f, { writableRoots: [f.cwd, f.scratch], shell: "unconfined" });
    expect(value.policy?.writableRoots).toEqual([f.cwd, f.scratch].sort());
    expect(inside(f.scratch, value.tmp)).toBe(true);
    expect(inside(f.scratch, fs.realpathSync(value.output))).toBe(true);
    expect(value.bytes).toBeGreaterThan(50 * 1024);
    expect(fs.readdirSync(f.parentTemp)).toEqual([]);
  });

  it("does not normalize away a native symlink/.. escape before checking TMPDIR", async () => {
    const f = fixture();
    const nested = path.join(f.parentTemp, "nested");
    fs.mkdirSync(nested);
    const link = f.scratchPath("l");
    fs.symlinkSync(nested, link, "junction");
    scratchPaths.push(link);
    const callerTemp = `${link}${path.sep}..`;
    vi.stubEnv("TMPDIR", callerTemp);
    const { value } = await report(f, { writableRoots: [f.cwd, f.scratch], shell: "unconfined" });
    if (process.platform === "win32") {
      // Win32 removes junction\.. lexically before following the junction: this is scratch, not an escape.
      expect(fs.realpathSync.native(callerTemp)).toBe(f.scratch);
      expect(inside(f.scratch, fs.realpathSync.native(value.tmp))).toBe(true);
      expect(fs.readdirSync(nested)).toEqual([]);
    } else {
      expect(value.tmp).not.toBe(callerTemp);
    }
    expect(inside(f.scratch, fs.realpathSync.native(value.output))).toBe(true);
    expect(fs.readdirSync(f.parentTemp)).toEqual(["nested"]);
  });

  it("rejects a configured task-temp root with symlink ancestry before spawning", async () => {
    const f = fixture(undefined, true, true, (root, shortRoot) => {
      const link = path.join(root, "task-temp-link");
      fs.symlinkSync(shortRoot, link, "junction");
      return link;
    });
    const before = fs.readdirSync(f.taskTmpRoot).filter(name => /^r\.[A-Za-z0-9]{6}$/.test(name)).sort();
    await expect(f.manager.spawn({ task: "filesystem probe", transport: "process", actorId: "unsafe-tmp-root" }))
      .rejects.toThrow("Task temporary root contains a symlink");
    expect(fs.readdirSync(f.taskTmpRoot).filter(name => /^r\.[A-Za-z0-9]{6}$/.test(name)).sort()).toEqual(before);
  });

  it("refuses a selected TMPDIR whose bootstrap path exceeds 60 bytes", async () => {
    const f = fixture();
    const longRoot = path.join(f.root, `long-${"x".repeat(64)}`);
    const longTmp = path.join(longRoot, "scratch");
    fs.mkdirSync(longTmp, { recursive: true });
    const policy = { readOnly: false, writableRoots: [longRoot], shell: "unconfined" };
    await expect(new ProcessTransport().launch({
      id: "overbudget", name: "overbudget", cwd: f.cwd, workerPath: f.worker,
      workerArguments: ["--write-policy", JSON.stringify(policy), "--tmpdir", longTmp],
    })).rejects.toThrow("60-byte bootstrap limit");
    expect(fs.existsSync(longTmp)).toBe(true);
  });

  it("refuses a replaced grant root before allocating temporary files through its symlink", async () => {
    const f = fixture();
    fs.renameSync(f.scratch, path.join(f.root, "scratch-original"));
    fs.symlinkSync(f.parentTemp, f.scratch, "junction");
    await expect(new ProcessTransport().launch({
      id: "replaced-root", name: "replaced-root", cwd: f.cwd, workerPath: f.worker,
      workerArguments: ["--write-policy", JSON.stringify({ readOnly: false, writableRoots: [f.cwd, f.scratch], shell: "unconfined" })],
    })).rejects.toThrow("writable root changed outside the effective policy");
    expect(fs.readdirSync(f.parentTemp)).toEqual([]);
  });

  it("checks revoked launch authority before allocating a new temporary directory", async () => {
    const f = fixture();
    const before = fs.readdirSync(f.scratch).sort();
    await expect(new ProcessTransport().launch({
      id: "revoked", name: "revoked", cwd: f.cwd, workerPath: f.worker, authorize: () => false,
      workerArguments: ["--write-policy", JSON.stringify({ readOnly: false, writableRoots: [f.cwd, f.scratch], shell: "unconfined" })],
    })).rejects.toThrow("no longer authorized");
    expect(fs.readdirSync(f.scratch).sort()).toEqual(before);
  });

  it("does not treat a sibling-prefix path as being inside an authorized root", async () => {
    const f = fixture();
    const sibling = `${f.scratch}-sibling`;
    fs.mkdirSync(sibling);
    scratchPaths.push(sibling);
    vi.stubEnv("TMPDIR", sibling);
    vi.stubEnv("TMP", sibling);
    vi.stubEnv("TEMP", sibling);
    const { value } = await report(f, { writableRoots: [f.cwd, f.scratch], shell: "unconfined" });
    expect(value.policy?.writableRoots).toEqual([f.cwd, f.scratch].sort());
    expect(inside(f.scratch, value.tmp)).toBe(true);
    expect(inside(f.scratch, value.output)).toBe(true);
    expect(value.env.TMPDIR).toBe(value.tmp);
    expect(value.bytes).toBeGreaterThan(50 * 1024);
  });
});
