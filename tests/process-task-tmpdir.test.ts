import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";

// Trust-boundary regression for smarty-dev#10544. Fixtures remain for inspection.
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
});

const fixture = () => {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-process-task-tmpdir-")));
  const cwd = path.join(root, "cwd");
  const scratch = path.join(root, "scratch");
  const parentTemp = path.join(root, "parent-temp");
  const runRoot = path.join(root, "runs");
  for (const directory of [cwd, scratch, parentTemp]) fs.mkdirSync(directory);
  vi.stubEnv("TMPDIR", parentTemp);
  vi.stubEnv("TMP", parentTemp);
  vi.stubEnv("TEMP", parentTemp);

  const worker = path.join(root, "worker.mjs");
  fs.writeFileSync(worker, `import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i], process.argv[i + 1]);
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
    ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [], timeoutMs: 8_000, retainRuns: true,
  }, { workerPath: worker, fullCodeMode: false, runRoot });
  managers.push(manager);
  return { root, cwd, scratch, parentTemp, runRoot, manager, worker };
};

const report = async (f: ReturnType<typeof fixture>, request: Partial<Parameters<AgentManager["spawn"]>[0]>) => {
  const handle = await f.manager.spawn({ task: "filesystem probe", transport: "process", ...request });
  const result = await f.manager.wait(handle.id);
  expect(result.status, result.error).toBe("completed");
  return { result, value: JSON.parse(result.text) as {
    tmp: string; env: Record<string, string | undefined>; argTmpdir: string | null;
    policy: { readOnly: boolean; writableRoots: string[]; shell: string } | null;
    output: string; payload: string; bytes: number;
  } };
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

  it.each(["equal", "inside"] as const)("preserves caller TMPDIR when it is %s an authorized root", async placement => {
    const f = fixture();
    const callerTemp = placement === "equal" ? f.scratch : path.join(f.scratch, "caller-temp");
    if (placement === "inside") fs.mkdirSync(callerTemp);
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

  it("preserves a temp symlink when its lexical and real paths are both granted", async () => {
    const f = fixture();
    const callerTemp = path.join(f.scratch, "linked-temp");
    fs.symlinkSync(f.cwd, callerTemp, "junction");
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

  it("honors the default task --tmpdir before the worker starts", async () => {
    const f = fixture();
    const { result, value } = await report(f, {});
    const expected = path.join(f.manager.runDirectory(result.id)!, "tmp");
    expect(value.argTmpdir).toBe(expected);
    expect(value.tmp).toBe(expected);
    expect(value.env).toEqual({ TMPDIR: expected, TMP: expected, TEMP: expected });
    expect(value.policy).toEqual({ readOnly: false, writableRoots: [f.cwd, expected].sort(), shell: "unconfined" });
    expect(inside(expected, value.output)).toBe(true);
    expect(value.bytes).toBeGreaterThan(50 * 1024);
  });

  it("does not preserve a lexically contained TMPDIR symlink that escapes the root", async () => {
    const f = fixture();
    const escape = path.join(f.scratch, "temp-escape");
    fs.symlinkSync(f.parentTemp, escape, "junction");
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
    await expect(new ProcessTransport().launch({
      id: "revoked", name: "revoked", cwd: f.cwd, workerPath: f.worker, authorize: () => false,
      workerArguments: ["--write-policy", JSON.stringify({ readOnly: false, writableRoots: [f.cwd, f.scratch], shell: "unconfined" })],
    })).rejects.toThrow("no longer authorized");
    expect(fs.readdirSync(f.scratch)).toEqual([]);
  });

  it("does not treat a sibling-prefix path as being inside an authorized root", async () => {
    const f = fixture();
    const sibling = `${f.scratch}-sibling`;
    fs.mkdirSync(sibling);
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
