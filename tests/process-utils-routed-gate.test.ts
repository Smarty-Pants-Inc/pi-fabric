import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { spawnDetached } from "../src/agents/transports/process-utils.js";
import { taskScopeShim } from "../src/agents/transports/task-cgroup.js";

// Recorded-spawn probe: the real production spawnDetached runs up to its single
// spawn() call; the mock records the arguments and throws a sentinel synchronously,
// so no systemd unit, child process, signal, or product cleanup path ever runs.
vi.mock("node:child_process", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn() };
});

const SENTINEL = new Error("recorded-spawn sentinel");
const DIR_FD = 9; // Never opened: spawn is mocked, the number is only recorded.
const CAP = 256 * 1024 * 1024;

type Recorded = { command: string; args: string[]; options: { stdio?: unknown } };
let recorded: Recorded[] = [];
let fixture: string;
let savedTmpdir: string | undefined;

beforeEach(() => {
  recorded = [];
  // Scoped launches mkdtemp under os.tmpdir() before spawn; keep that inside an owned fixture.
  fixture = fs.mkdtempSync(path.join(os.tmpdir(), "routed-gate-"));
  savedTmpdir = process.env.TMPDIR;
  process.env.TMPDIR = fixture;
  vi.mocked(spawn).mockImplementation(((command: string, args: string[], options: { stdio?: unknown }) => {
    recorded.push({ command, args: [...args], options });
    throw SENTINEL;
  }) as unknown as typeof spawn);
});

afterEach(() => {
  if (savedTmpdir === undefined) delete process.env.TMPDIR; else process.env.TMPDIR = savedTmpdir;
  fs.rmSync(fixture, { recursive: true, force: true }); // fixture only
  vi.mocked(spawn).mockReset();
});

const scope = (memoryMaxBytes?: number) => ({
  executable: "/nonexistent/systemd-run", slice: "test.slice", warn: () => undefined,
  ...(memoryMaxBytes === undefined ? {} : { memoryMaxBytes }),
});

async function record(options: { inherit?: number[]; capped?: boolean; scoped?: boolean; custodian: boolean }): Promise<Recorded> {
  const authority = options.inherit ? { inheritFds: options.inherit } : undefined;
  const scoped = options.scoped ?? true;
  await expect(spawnDetached(
    "worker.js", ["--x"], fixture, authority, {},
    scoped ? scope(options.capped ? CAP : undefined) : undefined,
    undefined, options.custodian,
  )).rejects.toBe(SENTINEL);
  expect(recorded).toHaveLength(1);
  return recorded[0]!;
}

const shimOf = (call: Recorded): string => {
  const index = call.args.indexOf("-c");
  expect(index).toBeGreaterThan(0);
  expect(call.args[index - 1]).toBe("/bin/sh");
  return call.args[index + 1]!;
};

const pipeSlots = (stdio: unknown): number[] =>
  Array.isArray(stdio) ? stdio.flatMap((slot, index) => (slot === "pipe" ? [index] : [])) : [];

describe.skipIf(process.platform !== "linux")("routed inheritFds with capped gate (recorded spawn)", () => {
  it.each([
    { custodian: true, fd3: "ipc" },
    { custodian: false, fd3: "ignore" },
  ])("capped inherit=[dir] custodian=$custodian -> fd3 $fd3, dir fd4, gate fd5", async ({ custodian, fd3 }) => {
    const call = await record({ inherit: [DIR_FD], capped: true, custodian });
    expect(call.options.stdio).toEqual(["ignore", "ignore", "ignore", fd3, DIR_FD, "pipe"]);
    expect(pipeSlots(call.options.stdio)).toEqual([5]);
    expect(shimOf(call)).toBe(taskScopeShim(5));
    expect(shimOf(call)).toContain("<&5");
    expect(shimOf(call)).not.toMatch(/<&[0-46-9]/);
    expect(call.args).toContain(`MemoryMax=${CAP}`);
  });

  it.each([
    { custodian: true, stdio: ["ignore", "ignore", "ignore", "ipc", "pipe"], gate: 4 },
    { custodian: false, stdio: ["ignore", "ignore", "ignore", "pipe"], gate: 3 },
  ])("capped no inherit custodian=$custodian -> gate fd$gate", async ({ custodian, stdio, gate }) => {
    const call = await record({ capped: true, custodian });
    expect(call.options.stdio).toEqual(stdio);
    expect(pipeSlots(call.options.stdio)).toEqual([gate]);
    expect(shimOf(call)).toBe(taskScopeShim(gate));
  });

  it.each([true, false])("uncapped scoped inherit=[dir] custodian=%s -> dir fd4, no gate pipe", async (custodian) => {
    const call = await record({ inherit: [DIR_FD], capped: false, custodian });
    expect(call.options.stdio).toEqual(["ignore", "ignore", "ignore", custodian ? "ipc" : "ignore", DIR_FD]);
    expect(pipeSlots(call.options.stdio)).toEqual([]);
    expect(shimOf(call)).not.toContain("<&");
    expect(call.args.some(arg => arg.startsWith("MemoryMax="))).toBe(false);
  });

  it.each([true, false])("unscoped legacy inherit=[dir] custodian=%s keeps fd3/fd4", async (custodian) => {
    const call = await record({ inherit: [DIR_FD], scoped: false, custodian });
    expect(call.options.stdio).toEqual(["ignore", "ignore", "ignore", custodian ? "ipc" : "ignore", DIR_FD]);
    expect(pipeSlots(call.options.stdio)).toEqual([]);
    expect(call.args).not.toContain("/bin/sh");
    expect(call.args.slice(0, 2)).toEqual(["worker.js", "--x"]);
  });
});
