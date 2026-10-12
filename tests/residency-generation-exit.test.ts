import fs from "node:fs";
import { execFileSync, spawn } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import {
  createResidentGeneration, pruneResidentGenerations, readResidentGenerationExit, residentGenerationExitPath,
  residentGenerationRecords, writeResidentGenerationError, writeResidentGenerationExit,
} from "../src/residency/generation-exit.js";

// smarty-dev#7950: a resident child that exits with no host-written reason (here SIGKILL) still leaves exactly one
// durable record naming its generation, and its launcher never writes a file of the generation that owns the root now.
const launcherPath = path.resolve("dist/residency/launcher.js");
const posix = process.platform !== "win32";
const waitFor = async (predicate: () => boolean, boundMs: number): Promise<void> => {
  const until = Date.now() + boundMs;
  while (!predicate()) {
    if (Date.now() >= until) throw new Error("Timed out");
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};
/** Every file under `directory` with its bytes, except the paths `skip` names. */
const contents = (directory: string, skip: (file: string) => boolean, files = new Map<string, string>()): Map<string, string> => {
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const file = path.join(directory, entry.name);
    if (skip(file)) continue;
    if (entry.isDirectory()) contents(file, skip, files);
    else files.set(file, fs.readFileSync(file, "utf8"));
  }
  return files;
};

describe.skipIf(!fs.existsSync(launcherPath) || !posix)("resident launcher: the exit record of an unreported exit", () => {
  it("a child SIGKILLed after it owned the root leaves exactly one record naming its generation; the successor's files are byte-unchanged", { timeout: 60_000 }, async () => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-generation-exit-"));
    const root = path.join(base, "root");
    fs.mkdirSync(root, { mode: 0o700 });
    const generation = "0b6f4e2a-predecessor-generation";
    const hostBinary = path.join(base, "fake-host.mjs");
    // Like the real host: it claims the root (its generation directory, then owner.json naming its token) and is then
    // killed before it can report. Just before, a successor took the root over (a new owner.json and launcher.log).
    fs.writeFileSync(hostBinary, `import fs from 'node:fs'; import path from 'node:path';
const root = ${JSON.stringify(root)};
fs.mkdirSync(path.join(root, 'generations'), { mode: 0o700 });
fs.mkdirSync(path.join(root, 'generations', ${JSON.stringify(generation)}), { mode: 0o700 });
fs.writeFileSync(path.join(root, 'owner.json'), JSON.stringify({ format: 1, hostId: 'resident:fake', pid: process.pid, token: ${JSON.stringify(generation)}, startedAt: Date.now(), readyAt: Date.now() }));
process.stdout.write('claimed\\n');
const go = ${JSON.stringify(path.join(base, "go"))};
const timer = setInterval(() => {
  if (!fs.existsSync(go)) return;
  clearInterval(timer);
  fs.writeFileSync(path.join(root, 'owner.json'), JSON.stringify({ format: 1, hostId: 'resident:successor', pid: 1, token: 'successor-generation-token', startedAt: Date.now() }));
  fs.writeFileSync(path.join(root, 'launcher.log'), JSON.stringify({ event: 'launcher-started', pid: 4242 }) + '\\n');
  process.stdout.write('succeeded\\n');
}, 20);
setInterval(() => {}, 1000);
`);
    const configPath = path.join(root, "config.json");
    fs.writeFileSync(configPath, JSON.stringify({ cwd: root, piBinary: hostBinary }));
    const launcher = spawn(process.execPath, [launcherPath, "--config", configPath], { cwd: root, stdio: "ignore" });
    try {
      const childLog = path.join(root, "child-stderr.log");
      await waitFor(() => fs.existsSync(childLog) && fs.readFileSync(childLog, "utf8").includes("claimed"), 20_000);
      const child = JSON.parse(fs.readFileSync(path.join(root, "owner.json"), "utf8")).pid as number;
      await new Promise(resolve => setTimeout(resolve, 300)); // the launcher has observed its child as the owner
      // The successor's root files, taken over before the predecessor dies.
      fs.writeFileSync(path.join(base, "go"), "");
      await waitFor(() => fs.readFileSync(childLog, "utf8").includes("succeeded"), 10_000);
      const own = (file: string) => file.startsWith(path.join(root, "generations", generation)) || file === childLog;
      const successor = contents(root, own);
      process.kill(child, "SIGKILL");
      await new Promise<void>(resolve => { if (launcher.exitCode !== null) resolve(); else launcher.once("exit", () => resolve()); });
      expect(contents(root, own)).toEqual(successor);
      const records = residentGenerationRecords(root, "exit.json", 10);
      expect(records).toHaveLength(1);
      expect(readResidentGenerationExit(root, generation)).toMatchObject({ format: 1, generation, reason: "unreported", writer: "launcher",
        hostId: "resident:fake", pid: child, signal: "SIGKILL" });
    } finally {
      launcher.kill("SIGKILL");
      fs.rmSync(base, { recursive: true, force: true });
    }
  });
});

describe("generation records: created once, never following or blocking", () => {
  const dir = (): string => fs.mkdtempSync(path.join(os.tmpdir(), "resident-generation-"));
  const record = (generation: string) => ({ generation, reason: "unreported" as const, writer: "launcher" as const });

  it("writes exactly one exit record per generation (the host's reason wins when it is first)", () => {
    const root = dir();
    try {
      createResidentGeneration(root, "gen-aaaaaaaa");
      expect(writeResidentGenerationExit(root, { generation: "gen-aaaaaaaa", reason: "idle-exit", writer: "host" })).toBe(true);
      expect(writeResidentGenerationExit(root, record("gen-aaaaaaaa"))).toBe(false);
      expect(readResidentGenerationExit(root, "gen-aaaaaaaa")).toMatchObject({ reason: "idle-exit", writer: "host" });
      if (posix) expect(fs.statSync(residentGenerationExitPath(root, "gen-aaaaaaaa")).mode & 0o777).toBe(0o600);
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("writes nothing for a generation that never claimed the root (no directory), and creates none", () => {
    const root = dir();
    try {
      expect(writeResidentGenerationExit(root, record("gen-unclaimed"))).toBe(false);
      expect(writeResidentGenerationError(root, { generation: "gen-unclaimed", error: "boom", occurredAt: 1 })).toBe(false);
      expect(fs.readdirSync(root)).toEqual([]);
      expect(writeResidentGenerationExit(root, record("../escape"))).toBe(false);
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.skipIf(!posix)("does not follow a planted symlink or block on a planted FIFO", { timeout: 15_000 }, () => {
    const root = dir();
    try {
      createResidentGeneration(root, "gen-planted1");
      const target = path.join(root, "elsewhere");
      fs.writeFileSync(target, "");
      fs.symlinkSync(target, residentGenerationExitPath(root, "gen-planted1"));
      expect(writeResidentGenerationExit(root, record("gen-planted1"))).toBe(false);
      expect(fs.readFileSync(target, "utf8")).toBe("");
      createResidentGeneration(root, "gen-planted2");
      execFileSync("mkfifo", [residentGenerationExitPath(root, "gen-planted2")]);
      const started = Date.now();
      expect(writeResidentGenerationExit(root, record("gen-planted2"))).toBe(false);
      expect(Date.now() - started).toBeLessThan(1_000);
      // A symlinked generation directory is not an owned directory: refused.
      fs.mkdirSync(path.join(root, "outside"));
      fs.symlinkSync(path.join(root, "outside"), path.join(root, "generations", "gen-linked01"));
      expect(writeResidentGenerationExit(root, record("gen-linked01"))).toBe(false);
      expect(fs.readdirSync(path.join(root, "outside"))).toEqual([]);
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("prunes records older than a week, keeping newer ones and generations without a record", () => {
    const root = dir();
    try {
      for (const generation of ["gen-old00001", "gen-new00001", "gen-live0001"]) createResidentGeneration(root, generation);
      writeResidentGenerationExit(root, record("gen-old00001"));
      writeResidentGenerationExit(root, record("gen-new00001"));
      const old = (Date.now() - 8 * 24 * 60 * 60 * 1_000) / 1_000;
      fs.utimesSync(residentGenerationExitPath(root, "gen-old00001"), old, old);
      pruneResidentGenerations(root);
      expect(fs.readdirSync(path.join(root, "generations")).sort()).toEqual(["gen-live0001", "gen-new00001"]);
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });
});
