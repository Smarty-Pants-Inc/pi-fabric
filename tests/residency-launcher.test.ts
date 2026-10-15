import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFile, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { observeResidentOwner } from "../src/residency/launcher-owner.js";

const launcherPath = path.resolve("dist/residency/launcher.js");
const hasLauncher = fs.existsSync(launcherPath);

describe("resident launcher owner observation", () => {
  it("waits while its child has not claimed residency", () => {
    expect(observeResidentOwner(undefined, 20, false)).toEqual({
      claimed: false,
      observedOwner: false,
      closeInput: false,
    });
  });

  it("keeps stdin open while its child owns residency", () => {
    expect(observeResidentOwner(20, 20, false)).toEqual({
      claimed: true,
      observedOwner: true,
      closeInput: false,
    });
  });

  it("closes a duplicate child when another live host owns residency", () => {
    expect(observeResidentOwner(10, 20, false)).toEqual({
      claimed: false,
      observedOwner: true,
      closeInput: true,
    });
  });

  it("closes stdin after its owned host releases residency", () => {
    expect(observeResidentOwner(undefined, 20, true)).toEqual({
      claimed: true,
      observedOwner: false,
      closeInput: true,
    });
  });
});

// The persistent-actor path inherits the owner's environment twice: the
// launcher spawns the resident host pi with `{ ...process.env }`, and the
// host's AgentManager passes the resolved (shim) binary to the worker, which
// spawns the child with `{ ...process.env }` again. The launcher link is
// regression-tested here with a fake host that reports only presence booleans
// of a synthetic sentinel env var — never values — mirroring what the
// LocalTerm shim injects into the parent pi's environment.
describe.skipIf(!hasLauncher || process.platform === "win32")("resident launcher env inheritance", () => {
  it("propagates the owner environment into the resident host process", { timeout: 30_000 }, async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-launcher-env-"));
    const present = "FAKE_HOST_SENTINEL_KEY";
    const envLog = path.join(root, "host-env.json");
    const hostBinary = path.join(root, "fake-host.mjs");
    fs.writeFileSync(
      hostBinary,
      [
        "#!/usr/bin/env node",
        "import fs from 'node:fs';",
        `const name = ${JSON.stringify(present)};`,
        "fs.writeFileSync(process.env.FAKE_HOST_ENV_LOG, JSON.stringify({",
        "  [name]: process.env[name] ? 'present' : 'absent',",
        "}));",
        "process.exit(0);",
        "",
      ].join("\n"),
    );
    const configPath = path.join(root, "config.json");
    fs.writeFileSync(
      configPath,
      JSON.stringify({ cwd: root, piBinary: hostBinary }),
    );

    try {
      await new Promise<void>((resolve) => {
        execFile(
          process.execPath,
          [launcherPath, "--config", configPath],
          {
            cwd: root,
            env: {
              ...process.env,
              [present]: "sentinel",
              FAKE_HOST_ENV_LOG: envLog,
            },
            timeout: 25_000,
          },
          // The launcher exits non-zero when the host exits without claiming
          // residency (expected here — the fake host exits immediately); the
          // env log is the assertion target, not the exit code.
          () => resolve(),
        );
      });

      expect(fs.existsSync(envLog)).toBe(true);
      expect(JSON.parse(fs.readFileSync(envLog, "utf8"))).toEqual({
        [present]: "present",
      });
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

// smarty-dev#7770 / #2010: the host names its exit itself while it owns the
// root; after a clean owned exit the launcher writes no shared-root files (only its generation-private record).
describe.skipIf(!hasLauncher || process.platform === "win32")("resident launcher after a clean owned exit", () => {
  it("writes no shared-root files after the child exits", { timeout: 30_000 }, async () => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-launcher-exit-"));
    const root = path.join(base, "root");
    fs.mkdirSync(root);
    const owner = path.join(root, "owner.json");
    const released = path.join(base, "released.json");
    const hostBinary = path.join(base, "fake-host.mjs");
    fs.writeFileSync(hostBinary, `import fs from 'node:fs'; import path from 'node:path';
const root = ${JSON.stringify(root)};
const snapshot = () => Object.fromEntries(fs.readdirSync(root).filter(name => name !== 'generations').map(name => { const s = fs.statSync(path.join(root, name)); return [name, s.size + ':' + s.mtimeMs]; }));
fs.writeFileSync(${JSON.stringify(owner)}, JSON.stringify({ pid: process.pid }));
setTimeout(() => {
  // Like the real host: its line, then the release, then exit 0.
  fs.appendFileSync(path.join(root, 'launcher.log'), JSON.stringify({ event: 'resident-exit', at: Date.now(), reason: 'idle-exit', pid: process.pid }) + '\\n');
  fs.rmSync(${JSON.stringify(owner)});
  fs.writeFileSync(${JSON.stringify(released)}, JSON.stringify(snapshot()));
  process.exit(0);
}, 500);
setInterval(() => {}, 1000);
`);
    const configPath = path.join(root, "config.json");
    fs.writeFileSync(configPath, JSON.stringify({ cwd: root, piBinary: hostBinary }));
    try {
      await new Promise<void>(resolve => {
        execFile(process.execPath, [launcherPath, "--config", configPath], { cwd: root, timeout: 25_000 }, () => resolve());
      });
      await new Promise(resolve => setTimeout(resolve, 500));
      const before = JSON.parse(fs.readFileSync(released, "utf8")) as Record<string, string>;
      const after = Object.fromEntries(fs.readdirSync(root).filter(name => name !== "generations").map(name => {
        const stat = fs.statSync(path.join(root, name));
        return [name, `${stat.size}:${stat.mtimeMs}`];
      }));
      expect(after).toEqual(before);
      const records = fs.readdirSync(path.join(root, "generations"));
      expect(records).toEqual(process.platform === "linux" ? [expect.stringMatching(/^[0-9a-f-]{36}\.exit\.json$/)] : []);
      const rows = fs.readFileSync(path.join(root, "launcher.log"), "utf8").trim().split("\n").map(line => JSON.parse(line) as Record<string, unknown>);
      expect(rows.at(-1)).toMatchObject({ event: "resident-exit", reason: "idle-exit" });
    } finally {
      fs.rmSync(base, { recursive: true, force: true });
    }
  });
});

// smarty-dev#7950: an owned child that exits with no host-written reason (SIGKILL, a crash) still leaves exactly one
// durable record naming its generation, and the launcher's write never touches a file a successor owns. Real
// launcher, real child process, real signals; the child is a fake host that publishes owner.json like the real one.
describe.skipIf(!hasLauncher || process.platform !== "linux")("resident launcher: per-generation exit record", () => {
  const startTime = (pid: number): string => {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    return stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19]!;
  };
  const waitFor = async (predicate: () => boolean, ms = 15_000): Promise<void> => {
    const until = Date.now() + ms;
    while (!predicate()) {
      if (Date.now() > until) throw new Error("Timed out");
      await new Promise(resolve => setTimeout(resolve, 20));
    }
  };
  const state = (pid: number): string | undefined => {
    try { const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8"); return stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0]; }
    catch { return undefined; }
  };
  const tree = (dir: string): Record<string, string> => Object.fromEntries(fs.readdirSync(dir, { recursive: true, withFileTypes: true })
    .filter(entry => entry.isFile()).map(entry => path.join(entry.parentPath, entry.name))
    .map(file => [path.relative(dir, file), fs.readFileSync(file, "utf8")]));
  const setup = (report: "none" | "complete" | "partial" | "die-after-publish") => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-launcher-generation-"));
    const root = path.join(base, "root");
    fs.mkdirSync(root);
    const hostBinary = path.join(base, "fake-host.mjs");
    // Like the real host: publish owner.json with the generation its launcher chose (PI_FABRIC_RESIDENT_GENERATION),
    // after making generations/. die-after-publish waits for the test's go file, publishes, and SIGKILLs itself.
    fs.writeFileSync(hostBinary, `import fs from 'node:fs'; import path from 'node:path';
const root = ${JSON.stringify(root)}, base = ${JSON.stringify(base)};
const generation = process.env.PI_FABRIC_RESIDENT_GENERATION;
fs.writeFileSync(path.join(base, 'generation'), generation);
const stat = fs.readFileSync('/proc/self/stat', 'utf8');
const processStartTime = stat.slice(stat.lastIndexOf(')') + 2).split(' ')[19];
${report === "die-after-publish" ? `fs.writeFileSync(path.join(base, 'ready'), String(process.pid));
while (!fs.existsSync(path.join(base, 'go'))) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);` : ""}
fs.mkdirSync(path.join(root, 'generations'), { mode: 0o700, recursive: true });
fs.writeFileSync(path.join(root, 'owner.json'), JSON.stringify({ pid: process.pid, processStartTime, token: generation }));
${report === "die-after-publish" ? "process.kill(process.pid, 'SIGKILL');" : ""}
${report === "complete" || report === "partial" ? `fs.writeFileSync(path.join(root, 'generations', generation + '.exit.json'), ${report === "complete"
    ? "JSON.stringify({ format: 1, generation, writer: 'host', reason: 'idle-exit', pid: process.pid, at: Date.now() })"
    : `'{"format":1,"gen'`}, { flag: 'wx', mode: 0o600 });` : ""}
setInterval(() => {}, 1000);
`);
    const configPath = path.join(root, "config.json");
    fs.writeFileSync(configPath, JSON.stringify({ cwd: root, piBinary: hostBinary }));
    const launcher = spawn(process.execPath, [launcherPath, "--config", configPath], { cwd: root, stdio: "ignore" });
    const exited = new Promise(resolve => launcher.once("exit", resolve));
    const owner = () => { try { return JSON.parse(fs.readFileSync(path.join(root, "owner.json"), "utf8")) as { pid: number }; } catch { return undefined; } };
    return { base, root, launcher, exited, owner,
      get generation() { return fs.readFileSync(path.join(base, "generation"), "utf8"); },
      ready: () => { try { return Number(fs.readFileSync(path.join(base, "ready"), "utf8")) || undefined; } catch { return undefined; } },
      records: () => fs.existsSync(path.join(root, "generations")) ? fs.readdirSync(path.join(root, "generations")) : [],
      close: () => { if (launcher.exitCode === null && launcher.signalCode === null) launcher.kill("SIGKILL"); fs.rmSync(base, { recursive: true, force: true }); } };
  };

  it("a SIGKILLed owned child leaves exactly one record naming its generation; a successor's root files are byte-unchanged", { timeout: 30_000 }, async () => {
    const f = setup("none");
    try {
      await waitFor(() => f.owner() !== undefined);
      const child = f.owner()!.pid;
      await new Promise(resolve => setTimeout(resolve, 500)); // the launcher observes ownership (50 ms loop)
      // Freeze the launcher, kill its owned child, and let a successor take the root before the launcher runs again.
      process.kill(f.launcher.pid!, "SIGSTOP");
      process.kill(child, "SIGKILL");
      await waitFor(() => state(child) === "Z"); // dead, not yet reaped by the stopped launcher
      const successor = randomUUID();
      fs.writeFileSync(path.join(f.root, "owner.json"), JSON.stringify({ pid: process.pid, processStartTime: startTime(process.pid), token: successor }));
      fs.appendFileSync(path.join(f.root, "launcher.log"), JSON.stringify({ event: "successor-line" }) + "\n");
      fs.writeFileSync(path.join(f.root, "error.json"), "successor");
      fs.writeFileSync(path.join(f.root, "generations", `${successor}.marker`), "successor");
      const before = tree(f.root);
      process.kill(f.launcher.pid!, "SIGCONT");
      await f.exited;
      const after = tree(f.root);
      const name = path.join("generations", `${f.generation}.exit.json`);
      expect(Object.keys(after).filter(file => !(file in before))).toEqual([name]);
      for (const [file, bytes] of Object.entries(before)) expect(after[file], file).toBe(bytes);
      expect(JSON.parse(after[name]!)).toMatchObject({ format: 1, generation: f.generation, writer: "launcher", reason: "unreported",
        pid: child, code: null, signal: "SIGKILL" });
      expect(fs.statSync(path.join(f.root, name)).mode & 0o777).toBe(0o600);
    } finally { f.close(); }
  });

  it("a child SIGKILLed right after it publishes owner.json, before any launcher sample: the launcher writes the generation record; a successor's files are byte-identical", { timeout: 30_000 }, async () => {
    const f = setup("die-after-publish");
    try {
      await waitFor(() => f.ready() !== undefined);
      const child = f.ready()!;
      // The launcher is stopped for the whole first sampling window: it never sees owner.json.
      process.kill(f.launcher.pid!, "SIGSTOP");
      fs.writeFileSync(path.join(f.base, "go"), "");
      await waitFor(() => state(child) === "Z");
      const successor = randomUUID();
      fs.writeFileSync(path.join(f.root, "owner.json"), JSON.stringify({ pid: process.pid, processStartTime: startTime(process.pid), token: successor }));
      fs.appendFileSync(path.join(f.root, "launcher.log"), JSON.stringify({ event: "successor-line" }) + "\n");
      fs.writeFileSync(path.join(f.root, "error.json"), "successor");
      const before = tree(f.root);
      process.kill(f.launcher.pid!, "SIGCONT");
      await f.exited;
      const after = tree(f.root);
      const name = path.join("generations", `${f.generation}.exit.json`);
      expect(Object.keys(after).filter(file => !(file in before))).toEqual([name]);
      for (const [file, bytes] of Object.entries(before)) expect(after[file], file).toBe(bytes);
      expect(JSON.parse(after[name]!)).toMatchObject({ generation: f.generation, writer: "launcher", reason: "unreported", pid: child, signal: "SIGKILL" });
    } finally { f.close(); }
  });

  it("a child that wrote its own record before a SIGKILL keeps exactly that one record", { timeout: 30_000 }, async () => {
    const f = setup("complete");
    try {
      await waitFor(() => f.owner() !== undefined && f.records().length === 1);
      await new Promise(resolve => setTimeout(resolve, 500));
      process.kill(f.owner()!.pid, "SIGKILL");
      await f.exited;
      expect(f.records()).toEqual([`${f.generation}.exit.json`]);
      expect(JSON.parse(fs.readFileSync(path.join(f.root, "generations", `${f.generation}.exit.json`), "utf8")))
        .toMatchObject({ writer: "host", reason: "idle-exit" });
    } finally { f.close(); }
  });

  it("a child that left an incomplete record before a SIGKILL: the launcher's diagnostic goes to its generation file; a successor's error.json is byte-identical", { timeout: 30_000 }, async () => {
    const f = setup("partial");
    try {
      await waitFor(() => f.owner() !== undefined && f.records().length === 1);
      const child = f.owner()!.pid;
      await new Promise(resolve => setTimeout(resolve, 500));
      process.kill(f.launcher.pid!, "SIGSTOP");
      process.kill(child, "SIGKILL");
      await waitFor(() => state(child) === "Z");
      // A successor claims the root and writes its own diagnostic before the predecessor's launcher runs again.
      fs.writeFileSync(path.join(f.root, "owner.json"), JSON.stringify({ pid: process.pid, processStartTime: startTime(process.pid), token: randomUUID() }));
      fs.writeFileSync(path.join(f.root, "error.json"), JSON.stringify({ error: "successor diagnostic" }));
      const before = tree(f.root);
      process.kill(f.launcher.pid!, "SIGCONT");
      await f.exited;
      const after = tree(f.root);
      const name = path.join("generations", `${f.generation}.launcher-exit.json`);
      expect(Object.keys(after).filter(file => !(file in before))).toEqual([name]);
      for (const [file, bytes] of Object.entries(before)) expect(after[file], file).toBe(bytes);
      expect(JSON.parse(after[name]!)).toMatchObject({ generation: f.generation, writer: "launcher", reason: "unreported", pid: child,
        signal: "SIGKILL", diagnostic: expect.stringContaining("the existing record is unreadable") });
    } finally { f.close(); }
  });
});
