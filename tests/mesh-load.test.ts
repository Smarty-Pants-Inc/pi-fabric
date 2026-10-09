import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync, type ChildProcessWithoutNullStreams } from "node:child_process";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import { MeshStore, type MeshStateBackendKind } from "../src/mesh/store.js";
import { importMeshState } from "../src/mesh/backend-migration.js";
import { createScratchRoot, requireScratchRoot, SCRATCH_MARKER } from "../scripts/mesh-load-scratch.js";

const script = fileURLToPath(new URL("../scripts/mesh-load.ts", import.meta.url));
const roots: string[] = [];
const running: ChildProcessWithoutNullStreams[] = [];
const tempRoot = () => {
  const root = createScratchRoot();
  roots.push(root);
  return root;
};
afterEach(async () => {
  // Stop leftovers through stdin, never a signal, so this also holds on Windows.
  await Promise.all(running.splice(0).map(child => child.exitCode !== null || child.signalCode !== null ? undefined : new Promise<void>(resolve => {
    child.once("exit", () => resolve());
    child.stdin.end("stop\n");
  })));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const start = (args: string[], env: NodeJS.ProcessEnv = {}, timeout?: number) => {
  const child = spawn("bun", [script, ...args], { stdio: ["pipe", "pipe", "pipe"], timeout,
    env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "file", ...env } });
  running.push(child);
  const out = { stdout: "", stderr: "" };
  child.stdout.setEncoding("utf8").on("data", chunk => { out.stdout += chunk; });
  child.stderr.setEncoding("utf8").on("data", chunk => { out.stderr += chunk; });
  const exit = new Promise<number | null>((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", code => resolve(code));
  });
  return { child, out, exit };
};
const runId = async (out: { stderr: string }) => {
  for (let i = 0; i < 300 && !/run ([0-9a-f]{8}) /.test(out.stderr); i++) await new Promise(resolve => setTimeout(resolve, 50));
  const id = /run ([0-9a-f]{8}) /.exec(out.stderr)?.[1];
  expect(id, out.stderr).toMatch(/^[0-9a-f]{8}$/);
  return id!;
};
const entriesOf = (root: string, stateBackend: MeshStateBackendKind = "file") => {
  const store = new MeshStore(root, 64 * 1024, 100, { stateBackend });
  try {
    expect(store.stateBackend, JSON.stringify(store.stateDiagnostics())).toBe(stateBackend);
    expect(store.stateDiagnostics().fallback).toBeUndefined();
    return store.listAll("", { fresh: true });
  } finally { store.closeState(); }
};
const keysOf = (root: string, run: string) => entriesOf(root)
  .filter(entry => JSON.stringify(entry).includes(`mesh-load-${run}-`)).map(entry => entry.key).sort();
const synthetic = (root: string, stateBackend: MeshStateBackendKind = "file") => entriesOf(root, stateBackend)
  .filter(entry => JSON.stringify(entry).includes("mesh-load-")).map(entry => entry.key);
const finalReport = (out: { stdout: string }) => JSON.parse(out.stdout.trim().split("\n").at(-1)!) as {
  final: boolean; backend: MeshStateBackendKind; workers: number; writesPerMin: number;
  custodyReads: number; custodyEntries: number; seededStateBytes: number; maxInFlight: number;
};
const waitFor = async (check: () => boolean, ms = 20_000) => {
  const deadline = Date.now() + ms;
  while (!check() && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 100));
  return check();
};

describe("mesh-load root safety", () => {
  const probe = (root: string, env: NodeJS.ProcessEnv = {}, extra: string[] = [], cwd?: string) => spawnSync("bun",
    [script, "--root", root, "--target-writes-per-min", "12", "--target-processes", "1",
      "--seed-state-mb", "0.3", "--duration", "0.01", ...extra],
    { encoding: "utf8", timeout: 5_000, ...(cwd ? { cwd } : {}), env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "sqlite", ...env } });
  const refused = (result: ReturnType<typeof probe>, code: string) => {
    expect(result.error, result.stderr).toBeUndefined();
    expect(result.signal, result.stderr).toBeNull();
    expect(result.status, result.stderr).toBe(2);
    expect(result.stderr).toContain(`MeshLoadRootError [${code}]`);
    expect(result.stderr).not.toMatch(/mesh-load: (?:seeded|run)/);
    expect(result.stdout).toBe("");
  };

  it("refuses a live root, its parent and descendants before any backend is opened, including worker/dry-run entry", () => {
    const parent = tempRoot();
    const live = path.join(parent, "live");
    fs.mkdirSync(live);
    const sentinel = path.join(live, "state.db");
    fs.writeFileSync(sentinel, "do not open this live database");
    const before = fs.readdirSync(live);
    for (const selector of ["PI_FABRIC_MESH_DIR", "PI_FABRIC_MESH_ROOT"]) {
      for (const root of [live, parent, path.join(live, "child")]) {
        for (const extra of [[], ["--worker", "--id", "guard-test"], ["--dry-run"]]) {
          refused(probe(root, { [selector]: live }, extra), "MESH_LOAD_LIVE_ROOT");
        }
      }
    }
    expect(fs.readdirSync(live)).toEqual(before);
    expect(fs.readFileSync(sentinel, "utf8")).toBe("do not open this live database");
    expect(fs.existsSync(path.join(parent, "state.db"))).toBe(false);
  });

  it("refuses symlink aliases of live roots, live config aliases, and missing descendants through aliases", () => {
    const parent = tempRoot();
    const live = path.join(parent, "live");
    const alias = path.join(parent, "alias");
    fs.mkdirSync(live);
    fs.symlinkSync(live, alias, process.platform === "win32" ? "junction" : "dir");
    for (const root of [alias, path.join(alias, "missing")]) refused(probe(root, { PI_FABRIC_MESH_DIR: live }), "MESH_LOAD_LIVE_ROOT");
    refused(probe(live, { PI_FABRIC_MESH_DIR: alias }), "MESH_LOAD_LIVE_ROOT");
    expect(fs.readdirSync(live)).toEqual([]);
  });

  it.each(["dir", "root"])("refuses fabric.json mesh.%s from both host and project config, relative to the project root", (key) => {
    const parent = tempRoot();
    const live = path.join(parent, "configured-live");
    const agent = path.join(parent, "agent");
    const projectConfig = path.join(parent, ".pi");
    fs.mkdirSync(live);
    fs.mkdirSync(agent);
    fs.mkdirSync(projectConfig);
    const env = { PI_FABRIC_MESH_ROOT: "", PI_FABRIC_MESH_DIR: "", PI_FABRIC_PROJECT_ROOT: parent, PI_CODING_AGENT_DIR: agent };
    for (const configDir of [agent, projectConfig]) {
      fs.writeFileSync(path.join(configDir, "fabric.json"), JSON.stringify({ mesh: { [key]: "configured-live" } }));
      refused(probe(live, env, [], parent), "MESH_LOAD_LIVE_ROOT");
    }
    expect(fs.readdirSync(live)).toEqual([]);
  });

  it("refuses every shared-home default mesh and its parent even with an environment override", () => {
    const home = tempRoot();
    const namespace = path.join(home, ".local", "share", "smarty-dev", "fabric-mesh");
    const live = path.join(namespace, "default");
    fs.mkdirSync(live, { recursive: true });
    for (const root of [live, namespace, path.join(live, "missing")]) {
      refused(probe(root, { HOME: home, USERPROFILE: home, PI_FABRIC_MESH_DIR: path.join(home, "other-live") }), "MESH_LOAD_LIVE_ROOT");
    }
    expect(fs.readdirSync(live)).toEqual([]);
  });

  it("refuses unmarked existing and nonexistent explicit roots without creating state or a marker", () => {
    const parent = tempRoot();
    const unmarked = path.join(parent, "unmarked");
    fs.mkdirSync(unmarked);
    for (const root of [unmarked, path.join(parent, "missing")]) {
      refused(probe(root), "MESH_LOAD_NOT_SCRATCH");
      refused(probe(root, {}, ["--worker", "--id", "guard-test"]), "MESH_LOAD_NOT_SCRATCH");
    }
    expect(fs.readdirSync(unmarked)).toEqual([]);
    expect(fs.existsSync(path.join(parent, "missing"))).toBe(false);
  });

  it("rejects copied markers and symlink markers", () => {
    const source = tempRoot();
    const parent = tempRoot();
    const copy = path.join(parent, "copy");
    const alias = path.join(parent, "alias");
    fs.mkdirSync(copy);
    fs.mkdirSync(alias);
    fs.copyFileSync(path.join(source, SCRATCH_MARKER), path.join(copy, SCRATCH_MARKER));
    fs.symlinkSync(path.join(source, SCRATCH_MARKER), path.join(alias, SCRATCH_MARKER));
    for (const root of [copy, alias]) refused(probe(root), "MESH_LOAD_NOT_SCRATCH");
  });

  it("creates a private mkdtemp root with its marker and permits a rerun on that marked root", async () => {
    const first = start(["--target-writes-per-min", "12", "--target-processes", "1", "--duration", "0.01"], {}, 5_000);
    await runId(first.out);
    const root = /run [0-9a-f]{8} root (.+) backend=/.exec(first.out.stderr)![1]!;
    roots.push(root);
    expect(await first.exit, first.out.stderr).toBe(0);
    expect(path.dirname(root)).toBe(fs.realpathSync(os.tmpdir()));
    expect(path.basename(root)).toMatch(/^fabric-mesh-load-/);
    expect(JSON.parse(fs.readFileSync(path.join(root, SCRATCH_MARKER), "utf8"))).toEqual({ format: "mesh-load-scratch/1", root });
    if (process.platform !== "win32") expect(fs.statSync(root).mode & 0o777).toBe(0o700);
    expect(requireScratchRoot(root)).toBe(root);
    const again = start(["--root", root, "--target-writes-per-min", "12", "--target-processes", "1", "--duration", "0.01"], {}, 5_000);
    expect(await again.exit, again.out.stderr).toBe(0);
    expect(finalReport(again.out)).toMatchObject({ final: true, backend: "file" });
    expect(synthetic(root)).toEqual([]);
  });

  it("refuses to create scratch when TMPDIR is inside live mesh, without creating even a marker", () => {
    const live = tempRoot();
    const before = fs.readdirSync(live);
    const result = spawnSync("bun", [script, "--target-writes-per-min", "12", "--target-processes", "1", "--duration", "0.01"],
      { encoding: "utf8", timeout: 5_000, env: { ...process.env, TMPDIR: live, TMP: live, TEMP: live, PI_FABRIC_MESH_DIR: live } });
    refused(result, "MESH_LOAD_LIVE_ROOT");
    expect(fs.readdirSync(live)).toEqual(before);
  });
});

describe("mesh-load", () => {
  it("drives real mesh writes and cleans synthetic presence on termination", async () => {
    const root = tempRoot();
    const { out, exit } = start(["--root", root, "--target-writes-per-min", "1200", "--target-processes", "2", "--duration", "10", "--profile", "fleet"]);
    const exitCode = await exit;
    expect(exitCode, out.stderr).toBe(0);
    const reports = out.stdout.split("\n").filter(Boolean).map(line => JSON.parse(line) as { workers: number; writesPerMin: number; busyPct: number; timeouts: number });
    expect(reports.length).toBeGreaterThan(0);
    expect(reports.some(report => report.workers >= 2)).toBe(true);
    // ponytail: a shared CI runner cannot promise a rate; real writes are the claim here.
    expect(reports.at(-1)!.writesPerMin).toBeGreaterThan(0);
    expect(synthetic(root)).toEqual([]);
  }, 30_000);

  it("isolates concurrent runs on one root: stopping one leaves the other's keys", async () => {
    const root = tempRoot();
    const a = start(["--root", root, "--target-writes-per-min", "60", "--target-processes", "1", "--duration", "0"]);
    const b = start(["--root", root, "--target-writes-per-min", "60", "--target-processes", "1", "--duration", "0"]);
    const [runA, runB] = [await runId(a.out), await runId(b.out)];
    expect(runA).not.toBe(runB);
    expect(await waitFor(() => keysOf(root, runA).length === 2 && keysOf(root, runB).length === 2)).toBe(true);
    const keysB = keysOf(root, runB);
    expect(keysB.some(key => key.startsWith(`actors/mesh-load-${runB}-1/`))).toBe(true);
    a.child.stdin.write("stop\n");
    expect(await a.exit, a.out.stderr).toBe(0);
    expect(keysOf(root, runA)).toEqual([]);
    expect(keysOf(root, runB)).toEqual(keysB);
    b.child.stdin.write("stop\n");
    expect(await b.exit, b.out.stderr).toBe(0);
    expect(synthetic(root)).toEqual([]);
  }, 60_000);

  it("seeds hub-shaped state, mixes keyed puts and custody ops, caps in-flight writes and keeps the seed", async () => {
    const root = tempRoot();
    // 60000/min is a 1 ms pace; two workers contend for the lock, so writes wait and the cap must skip.
    const { out, exit } = start(["--root", root, "--target-writes-per-min", "60000", "--target-processes", "2", "--max-workers", "2",
      "--seed-state-mb", "0.3", "--put-share", "0.4", "--custody-share", "0.4", "--duration", "6", "--control-interval", "1"], { PI_FABRIC_LOCK_STATS: "1" });
    expect(await exit, out.stderr).toBe(0);
    expect(out.stderr).toMatch(/seeded \d+ records; state\.json is \d+ bytes/);
    const final = JSON.parse(out.stdout.trim().split("\n").at(-1)!) as { final: boolean; backend: string; workers: number; skipped: number; maxInFlight: number; writesPerMin: number;
      custodyReads: number; custodyEntries: number; seededStateBytes: number };
    expect(final).toMatchObject({ final: true, backend: "file", workers: 2, maxInFlight: 1 });
    expect(out.stderr).toMatch(/run [0-9a-f]{8} root .+ backend=file/);
    expect(final.seededStateBytes).toBeGreaterThanOrEqual(300_000);
    expect(final.custodyReads).toBeGreaterThan(0);
    expect(final.skipped).toBeGreaterThan(0);
    expect(final.writesPerMin).toBeGreaterThan(0);
    expect(fs.statSync(path.join(root, "state.json")).size).toBeGreaterThanOrEqual(300_000);
    // L8 records all three paced kinds plus heartbeats: keyed puts and heartbeats are writeBatch, custody ops custody.
    const stats = fs.readdirSync(path.join(root, "lock-stats")).map(file => fs.readFileSync(path.join(root, "lock-stats", file), "utf8")).join("");
    for (const lockClass of ["writeBatch", "custody", "publish"]) expect(stats).toContain(`"${lockClass}"`);
    const seeds = entriesOf(root).filter(entry => entry.key.startsWith("fleet/actors/load-seed-a")).length;
    expect(seeds).toBeGreaterThan(0);
    expect(final.custodyEntries).toBeGreaterThanOrEqual(seeds);
    // Run keys (presence, actor, keyed puts) are gone; the seed stays, and a second seed is a no-op.
    expect(synthetic(root)).toEqual([]);
    const again = start(["--root", root, "--target-writes-per-min", "60", "--target-processes", "1", "--seed-state-mb", "0.3", "--duration", "1"]);
    expect(await again.exit, again.out.stderr).toBe(0);
    expect(again.out.stderr).toContain("seeded 0 records");
    expect(entriesOf(root).filter(entry => entry.key.startsWith("fleet/actors/load-seed-a")).length).toBe(seeds);
  }, 60_000);

  it("seeds the effective SQLite backend and custody scans its retained records, not the moved marker", async () => {
    const root = tempRoot();
    // Production sqlite mode requires an imported root, including for a fresh private mesh.
    expect(await importMeshState(root)).toMatchObject({ backend: "sqlite", entries: 0 });
    const statePath = path.join(root, "state.json");
    const marker = fs.readFileSync(statePath, "utf8");
    expect(JSON.parse(marker)).toMatchObject({ format: "sqlite", movedTo: "state.db" });
    expect(JSON.parse(marker).entries).toBeUndefined();
    expect(entriesOf(root, "sqlite")).toEqual([]);

    const env = { PI_FABRIC_MESH_STATE_BACKEND: "sqlite" };
    const run = start(["--root", root, "--target-writes-per-min", "1200", "--target-processes", "1", "--max-workers", "1",
      "--seed-state-mb", "0.3", "--put-share", "0.25", "--custody-share", "0.5", "--duration", "4"], env, 15_000);
    expect(await run.exit, run.out.stderr).toBe(0);
    expect(run.out.stderr).toMatch(/run [0-9a-f]{8} root .+ backend=sqlite/);
    expect(run.out.stderr).not.toMatch(/fallback|mesh-load worker .*:/);
    const seeded = /seeded (\d+) records; backend=sqlite state\.db is (\d+) bytes \+ wal (\d+) bytes \((\d+) bytes total\)/.exec(run.out.stderr);
    expect(seeded, run.out.stderr).not.toBeNull();
    const final = finalReport(run.out);
    expect(final).toMatchObject({ final: true, backend: "sqlite", workers: 1, maxInFlight: 1 });
    expect(final.writesPerMin).toBeGreaterThan(0);
    expect(final.custodyReads).toBeGreaterThan(0);
    expect(final.seededStateBytes).toBeGreaterThanOrEqual(300_000);
    expect(final.seededStateBytes).toBe(Number(seeded![2]) + Number(seeded![3]));
    expect(final.seededStateBytes).toBe(Number(seeded![4]));
    expect(Number(seeded![2])).toBeGreaterThan(0);
    // The marker never grows or acquires seed entries; all retained live records are in SQLite.
    expect(fs.readFileSync(statePath, "utf8")).toBe(marker);
    const seeds = entriesOf(root, "sqlite");
    expect(seeds.length).toBe(Number(seeded![1]));
    expect(seeds.length).toBeGreaterThan(0);
    expect(seeds.every(entry => entry.key.startsWith("fleet/actors/load-seed-a"))).toBe(true);
    expect(seeds[0]!.value).toMatchObject({ kind: "actor", model: "load-seed-model", summary: "s".repeat(820) });
    expect(final.custodyEntries).toBeGreaterThanOrEqual(seeds.length);
    // One worker has at most eight rotating put keys and two heartbeat keys in addition to seeds.
    expect(final.custodyEntries).toBeLessThanOrEqual(seeds.length + 10);
    expect(synthetic(root, "sqlite")).toEqual([]);

    // Last-handle close checkpoints/removes the WAL. Test idempotence against the now-stable DB
    // size, not a transient WAL high-water mark that may legitimately need another seed batch.
    const stableBytes = fs.statSync(path.join(root, "state.db")).size;
    const repeatMb = Math.min(0.3, Math.floor(stableBytes / 1000) / 1000);
    expect(repeatMb).toBeGreaterThan(0);
    const again = start(["--root", root, "--target-writes-per-min", "12", "--target-processes", "1",
      "--seed-state-mb", String(repeatMb), "--duration", "1"], env, 5_000);
    expect(await again.exit, again.out.stderr).toBe(0);
    expect(again.out.stderr).toContain("seeded 0 records; backend=sqlite");
    expect(finalReport(again.out)).toMatchObject({ final: true, backend: "sqlite", custodyReads: 0, custodyEntries: 0 });
    expect(finalReport(again.out).seededStateBytes).toBeGreaterThanOrEqual(repeatMb * 1e6);
    expect(entriesOf(root, "sqlite")).toEqual(seeds);
    expect(synthetic(root, "sqlite")).toEqual([]);
    expect(fs.readFileSync(statePath, "utf8")).toBe(marker);
  }, 30_000);

  it.each([
    ["state.db", "ENOENT"], ["state.db", "EACCES"], ["state.db-wal", "EACCES"],
  ] as const)("fails loudly and promptly when SQLite seed measurement of %s raises %s", async (file, code) => {
    const root = tempRoot();
    await importMeshState(root);
    const marker = fs.readFileSync(path.join(root, "state.json"), "utf8");
    const preload = path.join(root, "measurement-failure.ts");
    // Only the script's direct stat caller is faulted. Store open/guards still use real fs and
    // SQLite, so this proves measurement failure rather than failing before the real seed path.
    fs.writeFileSync(preload, `import fs from "node:fs";
const original = fs.statSync;
const target = ${JSON.stringify(path.join(root, file))};
const script = ${JSON.stringify(script.replace(/\\/g, "/"))};
fs.statSync = function(file, ...args) {
  const caller = new Error().stack?.split("\\n")[2]?.replace(/\\\\/g, "/") ?? "";
  if (String(file) === target && caller.includes(script)) {
    throw Object.assign(new Error("injected seed measurement ${code}"), { code: ${JSON.stringify(code)} });
  }
  return original.call(fs, file, ...args);
};
`);
    const result = spawnSync("bun", ["--preload", preload, script, "--root", root, "--target-writes-per-min", "12",
      "--target-processes", "1", "--seed-state-mb", "0.3", "--duration", "1"], {
      encoding: "utf8", timeout: 5_000, env: { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "sqlite" },
    });
    expect(result.error, result.stderr).toBeUndefined();
    expect(result.signal, result.stderr).toBeNull();
    expect(result.status, result.stderr).not.toBe(0);
    expect(result.stderr).toContain("failed to measure SQLite seed state");
    expect(result.stderr).toContain(`injected seed measurement ${code}`);
    expect(result.stderr).toContain(path.join(root, file));
    expect(result.stderr).not.toMatch(/mesh-load: (?:seeded|run)/);
    expect(result.stdout).not.toContain('"final":true');
    expect(entriesOf(root, "sqlite")).toEqual([]);
    expect(fs.readFileSync(path.join(root, "state.json"), "utf8")).toBe(marker);
  }, 15_000);

  it("does not add workers once the target rate is met", async () => {
    const root = tempRoot();
    // Several 2 s control windows at an easy target: the old controller added a worker on each check.
    const { out, exit } = start(["--root", root, "--target-writes-per-min", "600", "--target-processes", "1", "--duration", "9", "--control-interval", "2"]);
    expect(await exit, out.stderr).toBe(0);
    const final = JSON.parse(out.stdout.trim().split("\n").at(-1)!) as { workers: number; maxInFlight: number };
    expect(final.workers).toBe(1);
    expect(final.maxInFlight).toBe(1);
    expect(synthetic(root)).toEqual([]);
  }, 30_000);

  it("rejects an invalid --duration and an unreachable --target-writes-per-min", () => {
    const root = tempRoot();
    const env = { ...process.env, PI_FABRIC_MESH_STATE_BACKEND: "file" };
    const run = (...extra: string[]) => spawnSync("bun", [script, "--root", root, "--target-processes", "1", "--dry-run", ...extra], { encoding: "utf8", env });
    for (const duration of ["-1", "abc"]) {
      const result = run("--target-writes-per-min", "60", "--duration", duration);
      expect(result.status, result.stderr).toBe(2);
      expect(result.stderr).toContain("--duration must be a number of seconds >= 0");
    }
    for (const target of ["5", "0"]) {
      const result = run("--target-writes-per-min", target);
      expect(result.status, result.stderr).toBe(2);
      expect(result.stderr).toContain("--target-writes-per-min must be at least 12");
    }
    const unbounded = run("--target-writes-per-min", "12", "--duration", "0");
    expect(unbounded.status, unbounded.stderr).toBe(0);
    expect(JSON.parse(unbounded.stdout)).toMatchObject({ dryRun: true, durationSec: 0, workerCap: 1, seedStateMb: 0, putShare: 0, custodyShare: 0, maxInFlight: 1, controlIntervalSec: 30 });
    for (const [flag, value, message] of [["--seed-state-mb", "31", "--seed-state-mb must be"], ["--seed-state-mb", "-1", "--seed-state-mb must be"],
      ["--put-share", "1.5", "--put-share must be"], ["--custody-share", "-0.1", "--custody-share must be"], ["--max-in-flight", "0", "--max-in-flight must be"], ["--control-interval", "0", "--control-interval must be"]] as const) {
      const result = run("--target-writes-per-min", "60", flag, value);
      expect(result.status, result.stderr).toBe(2);
      expect(result.stderr).toContain(message);
    }
    const help = spawnSync("bun", [script, "--help"], { encoding: "utf8", env });
    expect(help.status).toBe(0);
    expect(help.stdout).toContain("0 (default) is explicit unbounded mode");
    expect(synthetic(root)).toEqual([]);
  }, 30_000);
});
