// Keep: #1395 P2b process-ownership trust boundary/data safety. A stalled native
// worker previously survived stop() while its private run directory was deleted.
// No broker is needed: real private child/grandchild fixtures exercise the same
// WorkerClient boundary; one real FILE counterprobe checks the controller finally.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { WorkerClient, runControlProbe, runFileCounterprobe } from "./helpers/nats-path-b-probe.js";
import { same, startTime, stopOwned, type Owned } from "./helpers/owned-processes.js";
import { teardownWithin } from "./helpers/nats-cluster.js";

const fixtureSource = `#!/usr/bin/env node
import fs from "node:fs";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createInterface } from "node:readline";
const descendant = process.argv[2] === "--descendant";
const configFile = process.argv[3];
const config = JSON.parse(fs.readFileSync(configFile, "utf8"));
await import(config.ownershipPreload);
let child;
const startChild = async level => {
 const result = spawn(process.execPath, [import.meta.filename, "--descendant", configFile, level], {
  env: process.env, stdio: ["ignore", "ignore", "ignore", "ipc"]
 });
 result.once("exit", (code, signal) => fs.appendFileSync(configFile + ".exits", JSON.stringify({ pid: result.pid, code, signal }) + "\\n"));
 await once(result, "message"); return result;
};
if (descendant) {
 if (process.argv[4] === "child") child = await startChild("grandchild");
 process.send({ ready: true });
 // The parent remains alive while cleanup kills leaves, so native exits are reaped.
 process.on("SIGTERM", () => {
  if (process.argv[4] === "grandchild") return; // Real leaf requires SIGKILL escalation.
  if (!child || child.exitCode !== null || child.signalCode !== null) process.exit(0);
  else child.once("exit", () => process.exit(0));
 });
 setInterval(() => {}, 1000);
} else {
 const emit = value => process.stdout.write(JSON.stringify(value) + "\\n");
 if (config.fixtureMode !== "natural") child = await startChild("child");
 if (config.fixtureMode === "fail-ready") emit({ type: "fatal", error: "seeded startup failure" });
 else emit({ type: "ready", role: config.role, stateBackend: config.stateBackend, participantId: "fixture-owner" });
 const lines = createInterface({ input: process.stdin });
 for await (const line of lines) {
  const request = JSON.parse(line);
  if (request.type === "exit-parent") {
   emit({ type: "response", requestId: request.requestId, value: { exiting: true } });
   process.exit(0);
  }
  if (request.type === "shutdown" && config.fixtureMode !== "natural") continue;
  emit({ type: "response", requestId: request.requestId, value: { stopped: true } });
  if (request.type === "shutdown") { lines.close(); process.stdin.destroy(); break; }
 }
}
`;

const roots: string[] = [];
const clients: WorkerClient[] = [];
const emergency: Owned[] = [];
const root = () => {
  const value = fs.mkdtempSync(path.join(process.env.TMPDIR || os.tmpdir(), "probe-cleanup-"));
  fs.chmodSync(value, 0o700); roots.push(value); return value;
};
function runtime(dir: string) {
  const file = path.join(dir, "fixture.mjs");
  fs.writeFileSync(file, fixtureSource, { mode: 0o700 });
  vi.stubEnv("PI_FABRIC_PROBE_RUNTIME", file);
  return file;
}
function fixture(mode = "stall") {
  const dir = root(); runtime(dir);
  const config = path.join(dir, "config.json");
  fs.writeFileSync(config, JSON.stringify({ role: "owner", stateBackend: "file", fixtureMode: mode }));
  const client = new WorkerClient(config, { ...process.env }); clients.push(client);
  if (client.child.pid) emergency.push({ pid: client.child.pid, started: startTime(client.child.pid), at: Date.now(), argv: [] });
  return { dir, config, client };
}
function records(config: string): Owned[] {
  const file = path.join(`${config}.ownership`, "launches.jsonl");
  return fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line) as Owned);
}
function shortenShutdown() {
  const request = WorkerClient.prototype.request;
  vi.spyOn(WorkerClient.prototype, "request").mockImplementation(function(this: WorkerClient, type, body, timeout) {
    return request.call(this, type, body, type === "shutdown" ? 60 : timeout);
  });
}
function allRunBirths(dir: string): Owned[] {
  const configs = path.join(dir, "worker-config");
  if (!fs.existsSync(configs)) return [];
  return fs.readdirSync(configs).filter(name => name.endsWith(".json")).flatMap(name => records(path.join(configs, name)));
}
function observeRunDeletion() {
  const remove = fs.rmSync;
  const deleted: Array<{ dir: string; owned: Owned[] }> = [];
  vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
    if (path.basename(String(target)).startsWith("nats-path-b-probe-")) {
      const owned = allRunBirths(String(target));
      expect(owned.length).toBeGreaterThan(0);
      expect(owned.filter(same)).toEqual([]); // Native births gone BEFORE deletion.
      deleted.push({ dir: String(target), owned });
    }
    return remove(target, options);
  });
  return deleted;
}

afterEach(async () => {
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  // Emergency authority also comes ONLY from captured launch births, never a PID
  // sampled anew after exit (which could belong to an unrelated reused process).
  for (const entry of [...emergency].reverse()) await stopOwned(entry, 300, 1_000);
  for (const client of clients) {
    await teardownWithin("fixture native close", () => new Promise<void>(resolve => {
      if (client.child.exitCode !== null || client.child.signalCode !== null || !client.child.pid) resolve();
      else client.child.once("close", () => resolve());
    }));
  }
  expect(emergency.filter(same)).toEqual([]);
  for (const dir of roots) fs.rmSync(dir, { recursive: true, force: true });
  roots.length = 0; clients.length = 0; emergency.length = 0;
});

describe.skipIf(process.platform !== "linux")("Path B probe owned native cleanup", () => {
  it("forces the retained child/grandchild tree first, reports timeout, and preserves unrelated/stale births", async () => {
    shortenShutdown();
    const { client, config, dir } = fixture(); await client.ready;
    const owned = records(config); emergency.push(...owned);
    expect(new Set(owned.map(row => row.pid)).size).toBe(3);
    const sibling = spawn(process.execPath, ["-e", 'process.stdout.write("ready\\n");setInterval(()=>{},1000)'], { stdio: ["ignore", "pipe", "ignore"] });
    const siblingBirth = { pid: sibling.pid!, started: startTime(sibling.pid!), at: Date.now(), argv: [] };
    emergency.push(siblingBirth); await once(sibling.stdout!, "data");
    const staleSibling = { ...siblingBirth, started: String(Number(siblingBirth.started) - 1) };
    await stopOwned(staleSibling, 10, 10);
    // Exercise the controller's retained log too: a stale launch birth for a live
    // unrelated sibling must not turn its PID into signal authority.
    fs.appendFileSync(path.join(`${config}.ownership`, "launches.jsonl"), `${JSON.stringify(staleSibling)}\n`);
    expect(same(siblingBirth)).toBe(true);
    const failure = await client.stop().then(() => undefined, error => error);
    expect(String(failure)).toContain("probe worker request timed out: shutdown");
    expect(client.cleanupProven).toBe(true);
    expect(client.forced.length).toBe(3);
    expect(client.forced.some(row => row.pid === sibling.pid)).toBe(false);
    const exits = fs.readFileSync(`${config}.exits`, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(exits.some(row => row.signal === "SIGKILL")).toBe(true);
    expect(owned.filter(same)).toEqual([]);
    expect(same(siblingBirth)).toBe(true);
    expect(client.child.signalCode).toBe("SIGTERM");
    expect(fs.existsSync(dir)).toBe(true);
    console.log("PROBE_STALL_CLEANUP", JSON.stringify({ owned, forced: client.forced, sibling: siblingBirth, siblingPreserved: true,
      exit: client.child.exitCode, signal: client.child.signalCode, cleanupProven: client.cleanupProven, failure: String(failure) }));
  });

  it("keeps natural shutdown code 0 without redundant force", async () => {
    const { client, config } = fixture("natural"); await client.ready;
    const owned = records(config); emergency.push(...owned);
    await client.stop();
    expect(client.child.exitCode).toBe(0); expect(client.child.signalCode).toBeNull();
    expect(client.forced).toEqual([]); expect(client.cleanupProven).toBe(true);
    expect(owned.filter(same)).toEqual([]);
    console.log("PROBE_NATURAL_CLEANUP", JSON.stringify({ owned, exit: client.child.exitCode, forced: client.forced }));
  });

  it("does not early-return on an exited parent with live self-recorded orphans", async () => {
    const { client, config } = fixture(); await client.ready;
    const owned = records(config); emergency.push(...owned);
    await client.request("exit-parent");
    if (client.child.exitCode === null) await once(client.child, "close");
    expect(client.child.exitCode).toBe(0);
    expect(owned.filter(same)).toHaveLength(2);
    const failure = await client.stop().then(() => undefined, error => error);
    expect(String(failure)).toContain("Owned probe birth did not exit");
    expect(client.cleanupProven).toBe(true); expect(owned.filter(same)).toEqual([]);
    expect(client.forced).toHaveLength(2);
    console.log("PROBE_ORPHAN_CLEANUP", JSON.stringify({ owned, forced: client.forced, cleanupProven: client.cleanupProven }));
  });

  it("fails closed on unreadable owned identity rather than treating permission failure as absence", async () => {
    shortenShutdown();
    const { client, config, dir } = fixture(); await client.ready;
    const owned = records(config); emergency.push(...owned);
    const read = fs.readFileSync;
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      if (String(file) === `/proc/${client.child.pid}/stat`) throw Object.assign(new Error("seeded identity permission failure"), { code: "EACCES" });
      return Reflect.apply(read, fs, [file, ...args]);
    }) as typeof fs.readFileSync);
    const failure = await client.stop().then(() => undefined, error => error);
    expect(String(failure)).toContain("seeded identity permission failure");
    expect(client.cleanupProven).toBe(false); expect(fs.existsSync(dir)).toBe(true);
    vi.restoreAllMocks();
    console.log("PROBE_UNKNOWN_IDENTITY", JSON.stringify({ owned, preserved: fs.existsSync(dir), cleanupProven: client.cleanupProven, failure: String(failure) }));
  });

  it("retains launch receipts when ancestry enumeration fails, but cannot normalize that uncertainty into cleanup proof", async () => {
    shortenShutdown();
    const { client, config, dir } = fixture(); await client.ready;
    const owned = records(config); emergency.push(...owned);
    const read = fs.readdirSync;
    vi.spyOn(fs, "readdirSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
      if (String(file) === "/proc") throw Object.assign(new Error("seeded proc enumeration failure"), { code: "EACCES" });
      return Reflect.apply(read, fs, [file, ...args]);
    }) as typeof fs.readdirSync);
    const failure = await client.stop().then(() => undefined, error => error);
    expect(String(failure)).toContain("seeded proc enumeration failure");
    expect(client.forced).toHaveLength(3); expect(owned.filter(same)).toEqual([]);
    expect(client.cleanupProven).toBe(false); expect(fs.existsSync(dir)).toBe(true);
    console.log("PROBE_UNKNOWN_TREE", JSON.stringify({ owned, forced: client.forced, preserved: fs.existsSync(dir), cleanupProven: client.cleanupProven, failure: String(failure) }));
  });

  it("propagates startup and graceful cleanup errors, and deletes only after every owned birth is gone", async () => {
    shortenShutdown(); const dir = root(); runtime(dir);
    // The runtime fixture reads the real worker config and reports a startup failure.
    const read = fs.readFileSync;
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      const value = Reflect.apply(read, fs, [file, ...args]);
      if (String(file).includes(`${path.sep}worker-config${path.sep}`) && String(file).endsWith(".json"))
        return JSON.stringify({ ...JSON.parse(String(value)), fixtureMode: "fail-ready" });
      return value;
    }) as typeof fs.readFileSync);
    const deleted = observeRunDeletion();
    const failure = await runFileCounterprobe({ n: 1 }).then(() => undefined, error => error);
    expect(String(failure)).toContain("seeded startup failure");
    expect(String(failure)).toContain("probe worker request timed out: shutdown");
    expect(deleted).toHaveLength(1); expect(fs.existsSync(deleted[0]!.dir)).toBe(false);
    console.log("PROBE_CONTROLLER_STARTUP_FAILURE", JSON.stringify({ failure: String(failure), deleted }));
  });

  it("preserves controller runDir on unprovable cleanup and never deletes an external mesh root", async () => {
    shortenShutdown(); const dir = root(); runtime(dir);
    const external = root(); fs.writeFileSync(path.join(external, "sentinel"), "external-owned");
    const read = fs.readFileSync;
    let unknownPid: number | undefined;
    const stop = WorkerClient.prototype.stop;
    vi.spyOn(WorkerClient.prototype, "stop").mockImplementation(function(this: WorkerClient) { unknownPid = this.child.pid; return stop.call(this); });
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      if (unknownPid && String(file) === `/proc/${unknownPid}/stat`) throw Object.assign(new Error("seeded identity permission failure"), { code: "EACCES" });
      const value = Reflect.apply(read, fs, [file, ...args]);
      if (String(file).includes(`${path.sep}worker-config${path.sep}`) && String(file).endsWith(".json"))
        return JSON.stringify({ ...JSON.parse(String(value)), fixtureMode: "fail-ready" });
      return value;
    }) as typeof fs.readFileSync);
    const failure = await runControlProbe({ n: 1, backend: "FILE-fixed", transport: "file", meshRoots: { owner: external, sender: external } }).then(() => undefined, error => error);
    expect(String(failure)).toContain("seeded startup failure"); expect(String(failure)).toContain("private run directory preserved:");
    const preserved = /private run directory preserved: ([^;\n]+)/.exec(String(failure))?.[1];
    expect(preserved).toBeTruthy(); expect(fs.existsSync(preserved!)).toBe(true);
    vi.restoreAllMocks(); emergency.push(...allRunBirths(preserved!)); roots.push(preserved!);
    expect(fs.readFileSync(path.join(external, "sentinel"), "utf8")).toBe("external-owned");
    console.log("PROBE_CONTROLLER_PRESERVED", JSON.stringify({ preserved, owned: allRunBirths(preserved!), externalPreserved: true, failure: String(failure) }));
  });

  it("keeps root deletion errors separate from the original probe failure", async () => {
    const dir = root(); runtime(dir);
    const read = fs.readFileSync;
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
      const value = Reflect.apply(read, fs, [file, ...args]);
      if (String(file).includes(`${path.sep}worker-config${path.sep}`) && String(file).endsWith(".json"))
        return JSON.stringify({ ...JSON.parse(String(value)), fixtureMode: "natural", stateBackend: "wrong-backend" });
      return value;
    }) as typeof fs.readFileSync);
    const remove = fs.rmSync; let preserved: string | undefined;
    vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
      if (path.basename(String(file)).startsWith("nats-path-b-probe-")) { preserved = String(file); throw new Error("seeded root deletion failure"); }
      return remove(file, options);
    });
    const failure = await runFileCounterprobe({ n: 1 }).then(() => undefined, error => error);
    expect(String(failure)).toContain("silent state-backend fallback refused");
    expect(String(failure)).toContain("seeded root deletion failure");
    expect(preserved).toBeTruthy(); roots.push(preserved!);
    expect(allRunBirths(preserved!).filter(same)).toEqual([]);
  });

  it("handles failed spawn without an unobserved ready rejection or unknown live process", async () => {
    const dir = root(); vi.stubEnv("PI_FABRIC_PROBE_RUNTIME", path.join(dir, "missing-runtime"));
    const config = path.join(dir, "config.json"); fs.writeFileSync(config, "{}");
    const client = new WorkerClient(config, { ...process.env }); clients.push(client);
    await expect(client.ready).rejects.toThrow("ENOENT");
    await expect(client.stop()).rejects.toThrow("probe worker exit status");
    expect(client.cleanupProven).toBe(true); expect(client.forced).toEqual([]);
  });

  it("does not return a real successful FILE metric when graceful cleanup was faulted", async () => {
    const request = WorkerClient.prototype.request;
    vi.spyOn(WorkerClient.prototype, "request").mockImplementation(function(this: WorkerClient, type, body, timeout) {
      // Withhold only shutdown at the existing RPC boundary. Invocation/evidence use
      // the genuine Bun worker and shared native stage assembler, never fake ACKs.
      if (type === "shutdown") return Promise.reject(new Error("seeded real counterprobe shutdown timeout"));
      return request.call(this, type, body, timeout);
    });
    const deleted = observeRunDeletion();
    const failure = await runFileCounterprobe({ n: 1, loadShape: "synchronized-burst", deadlineMs: 10_000 }).then(() => undefined, error => error);
    expect(String(failure)).toContain("seeded real counterprobe shutdown timeout");
    expect(deleted).toHaveLength(1); expect(deleted[0]!.owned.length).toBeGreaterThanOrEqual(2);
    console.log("PROBE_REAL_FILE_FINALLY", JSON.stringify({ failure: String(failure), deleted }));
  }, 45_000);
});
