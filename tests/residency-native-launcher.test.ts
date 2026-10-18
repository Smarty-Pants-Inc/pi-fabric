import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { supervise } from "../src/residency/launcher.js";
import { launchNativePlacementWorker } from "../src/agents/transports/native-placement.js";
import type { AgentTransportLaunch } from "../src/agents/types.js";
import { LaunchRecordStore, makeLaunchRecord, validateLaunchCensus } from "../src/residency/launch-record.js";
import { prepareResidentHostPlacement } from "../src/residency/placement.js";
import { reconcileNativeLaunchRecords, systemdRunArguments, systemdRunEnvironment, verifyNativeLaunchRecord, watchNativeSystemdUnit } from "../src/residency/native-launch.js";

const roots: string[] = [];
const root = (): string => { const value = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-native-launcher-")); roots.push(value); return value; };
afterEach(() => { for (const value of roots.splice(0)) fs.rmSync(value, { recursive: true, force: true }); });

const startParent = (store: LaunchRecordStore): void => {
  const record = makeLaunchRecord({ launchSpecId: "root-spec", orgId: "org-a", workId: "work-a", role: "org-agent",
    profile: "root", sourcePin: "pin", memoryProfile: "root", origin: { class: "background-agent", originRef: "root-a" } }, "root-a", 1);
  store.prepare(record); store.transition(record.native_launch_id, 1, "creating");
  store.transition(record.native_launch_id, 1, "closed-started", {
    boot_id: "12345678-1234-1234-1234-123456789abc", invocation_id: "1234567890abcdef1234567890abcdef", main_pid_start: "123:456",
  });
};

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const waitUntil = async (done: () => boolean, ms = 3_000) => {
  const deadline = Date.now() + ms;
  while (!done() && Date.now() < deadline) await sleep(10);
  return done();
};

const actorPlacementFixture = (directory: string) => {
  const recordRoot = path.join(directory, "actor-records"); fs.mkdirSync(recordRoot, { mode: 0o700 });
  const store = new LaunchRecordStore(recordRoot);
  const rootRecord = makeLaunchRecord({ launchSpecId: "root-spec", orgId: "org-a", workId: "work-a", role: "org-agent", profile: "root",
    sourcePin: "pin", memoryProfile: "root", origin: { class: "background-agent", originRef: "root-a" } }, "root-a", 1);
  store.prepare(rootRecord); store.transition("root-a", 1, "creating");
  store.transition("root-a", 1, "closed-started", { boot_id: "12345678-1234-1234-1234-123456789abc",
    invocation_id: "1234567890abcdef1234567890abcdef", main_pid_start: "123:456" });
  const resident = makeLaunchRecord({ launchSpecId: "resident-spec", orgId: "org-a", workId: "work-a", parentNativeLaunchId: "root-a",
    role: "resident-host", profile: "resident", sourcePin: "pin", memoryProfile: "resident",
    origin: { class: "background-agent", originRef: "resident-a" } }, "resident-a", 1);
  store.prepare(resident); store.transition("resident-a", 1, "creating");
  store.transition("resident-a", 1, "closed-started", { boot_id: "12345678-1234-1234-1234-123456789abc",
    invocation_id: "abcdef1234567890abcdef1234567890", main_pid_start: "124:457" });
  const statePath = path.join(directory, "unit-state.json");
  const blockStop = path.join(directory, "block-stop"); fs.writeFileSync(blockStop, "1");
  const worker = path.join(directory, "worker.mjs"); const workerReady = path.join(directory, "worker-ready");
  fs.writeFileSync(worker, `#!${process.execPath}\nimport fs from 'node:fs'; fs.writeFileSync(${JSON.stringify(workerReady)}, 'ready'); setInterval(() => {}, 1_000);\n`, { mode: 0o700 });
  fs.chmodSync(worker, 0o700);
  const launchLog = path.join(directory, "systemd-run.log"); const controlLog = path.join(directory, "systemctl.log");
  const invocation = "fedcba0987654321fedcba0987654321";
  const systemdRun = path.join(directory, "systemd-run");
  fs.writeFileSync(systemdRun, `#!${process.execPath}
const fs = require('node:fs'); const { spawn } = require('node:child_process'); const args = process.argv.slice(2);
const unit = args[args.indexOf('--unit') + 1]; const separator = args.indexOf('--'); const command = args.slice(separator + 1);
fs.appendFileSync(${JSON.stringify(launchLog)}, JSON.stringify({ unit, args }) + '\\n');
const main = spawn(command[0], command.slice(1), { stdio: 'ignore', env: process.env });
fs.writeFileSync(${JSON.stringify(statePath)}, JSON.stringify({ unit, pid: main.pid, invocation: '${invocation}', active: true }));
main.on('exit', (code, signal) => { fs.writeFileSync(${JSON.stringify(statePath)}, JSON.stringify({ unit, pid: main.pid, invocation: '${invocation}', active: false })); process.exit(signal ? 1 : code ?? 0); });
`, { mode: 0o700 }); fs.chmodSync(systemdRun, 0o700);
  const systemctl = path.join(directory, "systemctl");
  fs.writeFileSync(systemctl, `#!${process.execPath}
const fs = require('node:fs'); const args = process.argv.slice(2); const unit = args.find(value => value.endsWith('.service'));
let state; try { state = JSON.parse(fs.readFileSync(${JSON.stringify(statePath)}, 'utf8')); } catch { state = undefined; }
if (args.includes('stop')) {
  fs.appendFileSync(${JSON.stringify(controlLog)}, JSON.stringify({ op: 'stop', unit, args }) + '\\n');
  if (fs.existsSync(${JSON.stringify(blockStop)})) process.exit(0);
  if (state?.pid) { try { process.kill(state.pid, 'SIGTERM'); } catch {} }
  process.exit(0);
}
let active = state?.unit === unit && state.active === true;
if (active && state?.pid) { try { process.kill(state.pid, 0); } catch { active = false; } }
const row = { op: 'show', unit, active, invocation: state?.unit === unit ? state.invocation : null };
fs.appendFileSync(${JSON.stringify(controlLog)}, JSON.stringify(row) + '\\n');
process.stdout.write('LoadState=' + (state?.unit === unit ? 'loaded' : 'not-found') + '\\nActiveState=' + (active ? 'active' : 'inactive') + '\\nInvocationID=' + (state?.unit === unit ? state.invocation : '') + '\\nMainPID=' + (active ? state.pid : 0) + '\\n');
`, { mode: 0o700 }); fs.chmodSync(systemctl, 0o700);
  const request = {
    id: "actor-a", name: "actor-a", cwd: directory, workerPath: worker, workerArguments: [],
    placementGrant: {
      decision: { grant: { launchId: "actor-a", launchSpecId: "actor-spec" }, generation: 1, fence: 9 },
      record: { orgId: "org-a", workId: "work-a", parentNativeLaunchId: "resident-a", role: "actor-activation",
        profile: "actor", sourcePin: "pin", memoryProfile: "actor", origin: { class: "background-agent", originRef: "actor-a" }, recordRoot },
      revalidate: () => {},
    },
  } as unknown as AgentTransportLaunch;
  return { store, request, workerReady, statePath, blockStop, launchLog, controlLog, systemdRun, systemctl, invocation, directory };
};

const writeContext = (context: string, calls: string, output = "caller-verified=1\n", exitCode = 0, delayMs = 0, stderr = ""): void => {
  fs.writeFileSync(context, `#!${process.execPath}\nconst fs = require('node:fs');\nif (process.argv[2] === '--capabilities') {\n  fs.appendFileSync(${JSON.stringify(calls)}, 'probe\\n');\n  setTimeout(() => {\n    if (${JSON.stringify(stderr)}) process.stderr.write(${JSON.stringify(stderr)});\n    process.stdout.write(${JSON.stringify(output)});\n    process.exit(${exitCode});\n  }, ${delayMs});\n} else process.stdout.write('{}');\n`, { mode: 0o700 });
  fs.chmodSync(context, 0o700);
};

const fixture = (directory: string, options: { failCreate?: boolean; holdHost?: boolean; nativeLaunchId?: string } = {}) => {
  const residencyRoot = path.join(directory, "resident"); fs.mkdirSync(residencyRoot, { mode: 0o700 });
  const recordRoot = path.join(directory, "records"); fs.mkdirSync(recordRoot, { mode: 0o700 });
  const store = new LaunchRecordStore(recordRoot); startParent(store);
  const residentId = options.nativeLaunchId ?? "resident-a";
  const recordPath = path.join(recordRoot, `${residentId}.json`);
  const stateLog = path.join(directory, "before-create.json");
  const pidFile = path.join(directory, "main-pid");
  const unitCreated = path.join(directory, "unit-created");
  const unitState = path.join(directory, "unit-state.json");
  const stopLog = path.join(directory, "systemctl-stop.log");
  const stopBlocked = path.join(directory, "stop-blocked");
  const releaseHost = path.join(directory, "release-host");
  const invocation = "1234567890abcdef1234567890abcdef";
  if (options.holdHost) fs.writeFileSync(stopBlocked, "1");
  const context = path.join(directory, "smarty-launch-context");
  const capabilityCalls = path.join(directory, "capability-calls");
  writeContext(context, capabilityCalls);
  const nativeSystemdRun = path.join(directory, "systemd-run");
  fs.writeFileSync(nativeSystemdRun, `#!${process.execPath}
const fs = require('node:fs'); const { spawn } = require('node:child_process');
const path = require('node:path'); const args = process.argv.slice(2); const unitAt = args.indexOf('--unit'); const unit = args[unitAt + 1]; const separator = args.indexOf('--');
const nativeLaunchId = unit.slice('smarty-launch-'.length).replace(/-g[0-9]+\\.service$/, '');
const primaryRecordPath = path.join(${JSON.stringify(recordRoot)}, nativeLaunchId + '.json');
const shadowRecordPath = path.join(${JSON.stringify(recordRoot)}, 'shadow', nativeLaunchId + '.json');
const recordPath = fs.existsSync(primaryRecordPath) ? primaryRecordPath : shadowRecordPath;
const record = JSON.parse(fs.readFileSync(recordPath, 'utf8'));
fs.writeFileSync(${JSON.stringify(stateLog)}, JSON.stringify({ unit, state: record.fence.state, mode: fs.statSync(recordPath).mode & 0o777, args }));
${options.holdHost ? "" : `fs.writeFileSync(${JSON.stringify(residencyRoot + "/idle-exit.json")}, JSON.stringify({ format: 1, reason: 'root-dead-idle', rootId: 'root-a', pid: process.pid, token: process.env.PI_FABRIC_RESIDENT_GENERATION, at: Date.now(), idleMs: 1 }));`}
${options.failCreate ? `fs.writeFileSync(${JSON.stringify(unitCreated)}, '1'); process.exit(44);` : `if (separator < 0 || unitAt < 0) process.exit(45);
const command = args.slice(separator + 1); const main = spawn(command[0], command.slice(1), { stdio: 'ignore', env: process.env, detached: true });
main.unref(); fs.writeFileSync(${JSON.stringify(pidFile)}, String(main.pid));
fs.writeFileSync(${JSON.stringify(unitState)}, JSON.stringify({ unit, pid: main.pid, invocation: '${invocation}' }));
fs.writeFileSync(${JSON.stringify(unitCreated)}, '1'); process.exit(0);`}
`, { mode: 0o700 }); fs.chmodSync(nativeSystemdRun, 0o700);
  const nativeSystemctl = path.join(directory, "systemctl");
  fs.writeFileSync(nativeSystemctl, `#!${process.execPath}
const fs = require('node:fs'); const args = process.argv.slice(2); const unit = args.find(value => value.endsWith('.service'));
if (args.includes('stop')) {
  fs.appendFileSync(${JSON.stringify(stopLog)}, JSON.stringify({ unit, args }) + '\\n');
  if (fs.existsSync(${JSON.stringify(stopBlocked)})) process.exit(0);
  let pid = 0; try { pid = Number(fs.readFileSync(${JSON.stringify(pidFile)}, 'utf8')); } catch {}
  if (pid > 0) { try { process.kill(pid, 'SIGTERM'); } catch {} }
  process.exit(0);
}
let state; try { state = JSON.parse(fs.readFileSync(${JSON.stringify(unitState)}, 'utf8')); } catch { state = undefined; }
let active = false; const pid = state?.unit === unit ? state.pid : 0;
if (pid > 0) { try { const stat = fs.readFileSync('/proc/' + pid + '/stat', 'utf8'); active = stat.slice(stat.lastIndexOf(')') + 2).trim().split(/\\s+/)[0] !== 'Z'; } catch {} }
const loaded = fs.existsSync(${JSON.stringify(unitCreated)});
process.stdout.write('LoadState=' + (loaded ? 'loaded' : 'not-found') + '\\nActiveState=' + (active ? 'active' : 'inactive') + '\\nInvocationID=' + (state?.unit === unit ? state.invocation : '') + '\\nMainPID=' + (active ? pid : 0) + '\\n');
`, { mode: 0o700 }); fs.chmodSync(nativeSystemctl, 0o700);
  const host = path.join(directory, "host.mjs"); const hostStarted = path.join(directory, "host-started"); const hostEnvironment = path.join(directory, "host-environment.json");
  const hostWait = options.holdHost
    ? `while (!fs.existsSync(${JSON.stringify(releaseHost)})) await new Promise(resolve => setTimeout(resolve, 10));`
    : "await new Promise(resolve => setTimeout(resolve, 500));";
  fs.writeFileSync(host, `import fs from 'node:fs'; fs.writeFileSync(${JSON.stringify(hostStarted)}, 'started'); fs.writeFileSync(${JSON.stringify(hostEnvironment)}, JSON.stringify({ generation: process.env.PI_FABRIC_RESIDENT_GENERATION, config: process.env.PI_FABRIC_RESIDENT_CONFIG })); ${hostWait}\n`);
  const configPath = path.join(residencyRoot, "config.json");
  fs.writeFileSync(configPath, JSON.stringify({ cwd: directory, piBinary: host, rootId: "root-a", sessionId: "session-a",
    residencyRoot, watchdog: { enabled: false },
    agents: { placementDecider: { command: [process.execPath], contextCommand: [context], mode: "enforce", wakeSourceMachine: "ryzen1", recordRoot } },
    placement: { nativeLaunchId: residentId, launchSpecId: "resident-spec", generation: 7, fenceToken: 11,
      orgId: "org-a", workId: "work-a", parentNativeLaunchId: "root-a", role: "resident-host", profile: "resident",
      sourcePin: "pin", memoryProfile: "resident", origin: { class: "background-agent", originRef: residentId }, recordRoot },
  }));
  return { configPath, store, recordPath, stateLog, hostStarted, hostEnvironment, residencyRoot, nativeSystemdRun, nativeSystemctl, context, capabilityCalls,
    stopLog, stopBlocked, releaseHost, pidFile };
};

 describe("resident native systemd launch", () => {
  it("stops the resident unit, not its client, and retains the resident cap without inactive read-back", async () => {
    const directory = root(); const f = fixture(directory, { holdHost: true }); const controller = new AbortController();
    const running = supervise(f.configPath, { nativeSystemdRun: f.nativeSystemdRun, nativeSystemctl: f.nativeSystemctl,
      nativeUnitStopTimeoutMs: 150, signal: controller.signal });
    try {
      expect(await waitUntil(() => fs.existsSync(f.hostStarted))).toBe(true);
      expect(await waitUntil(() => f.store.get("resident-a")?.fence.state === "closed-started")).toBe(true);
      controller.abort();
      await waitUntil(() => fs.existsSync(f.stopLog), 1_000);
      expect(await Promise.race([running.then(() => "settled"), sleep(250).then(() => "pending")])).toBe("pending");
      expect(f.store.get("resident-a")).toMatchObject({ fence: { state: "closed-started" }, terminal: null });
      expect(f.store.get("resident-a")).toMatchObject({ fence: { state: "closed-started" }, terminal: null });
      expect(fs.readFileSync(f.stopLog, "utf8")).toContain('"unit":"smarty-launch-resident-a-g7.service"');
      const config = JSON.parse(fs.readFileSync(f.configPath, "utf8")) as { placement: { nativeLaunchId: string; origin: { originRef: string } } };
      config.placement.nativeLaunchId = "resident-b"; config.placement.origin.originRef = "resident-b";
      fs.writeFileSync(f.configPath, JSON.stringify(config));
      await expect(supervise(f.configPath, { nativeSystemdRun: f.nativeSystemdRun, nativeSystemctl: f.nativeSystemctl }))
        .rejects.toThrow("More than one resident host per root");
      expect(f.store.get("resident-b")).toMatchObject({ fence: { state: "closed-unstarted" }, terminal: { outcome: "never_started" } });
      fs.rmSync(f.stopBlocked, { force: true }); fs.writeFileSync(f.releaseHost, "exit");
      expect(await waitUntil(() => f.store.get("resident-a")?.terminal?.outcome === "exited")).toBe(true);
      await expect(running).rejects.toThrow(/read-back did not prove inactive invocation/);
    } finally {
      fs.rmSync(f.stopBlocked, { force: true });
      if (!fs.existsSync(f.releaseHost)) fs.writeFileSync(f.releaseHost, "exit");
      await waitUntil(() => f.store.get("resident-a")?.terminal?.outcome === "exited", 2_000);
    }
  }, 10_000);

  it("keeps actor custody and blocks replacement while the exact systemd unit remains active", async () => {
    const directory = root(); const f = actorPlacementFixture(directory);
    const handle = await launchNativePlacementWorker(f.request, f.request.workerPath, [], directory,
      { PATH: process.env.PATH ?? "/usr/bin:/bin", HOME: process.env.HOME ?? "/home/test" },
      { systemdRun: f.systemdRun, systemctl: f.systemctl, lifecycle: { pollMs: 10, registerMs: 1_000, stopTimeoutMs: 150 } });
    try {
      expect(await waitUntil(() => fs.existsSync(f.workerReady))).toBe(true);
      expect(await waitUntil(() => f.store.get("actor-a")?.fence.state === "closed-started")).toBe(true);
      await expect(handle.stop()).rejects.toThrow(/did not prove inactive invocation/);
      expect(await Promise.race([handle.closed!.then(() => true), sleep(50).then(() => false)])).toBe(false);
      expect(f.store.get("actor-a")).toMatchObject({ fence: { state: "closed-started" }, terminal: null });
      await expect(launchNativePlacementWorker(f.request, f.request.workerPath, [], directory,
        { PATH: process.env.PATH ?? "/usr/bin:/bin", HOME: process.env.HOME ?? "/home/test" },
        { systemdRun: f.systemdRun, systemctl: f.systemctl, lifecycle: { pollMs: 10, stopTimeoutMs: 150 } }))
        .rejects.toThrow(/identity already exists/);
      expect(fs.readFileSync(f.launchLog, "utf8").trim().split(/\r?\n/u)).toHaveLength(1);
      fs.rmSync(f.blockStop, { force: true });
      await handle.stop();
      await handle.closed!;
      expect(f.store.get("actor-a")?.terminal?.outcome).toBe("exited");
      const events = fs.readFileSync(f.controlLog, "utf8").trim().split(/\r?\n/u).map(line => JSON.parse(line) as { op: string; unit: string; active?: boolean; invocation?: string });
      expect(events.some(event => event.op === "stop" && event.unit === "smarty-launch-actor-a-g1.service")).toBe(true);
      expect(events.some(event => event.op === "show" && event.unit === "smarty-launch-actor-a-g1.service" &&
        event.active === false && event.invocation === f.invocation)).toBe(true);
    } finally {
      if (!f.store.get("actor-a")?.terminal) {
        fs.rmSync(f.blockStop, { force: true });
        await handle.stop().catch(() => undefined);
      }
    }
  }, 10_000);

  it("passes only named non-secret environment and keeps NATS credentials in systemd credentials", () => {
    const credentials = path.join(os.tmpdir(), "mesh-user.creds");
    const environment = {
      PATH: "/usr/bin:/bin", HOME: "/home/test", LANG: "C.UTF-8", TZ: "UTC", XDG_RUNTIME_DIR: "/run/user/1000",
      FOO_TOKEN: "x", OPENAI_API_KEY: "y", SOME_SECRET: "z", PI_FABRIC_RESIDENT_LAUNCH_TOKEN: "launch-token",
      PI_FABRIC_MESH_TRANSPORT: "nats", PI_FABRIC_MESH_NATS: JSON.stringify({ servers: ["tls://nats.example:4222"],
        credentials, machine: "ryzen1", custodyStream: "CUSTODY", livenessStream: "LIVE" }),
    };
    const args = systemdRunArguments("smarty-launch-actor-a-g1.service", os.tmpdir(), ["/bin/true"], environment);
    const safeEnvironment = systemdRunEnvironment(environment);
    expect(args.some(arg => arg.startsWith("--setenv=PATH="))).toBe(true);
    expect(args.some(arg => arg.startsWith("--setenv=HOME="))).toBe(true);
    expect(args.some(arg => arg.startsWith("--setenv=PI_FABRIC_MESH_TRANSPORT="))).toBe(true);
    expect(args).toContain(`--property=LoadCredential=pi-fabric-mesh-nats:${credentials}`);
    expect(args).not.toContain("--collect");
    for (const [name, secret] of [["FOO_TOKEN", "x"], ["OPENAI_API_KEY", "y"], ["SOME_SECRET", "z"],
      ["PI_FABRIC_RESIDENT_LAUNCH_TOKEN", "launch-token"]] as const) {
      expect(args).not.toContain(`--setenv=${name}=${secret}`);
      expect(args.some(arg => arg.startsWith(`--setenv=${name}=`))).toBe(false);
      expect(safeEnvironment).not.toHaveProperty(name);
      expect(args).not.toContain(secret);
    }
    expect(JSON.parse(safeEnvironment.PI_FABRIC_MESH_NATS!)).toMatchObject({ credentials: "%d/pi-fabric-mesh-nats" });
    expect(safeEnvironment.PI_FABRIC_MESH_NATS).not.toContain(credentials);
  });

  it("uses the literal unit and publishes creating durably before native create, then closes started with read-back identity", async () => {
    const directory = root(); const f = fixture(directory);
    writeContext(f.context, f.capabilityCalls, "systemd=1\ncaller-verified=1\n");
    expect(systemdRunArguments("smarty-launch-resident-a-g7.service", directory, ["/bin/true"])).toContain("smarty-launch-resident-a-g7.service");
    await supervise(f.configPath, { nativeSystemdRun: f.nativeSystemdRun, nativeSystemctl: f.nativeSystemctl });
    expect(JSON.parse(fs.readFileSync(f.stateLog, "utf8"))).toMatchObject({
      unit: "smarty-launch-resident-a-g7.service", state: "creating", mode: 0o600,
      args: expect.arrayContaining([expect.stringMatching(/^--setenv=PI_FABRIC_RESIDENT_GENERATION=/u),
        expect.stringMatching(/^--setenv=PI_FABRIC_RESIDENT_CONFIG=/u)]),
    });
    expect(JSON.parse(fs.readFileSync(f.hostEnvironment, "utf8"))).toMatchObject({
      generation: expect.any(String), config: expect.stringContaining("config.json"),
    });
    expect(f.store.get("resident-a")).toMatchObject({
      unit: "smarty-launch-resident-a-g7.service", fence: { token: 11, state: "closed-started" },
      invocation_id: "1234567890abcdef1234567890abcdef", main_pid_start: expect.stringMatching(/^\d+:[1-9]\d*$/),
      terminal: { outcome: "exited" },
    });
  }, 10_000);

  it("closes never-started when systemd read-back proves the refused unit never ran", async () => {
    const directory = root(); const f = fixture(directory, { failCreate: true });
    await supervise(f.configPath, { nativeSystemdRun: f.nativeSystemdRun, nativeSystemctl: f.nativeSystemctl });
    expect(JSON.parse(fs.readFileSync(f.stateLog, "utf8"))).toMatchObject({ state: "creating" });
    expect(f.store.get("resident-a")).toMatchObject({ fence: { state: "closed-unstarted" }, terminal: { outcome: "never_started" } });
  }, 10_000);

  it("refuses enforce startup when caller verification is absent", async () => {
    const directory = root(); const f = fixture(directory);
    writeContext(f.context, f.capabilityCalls, "caller-verified=0\n");
    await expect(supervise(f.configPath, { nativeSystemdRun: f.nativeSystemdRun, nativeSystemctl: f.nativeSystemctl }))
      .rejects.toThrow(/smarty-dev#14903/);
    expect(fs.existsSync(f.hostStarted)).toBe(false);
    expect(fs.readFileSync(f.capabilityCalls, "utf8")).toContain("probe");
    expect(f.store.get("resident-a")).toBeUndefined();
  }, 10_000);

  it("keeps mode-off baseline on the legacy direct launch path", async () => {
    const directory = root(); const f = fixture(directory);
    writeContext(f.context, f.capabilityCalls, "caller-verified=0\n");
    const config = JSON.parse(fs.readFileSync(f.configPath, "utf8")) as { agents: { placementDecider: { mode: string } } };
    config.agents.placementDecider.mode = "off";
    fs.writeFileSync(f.configPath, JSON.stringify(config));
    await supervise(f.configPath);
    expect(fs.existsSync(f.hostStarted)).toBe(true);
    expect(fs.existsSync(f.capabilityCalls)).toBe(false);
    expect(fs.existsSync(f.stateLog)).toBe(false);
    expect(fs.existsSync(f.recordPath)).toBe(false);
  }, 10_000);

  it("does not let shadow record-path failure withhold the baseline host", async () => {
    const directory = root(); const f = fixture(directory);
    writeContext(f.context, f.capabilityCalls, "caller-verified=0\n");
    const blocker = path.join(directory, "record-blocker"); fs.writeFileSync(blocker, "not-a-directory");
    const unavailableRoot = path.join(blocker, "launches");
    const config = JSON.parse(fs.readFileSync(f.configPath, "utf8")) as {
      agents: { placementDecider: { mode: string; recordRoot: string } };
      placement: { recordRoot: string };
    };
    config.agents.placementDecider.mode = "shadow";
    config.agents.placementDecider.recordRoot = unavailableRoot;
    config.placement.recordRoot = unavailableRoot;
    fs.writeFileSync(f.configPath, JSON.stringify(config));
    await supervise(f.configPath);
    expect(fs.existsSync(f.hostStarted)).toBe(true);
    expect(fs.existsSync(f.capabilityCalls)).toBe(false);
    expect(fs.existsSync(f.stateLog)).toBe(false);
  }, 10_000);

  it("keeps shadow resident records outside the enforce census on the same host", async () => {
    const directory = root(); const f = fixture(directory, { nativeLaunchId: "resident-shadow" });
    const config = JSON.parse(fs.readFileSync(f.configPath, "utf8")) as ResidentHostConfig;
    config.agents.placementDecider!.mode = "shadow";
    const shadowConfig = await prepareResidentHostPlacement(config, "resident-shadow");
    expect(shadowConfig.placement).toMatchObject({ nativeLaunchId: "resident-shadow", parentNativeLaunchId: null, role: "resident-host" });
    fs.writeFileSync(f.configPath, JSON.stringify(shadowConfig));
    await supervise(f.configPath, { nativeSystemdRun: path.join(directory, "missing-systemd-run"), nativeSystemctl: f.nativeSystemctl });

    const shadowStore = new LaunchRecordStore(path.join(f.store.root, "shadow"));
    expect(shadowStore.get("resident-shadow")).toMatchObject({ role: "resident-host", parent_native_launch_id: null, fence: { state: "creating" } });
    expect(f.store.get("resident-shadow")).toBeUndefined();
    expect(() => validateLaunchCensus(f.store.census())).not.toThrow();

    fs.rmSync(path.join(f.residencyRoot, "idle-exit.json"), { force: true });
    const enforceConfig = JSON.parse(JSON.stringify(shadowConfig)) as ResidentHostConfig;
    enforceConfig.agents.placementDecider!.mode = "enforce";
    enforceConfig.placement!.nativeLaunchId = "resident-enforce";
    enforceConfig.placement!.generation = 8;
    enforceConfig.placement!.fenceToken = 12;
    enforceConfig.placement!.orgId = "org-a";
    enforceConfig.placement!.workId = "work-a";
    enforceConfig.placement!.parentNativeLaunchId = "root-a";
    enforceConfig.placement!.origin.originRef = "resident-enforce";
    fs.writeFileSync(f.configPath, JSON.stringify(enforceConfig));
    await supervise(f.configPath, { nativeSystemdRun: f.nativeSystemdRun, nativeSystemctl: f.nativeSystemctl });
    expect(f.store.get("resident-enforce")).toMatchObject({ fence: { state: "closed-started" }, terminal: { outcome: "exited" } });
    expect(() => validateLaunchCensus(f.store.census())).not.toThrow();
  }, 15_000);

  it("runs launch-record reconciliation during enforce-mode host startup", async () => {
    const directory = root(); const rootId = "session:native-launch-reconcile";
    const meshRoot = path.join(directory, "mesh"); const residencyRoot = residentRoot(meshRoot, rootId);
    fs.mkdirSync(residencyRoot, { recursive: true, mode: 0o700 });
    const recordRoot = path.join(directory, "records"); fs.mkdirSync(recordRoot, { mode: 0o700 });
    const store = new LaunchRecordStore(recordRoot);
    const open = makeLaunchRecord({ launchSpecId: "open-root", orgId: "org-a", workId: "work-a", role: "org-agent", profile: "root",
      sourcePin: "pin", memoryProfile: "root", origin: { class: "background-agent", originRef: "root-a" } }, "open-root", 1);
    store.prepare(open);
    const config: ResidentHostConfig = {
      format: RESIDENT_HOST_FORMAT, rootId, sessionId: "native-launch-reconcile", cwd: directory, projectRoot: directory,
      meshRoot, actorRoot: path.join(directory, "actors"), residencyRoot, fullCodeMode: true,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, placementDecider: { command: [process.execPath], contextCommand: ["/bin/false"],
        mode: "enforce", wakeSourceMachine: "ryzen1", recordRoot } },
      mesh: DEFAULT_FABRIC_CONFIG.mesh, retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath: path.resolve("dist/agents/worker.js"), fabricExtensionPath: path.resolve("dist/index.js"),
      piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    };
    fs.writeFileSync(path.join(residencyRoot, "config.json"), JSON.stringify(config));
    const host = new ResidentHost(config);
    try {
      await host.start();
      expect(store.get("open-root")).toMatchObject({ fence: { state: "closed-unstarted" }, terminal: { outcome: "never_started" } });
    } finally { await host.close(); }
  }, 15_000);

  it("reconciles an interrupted creating record and a pre-create open record under startup verification", async () => {
    const directory = root(); const store = new LaunchRecordStore(path.join(directory, "records")); startParent(store);
    const creating = makeLaunchRecord({ launchSpecId: "resident-spec", orgId: "org-a", workId: "work-a", parentNativeLaunchId: "root-a",
      role: "resident-host", profile: "resident", sourcePin: "pin", memoryProfile: "resident",
      origin: { class: "background-agent", originRef: "resident-b" } }, "resident-b", 1);
    store.prepare(creating); store.transition(creating.native_launch_id, creating.generation, "creating");
    const open = makeLaunchRecord({ launchSpecId: "open-root", orgId: "org-b", workId: "work-b", role: "org-agent", profile: "root",
      sourcePin: "pin", memoryProfile: "root", origin: { class: "background-agent", originRef: "open-root" } }, "open-root", 1);
    store.prepare(open);
    const nativeSystemctl = path.join(directory, "systemctl");
    fs.writeFileSync(nativeSystemctl, `#!${process.execPath}
const unit = process.argv.slice(2).find(value => value.endsWith('.service'));
if (unit !== ${JSON.stringify(creating.unit)}) process.exit(1);
process.stdout.write('LoadState=loaded\\nActiveState=active\\nInvocationID=abcdef1234567890abcdef1234567890\\nMainPID=${process.pid}\\n');
`, { mode: 0o700 }); fs.chmodSync(nativeSystemctl, 0o700);
    const result = await reconcileNativeLaunchRecords(store, nativeSystemctl);
    expect(result.find(record => record.native_launch_id === "resident-b")).toMatchObject({ fence: { state: "closed-started" },
      invocation_id: "abcdef1234567890abcdef1234567890", main_pid_start: expect.stringMatching(new RegExp(`^${process.pid}:[1-9][0-9]*$`)) });
    expect(store.get("open-root")).toMatchObject({ fence: { state: "closed-unstarted" }, terminal: { outcome: "never_started" } });
  });

  it("releases exact not-found resident records and keeps mismatched invocation evidence fenced", async () => {
    const directory = root(); const store = new LaunchRecordStore(path.join(directory, "records")); startParent(store);
    const residentRecord = (id: string) => makeLaunchRecord({ launchSpecId: `${id}-spec`, orgId: "org-a", workId: "work-a",
      parentNativeLaunchId: "root-a", role: "resident-host", profile: "resident", sourcePin: "pin", memoryProfile: "resident",
      origin: { class: "background-agent", originRef: id } }, id, 1);
    const neverStarted = residentRecord("resident-never-started");
    store.prepare(neverStarted); store.transition(neverStarted.native_launch_id, neverStarted.generation, "creating");
    const uncertain = residentRecord("resident-uncertain");
    const rootUnit = store.get("root-a")!.unit;
    const nativeSystemctl = path.join(directory, "systemctl");
    fs.writeFileSync(nativeSystemctl, `#!${process.execPath}
const unit = process.argv.slice(2).find(value => value.endsWith('.service'));
if (unit === ${JSON.stringify(rootUnit)}) process.stdout.write('LoadState=loaded\\nActiveState=active\\nInvocationID=1234567890abcdef1234567890abcdef\\nMainPID=${process.pid}\\n');
else process.stdout.write('LoadState=not-found\\nActiveState=inactive\\nInvocationID=' + (unit === ${JSON.stringify(uncertain.unit)} ? 'abcdef1234567890abcdef1234567890' : '') + '\\nMainPID=0\\n');
`, { mode: 0o700 }); fs.chmodSync(nativeSystemctl, 0o700);

    await reconcileNativeLaunchRecords(store, nativeSystemctl);
    expect(store.get(neverStarted.native_launch_id)).toMatchObject({ fence: { state: "closed-unstarted" }, terminal: { outcome: "never_started" } });
    const started = await store.admit(residentRecord("resident-started"), true);
    store.transition(started.native_launch_id, started.generation, "closed-started", {
      boot_id: "12345678-1234-1234-1234-123456789abc", invocation_id: "fedcba0987654321fedcba0987654321", main_pid_start: "124:457",
    });
    await reconcileNativeLaunchRecords(store, nativeSystemctl);
    expect(store.get(started.native_launch_id)).toMatchObject({ fence: { state: "closed-started" }, terminal: { outcome: "exited" } });

    const replacement = await store.admit(residentRecord("resident-replacement"), true);
    expect(replacement.fence.state).toBe("creating");
    store.closeUnstartedBeforeCreate(replacement.native_launch_id, replacement.generation);
    store.prepare(uncertain); store.transition(uncertain.native_launch_id, uncertain.generation, "creating");
    await reconcileNativeLaunchRecords(store, nativeSystemctl);
    expect(store.get(uncertain.native_launch_id)).toMatchObject({ fence: { state: "closed-uncertain" }, terminal: null });
    await expect(store.admit(residentRecord("resident-blocked"), true)).rejects.toThrow("More than one resident host per root");
  });

  it("does not release an unstarted not-found unit until its launch client exits", async () => {
    const directory = root(); const store = new LaunchRecordStore(path.join(directory, "records"));
    const record = makeLaunchRecord({ launchSpecId: "resident-spec", orgId: "org-a", workId: "work-a", parentNativeLaunchId: "root-a",
      role: "resident-host", profile: "resident", sourcePin: "pin", memoryProfile: "resident",
      origin: { class: "background-agent", originRef: "resident-wait" } }, "resident-wait", 1);
    store.prepare(record); store.transition(record.native_launch_id, record.generation, "creating");
    const nativeSystemctl = path.join(directory, "systemctl");
    fs.writeFileSync(nativeSystemctl, `#!${process.execPath}\nprocess.stdout.write('LoadState=not-found\\nActiveState=inactive\\nInvocationID=\\nMainPID=0\\n');\n`, { mode: 0o700 });
    fs.chmodSync(nativeSystemctl, 0o700);
    const client = spawn(process.execPath, ["-e", "setTimeout(() => {}, 100)"], { stdio: "ignore" });
    const lifetime = watchNativeSystemdUnit(client, store, record, nativeSystemctl, { pollMs: 5 });
    await sleep(20);
    expect(store.get(record.native_launch_id)?.fence.state).toBe("creating");
    await lifetime.closed;
    expect(store.get(record.native_launch_id)).toMatchObject({ fence: { state: "closed-unstarted" }, terminal: { outcome: "never_started" } });
  }, 10_000);

  it("terminalizes a started watcher after exact unit read-back reports not-found", async () => {
    const directory = root(); const store = new LaunchRecordStore(path.join(directory, "records"));
    const record = makeLaunchRecord({ launchSpecId: "resident-spec", orgId: "org-a", workId: "work-a", parentNativeLaunchId: "root-a",
      role: "resident-host", profile: "resident", sourcePin: "pin", memoryProfile: "resident",
      origin: { class: "background-agent", originRef: "resident-not-found" } }, "resident-not-found", 1);
    store.prepare(record); store.transition(record.native_launch_id, record.generation, "creating");
    store.transition(record.native_launch_id, record.generation, "closed-started", {
      boot_id: "12345678-1234-1234-1234-123456789abc", invocation_id: "fedcba0987654321fedcba0987654321", main_pid_start: "124:457",
    });
    const nativeSystemctl = path.join(directory, "systemctl");
    fs.writeFileSync(nativeSystemctl, `#!${process.execPath}\nprocess.stdout.write('LoadState=not-found\\nActiveState=inactive\\nInvocationID=\\nMainPID=0\\n');\n`, { mode: 0o700 });
    fs.chmodSync(nativeSystemctl, 0o700);
    const client = spawn(process.execPath, ["-e", "setTimeout(() => {}, 30)"], { stdio: "ignore" });
    const lifetime = watchNativeSystemdUnit(client, store, record, nativeSystemctl, { pollMs: 5 });
    await lifetime.closed;
    expect(store.get(record.native_launch_id)).toMatchObject({ fence: { state: "closed-started" }, terminal: { outcome: "exited" } });
  }, 10_000);

  it("verifies and closes a crash-window record left at creating before read-back", async () => {
    const directory = root(); const store = new LaunchRecordStore(path.join(directory, "records"));
    const record = makeLaunchRecord({ launchSpecId: "resident-spec", orgId: "org-a", workId: "work-a", parentNativeLaunchId: "root-a",
      role: "resident-host", profile: "resident", sourcePin: "pin", memoryProfile: "resident",
      origin: { class: "background-agent", originRef: "resident-a" } }, "resident-a", 7);
    store.prepare(record); store.transition(record.native_launch_id, 7, "creating");
    expect(store.get("resident-a")?.fence.state).toBe("creating");
    const missingSystemctl = path.join(directory, "missing-systemctl");
    fs.writeFileSync(missingSystemctl, `#!${process.execPath}\nprocess.exit(1);\n`, { mode: 0o700 }); fs.chmodSync(missingSystemctl, 0o700);
    expect((await verifyNativeLaunchRecord(store, "resident-a", missingSystemctl)).fence.state).toBe("closed-uncertain");

    const second = makeLaunchRecord({ launchSpecId: "resident-spec", orgId: "org-a", workId: "work-a", parentNativeLaunchId: "root-a",
      role: "resident-host", profile: "resident", sourcePin: "pin", memoryProfile: "resident",
      origin: { class: "background-agent", originRef: "resident-b" } }, "resident-b", 8);
    store.prepare(second); store.transition(second.native_launch_id, 8, "creating");
    const liveSystemctl = path.join(directory, "live-systemctl");
    fs.writeFileSync(liveSystemctl, `#!${process.execPath}\nprocess.stdout.write('LoadState=loaded\\nActiveState=active\\nInvocationID=1234567890abcdef1234567890abcdef\\nMainPID=${process.pid}\\n');\n`, { mode: 0o700 }); fs.chmodSync(liveSystemctl, 0o700);
    const verified = await verifyNativeLaunchRecord(store, "resident-b", liveSystemctl);
    expect(verified).toMatchObject({ fence: { state: "closed-started" }, invocation_id: "1234567890abcdef1234567890abcdef" });
    expect(verified.main_pid_start).toMatch(new RegExp(`^${process.pid}:[1-9][0-9]*$`, "u"));
  });
});
