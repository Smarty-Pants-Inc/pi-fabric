import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { supervise } from "../src/residency/launcher.js";
import { LaunchRecordStore, makeLaunchRecord } from "../src/residency/launch-record.js";
import { systemdRunArguments, verifyNativeLaunchRecord } from "../src/residency/native-launch.js";

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

const writeContext = (context: string, calls: string, output = "caller-verified=1\n", exitCode = 0, delayMs = 0, stderr = ""): void => {
  fs.writeFileSync(context, `#!${process.execPath}\nconst fs = require('node:fs');\nif (process.argv[2] === '--capabilities') {\n  fs.appendFileSync(${JSON.stringify(calls)}, 'probe\\n');\n  setTimeout(() => {\n    if (${JSON.stringify(stderr)}) process.stderr.write(${JSON.stringify(stderr)});\n    process.stdout.write(${JSON.stringify(output)});\n    process.exit(${exitCode});\n  }, ${delayMs});\n} else process.stdout.write('{}');\n`, { mode: 0o700 });
  fs.chmodSync(context, 0o700);
};

const fixture = (directory: string, options: { failCreate?: boolean; failReadback?: boolean } = {}) => {
  const residencyRoot = path.join(directory, "resident"); fs.mkdirSync(residencyRoot, { mode: 0o700 });
  const recordRoot = path.join(directory, "records"); fs.mkdirSync(recordRoot, { mode: 0o700 });
  const store = new LaunchRecordStore(recordRoot); startParent(store);
  const recordPath = path.join(recordRoot, "resident-a.json");
  const stateLog = path.join(directory, "before-create.json");
  const pidFile = path.join(directory, "main-pid");
  const invocation = "1234567890abcdef1234567890abcdef";
  const failReadback = path.join(directory, "fail-readback");
  if (options.failReadback) fs.writeFileSync(failReadback, "1");
  const context = path.join(directory, "smarty-launch-context");
  const capabilityCalls = path.join(directory, "capability-calls");
  writeContext(context, capabilityCalls);
  const nativeSystemdRun = path.join(directory, "systemd-run");
  fs.writeFileSync(nativeSystemdRun, `#!${process.execPath}
const fs = require('node:fs'); const { spawn } = require('node:child_process');
const args = process.argv.slice(2); const unitAt = args.indexOf('--unit'); const unit = args[unitAt + 1]; const separator = args.indexOf('--');
const record = JSON.parse(fs.readFileSync(${JSON.stringify(recordPath)}, 'utf8'));
fs.writeFileSync(${JSON.stringify(stateLog)}, JSON.stringify({ unit, state: record.fence.state, mode: fs.statSync(${JSON.stringify(recordPath)}).mode & 0o777, args }));
fs.writeFileSync(${JSON.stringify(residencyRoot + "/idle-exit.json")}, JSON.stringify({ format: 1, reason: 'root-dead-idle', rootId: 'root-a', pid: process.pid, token: process.env.PI_FABRIC_RESIDENT_GENERATION, at: Date.now(), idleMs: 1 }));
${options.failCreate ? "process.exit(44);" : `if (separator < 0 || unitAt < 0) process.exit(45);
const command = args.slice(separator + 1); const main = spawn(command[0], command.slice(1), { stdio: 'ignore', env: process.env });
fs.writeFileSync(${JSON.stringify(pidFile)}, String(main.pid));
main.on('exit', (code, signal) => process.exit(signal ? 1 : code ?? 1));`}
`, { mode: 0o700 }); fs.chmodSync(nativeSystemdRun, 0o700);
  const nativeSystemctl = path.join(directory, "systemctl");
  fs.writeFileSync(nativeSystemctl, `#!${process.execPath}
const fs = require('node:fs');
if (${JSON.stringify(failReadback)} && fs.existsSync(${JSON.stringify(failReadback)})) process.exit(1);
process.stdout.write('InvocationID=${invocation}\\nMainPID=' + fs.readFileSync(${JSON.stringify(pidFile)}, 'utf8') + '\\n');
`, { mode: 0o700 }); fs.chmodSync(nativeSystemctl, 0o700);
  const host = path.join(directory, "host.mjs"); const hostStarted = path.join(directory, "host-started"); const hostEnvironment = path.join(directory, "host-environment.json");
  fs.writeFileSync(host, `import fs from 'node:fs'; fs.writeFileSync(${JSON.stringify(hostStarted)}, 'started'); fs.writeFileSync(${JSON.stringify(hostEnvironment)}, JSON.stringify({ generation: process.env.PI_FABRIC_RESIDENT_GENERATION, config: process.env.PI_FABRIC_RESIDENT_CONFIG })); await new Promise(resolve => setTimeout(resolve, 500));\n`);
  const configPath = path.join(residencyRoot, "config.json");
  fs.writeFileSync(configPath, JSON.stringify({ cwd: directory, piBinary: host, rootId: "root-a", sessionId: "session-a",
    residencyRoot, watchdog: { enabled: false },
    agents: { placementDecider: { command: [process.execPath], contextCommand: [context], mode: "enforce", wakeSourceMachine: "ryzen1", recordRoot } },
    placement: { nativeLaunchId: "resident-a", launchSpecId: "resident-spec", generation: 7, fenceToken: 11,
      orgId: "org-a", workId: "work-a", parentNativeLaunchId: "root-a", role: "resident-host", profile: "resident",
      sourcePin: "pin", memoryProfile: "resident", origin: { class: "background-agent", originRef: "resident-a" }, recordRoot },
  }));
  return { configPath, store, recordPath, stateLog, hostStarted, hostEnvironment, residencyRoot, nativeSystemdRun, nativeSystemctl, context, capabilityCalls };
};

 describe("resident native systemd launch", () => {
  it("uses the literal unit and publishes creating durably before native create, then closes started with read-back identity", async () => {
    const directory = root(); const f = fixture(directory);
    writeContext(f.context, f.capabilityCalls, "systemd=1\ncaller-verified=1\n");
    expect(systemdRunArguments("smarty-launch-resident-a-g7.service", directory, ["/bin/true"])).toContain("smarty-launch-resident-a-g7.service");
    await supervise(f.configPath, { nativeSystemdRun: f.nativeSystemdRun, nativeSystemctl: f.nativeSystemctl });
    expect(JSON.parse(fs.readFileSync(f.stateLog, "utf8"))).toMatchObject({
      unit: "smarty-launch-resident-a-g7.service", state: "creating", mode: 0o600,
      args: expect.arrayContaining(["--setenv=PI_FABRIC_RESIDENT_GENERATION", "--setenv=PI_FABRIC_RESIDENT_CONFIG"]),
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

  it.each([{ failCreate: true }, { failReadback: true }])("closes uncertain when native creation/read-back cannot be proven (%j)", async options => {
    const directory = root(); const f = fixture(directory, options);
    await supervise(f.configPath, { nativeSystemdRun: f.nativeSystemdRun, nativeSystemctl: f.nativeSystemctl });
    expect(JSON.parse(fs.readFileSync(f.stateLog, "utf8"))).toMatchObject({ state: "creating" });
    expect(f.store.get("resident-a")?.fence.state).toBe("closed-uncertain");
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

  it("verifies and closes a crash-window record left at creating before read-back", () => {
    const directory = root(); const store = new LaunchRecordStore(path.join(directory, "records"));
    const record = makeLaunchRecord({ launchSpecId: "resident-spec", orgId: "org-a", workId: "work-a", parentNativeLaunchId: "root-a",
      role: "resident-host", profile: "resident", sourcePin: "pin", memoryProfile: "resident",
      origin: { class: "background-agent", originRef: "resident-a" } }, "resident-a", 7);
    store.prepare(record); store.transition(record.native_launch_id, 7, "creating");
    expect(store.get("resident-a")?.fence.state).toBe("creating");
    const missingSystemctl = path.join(directory, "missing-systemctl");
    fs.writeFileSync(missingSystemctl, `#!${process.execPath}\nprocess.exit(1);\n`, { mode: 0o700 }); fs.chmodSync(missingSystemctl, 0o700);
    expect(verifyNativeLaunchRecord(store, "resident-a", missingSystemctl).fence.state).toBe("closed-uncertain");

    const second = makeLaunchRecord({ launchSpecId: "resident-spec", orgId: "org-a", workId: "work-a", parentNativeLaunchId: "root-a",
      role: "resident-host", profile: "resident", sourcePin: "pin", memoryProfile: "resident",
      origin: { class: "background-agent", originRef: "resident-b" } }, "resident-b", 8);
    store.prepare(second); store.transition(second.native_launch_id, 8, "creating");
    const liveSystemctl = path.join(directory, "live-systemctl");
    fs.writeFileSync(liveSystemctl, `#!${process.execPath}\nprocess.stdout.write('InvocationID=1234567890abcdef1234567890abcdef\\nMainPID=${process.pid}\\n');\n`, { mode: 0o700 }); fs.chmodSync(liveSystemctl, 0o700);
    const verified = verifyNativeLaunchRecord(store, "resident-b", liveSystemctl);
    expect(verified).toMatchObject({ fence: { state: "closed-started" }, invocation_id: "1234567890abcdef1234567890abcdef" });
    expect(verified.main_pid_start).toMatch(new RegExp(`^${process.pid}:[1-9][0-9]*$`, "u"));
  });
});
