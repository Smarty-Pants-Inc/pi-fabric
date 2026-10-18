import { spawnSync } from "node:child_process";
import fs from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { describe, expect, it, vi } from "vitest";
import { taskAgentEnvironment } from "../src/agents/task-environment.js";
import { fleetHostMarker, hostedAdmissionMarker, hostedOnly, isHostedOnlyAllowed, rememberFleetHost } from "../scripts/hosted-only.mjs";

// Data-safety/trust-boundary tests. All positive runner evidence is pure/injected;
// config probes collect exclusions only, and the guard probe records rather than deletes.
const runner: NodeJS.ProcessEnv = {
  CI: "true", GITHUB_ACTIONS: "true", RUNNER_ENVIRONMENT: "github-hosted", RUNNER_TEMP: "/home/runner/work/_temp",
  FABRIC_TRANSPORT_HOSTED_DELETE: "1", FABRIC_TRANSPORT_HOSTED_KILL: "1", FABRIC_TRANSPORT_HOSTED_NATS_RESET: "1",
};
const directory = () => "/home/runner/work/_temp";
const hostedOnlySuites = [
  "tests/actor-child-completion-idle-preflight.test.ts",
  "tests/actor-manager.test.ts",
  "tests/actor-remove-dead-root.test.ts",
  "tests/agent-manager.test.ts",
  "tests/agent-release-observation.test.ts",
  "tests/agents-provider.test.ts",
  "tests/residency-commit-fence.test.ts",
  "tests/residency-stopped-writer-retention.test.ts",
  "tests/residency.test.ts",
  "tests/routed-session-boundaries.test.ts",
  "tests/stall-alarms.test.ts",
  "tests/task-write-policy.test.ts",
  "tests/temp-run-sweep.test.ts",
  "tests/verified-remove-windows.test.ts",
  "tests/windows-temp-root-native.test.ts",
];
const natsBrokerSuites = [
  "tests/mesh-state-backend.test.ts",
  "tests/mesh-state-backend-multiprocess.test.ts",
  "tests/mesh-backend-migration-nats.test.ts",
  "tests/state-backend-nats-election.test.ts",
  "tests/state-backend-nats-publish-dedupe.test.ts",
  "tests/state-backend-nats-pull-recovery.test.ts",
  "tests/nats-auth-server.test.ts",
];

describe("hosted-only authorization", () => {
  it("allows a real-runner environment without a fleet marker, regardless of unrelated opt-ins", () => {
    expect(isHostedOnlyAllowed(runner, directory)).toBe(true);
    expect(isHostedOnlyAllowed({ ...runner, FABRIC_TRANSPORT_HOSTED_DELETE: "0" }, directory)).toBe(true);
    expect(isHostedOnlyAllowed({ ...runner, RUNNER_TEMP: "D:\\a\\_temp" }, () => "D:\\a\\_temp")).toBe(true);
  });

  it.each(["worktree-agent@fixture", "task-agent@fixture"])("refuses SMARTY_ROLE=%j without a parent's admission marker", role => {
    const probe = vi.fn(directory);
    expect(isHostedOnlyAllowed({ ...runner, SMARTY_ROLE: role }, probe)).toBe(false);
    expect(probe).not.toHaveBeenCalled();
  });

  it.each(["worktree-agent@fixture", "", "false"])("refuses a recorded fleet veto even with SMARTY_ROLE=%j", role => {
    const probe = vi.fn(directory);
    expect(isHostedOnlyAllowed({ ...runner, SMARTY_ROLE: role, [fleetHostMarker]: "1" }, probe)).toBe(false);
    expect(probe).not.toHaveBeenCalled();
  });

  it("keeps the veto after attribution scrubbing and in an inherited child environment", () => {
    const env: NodeJS.ProcessEnv = { ...runner, SMARTY_ROLE: "task-agent@fixture" };
    rememberFleetHost(env);
    delete env.SMARTY_ROLE;
    expect(env[fleetHostMarker]).toBe("1");
    rememberFleetHost(env);
    expect(isHostedOnlyAllowed({ ...env }, directory)).toBe(false);
    expect(isHostedOnlyAllowed({ ...runner, [fleetHostMarker]: "" }, directory)).toBe(false);
  });

  it.each([
    { GITHUB_ACTIONS: undefined }, { GITHUB_ACTIONS: "false" }, { GITHUB_ACTIONS: "1" },
    { RUNNER_ENVIRONMENT: undefined }, { RUNNER_ENVIRONMENT: "self-hosted" },
    { RUNNER_TEMP: undefined }, { RUNNER_TEMP: "relative" },
    { RUNNER_TEMP: "/home/paul" }, { RUNNER_TEMP: "/home/paul/fake-runner" }, { RUNNER_TEMP: "/srv/scratch/runner" },
  ])("refuses incomplete, self-hosted or fleet evidence: %j", override => {
    expect(isHostedOnlyAllowed({ ...runner, ...override }, directory)).toBe(false);
  });

  it("refuses missing/non-directory or physically fleet-backed temp; keeps path boundary counterexamples", () => {
    expect(isHostedOnlyAllowed(runner, () => undefined)).toBe(false);
    expect(isHostedOnlyAllowed(runner, () => "/srv/scratch/alias-target")).toBe(false);
    expect(isHostedOnlyAllowed(runner, () => "/home/paul/alias-target")).toBe(false);
    expect(isHostedOnlyAllowed({ ...runner, RUNNER_TEMP: "/home/pauline/runner" }, () => "/home/pauline/runner")).toBe(true);
  });

  it("checks the real filesystem by default and refuses a file or missing runner temp", () => {
    const root = fs.mkdtempSync(join(tmpdir(), "runner-directory-probe-"));
    const file = join(root, "not-a-directory");
    fs.writeFileSync(file, "fixture");
    expect(isHostedOnlyAllowed({ ...runner, RUNNER_TEMP: file })).toBe(false);
    expect(isHostedOnlyAllowed({ ...runner, RUNNER_TEMP: join(root, "missing") })).toBe(false);
  });

  it("passes admitted runner markers to a worker child but never admits a non-hosted parent", () => {
    const hostedParent: NodeJS.ProcessEnv = {
      ...process.env,
      GITHUB_ACTIONS: "true",
      RUNNER_ENVIRONMENT: "github-hosted",
      RUNNER_TEMP: process.platform === "win32" ? tmpdir() : "/tmp",
    };
    delete hostedParent.SMARTY_ROLE;
    delete hostedParent[fleetHostMarker];
    expect(isHostedOnlyAllowed(hostedParent)).toBe(true);

    const probe = (environment: NodeJS.ProcessEnv) => {
      const moduleUrl = pathToFileURL(join(process.cwd(), "scripts", "hosted-only.mjs")).href;
      const guardUrl = pathToFileURL(join(process.cwd(), "tests", "test-deletion-guard.mjs")).href;
      const target = join(process.cwd(), "hosted-only-probe-never-created");
      const source = `import fs from "node:fs";
        const delegated = [];
        fs.rmSync = target => delegated.push(String(target));
        const { isHostedOnlyAllowed } = await import(${JSON.stringify(moduleUrl)});
        await import(${JSON.stringify(guardUrl)});
        let error;
        try { fs.rmSync(${JSON.stringify(target)}); } catch (caught) { error = caught.code; }
        process.stdout.write(JSON.stringify({
          allowed: isHostedOnlyAllowed(process.env),
          runner: [process.env.GITHUB_ACTIONS, process.env.RUNNER_ENVIRONMENT, process.env.RUNNER_TEMP],
          error: error ?? null,
          delegated,
        }));`;
      return spawnSync(process.execPath, ["--input-type=module", "--eval", source], {
        env: { ...environment, NODE_OPTIONS: "" }, encoding: "utf8", timeout: 15_000,
      });
    };

    const hostedChild = taskAgentEnvironment(hostedParent);
    expect(hostedChild).toMatchObject({
      GITHUB_ACTIONS: hostedParent.GITHUB_ACTIONS,
      RUNNER_ENVIRONMENT: hostedParent.RUNNER_ENVIRONMENT,
      RUNNER_TEMP: hostedParent.RUNNER_TEMP,
      SMARTY_ROLE: "task-agent",
      [hostedAdmissionMarker]: "1",
    });
    const hostedResult = probe(hostedChild);
    expect(hostedResult.status, hostedResult.stderr).toBe(0);
    expect(JSON.parse(hostedResult.stdout)).toEqual({
      allowed: true,
      runner: [hostedParent.GITHUB_ACTIONS, hostedParent.RUNNER_ENVIRONMENT, hostedParent.RUNNER_TEMP],
      error: null,
      delegated: [join(process.cwd(), "hosted-only-probe-never-created")],
    });
    expect(hostedResult.stderr).toContain("TEST_DELETION_REPORT:");

    const localChild = taskAgentEnvironment({ ...hostedParent, GITHUB_ACTIONS: "false" });
    const localResult = probe(localChild);
    expect(localResult.status, localResult.stderr).toBe(0);
    expect(JSON.parse(localResult.stdout)).toMatchObject({ allowed: false, error: "ERR_TEST_DELETION_GUARD", delegated: [] });

    const fleetChild = taskAgentEnvironment({ ...hostedParent, [fleetHostMarker]: "1" });
    const fleetResult = probe(fleetChild);
    expect(fleetResult.status, fleetResult.stderr).toBe(0);
    expect(JSON.parse(fleetResult.stdout)).toMatchObject({ allowed: false, error: "ERR_TEST_DELETION_GUARD", delegated: [] });
  });

  it("reports one actionable refusal per case", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const fleet = { ...runner, [fleetHostMarker]: "1" };
      expect(hostedOnly("predicate refusal probe", fleet)).toBe(false);
      expect(hostedOnly("predicate refusal probe", fleet)).toBe(false);
      expect(warn.mock.calls).toEqual([["hosted-only (predicate refusal probe): push and let CI run it"]]);
    } finally { warn.mockRestore(); }
  });
});

it("config and inherited deletion guard refuse the reported fleet sequence after isolation", () => {
  const source = `
    import fs from 'node:fs';
    import { pathToFileURL } from 'node:url';
    const calls = [];
    // Intercept all deleters BEFORE config/guard import. Even a regression only records.
    for (const name of ['rmSync', 'rmdirSync', 'unlinkSync', 'rm', 'rmdir', 'unlink']) {
      fs[name] = (...args) => { calls.push(name); if (typeof args.at(-1) === 'function') args.at(-1)(null); };
    }
    for (const name of ['rm', 'rmdir', 'unlink']) fs.promises[name] = async () => { calls.push(name); };
    const config = (await import(pathToFileURL(process.cwd() + '/vitest.config.cts'))).default;
    const expectedHostedExcludes = ${JSON.stringify(hostedOnlySuites)};
    const expectedNatsExcludes = ${JSON.stringify(natsBrokerSuites)};
    const skipNats = process.env.FABRIC_TEST_SKIP_NATS === '1';
    const before = calls.length;
    await import(pathToFileURL(process.cwd() + '/tests/test-deletion-guard.mjs'));
    let code;
    try { fs.rmSync(process.cwd() + '/HOSTED-REFUSAL-NOT-A-REAL-TARGET'); } catch (error) { code = error.code; }
    console.log('FLEET:' + JSON.stringify({ role: process.env.SMARTY_ROLE, veto: process.env.PI_TEST_FLEET_HOST,
      excludes: config.test.exclude, expectedHostedExcludes, expectedNatsExcludes, skipNats,
      projects: config.test.projects.map(p => p.test.exclude), code, delegated: calls.length - before }));
  `;
  const child = join(fs.mkdtempSync(join(tmpdir(), "hosted-gate-")), "probe.mjs");
  fs.writeFileSync(child, source);
  const result = spawnSync(process.execPath, ["--experimental-transform-types", child], {
    cwd: process.cwd(), encoding: "utf8", timeout: 20_000,
    env: { ...process.env, ...runner, SMARTY_ROLE: "worktree-agent@fixture", NODE_OPTIONS: "" },
  });
  expect(result.status, result.stdout + result.stderr).toBe(0);
  const line = result.stdout.split(/\r?\n/).find(line => line.startsWith("FLEET:"));
  expect(line, result.stdout + result.stderr).toBeDefined();
  const output = JSON.parse(line!.slice(6)) as {
    role?: string;
    veto: string;
    excludes: string[];
    expectedHostedExcludes: string[];
    expectedNatsExcludes: string[];
    skipNats: boolean;
    projects: string[][];
    code?: string;
    delegated: number;
  };
  expect(output.role).toBeUndefined();
  expect(output.veto).toBe("1");
  expect(output.excludes).toEqual(expect.arrayContaining(output.expectedHostedExcludes));
  expect(output.excludes.filter(exclude => !output.expectedHostedExcludes.includes(exclude)))
    .toEqual(output.skipNats ? output.expectedNatsExcludes : []);
  for (const project of output.projects) expect(project).toEqual(expect.arrayContaining(output.excludes));
  expect(output.code).toBe("ERR_TEST_DELETION_GUARD");
  expect(output.delegated).toBe(0);
  expect(result.stderr).toContain("hosted-only (native/tool or reverted deletion suites): push and let CI run it");
});
