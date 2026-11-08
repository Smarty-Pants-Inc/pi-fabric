import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import { tmpdir } from "node:os";
import { basename, delimiter, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { parse } from "yaml";

type Step = {
  name?: string;
  uses?: string;
  run?: string;
  if?: string;
  shell?: string;
  env?: Record<string, string>;
  with?: Record<string, unknown>;
  "continue-on-error"?: boolean;
};
type Job = {
  name?: string;
  steps?: Step[];
  needs?: string | string[];
  if?: string;
  env?: Record<string, string>;
  "runs-on"?: string | string[];
  "continue-on-error"?: boolean;
  "timeout-minutes"?: number;
  strategy?: { "fail-fast"?: boolean; matrix?: { shard?: number[]; [key: string]: unknown } };
};
type Workflow = {
  name?: string;
  on?: {
    push?: { branches?: string[]; [key: string]: unknown };
    pull_request?: { branches?: string[]; [key: string]: unknown };
    pull_request_target?: unknown;
    workflow_dispatch?: unknown;
  };
  permissions?: { contents?: string; [key: string]: unknown };
  jobs: Record<string, Job>;
  concurrency: { group: string; "cancel-in-progress": string | boolean };
};
type ActionConfig = {
  inputs?: Record<string, { default?: string }>;
  runs?: { steps?: Step[] };
};
type ExpandedStep = { step: Step; localActions: string[] };

const root = new URL("../", import.meta.url);
const manifest = JSON.parse(fs.readFileSync(new URL("package.json", root), "utf8"));
const workflowDirectory = fileURLToPath(new URL(".github/workflows/", root));
const workflows = fs.readdirSync(workflowDirectory).filter((name) => /\.y.*ml$/.test(name));
const workflow = () => parse(fs.readFileSync(new URL(".github/workflows/test.yml", root), "utf8")) as Workflow;
const i9Workflow = () => parse(fs.readFileSync(new URL(".github/workflows/windows-i9.yml", root), "utf8")) as Workflow;
const workflowInventory = () => Object.fromEntries(workflows.map((file) => [
  file,
  parse(fs.readFileSync(new URL(`.github/workflows/${file}`, root), "utf8")) as Workflow,
]));
const action = (name: string) => parse(fs.readFileSync(new URL(`.github/actions/${name}/action.yml`, root), "utf8")) as ActionConfig;
const setupBunAction = "./.github/actions/setup-bun";
function expandedStepsWithOrigins({ steps = [] }: Job, localActions: string[] = []): ExpandedStep[] {
  return steps.flatMap((step) => {
    if (!step.uses?.startsWith("./.github/actions/")) return [{ step, localActions }];
    const nested = parse(fs.readFileSync(new URL(`${step.uses}/action.yml`, root), "utf8")) as ActionConfig;
    return expandedStepsWithOrigins({ steps: nested.runs?.steps ?? [] }, [...localActions, step.uses]);
  });
}
const expandedSteps = (job: Job): Step[] => expandedStepsWithOrigins(job).map(({ step }) => step);
const acceptance = (os: string) => workflow().jobs[`${os}-acceptance`] as Job;

const i9JobId = "windows-i9";
const i9WorkflowFile = "windows-i9.yml";
const i9If = "github.ref == 'refs/heads/main' && (github.event_name == 'push' || github.event_name == 'workflow_dispatch')";
const i9ConcurrencyGroup = "windows-i9-${{ github.ref }}";
const originalPullRequestBranches = ["main", "refactor/sd-6477-**", "feat/sd-6477-**"];
const hostedRunnerLabels = ["ubuntu-latest", "windows-latest"];
const spoofableRunnerMarkers = ["GITHUB_ACTIONS", "RUNNER_ENVIRONMENT", "PI_TEST_HOSTED_ONLY_PARENT_ADMITTED", "PI_TEST_FLEET_HOST"];

function isSelfHosted(job: Job): boolean {
  const labels = Array.isArray(job["runs-on"]) ? job["runs-on"] : [job["runs-on"]];
  return labels.some((label) => typeof label === "string" && label.toLowerCase() === "self-hosted");
}

function assertI9RunnerAdmission(inventory: Record<string, Workflow>): void {
  expect(inventory[i9WorkflowFile]?.jobs[i9JobId]).toBeDefined();
  const selfHostedJobs = Object.entries(inventory).flatMap(([file, config]) =>
    Object.entries(config.jobs)
      .filter(([, job]) => isSelfHosted(job))
      .map(([id]) => `${file}:${id}`),
  ).sort();
  expect(selfHostedJobs).toEqual([`${i9WorkflowFile}:${i9JobId}`]);

  for (const [file, config] of Object.entries(inventory)) {
    for (const [id, job] of Object.entries(config.jobs)) {
      if (file === i9WorkflowFile && id === i9JobId) {
        expect(job["runs-on"]).toEqual(["self-hosted", "Windows", "X64", "smarty-ci-windows-x64"]);
      } else {
        // Unknown literals, expressions, and matrix selections fail closed.
        expect(hostedRunnerLabels).toContain(job["runs-on"]);
      }
    }
  }
}

function assertI9EventAdmission(i9Config: Workflow, testConfig: Workflow): void {
  expect(Object.keys(i9Config.on ?? {}).sort()).toEqual(["push", "workflow_dispatch"]);
  expect(i9Config.on?.push?.branches).toEqual(["main"]);
  expect(i9Config.on?.workflow_dispatch).toBeDefined();
  expect(i9Config.on?.pull_request).toBeUndefined();
  expect(i9Config.on?.pull_request_target).toBeUndefined();
  expect(i9Config.jobs[i9JobId]?.if).toBe(i9If);
  expect(i9Config.name).toBe("Windows i9");
  expect(i9Config.permissions?.contents).toBe("read");

  expect(Object.keys(testConfig.on ?? {}).sort()).toEqual(["pull_request", "push"]);
  expect(testConfig.on?.push?.branches).toEqual(["main"]);
  expect(testConfig.on?.pull_request?.branches).toEqual(originalPullRequestBranches);
  expect(testConfig.on?.workflow_dispatch).toBeUndefined();
  expect(testConfig.on?.pull_request_target).toBeUndefined();
  expect(testConfig.jobs[i9JobId]).toBeUndefined();
}

function assertI9WorkflowIsolation(testConfig: Workflow, i9Config: Workflow): void {
  expect(testConfig.jobs[i9JobId]).toBeUndefined();
  expect(testConfig.on?.workflow_dispatch).toBeUndefined();
  expect(i9Config.on?.workflow_dispatch).toBeDefined();
  expect(i9Config.concurrency.group).toBe(i9ConcurrencyGroup);
  expect(i9Config.concurrency.group).not.toBe(testConfig.concurrency.group);
  expect(i9Config.concurrency["cancel-in-progress"]).toBe(false);
}

function assertI9RunnerEnvironment(inventory: Record<string, Workflow>, setupAction: ActionConfig): void {
  expect(setupAction.inputs?.["runner-environment"]?.default).toBe("github-hosted");
  const actionSteps = Object.entries(inventory).flatMap(([file, config]) => Object.entries(config.jobs)
    .flatMap(([id, job]) => (job.steps ?? [])
      .filter((step) => step.uses === "./.github/actions/check-setup")
      .map((step) => ({ file, id, step }))));
  expect(actionSteps.some(({ file, id }) => file === i9WorkflowFile && id === i9JobId)).toBe(true);
  for (const { file, id, step } of actionSteps) {
    if (file === i9WorkflowFile && id === i9JobId) expect(step.with?.["runner-environment"]).toBe("self-hosted");
    else expect(step.with?.["runner-environment"]).not.toBe("self-hosted");
  }

  const admission = setupAction.runs?.steps?.find((step) => step.env?.EXPECTED_RUNNER_ENVIRONMENT !== undefined);
  expect(admission?.env?.EXPECTED_RUNNER_ENVIRONMENT).toBe("${{ inputs.runner-environment }}");
  expect((admission?.run ?? "").replace(/\s+/g, " ")).toMatch(
    /test\s+["']?\$RUNNER_ENVIRONMENT["']?\s+=\s+["']?\$EXPECTED_RUNNER_ENVIRONMENT["']?/,
  );
  expect(admission?.env?.RUNNER_ENVIRONMENT).toBeUndefined();
  expect(admission?.env?.GITHUB_ACTIONS).toBeUndefined();

  const i9 = inventory[i9WorkflowFile]!.jobs[i9JobId]!;
  const envs = [i9.env, ...(i9.steps ?? []).map((step) => step.env)].filter((env): env is Record<string, string> => env !== undefined);
  for (const marker of spoofableRunnerMarkers) {
    expect(envs.some((env) => Object.hasOwn(env, marker))).toBe(false);
    const assignment = new RegExp(`(?:\\$env:)?\\b${marker}\\s*=`);
    expect((i9.steps ?? []).some((step) => assignment.test(step.run ?? ""))).toBe(false);
  }
}

function hostedOnlyExclusionLines(): string[] {
  const source = fs.readFileSync(new URL("vitest.config.cts", root), "utf8");
  const marker = "const hostedOnlyDeletionSuites = [";
  const start = source.indexOf(marker);
  const end = source.indexOf("\n];", start);
  expect(start).toBeGreaterThanOrEqual(0);
  expect(end).toBeGreaterThan(start);
  const entries = source.slice(start + marker.length, end).split(/\r?\n/).filter((line) => line.trim().length > 0)
    .map((line) => {
      const match = /^\s*"([^"]+)"\s*,\s*\/\/\s*(.+?)\s*$/.exec(line);
      expect(match).not.toBeNull();
      expect(match![1]).toMatch(/^tests\/[^/]+\.test\.ts$/);
      expect(match![2]!.trim().length).toBeGreaterThan(0);
      return `${match![1]} # ${match![2]}`;
    });
  expect(entries).toHaveLength(15);
  return entries;
}

function assertI9ExclusionReport(job: Job, authoritativeLines: string[]): void {
  const report = job.steps?.find((step) => step.name === "Report hosted-only exclusions on i9");
  expect(report).toBeDefined();
  expect(report?.shell).toBe("pwsh");
  expect(report?.run).toContain("Write-Output");
  const lines = (report?.run ?? "").split(/\r?\n/);
  const opening = lines.findIndex((line) => line.includes("@'"));
  const closing = lines.findIndex((line, index) => index > opening && line.trim().startsWith("'@"));
  expect(opening).toBeGreaterThanOrEqual(0);
  expect(closing).toBeGreaterThan(opening);
  const reported = lines.slice(opening + 1, closing).map((line) => line.trim());
  expect(reported).toHaveLength(15);
  for (const entry of reported) expect(entry).toMatch(/^tests\/[^/]+\.test\.ts # \S.+$/);
  expect(reported).toEqual(authoritativeLines);
}

function assertI9JobOperations(i9Config: Workflow, testConfig: Workflow): void {
  const job = i9Config.jobs[i9JobId]!;
  const steps = job.steps ?? [];
  expect(job["continue-on-error"]).toBe(true);
  expect(job["timeout-minutes"]).toBe(90);
  expect(job.strategy?.matrix).toBeUndefined();

  const first = steps[0];
  const last = steps.at(-1);
  expect(first?.name).toMatch(/^Hold\b/);
  expect(last?.name).toMatch(/^Release\b/);
  expect(last?.if).toBe("always()");
  expect(first?.shell).toBe("pwsh");
  expect(last?.shell).toBe("pwsh");
  expect(first?.run).toContain("& 'C:\\ProgramData\\smarty-heavy-lock\\heavy-lock.ps1' -Hold $env:SMARTY_HEAVY_LOCK_MARKER");
  expect(last?.run).toContain("& 'C:\\ProgramData\\smarty-heavy-lock\\heavy-lock.ps1' -Release $env:SMARTY_HEAVY_LOCK_MARKER");
  const marker = first?.env?.SMARTY_HEAVY_LOCK_MARKER;
  for (const part of ["${{ runner.temp }}", "${{ github.run_id }}", "windows-i9", "${{ github.run_attempt }}"]) {
    expect(marker).toContain(part);
  }
  expect(first?.env).toBeDefined();
  expect(Object.keys(first?.env ?? {}).length).toBeGreaterThan(0);
  expect(Object.values(first?.env ?? {}).every((value) => value.length > 0)).toBe(true);
  expect(last?.env).toEqual(first?.env);

  const shards = steps.find((step) => step.name === "Affected tests (10 serial shards)");
  const shardRun = shards?.run ?? "";
  expect(shards?.shell).toBe("pwsh");
  expect(shards?.["continue-on-error"]).not.toBe(true);
  expect(shardRun.replace(/\s+/g, " ")).toMatch(/for\s*\(\$shard\s*=\s*1;\s*\$shard\s*-le\s*10;\s*\$shard\+\+\)/);
  expect(shardRun).toContain('bun run test:affected "--shard=$shard/10"');
  expect(shardRun).toContain('$env:PI_TEST_REPORT_DIR = $reportDir');
  expect(shardRun).toContain('"affected-windows-latest-$shard"');
  expect(shardRun).toContain("if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }");

  const shardIndex = steps.indexOf(shards!);
  const discoverIndex = steps.findIndex((step) => step.name === "Discover unsharded affected test files (no test execution)");
  const parityIndex = steps.findIndex((step) => step.run?.includes("node scripts/ci-test-counts.mjs"));
  const parity = steps[parityIndex];
  expect(discoverIndex).toBeGreaterThanOrEqual(0);
  expect(parityIndex).toBeGreaterThan(shardIndex);
  expect(parityIndex).toBeGreaterThan(discoverIndex);
  expect(parity?.run).toContain("windows-latest 10");
  expect(parity?.run).toContain("unsharded-selection.json");

  const regressions = steps.findIndex((step) => step.uses === "./.github/actions/check-regressions");
  const typeCheck = steps.findIndex((step) => step.run === "bunx vitest run tests/type-checker.test.ts");
  const smoke = steps.findIndex((step) => step.run === "bun run test:smoke");
  expect(regressions).toBeGreaterThanOrEqual(0);
  expect(typeCheck).toBeGreaterThan(regressions);
  expect(smoke).toBeGreaterThan(typeCheck);

  expect(testConfig.jobs["check-windows"]?.needs).toEqual(["windows-gates", "windows-tests", "windows-acceptance"]);
}

// These are contract assertions for PR CI, not a reason to execute the suite on an editing host.
describe("CI Node runtime prerequisites", () => {
  it.each(["linux", "windows"])("runs Python diagnostics with an explicit modern interpreter on %s", (os) => {
    const steps = expandedSteps(acceptance(os));
    const setup = steps.findIndex((step) => step.uses?.startsWith("actions/setup-python@"));
    const diagnostics = steps.findIndex((step) => step.run === "bunx vitest run tests/kernel-errors.test.ts");
    expect(setup).toBeGreaterThanOrEqual(0);
    expect(steps[setup]!.with!["python-version"]).toBe("3.14");
    expect(steps[setup]!.if).toBeUndefined();
    expect(diagnostics).toBeGreaterThan(setup);
    expect(steps[diagnostics]!.if).toBeUndefined();
    const prerequisites = steps.find((step) => step.run?.includes("choco install ripgrep"));
    expect(prerequisites!.run).toContain("mkdir -p /tmp");
  });

  it.each([
    ["extension-hidden-tools", "FABRIC_5492_REAL_PI"],
    ["sender-headers", "FABRIC_6207_REAL_PI"],
    ["main-provider-dead", "FABRIC_8742_REAL_PI"],
  ])("runs the real-Pi %s acceptance only after restoring the gated Linux build", (suite, flag) => {
    const job = acceptance("linux");
    const steps = expandedSteps(job);
    const restore = steps.findIndex((step) => step.name === "Restore gated build (including native executable modes)");
    const realPi = steps.findIndex((step) => step.run === `bunx vitest run tests/${suite}-real-pi.test.ts`);
    expect(job.needs).toBe("linux-gates");
    expect(workflow().jobs["linux-gates"]!.steps?.some((step: Step) => step.run === "bun run build")).toBe(true);
    expect(restore).toBeGreaterThanOrEqual(0);
    expect(realPi).toBeGreaterThan(restore);
    expect(steps[realPi]!.if).toBe("runner.os == 'Linux'");
    expect(steps[realPi]!.env).toEqual({ [flag!]: "1" });
  });

  it("requires the pinned NATS broker for session-channel acceptance on Linux", () => {
    const steps = expandedSteps(acceptance("linux"));
    const install = steps.find((step) => step.name === "Install pinned nats-server for session-channel acceptance");
    const gate = steps.find((step) => step.name === "Session-channel real-broker acceptance");
    expect(install?.if).toBe("runner.os == 'Linux'");
    expect(install?.run).toBe("bash scripts/install-pinned-nats-server.sh");
    const installer = fs.readFileSync(new URL("scripts/install-pinned-nats-server.sh", root), "utf8");
    expect(installer).toContain("version=2.14.7");
    expect(installer).toContain("e5c20b1cb2c0566b54c544312e91e011f9e130c5c80f16a14f4cf28ef30b8be2");
    expect(installer).toContain("https://github.com/nats-io/nats-server/releases/download/v${version}");
    expect(gate?.if).toBe("runner.os == 'Linux'");
    expect(gate?.env).toEqual({ PI_FABRIC_REQUIRE_NATS: "1", NATS_EXPECTED_SERVER_VERSION: "2.14.7" });
    expect(gate?.run).toBe("bunx vitest run --retry=0 tests/nats-session-channel.test.ts");
  });

  // Deliberately pinned jobs retain their own Node versions.
  const pinnedNodeJobs = new Set(["test.yml:mesh-lock-bench"]);
  it.each(workflows)("%s configures the declared Node runtime before install and execution", (file) => {
    const config = parse(fs.readFileSync(new URL(`.github/workflows/${file}`, root), "utf8")) as { jobs: Record<string, Job> };
    for (const [job, definition] of Object.entries(config.jobs)) {
      const expanded = expandedStepsWithOrigins(definition);
      const steps = expanded.map(({ step }) => step);
      // Bun's local installer invokes its freshly unpacked binary for the pinned version check.
      // That bootstrap is not application runtime; all later workflow/composite Bun commands remain covered.
      const executions = expanded.flatMap(({ step, localActions }, index) =>
        /\b(?:node|bun|bunx)\s/.test(step.run ?? "") && !localActions.includes(setupBunAction) ? [index] : []);
      if (executions.length === 0) continue;
      const setup = steps.findIndex((step) => step.uses?.startsWith("actions/setup-node@"));
      expect(setup, `${file}:${job} must set up Node`).toBeGreaterThanOrEqual(0);
      const checkout = steps.findIndex((step) => step.uses?.startsWith("actions/checkout@"));
      expect(checkout).toBeGreaterThanOrEqual(0);
      expect(checkout).toBeLessThan(setup);
      expect(manifest.engines.node).toBeTruthy();
      if (pinnedNodeJobs.has(`${file}:${job}`)) {
        expect(steps[setup]!.with?.["node-version"]).toBeTruthy();
      } else {
        expect(steps[setup]!.with?.["node-version-file"]).toBe("package.json");
        expect(steps[setup]!.with?.["node-version"]).toBeUndefined();
      }
      expect(steps[setup]!.if).toBeUndefined();
      for (const index of executions) expect(setup).toBeLessThan(index);
    }
  });
});

describe("pinned local Bun setup contract", () => {
  it("uses the local Bun action after checkout at every direct workflow setup site", () => {
    const inventory = workflowInventory();
    const consumers = Object.entries(inventory).flatMap(([file, config]) => Object.entries(config.jobs).flatMap(([job, definition]) =>
      (definition.steps ?? []).flatMap((step, index) => step.uses === setupBunAction ? [{ file, job, definition, index }] : [])));
    const externalConsumers = Object.entries(inventory).flatMap(([file, config]) => Object.entries(config.jobs).flatMap(([job, definition]) =>
      (definition.steps ?? []).filter((step) => step.uses?.startsWith("oven-sh/setup-bun@")).map(() => `${file}:${job}`)));

    expect(externalConsumers).toEqual([]);
    expect(consumers).toHaveLength(5);
    expect(consumers.filter(({ file }) => file === "test.yml")).toHaveLength(4);
    expect(consumers.filter(({ file }) => file === "entropy.yml")).toHaveLength(1);
    for (const { file, job, definition, index } of consumers) {
      const checkout = (definition.steps ?? []).findIndex((step) => step.uses?.startsWith("actions/checkout@"));
      expect(checkout, `${file}:${job} must check out before local Bun setup`).toBeGreaterThanOrEqual(0);
      expect(checkout).toBeLessThan(index);
    }

    const sharedSetup = action("check-setup").runs?.steps ?? [];
    expect(sharedSetup.some((step) => step.uses === setupBunAction)).toBe(true);
    expect(sharedSetup.some((step) => step.uses?.startsWith("oven-sh/setup-bun@"))).toBe(false);
    const sharedConsumers = Object.entries(inventory).flatMap(([file, config]) => Object.entries(config.jobs).flatMap(([job, definition]) =>
      (definition.steps ?? []).flatMap((step, index) => step.uses === "./.github/actions/check-setup"
        ? [{ file, job, definition, index }]
        : [])));
    expect(sharedConsumers.length).toBeGreaterThan(0);
    for (const { file, job, definition, index } of sharedConsumers) {
      const checkout = (definition.steps ?? []).findIndex((step) => step.uses?.startsWith("actions/checkout@"));
      expect(checkout, `${file}:${job} must check out before shared Bun setup`).toBeGreaterThanOrEqual(0);
      expect(checkout).toBeLessThan(index);
    }
  });

  it("keys the exact archive cache by SHA and scopes the GitHub token to the download step", () => {
    const steps = action("setup-bun").runs?.steps ?? [];
    const cache = steps.find((step) => step.uses === "actions/cache@v4");
    expect(cache?.with).toEqual({
      path: "${{ steps.pin.outputs.cache_dir }}/${{ steps.pin.outputs.asset }}",
      key: "bun-${{ runner.os }}-${{ runner.arch }}-${{ steps.pin.outputs.sha }}",
    });
    const authenticated = steps.filter((step) => step.env?.GH_TOKEN !== undefined);
    expect(authenticated).toHaveLength(1);
    expect(authenticated[0]?.env).toEqual({
      GH_TOKEN: "${{ github.token }}",
      BUN_ASSET: "${{ steps.pin.outputs.asset }}",
      BUN_CACHE_DIR: "${{ steps.pin.outputs.cache_dir }}",
    });
  });

  it.skipIf(process.platform === "win32")(
    "runs the extracted bunx before publishing PATH (POSIX fixture; Windows uses the native action in CI)",
    () => {
      const fixtureDir = fs.mkdtempSync(join(tmpdir(), "setup-bun-bunx-"));
      try {
        const runnerTemp = join(fixtureDir, "runner-temp");
        const cacheDir = join(fixtureDir, "cache");
        const toolsDir = join(fixtureDir, "tools");
        fs.mkdirSync(runnerTemp);
        fs.mkdirSync(cacheDir);
        fs.mkdirSync(toolsDir);

        const runnerOs = process.platform === "darwin" ? "macOS" : "Linux";
        const asset = runnerOs === "macOS" ? "bun-darwin-x64.zip" : "bun-linux-x64.zip";
        const archivePath = join(cacheDir, asset);
        const archive = Buffer.from("fixture archive; extraction is controlled by the unzip stub");
        fs.writeFileSync(archivePath, archive);
        const archiveSha = createHash("sha256").update(archive).digest("hex");
        const githubPath = join(fixtureDir, "github-path");
        const invocationPathLog = join(fixtureDir, "invocation-paths");
        const publicationLog = join(fixtureDir, "publication-states");
        const fixtureBun = join(fixtureDir, "fixture-bun");
        fs.writeFileSync(fixtureBun, [
          "#!/bin/sh",
          "set -eu",
          'printf \'%s\\n\' "$PATH" >> "$BUN_FIXTURE_PATH_LOG"',
          'if [ -s "$GITHUB_PATH" ]; then printf \'published\\n\' >> "$BUN_FIXTURE_PUBLICATION_LOG"; else printf \'not-published\\n\' >> "$BUN_FIXTURE_PUBLICATION_LOG"; fi',
          'case "${0##*/}" in bunx|bunx.exe) version="$BUN_FIXTURE_BUNX_VERSION" ;; *) version=1.4.4 ;; esac',
          'printf \'%s\\n\' "$version"',
          "",
        ].join("\n"));

        const unzipStub = join(toolsDir, "unzip");
        fs.writeFileSync(unzipStub, [
          "#!/bin/sh",
          "set -eu",
          '[ "$1" = "-q" ] && [ "$3" = "-d" ]',
          'asset=${2##*/}',
          'unpacked=${asset%.zip}',
          'mkdir -p "$4/$unpacked"',
          'cp "$BUN_FIXTURE_BUN" "$4/$unpacked/bun"',
          "",
        ].join("\n"));
        fs.chmodSync(unzipStub, 0o755);

        const installStep = action("setup-bun").runs?.steps?.find((step) => step.name === "Verify and install Bun");
        expect(installStep?.run).toBeTruthy();
        const installerEnv = {
          ...process.env,
          PATH: `${toolsDir}${delimiter}${process.env.PATH ?? ""}`,
          RUNNER_OS: runnerOs,
          RUNNER_ARCH: "X64",
          RUNNER_TEMP: runnerTemp,
          BUN_ASSET: asset,
          BUN_SHA: archiveSha,
          BUN_CACHE_DIR: cacheDir,
          GITHUB_PATH: githubPath,
          BUN_FIXTURE_BUN: fixtureBun,
          BUN_FIXTURE_PATH_LOG: invocationPathLog,
          BUN_FIXTURE_PUBLICATION_LOG: publicationLog,
          BUN_FIXTURE_BUNX_VERSION: "1.4.4",
        };
        const runInstaller = () => spawnSync("/bin/bash", ["-c", installStep!.run!], {
          cwd: cacheDir,
          env: installerEnv,
          encoding: "utf8",
        });

        const installed = runInstaller();
        expect(installed.status).toBe(0);
        expect(installed.stderr).toBe("");
        const publishedEntries = fs.readFileSync(githubPath, "utf8").trim().split(/\r?\n/);
        expect(publishedEntries).toHaveLength(1);
        const binDir = publishedEntries[0]!;
        expect(basename(binDir)).toBe(asset.slice(0, -".zip".length));
        expect(fs.readdirSync(binDir).sort()).toEqual(["bun", "bunx"]);

        const bunx = join(binDir, "bunx");
        expect(fs.lstatSync(bunx).isSymbolicLink()).toBe(true);
        expect(fs.readlinkSync(bunx)).toBe("bun");
        fs.accessSync(bunx, fs.constants.X_OK);
        const bunxRun = spawnSync(bunx, ["--version"], {
          env: {
            PATH: binDir,
            GITHUB_PATH: githubPath,
            BUN_FIXTURE_PATH_LOG: invocationPathLog,
            BUN_FIXTURE_PUBLICATION_LOG: publicationLog,
            BUN_FIXTURE_BUNX_VERSION: "1.4.4",
          },
          encoding: "utf8",
        });
        expect(bunxRun.status).toBe(0);
        expect(bunxRun.stdout.trim()).toBe("1.4.4");
        expect(fs.readFileSync(invocationPathLog, "utf8").trim().split(/\r?\n/).at(-1)).toBe(binDir);
        expect(fs.readFileSync(publicationLog, "utf8").trim().split(/\r?\n/)).toEqual([
          "not-published",
          "not-published",
          "published",
        ]);

        fs.writeFileSync(githubPath, "");
        installerEnv.BUN_FIXTURE_BUNX_VERSION = "1.4.5";
        const rejected = runInstaller();
        expect(rejected.status).not.toBe(0);
        expect(rejected.stderr).toContain("Expected bunx 1.4.4, got: 1.4.5");
        expect(fs.readFileSync(githubPath, "utf8")).toBe("");
        expect(fs.readFileSync(publicationLog, "utf8").trim().split(/\r?\n/)).toEqual([
          "not-published",
          "not-published",
          "published",
          "not-published",
          "not-published",
        ]);
      } finally {
        fs.rmSync(fixtureDir, { recursive: true, force: true });
      }
    },
  );
});

describe("Windows i9 test observation contract", () => {
  it("reserves exactly one named self-hosted job for windows-i9 and keeps every other job hosted", () => {
    assertI9RunnerAdmission(workflowInventory());
  });

  it("rejects any second self-hosted job within windows-i9.yml", () => {
    const mutant = structuredClone(workflowInventory());
    mutant[i9WorkflowFile]!.jobs["other-self-hosted"] = {
      "runs-on": ["self-hosted", "Windows", "other"],
    };
    expect(() => assertI9RunnerAdmission(mutant)).toThrow();
  });

  it("allows hosted jobs in another .yaml workflow with only the exact i9 exception", () => {
    const mutant = structuredClone(workflowInventory());
    mutant["other.yaml"] = structuredClone(mutant["entropy.yml"]!);
    assertI9RunnerAdmission(mutant);
  });

  it("rejects a self-hosted job added by another workflow", () => {
    const mutant = structuredClone(workflowInventory());
    mutant["newfile.yml"] = structuredClone(mutant["entropy.yml"]!);
    mutant["newfile.yml"]!.jobs["other-self-hosted"] = {
      name: "unapproved self-hosted job",
      "runs-on": ["self-hosted", "windows", "other"],
    };
    expect(() => assertI9RunnerAdmission(mutant)).toThrow();
  });

  it("rejects a duplicate windows-i9 exception in another workflow", () => {
    const mutant = structuredClone(workflowInventory());
    mutant["newfile.yml"] = structuredClone(mutant["entropy.yml"]!);
    mutant["newfile.yml"]!.jobs[i9JobId] = structuredClone(mutant[i9WorkflowFile]!.jobs[i9JobId]!);
    expect(() => assertI9RunnerAdmission(mutant)).toThrow();
  });

  const unresolvedRunnerMutations: Array<[string, (job: Job) => void]> = [
    ["a direct string expression", (job) => { job["runs-on"] = "${{ inputs.runner }}"; }],
    ["an expression in a runs-on array", (job) => { job["runs-on"] = ["${{ 'self-hosted' }}", "Windows"]; }],
    ["a self-hosted matrix selection", (job) => {
      job["runs-on"] = "${{ matrix.runner }}";
      job.strategy = { matrix: { runner: ["ubuntu-latest", "self-hosted"] } };
    }],
  ];
  it.each(unresolvedRunnerMutations)("fails closed for %s", (_case, configureJob) => {
    const mutant = structuredClone(workflowInventory());
    mutant["newfile.yml"] = structuredClone(mutant["entropy.yml"]!);
    const job: Job = { "runs-on": "ubuntu-latest" };
    configureJob(job);
    mutant["newfile.yml"]!.jobs["unresolved-runner"] = job;
    expect(() => assertI9RunnerAdmission(mutant)).toThrow();
  });

  it("keeps Test unchanged and allows i9 dispatch in its independent non-cancelling workflow", () => {
    const testConfig = workflow();
    const i9Config = i9Workflow();
    assertI9EventAdmission(i9Config, testConfig);
    assertI9WorkflowIsolation(testConfig, i9Config);
  });

  it("rejects reinserting windows-i9 into test.yml", () => {
    const testMutant = structuredClone(workflow());
    testMutant.jobs[i9JobId] = structuredClone(i9Workflow().jobs[i9JobId]!);
    expect(() => assertI9WorkflowIsolation(testMutant, i9Workflow())).toThrow();
  });

  it("rejects workflow_dispatch on the hosted Test workflow", () => {
    const testMutant = structuredClone(workflow());
    testMutant.on!.workflow_dispatch = {};
    expect(() => assertI9WorkflowIsolation(testMutant, i9Workflow())).toThrow();
  });

  it("rejects sharing Test's concurrency group with i9", () => {
    const testConfig = workflow();
    const i9Mutant = structuredClone(i9Workflow());
    i9Mutant.concurrency.group = testConfig.concurrency.group;
    expect(() => assertI9WorkflowIsolation(testConfig, i9Mutant)).toThrow();
  });

  it("rejects cancellation of an in-flight i9 run", () => {
    const testConfig = workflow();
    const i9Mutant = structuredClone(i9Workflow());
    i9Mutant.concurrency["cancel-in-progress"] = true;
    expect(() => assertI9WorkflowIsolation(testConfig, i9Mutant)).toThrow();
  });

  it("admits i9 only on trusted main push or dispatch and preserves Test's PR triggers", () => {
    assertI9EventAdmission(i9Workflow(), workflow());
  });

  it("rejects weaker i9 ref and OR conditions", () => {
    const weakenedConditions = [
      "github.ref == 'refs/heads/main' || (github.event_name == 'push' || github.event_name == 'workflow_dispatch')",
      "github.ref != 'refs/heads/main' && (github.event_name == 'push' || github.event_name == 'workflow_dispatch')",
    ];
    for (const condition of weakenedConditions) {
      const mutant = structuredClone(i9Workflow());
      mutant.jobs[i9JobId]!.if = condition;
      expect(() => assertI9EventAdmission(mutant, workflow())).toThrow();
    }
  });

  it.each([
    ["pull_request", "github.event_name == 'pull_request'"],
    ["pull_request_target", "github.event_name == 'pull_request_target'"],
  ])("rejects an i9 job condition that admits %s", (_event, addedEvent) => {
    const mutant = structuredClone(i9Workflow());
    mutant.jobs[i9JobId]!.if = `github.ref == 'refs/heads/main' && (github.event_name == 'push' || github.event_name == 'workflow_dispatch' || ${addedEvent})`;
    expect(() => assertI9EventAdmission(mutant, workflow())).toThrow();
  });

  it("rejects widened push branches, changed PR branches, or pull_request_target triggers", () => {
    const mutations: Array<{ target: "i9" | "test"; mutate: (config: Workflow) => void }> = [
      { target: "i9", mutate: (config) => { config.on!.push!.branches = ["main", "release/**"]; } },
      { target: "test", mutate: (config) => { config.on!.pull_request!.branches = ["main"]; } },
      { target: "i9", mutate: (config) => { config.on!.pull_request_target = { branches: ["main"] }; } },
      { target: "test", mutate: (config) => { config.on!.pull_request_target = { branches: ["main"] }; } },
    ];
    for (const { target, mutate } of mutations) {
      const i9Mutant = structuredClone(i9Workflow());
      const testMutant = structuredClone(workflow());
      mutate(target === "i9" ? i9Mutant : testMutant);
      expect(() => assertI9EventAdmission(i9Mutant, testMutant)).toThrow();
    }
  });

  it("defaults setup to hosted and compares every setup consumer's actual runner environment with the input", () => {
    assertI9RunnerEnvironment(workflowInventory(), action("check-setup"));
  });

  it("rejects a changed hosted default or self-hosted setup passed by another job", () => {
    const wrongDefault = structuredClone(action("check-setup"));
    wrongDefault.inputs!["runner-environment"]!.default = "self-hosted";
    expect(() => assertI9RunnerEnvironment(workflowInventory(), wrongDefault)).toThrow();

    const wrongConsumer = structuredClone(workflowInventory());
    const windowsSetup = wrongConsumer["test.yml"]!.jobs["windows-tests"]!.steps!.find((step) => step.uses === "./.github/actions/check-setup")!;
    windowsSetup.with = { ...windowsSetup.with, "runner-environment": "self-hosted" };
    expect(() => assertI9RunnerEnvironment(wrongConsumer, action("check-setup"))).toThrow();
  });

  it.each(spoofableRunnerMarkers)("rejects spoofing the %s runner marker", (marker) => {
    const mutant = structuredClone(workflowInventory());
    const i9 = mutant[i9WorkflowFile]!.jobs[i9JobId]!;
    i9.env = { ...i9.env, [marker]: "true" };
    expect(() => assertI9RunnerEnvironment(mutant, action("check-setup"))).toThrow();
  });

  it("reports the exact named hosted-only exclusions and their authoritative reasons", () => {
    assertI9ExclusionReport(i9Workflow().jobs[i9JobId]!, hostedOnlyExclusionLines());
  });

  it("rejects a missing hosted-only exclusion", () => {
    const mutant = structuredClone(i9Workflow());
    const report = mutant.jobs[i9JobId]!.steps!.find((step) => step.name === "Report hosted-only exclusions on i9")!;
    const expected = hostedOnlyExclusionLines()[0]!;
    report.run = report.run!.split(/\r?\n/).filter((line) => line.trim() !== expected).join("\n");
    expect(() => assertI9ExclusionReport(mutant.jobs[i9JobId]!, hostedOnlyExclusionLines())).toThrow();
  });

  it("rejects an unnamed hosted-only exclusion", () => {
    const mutant = structuredClone(i9Workflow());
    const report = mutant.jobs[i9JobId]!.steps!.find((step) => step.name === "Report hosted-only exclusions on i9")!;
    const expected = hostedOnlyExclusionLines()[0]!;
    const reason = expected.split(" # ")[1]!;
    report.run = report.run!.replace(expected, `# ${reason}`);
    expect(() => assertI9ExclusionReport(mutant.jobs[i9JobId]!, hostedOnlyExclusionLines())).toThrow();
  });

  it("rejects a hosted-only exclusion without a reason", () => {
    const mutant = structuredClone(i9Workflow());
    const report = mutant.jobs[i9JobId]!.steps!.find((step) => step.name === "Report hosted-only exclusions on i9")!;
    const expected = hostedOnlyExclusionLines()[0]!;
    report.run = report.run!.replace(expected, expected.split(" # ")[0]!);
    expect(() => assertI9ExclusionReport(mutant.jobs[i9JobId]!, hostedOnlyExclusionLines())).toThrow();
  });

  it("keeps i9 serial shards, native failures, parity, and Windows acceptance ordering", () => {
    assertI9JobOperations(i9Workflow(), workflow());
  });
});

describe("hosted sharded required checks", () => {
  it.each([["linux", "ubuntu-latest"], ["windows", "windows-latest"]])("requires every %s shard and singleton gate without renaming the required context", (platform, os) => {
    const { jobs } = workflow();
    const shards = jobs[`${platform}-tests`] as Job;
    expect(shards["runs-on"]).toBe(os);
    expect(shards.needs).toBe(`${platform}-gates`);
    expect(shards.strategy?.["fail-fast"]).toBe(false);
    expect(shards.strategy?.matrix?.shard).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    expect(shards.steps?.find((step) => step.name === "Affected tests")?.run).toBe("bun run test:affected --shard=${{ matrix.shard }}/10");
    expect(shards.steps?.find((step) => step.uses === "actions/checkout@v4")?.with?.["fetch-depth"]).toBe(0);
    expect(shards.steps?.some((step) => step.uses === "./.github/actions/check-setup")).toBe(true);
    const aggregate = jobs[`check-${platform}`] as Job;
    expect(aggregate.name).toBe(`check (${os})`);
    expect(Object.values(jobs).filter((job) => (job as Job).name === aggregate.name)).toHaveLength(1);
    expect(aggregate.if).toBe("always()");
    expect(aggregate.needs).toEqual([`${platform}-gates`, `${platform}-tests`, `${platform}-acceptance`,
      ...(platform === "linux" ? ["linux-runtime"] : [])]);
    expect(aggregate.steps?.at(-1)?.if).toBe("always()");
    expect(aggregate.steps?.at(-1)?.run).toContain('job.result !== "success"');
    expect(aggregate.steps?.find((step) => step.name === "Merge shard receipts and require file-level parity")?.run).toContain(`${os} 10`);
  });

  it("keeps hosted-only admission, pinned brokers, systemd, and singleton smoke/cleanup/package semantics", () => {
    const config = workflow();
    assertI9RunnerAdmission(workflowInventory());
    const setup = action("check-setup").runs!.steps as Step[];
    assertI9RunnerEnvironment(workflowInventory(), action("check-setup"));
    expect(setup.find((step) => step.name === "Install pinned nats-server for session-channel acceptance")?.run).toBe("bash scripts/install-pinned-nats-server.sh");
    expect(config.jobs["linux-tests"]!.steps?.some((step: Step) => step.uses === "./.github/actions/check-systemd")).toBe(true);
    expect(acceptance("windows").steps?.filter((step) => step.run === "bun run test:smoke")).toHaveLength(1);
    const runtime = config.jobs["linux-runtime"] as Job;
    expect(runtime.steps?.filter((step) => step.run === "bun run test:smoke")).toHaveLength(1);
    expect(runtime.needs).toEqual(["linux-gates", "linux-tests", "linux-acceptance"]);
    const steps = runtime.steps!;
    const pack = steps.findIndex((step) => step.name === "Package tested Linux runtime");
    const cleanup = steps.findIndex((step) => step.name === "Real Pi exit cleanup e2e (smarty-dev#8467)");
    const upload = steps.findIndex((step) => step.name === "Upload tested Linux runtime");
    expect(pack).toBeGreaterThanOrEqual(0);
    expect(cleanup).toBeGreaterThan(pack);
    expect(upload).toBeGreaterThan(cleanup);
    expect(steps[pack]!.if).toBe("runner.os == 'Linux' && github.event_name == 'push'");
    expect(steps[upload]!.if).toBe(steps[pack]!.if);
    expect(steps[cleanup]!.env).toEqual({ FABRIC_8467_REAL_PI: "1" });
    expect(config.concurrency.group).toContain("github.ref");
    expect(config.concurrency["cancel-in-progress"]).toContain("merge_group");
    expect(config.concurrency["cancel-in-progress"]).toContain("github.head_ref, 'mergify/'");
  });
});
