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
  id?: string;
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

function curlsGitHubReleaseDownload(source: string): boolean {
  return /\bcurl\b/i.test(source) && /github\.com\/[^\s"'`]*\/releases\/download(?:\/|\b)/i.test(source);
}

const hostedRunnerLabels = ["ubuntu-latest", "windows-latest"];
const linuxRunnerExpressions = {
  "test.yml:linux-tests": "${{ vars.PI_FABRIC_LINUX_RUNNER || 'ubuntu-latest' }}",
  "test.yml:check-linux": "${{ vars.PI_FABRIC_LINUX_RUNNER_LARGE || 'ubuntu-latest' }}",
  "test.yml:nats-state-conformance": "${{ vars.PI_FABRIC_LINUX_RUNNER_LARGE || 'ubuntu-latest' }}",
  "test.yml:idle-wakes": "${{ vars.PI_FABRIC_LINUX_RUNNER_LARGE || 'ubuntu-latest' }}",
};

function assertHostedRunnerSetup(inventory: Record<string, Workflow>, setupAction: ActionConfig): void {
  expect(setupAction.inputs?.["runner-environment"]?.default).toBe("github-hosted");
  const actionSteps = Object.entries(inventory).flatMap(([file, config]) => Object.entries(config.jobs)
    .flatMap(([id, job]) => (job.steps ?? []).filter((step) => step.uses === "./.github/actions/check-setup")
      .map((step) => ({ file, id, step }))));
  for (const { file, id, step } of actionSteps) {
    const key = `${file}:${id}`;
    const expected = key === "test.yml:linux-tests"
      ? "${{ vars.PI_FABRIC_LINUX_RUNNER == 'ubuntu-latest' && 'github-hosted' || 'self-hosted' }}"
      : key === "test.yml:check-linux"
        ? "${{ vars.PI_FABRIC_LINUX_RUNNER_LARGE == 'ubuntu-latest' && 'github-hosted' || 'self-hosted' }}"
        : undefined;
    expect(step.with?.["runner-environment"], key).toBe(expected);
  }

  const admission = setupAction.runs?.steps?.find((step) => step.env?.EXPECTED_RUNNER_ENVIRONMENT !== undefined);
  expect(admission?.env?.EXPECTED_RUNNER_ENVIRONMENT).toBe("${{ inputs.runner-environment }}");
  expect((admission?.run ?? "").replace(/\s+/g, " ")).toMatch(
    /test\s+["']?\$RUNNER_ENVIRONMENT["']?\s+=\s+["']?\$EXPECTED_RUNNER_ENVIRONMENT["']?/,
  );
  expect(admission?.env?.RUNNER_ENVIRONMENT).toBeUndefined();
  expect(admission?.env?.GITHUB_ACTIONS).toBeUndefined();
}

// These are contract assertions for PR CI, not a reason to execute the suite on an editing host.
describe("CI Node runtime prerequisites", () => {
  it.each(["linux", "windows"])("runs Python diagnostics with an explicit modern interpreter on %s", (os) => {
    const job = os === "linux" ? workflow().jobs["linux-tests"]! : acceptance(os);
    const steps = expandedSteps(job);
    const setup = steps.findIndex((step) => step.uses?.startsWith("actions/setup-python@"));
    const diagnostics = steps.findIndex((step) => step.run?.includes("bunx vitest run tests/kernel-errors.test.ts"));
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
  ])("runs the real-Pi %s acceptance from the same gated Linux build", (suite, flag) => {
    const job = workflow().jobs["linux-tests"]!;
    const steps = job.steps ?? [];
    const build = steps.findIndex((step) => step.run === "bun run build");
    const lanes = steps.find((step) => step.name === "Run Linux tests in eight logged parallel lanes")?.run ?? "";
    expect(build).toBeGreaterThanOrEqual(0);
    expect(lanes).toContain(`env ${flag}=1 bunx vitest run tests/${suite}-real-pi.test.ts`);
  });

  it("requires the pinned NATS broker for session-channel acceptance on Linux", () => {
    const job = workflow().jobs["linux-tests"]!;
    const steps = expandedSteps(job);
    const install = steps.find((step) => step.name === "Install pinned nats-server for session-channel acceptance");
    const lanes = job.steps?.find((step) => step.name === "Run Linux tests in eight logged parallel lanes")?.run ?? "";
    expect(install?.run).toBe("bash scripts/install-pinned-nats-server.sh");
    expect(install?.env?.GH_TOKEN).toBe("${{ github.token }}");
    const installer = fs.readFileSync(new URL("scripts/install-pinned-nats-server.sh", root), "utf8");
    expect(installer).toContain("version=2.14.7");
    expect(installer).toContain("e5c20b1cb2c0566b54c544312e91e011f9e130c5c80f16a14f4cf28ef30b8be2");
    expect(installer).toContain("gh release download");
    expect(lanes).toContain("env PI_FABRIC_REQUIRE_NATS=1 NATS_EXPECTED_SERVER_VERSION=2.14.7 bunx vitest run --retry=0 tests/nats-session-channel.test.ts");
  });

  it("uses authenticated gh for GitHub release assets and never curls a release URL", () => {
    const steps = Object.entries(workflowInventory()).flatMap(([file, config]) =>
      Object.entries(config.jobs).flatMap(([job, definition]) => expandedStepsWithOrigins(definition).map(({ step }) => ({
        location: `${file}:${job}:${step.name ?? step.uses ?? "unnamed"}`,
        step,
      }))),
    );
    const releaseDownloads = steps.filter(({ step }) => /\bgh\s+release\s+download\b/i.test(step.run ?? ""));
    expect(releaseDownloads.length).toBeGreaterThan(0);
    for (const { location, step } of releaseDownloads) {
      expect(step.env?.GH_TOKEN, `${location} must pass the GitHub token`).toBe("${{ github.token }}");
    }

    const curlDownloads = steps.filter(({ step }) => curlsGitHubReleaseDownload(step.run ?? "")).map(({ location }) => location);
    const installer = fs.readFileSync(new URL("scripts/install-pinned-nats-server.sh", root), "utf8");
    if (curlsGitHubReleaseDownload(installer)) curlDownloads.push("scripts/install-pinned-nats-server.sh");
    expect(curlDownloads).toEqual([]);
    expect(installer).toMatch(/\bgh\s+release\s+download\b/i);
  });

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
      expect(steps[setup]!.with?.["node-version-file"]).toBe("package.json");
      expect(steps[setup]!.with?.["node-version"]).toBeUndefined();
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
    expect(consumers).toHaveLength(3);
    expect(consumers.filter(({ file }) => file === "test.yml")).toHaveLength(2);
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

  it("restores and explicitly saves the exact SHA-keyed archive cache with download-scoped token", () => {
    const steps = action("setup-bun").runs?.steps ?? [];
    const cacheActions = steps.filter((step) => step.uses?.startsWith("actions/cache"));
    expect(cacheActions.map((step) => step.uses)).toEqual([
      "actions/cache/restore@v4",
      "actions/cache/save@v4",
    ]);
    const restore = cacheActions[0]!;
    const save = cacheActions[1]!;
    const expectedCache = {
      path: "${{ steps.pin.outputs.cache_dir }}/${{ steps.pin.outputs.asset }}",
      key: "bun-${{ runner.os }}-${{ runner.arch }}-${{ steps.pin.outputs.sha }}",
    };
    expect(restore.id).toBe("restore");
    expect(restore.with).toEqual(expectedCache);
    expect(save.with).toEqual(expectedCache);
    expect(save.if).toBe("steps.restore.outputs.cache-hit != 'true'");

    const pinIndex = steps.findIndex((step) => step.id === "pin");
    const restoreIndex = steps.indexOf(restore);
    const downloadIndex = steps.findIndex((step) => step.name === "Download missing Bun archive");
    const saveIndex = steps.indexOf(save);
    const verifyIndex = steps.findIndex((step) => step.name === "Verify and install Bun");
    expect(pinIndex).toBeGreaterThanOrEqual(0);
    expect(downloadIndex).toBeGreaterThanOrEqual(0);
    expect(verifyIndex).toBeGreaterThanOrEqual(0);
    expect(pinIndex).toBeLessThan(restoreIndex);
    expect(restoreIndex).toBeLessThan(downloadIndex);
    expect(saveIndex).toBe(downloadIndex + 1);
    expect(saveIndex).toBeLessThan(verifyIndex);

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

describe("CI runner admission", () => {
  it("uses GitHub-hosted Linux defaults and keeps every other workflow job on its existing hosted runner", () => {
    const inventory = workflowInventory();
    for (const [file, config] of Object.entries(inventory)) {
      for (const [id, job] of Object.entries(config.jobs)) {
        const key = `${file}:${id}`;
        const linuxExpression = linuxRunnerExpressions[key as keyof typeof linuxRunnerExpressions];
        if (linuxExpression) expect(job["runs-on"], key).toBe(linuxExpression);
        else expect(hostedRunnerLabels, key).toContain(job["runs-on"]);
      }
    }
    expect(Object.keys(inventory["test.yml"]!.jobs).sort()).toEqual([
      "check-linux", "check-windows", "idle-wakes", "linux-tests", "nats-state-conformance",
      "windows-acceptance", "windows-gates", "windows-tests",
    ]);
    const test = inventory["test.yml"]!;
    expect(Object.values(test.jobs).filter((job) => ["check (ubuntu-latest)", "check (windows-latest)"].includes(job.name ?? "")
      ).map((job) => job.name).sort()).toEqual(["check (ubuntu-latest)", "check (windows-latest)"]);
    expect(Object.values(test.jobs).filter((job) => (job["runs-on"] ?? "").toString().includes("vars.PI_FABRIC_LINUX_RUNNER")))
      .toHaveLength(4);
  });
});

describe("Public Linux packing and required checks", () => {
  it.each([["linux", "ubuntu-latest"], ["windows", "windows-latest"]])("preserves every %s selected test and required aggregate context", (platform, os) => {
    const { jobs } = workflow();
    const tests = jobs[`${platform}-tests`] as Job;
    const aggregate = jobs[`check-${platform}`] as Job;
    expect(aggregate.name).toBe(`check (${os})`);
    expect(Object.values(jobs).filter((job) => job.name === aggregate.name)).toHaveLength(1);
    expect(aggregate.if).toBe("always()");
    expect(aggregate.needs).toEqual(platform === "linux"
      ? ["linux-tests"]
      : ["windows-gates", "windows-tests", "windows-acceptance"]);
    expect(aggregate.steps?.at(-1)?.if).toBe("always()");
    expect(aggregate.steps?.at(-1)?.run).toContain('job.result !== "success"');
    expect(aggregate.steps?.find((step) => step.name === "Merge shard receipts and require file-level parity")?.run).toContain(`${os} 10`);
    expect(tests.steps?.find((step) => step.uses === "actions/checkout@v4")?.with?.["fetch-depth"]).toBe(0);
    expect(tests.steps?.some((step) => step.uses === "./.github/actions/check-setup")).toBe(true);

    if (platform === "windows") {
      expect(tests["runs-on"]).toBe("windows-latest");
      expect(tests.needs).toBe("windows-gates");
      expect(tests.strategy?.["fail-fast"]).toBe(false);
      expect(tests.strategy?.matrix?.shard).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
      expect(tests.steps?.find((step) => step.name === "Affected tests")?.run).toBe("bun run test:affected --shard=${{ matrix.shard }}/10");
      expect(jobs["windows-acceptance"]?.["runs-on"]).toBe("windows-latest");
      expect(jobs["windows-gates"]?.["runs-on"]).toBe("windows-latest");
      expect(aggregate["runs-on"]).toBe("windows-latest");
    } else {
      expect(tests["runs-on"]).toBe("${{ vars.PI_FABRIC_LINUX_RUNNER || 'ubuntu-latest' }}");
      expect(tests.needs).toBeUndefined();
      expect(tests.strategy).toBeUndefined();
      const lanes = tests.steps?.find((step) => step.name === "Run Linux tests in eight logged parallel lanes")?.run ?? "";
      expect(tests["timeout-minutes"]).toBe(45);
      expect(lanes).toContain("max_lanes=4");
      expect(lanes).toContain('wait -n -p finished_pid "${lane_pids[@]}"');
      expect(lanes).toContain('while [ "${#lane_pids[@]}" -ge "$max_lanes" ]; do');
      expect(lanes.match(/bun run test:affected --shard=\d+\/10/g)).toHaveLength(10);
      for (let shard = 1; shard <= 10; shard++) expect(lanes).toContain(`bun run test:affected --shard=${shard}/10`);
      expect(lanes).toContain("run_logged session-channel env PI_FABRIC_REQUIRE_NATS=1 NATS_EXPECTED_SERVER_VERSION=2.14.7 bunx vitest run --retry=0 tests/nats-session-channel.test.ts");
      expect(lanes).toContain("bun run smoke:package-install");
      expect(lanes).toContain("bun run test:smoke");
      expect(lanes).toContain("bunx vitest run --project scope-e2e");
      expect(lanes).toContain("bun run bench:mesh-lock:check --out mesh-lock.json");
      expect(tests.steps?.some((step) => step.run === "bun run proof:check")).toBe(true);
      expect(tests.steps?.some((step) => step.run === "bun run typecheck")).toBe(true);
      expect(tests.steps?.some((step) => step.run === "node scripts/native-toolchain.mjs fetch")).toBe(true);
      expect(tests.steps?.some((step) => step.run === "bun run build")).toBe(true);
      expect(aggregate["runs-on"]).toBe("${{ vars.PI_FABRIC_LINUX_RUNNER_LARGE || 'ubuntu-latest' }}");
      expect(jobs["nats-state-conformance"]?.["runs-on"]).toBe("${{ vars.PI_FABRIC_LINUX_RUNNER_LARGE || 'ubuntu-latest' }}");
      expect(jobs["nats-state-conformance"]?.strategy).toBeUndefined();
      expect(jobs["idle-wakes"]?.["runs-on"]).toBe("${{ vars.PI_FABRIC_LINUX_RUNNER_LARGE || 'ubuntu-latest' }}");
      expect(jobs["idle-wakes"]?.needs).toBeUndefined();
      expect(jobs["idle-wakes"]?.steps?.some((step) => step.run?.includes("scripts/benchmark-idle-cpu.mjs"))).toBe(true);
      const nats = jobs["nats-state-conformance"]!;
      const profiles = nats.steps?.find((step) => step.name === "StateBackend conformance on single and cluster NATS profiles")?.run ?? "";
      for (const suite of [
        "tests/mesh-state-backend.test.ts", "tests/mesh-state-backend-multiprocess.test.ts",
        "tests/mesh-backend-migration-nats.test.ts", "tests/state-backend-nats-election.test.ts",
        "tests/nats-transport-replicas.test.ts", "tests/state-backend-nats-publish-dedupe.test.ts",
        "tests/state-backend-nats-pull-recovery.test.ts",
      ]) expect(profiles).toContain(suite);
      expect(nats.steps?.some((step) => step.run?.includes("tests/nats-auth-server.test.ts"))).toBe(true);
      expect(nats.steps?.find((step) => step.name === "Hosted authenticated NATS T2/T8 and Path B grants (source CLI)")?.run)
        .toBe("bunx vitest run tests/nats-auth-server.test.ts tests/nats-auth-path-b-*.test.ts tests/nats-messaging.test.ts tests/nats-transport-auth.test.ts tests/nats-inbox-faults.test.ts tests/nats-kv-watch.test.ts tests/nats-dead-letter.test.ts tests/nats-reconnect-auth.test.ts --retry=0");
    }
  });

  it("preserves runner admission, the full broker matrix, systemd, receipts and runtime publication order", () => {
    const config = workflow();
    const setupAction = action("check-setup");
    const setup = setupAction.runs!.steps as Step[];
    assertHostedRunnerSetup(workflowInventory(), setupAction);
    expect(setup.find((step) => step.name === "Install pinned nats-server for session-channel acceptance")?.run).toBe("bash scripts/install-pinned-nats-server.sh");
    expect(config.jobs["linux-tests"]!.steps?.some((step: Step) => step.uses === "./.github/actions/check-systemd")).toBe(true);
    expect(acceptance("windows").steps?.filter((step) => step.run === "bun run test:smoke")).toHaveLength(1);
    const linux = config.jobs["linux-tests"] as Job;
    const laneRun = linux.steps?.find((step) => step.name === "Run Linux tests in eight logged parallel lanes")?.run ?? "";
    expect(laneRun).toContain("run_logged verified-kernels bunx vitest run tests/verified-kernels.test.ts tests/verified-resources.test.ts tests/verified-providers.test.ts tests/verified-authority.test.ts tests/verified-lifecycle.test.ts tests/verified-storage.test.ts tests/verified-artifact.test.ts");
    expect(laneRun).toContain("run_logged launcher-regressions bunx vitest run tests/worker-e2e.test.ts tests/pi-binary.test.ts tests/script-runtime.test.ts tests/herdr-transport.test.ts tests/prewalk-handoff.test.ts tests/shell-middleware.test.ts");
    expect(laneRun).toContain("run_logged scope-e2e env FABRIC_SCOPE_E2E_REQUIRED=1");
    for (const command of [
      "bunx vitest run tests/ci-workflows.test.ts tests/ci-test-counts.test.ts",
      "bunx vitest run tests/kernel-errors.test.ts",
      "bunx vitest run tests/verified-kernels.test.ts tests/verified-resources.test.ts tests/verified-providers.test.ts tests/verified-authority.test.ts tests/verified-lifecycle.test.ts tests/verified-storage.test.ts tests/verified-artifact.test.ts",
      "bun run proof:dist",
      "bunx vitest run tests/worker-e2e.test.ts tests/pi-binary.test.ts tests/script-runtime.test.ts tests/herdr-transport.test.ts tests/prewalk-handoff.test.ts tests/shell-middleware.test.ts",
      "bunx vitest run tests/main-provider-dead-real-pi.test.ts",
      "bunx vitest run tests/extension-hidden-tools-real-pi.test.ts",
      "bunx vitest run tests/sender-headers-real-pi.test.ts",
    ]) expect(laneRun).toContain(command);
    expect(laneRun).toContain("exit \"$failed\"");
    expect(linux.steps?.filter((step) => step.name?.startsWith("Upload affected-test receipt "))).toHaveLength(10);
    const pack = linux.steps!.findIndex((step) => step.name === "Package tested Linux runtime");
    const cleanup = linux.steps!.findIndex((step) => step.name === "Real Pi exit cleanup e2e (smarty-dev#8467)");
    const upload = linux.steps!.findIndex((step) => step.name === "Upload tested Linux runtime");
    expect(pack).toBeGreaterThanOrEqual(0);
    expect(cleanup).toBeGreaterThan(pack);
    expect(upload).toBeGreaterThan(cleanup);
    expect(linux.steps![pack]!.if).toBe("github.event_name == 'push'");
    expect(linux.steps![upload]!.if).toBe(linux.steps![pack]!.if);
    expect(linux.steps![cleanup]!.env).toEqual({ FABRIC_8467_REAL_PI: "1" });
    expect(config.concurrency.group).toContain("github.ref");
    expect(config.concurrency["cancel-in-progress"]).toContain("merge_group");
    expect(config.concurrency["cancel-in-progress"]).toContain("github.head_ref, 'mergify/'");
  });
});
