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

function assertHostedRunnerSetup(inventory: Record<string, Workflow>, setupAction: ActionConfig): void {
  expect(setupAction.inputs?.["runner-environment"]?.default).toBe("github-hosted");
  const actionSteps = Object.values(inventory).flatMap((config) => Object.values(config.jobs)
    .flatMap((job) => (job.steps ?? []).filter((step) => step.uses === "./.github/actions/check-setup")));
  for (const step of actionSteps) expect(step.with?.["runner-environment"]).not.toBe("self-hosted");

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
    expect(install?.env?.GH_TOKEN).toBe("${{ github.token }}");
    const installer = fs.readFileSync(new URL("scripts/install-pinned-nats-server.sh", root), "utf8");
    expect(installer).toContain("version=2.14.7");
    expect(installer).toContain("e5c20b1cb2c0566b54c544312e91e011f9e130c5c80f16a14f4cf28ef30b8be2");
    expect(installer).toContain("gh release download");
    expect(gate?.if).toBe("runner.os == 'Linux'");
    expect(gate?.env).toEqual({ PI_FABRIC_REQUIRE_NATS: "1", NATS_EXPECTED_SERVER_VERSION: "2.14.7" });
    expect(gate?.run).toBe("bunx vitest run --retry=0 tests/nats-session-channel.test.ts");
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

describe("GitHub-hosted runner admission", () => {
  it("requires every workflow job to use an allowlisted hosted runner label", () => {
    for (const [file, config] of Object.entries(workflowInventory())) {
      for (const [id, job] of Object.entries(config.jobs)) {
        expect(hostedRunnerLabels, `${file}:${id}`).toContain(job["runs-on"]);
      }
    }
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
    const setupAction = action("check-setup");
    const setup = setupAction.runs!.steps as Step[];
    assertHostedRunnerSetup(workflowInventory(), setupAction);
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
