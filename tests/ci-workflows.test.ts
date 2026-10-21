import fs from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { parse } from "yaml";

type Step = {
  name?: string;
  uses?: string;
  run?: string;
  if?: string;
  env?: Record<string, string>;
  with?: Record<string, unknown>;
};
type Job = {
  name?: string;
  steps?: Step[];
  needs?: string | string[];
  if?: string;
  "runs-on"?: string;
  strategy?: { "fail-fast": boolean; matrix: { shard: number[] } };
};

const root = new URL("../", import.meta.url);
const manifest = JSON.parse(fs.readFileSync(new URL("package.json", root), "utf8"));
const workflowDirectory = fileURLToPath(new URL(".github/workflows/", root));
const workflows = fs.readdirSync(workflowDirectory).filter((name) => /\.ya?ml$/.test(name));
const workflow = () => parse(fs.readFileSync(new URL(".github/workflows/test.yml", root), "utf8"));
const action = (name: string) => parse(fs.readFileSync(new URL(`.github/actions/${name}/action.yml`, root), "utf8"));
const expandedSteps = ({ steps = [] }: Job): Step[] => steps.flatMap((step) =>
  step.uses?.startsWith("./.github/actions/")
    ? expandedSteps({ steps: parse(fs.readFileSync(new URL(`${step.uses}/action.yml`, root), "utf8")).runs.steps })
    : [step]);
const acceptance = (os: string) => workflow().jobs[`${os}-acceptance`] as Job;

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
    expect(workflow().jobs["linux-gates"].steps.some((step: Step) => step.run === "bun run build")).toBe(true);
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
      const steps = expandedSteps(definition);
      const executions = steps.flatMap((step, index) => /\b(?:node|bun|bunx)\s/.test(step.run ?? "") ? [index] : []);
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

describe("hosted sharded required checks", () => {
  it.each([["linux", "ubuntu-latest"], ["windows", "windows-latest"]])("requires every %s shard and singleton gate without renaming the required context", (platform, os) => {
    const { jobs } = workflow();
    const shards = jobs[`${platform}-tests`] as Job;
    expect(shards["runs-on"]).toBe(os);
    expect(shards.needs).toBe(`${platform}-gates`);
    expect(shards.strategy?.["fail-fast"]).toBe(false);
    expect(shards.strategy?.matrix.shard).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
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
    for (const job of Object.values(config.jobs) as Job[]) {
      expect(["ubuntu-latest", "windows-latest"]).toContain(job["runs-on"]);
    }
    const setup = action("check-setup").runs.steps as Step[];
    expect(setup.find((step) => step.name === "Require GitHub-hosted test admission")?.run).toContain('test "$RUNNER_ENVIRONMENT" = github-hosted');
    expect(setup.find((step) => step.name === "Install pinned nats-server for session-channel acceptance")?.run).toBe("bash scripts/install-pinned-nats-server.sh");
    expect(config.jobs["linux-tests"].steps.some((step: Step) => step.uses === "./.github/actions/check-systemd")).toBe(true);
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
