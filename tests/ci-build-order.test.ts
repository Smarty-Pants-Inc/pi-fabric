import fs from "node:fs";
import { fileURLToPath } from "node:url";
import { runInNewContext } from "node:vm";
import { describe, expect, it } from "vitest";
import { parse } from "yaml";

type Job = {
  name: string;
  if: string;
  "runs-on": string | string[];
  steps: Array<{ name?: string; run?: string; uses?: string }>;
  strategy?: { "fail-fast": boolean; matrix: { include: Array<{ name: string; runner: string[] }> } };
};
const workflow = parse(fs.readFileSync(fileURLToPath(new URL("../.github/workflows/test.yml", import.meta.url)), "utf8"));
const jobs = workflow.jobs as Record<string, Job>;
const sameRepoGuard = "github.event_name != 'pull_request' || github.event.pull_request.head.repo.full_name == github.repository";

describe.each(Object.entries(jobs))("CI build prerequisites for %s", (_id, job) => {
  const steps = job.steps;
  it("runs the YAML guard after installing Node and its existing parser dependency", () => {
    const guard = steps.findIndex((step) => step.run === "bash scripts/ci/no-hosted-runners.sh");
    const node = steps.findIndex((step) => step.uses === "actions/setup-node@v4");
    const dependencies = steps.findIndex((step) => step.run === "bun install --frozen-lockfile");
    expect(node).toBeGreaterThanOrEqual(0);
    expect(dependencies).toBeGreaterThan(node);
    expect(guard).toBeGreaterThan(dependencies);
  });
  it("builds the published workers before every test step", () => {
    const build = steps.findIndex((step) => step.run === "bun run build");
    const tests = steps.flatMap((step, index) => /^(bunx vitest|bun run test:)/.test(step.run ?? "") ? [index] : []);
    expect(build).toBeGreaterThanOrEqual(0);
    expect(tests.length).toBeGreaterThan(0);
    for (const index of tests) expect(build, `build must precede ${steps[index]!.name}`).toBeLessThan(index);
  });

  it("installs the artifact's pinned Bend release with a checksum", () => {
    const { bend } = JSON.parse(fs.readFileSync(fileURLToPath(new URL("../src/verified/generated/manifest.json", import.meta.url)), "utf8")) as { bend: string };
    const install = steps.find((step) => step.name === "Install pinned Bend proof compiler")?.run ?? "";
    expect(install).toContain(`https://github.com/bendlang/bend/releases/download/v${bend}/bend-${bend}-linux-x64.tar.gz`);
    expect(install).toMatch(/echo "[a-f0-9]{64}  \$RUNNER_TEMP\/bend\.tar\.gz" \| sha256sum -c -/);
  });

  it("refreshes apt metadata before installing Linux runtime prerequisites", () => {
    const prerequisites = steps.find((step) => step.name === "Install runtime prerequisites")?.run ?? "";
    const update = prerequisites.indexOf("sudo apt-get update");
    const install = prerequisites.indexOf("sudo apt-get install");
    expect(update).toBeGreaterThanOrEqual(0);
    expect(install).toBeGreaterThan(update);
  });
});

describe("CI source and runner policy (smarty-dev#1246)", () => {
  it.each(["test.yml", "entropy.yml"])("excludes fork PR source and pull_request_target in %s", (name) => {
    const definition = parse(fs.readFileSync(fileURLToPath(new URL(`../.github/workflows/${name}`, import.meta.url)), "utf8"));
    expect(definition.on).not.toHaveProperty("pull_request_target");
    for (const job of Object.values(definition.jobs) as Job[]) {
      expect(job.if).toBe(sameRepoGuard);
    }
  });

  it("keeps the required Linux check name and exact Forge labels", () => {
    expect(Object.keys(jobs)).toEqual(["check", "windows"]);
    expect(jobs.check!.if).toBe(sameRepoGuard);
    expect(jobs.check!.name).toBe("${{ matrix.name }}");
    expect(jobs.check!["runs-on"]).toBe("${{ matrix.runner }}");
    expect(jobs.check!.strategy).toEqual({
      "fail-fast": false,
      matrix: {
        include: [{ name: "check (ubuntu-latest)", runner: ["smarty-linux-x64"] }],
      },
    });
  });

  it("keeps the recorded hosted Windows check on PR, push and merge-queue heads", () => {
    expect(jobs.windows!.if).toBe(sameRepoGuard);
    expect(jobs.windows!.name).toBe("check (windows-latest)");
    expect(jobs.windows!["runs-on"]).toBe("windows-latest");
    expect(jobs.windows!.strategy).toBeUndefined();
    expect(workflow.on).toEqual({ push: { branches: ["main"] }, pull_request: { branches: ["main"] }, merge_group: {} });
    expect(jobs.windows!.steps).toEqual(jobs.check!.steps);
  });

  it("matches group 13's custom-only labels without assuming runner availability", () => {
    // No live labels fixture is checked in. R32's custom-only registrar is the
    // explicit assumption here, not a live group/registration or capacity probe.
    const assumedGroup13Labels = new Set(["smarty-linux-x64"]);
    const eligible = (labels: string[]) => labels.every((label) => assumedGroup13Labels.has(label));
    expect(eligible(["self-hosted", "smarty-linux-x64"])).toBe(false);
    expect(eligible(jobs.check!.strategy!.matrix.include[0]!.runner)).toBe(true);
  });

  it.each([
    ["pull_request", "refs/pull/254/merge", "Smarty-Pants-Inc/pi-fabric", ["check (ubuntu-latest)", "check (windows-latest)"]],
    ["pull_request", "refs/heads/main", "Smarty-Pants-Inc/pi-fabric", ["check (ubuntu-latest)", "check (windows-latest)"]],
    ["pull_request", "refs/pull/254/merge", "fork/pi-fabric", []],
    ["merge_group", "refs/heads/gh-readonly-queue/main/pr-254", "Smarty-Pants-Inc/pi-fabric", ["check (ubuntu-latest)", "check (windows-latest)"]],
    ["merge_group", "refs/heads/main", "Smarty-Pants-Inc/pi-fabric", ["check (ubuntu-latest)", "check (windows-latest)"]],
    ["push", "refs/heads/main", "Smarty-Pants-Inc/pi-fabric", ["check (ubuntu-latest)", "check (windows-latest)"]],
    ["push", "refs/heads/feature", "Smarty-Pants-Inc/pi-fabric", ["check (ubuntu-latest)", "check (windows-latest)"]],
    ["workflow_dispatch", "refs/heads/main", "Smarty-Pants-Inc/pi-fabric", ["check (ubuntu-latest)", "check (windows-latest)"]],
    ["pull_request_target", "refs/heads/main", "fork/pi-fabric", ["check (ubuntu-latest)", "check (windows-latest)"]],
  ])("job conditions for %s on %s from %s request only %j", (event, ref, source, expected) => {
    // Evaluate the exact Actions predicates (this subset uses JS-compatible operators).
    // Non-triggered events are defense-in-depth probes, not workflow admission.
    const github = {
      event_name: event,
      ref,
      repository: "Smarty-Pants-Inc/pi-fabric",
      event: { pull_request: { head: { repo: { full_name: source } } } },
    };
    const requested = Object.values(jobs)
      .filter((job) => runInNewContext(job.if, { github }, { timeout: 1_000 }))
      .flatMap((job) => job.strategy?.matrix.include.map((row) => row.name) ?? [job.name]);
    expect(requested).toEqual(expected);
    if (event === "pull_request" && source !== github.repository) {
      expect(requested).toEqual([]);
    }
  });

  it("preserves Linux-only Mergify conditions while restoring Windows PR coverage", () => {
    const mergify = parse(fs.readFileSync(fileURLToPath(new URL("../.mergify.yml", import.meta.url)), "utf8"));
    const queue = mergify.queue_rules[0];
    // Main (smarty-dev#2974) keeps queue_conditions and merge_conditions distinct; both must require Linux and never Windows.
    for (const conditions of [queue.queue_conditions, queue.merge_conditions]) {
      expect(conditions).toContain("check-success = check (ubuntu-latest)");
      expect(conditions).not.toContain("check-success = check (windows-latest)");
    }
  });
});
