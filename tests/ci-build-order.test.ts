import fs from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { parse } from "yaml";

const workflow = parse(fs.readFileSync(fileURLToPath(new URL("../.github/workflows/test.yml", import.meta.url)), "utf8"));
const steps = workflow.jobs.check.steps as Array<{ name?: string; run?: string }>;

describe("CI build prerequisites", () => {
  it("runs the YAML guard after installing Node and its existing parser dependency", () => {
    const guard = steps.findIndex((step) => step.run === "bash scripts/ci/no-hosted-runners.sh");
    const node = steps.findIndex((step) => (step as { uses?: string }).uses === "actions/setup-node@v4");
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

  it.each(["test.yml", "entropy.yml"])("excludes fork PR source and pull_request_target in %s", (name) => {
    const definition = parse(fs.readFileSync(fileURLToPath(new URL(`../.github/workflows/${name}`, import.meta.url)), "utf8"));
    expect(definition.on).not.toHaveProperty("pull_request_target");
    for (const job of Object.values(definition.jobs) as Array<{ if?: string }>) {
      expect(job.if).toBe("github.event_name != 'pull_request' || github.event.pull_request.head.repo.full_name == github.repository");
    }
  });

  it("lets both platforms finish when one fails", () => {
    expect(workflow.jobs.check.strategy["fail-fast"]).toBe(false);
  });

  it("keeps required check names mapped to the exact owned runner labels", () => {
    expect(workflow.jobs.check.name).toBe("${{ matrix.name }}");
    expect(workflow.jobs.check["runs-on"]).toBe("${{ matrix.runner }}");
    expect(workflow.jobs.check.strategy.matrix).toEqual({
      include: [
        { name: "check (ubuntu-latest)", runner: ["self-hosted", "smarty-linux-x64"] },
        { name: "check (windows-latest)", runner: ["self-hosted", "Windows", "X64", "smarty-ci-windows-x64"] },
      ],
    });
  });
});
