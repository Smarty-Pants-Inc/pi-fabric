import fs from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { parse } from "yaml";

type Step = {
  uses?: string;
  run?: string;
  if?: string;
  env?: Record<string, string>;
  with?: Record<string, unknown>;
};

type RouteRow = { fullName: string; title: string; status: string };
type RouteReport = { testResults: { name: string; assertionResults: RouteRow[] }[] };
type RouteGuard = { REQUIRED_ROUTE_SUITES: string[]; assertRouteTestReport: (report: RouteReport, platform: string) => void };
const loadRouteGuard = async () => await import(new URL("../scripts/assert-route-test-skips.mjs", import.meta.url).href) as RouteGuard;
const routeRow = (title: string, status = "passed"): RouteRow => ({ title, fullName: title, status });
const routeReport = (suites: string[], platform: string): RouteReport => ({ testResults: suites.map(name => ({ name, assertionResults:
  name === "task-control-route-residual.test.ts" ? [routeRow("inventory"), ...(platform === "linux" ? [
    routeRow("task.txt must not land"), routeRow("schema victim file must remain unchanged"), routeRow("images victim file must remain unchanged"),
  ] : [routeRow("expected skip: F1 swap requires enforced Linux Landlock", "skipped"), routeRow("expected skip: F0 schema requires enforced Linux Landlock", "skipped"), routeRow("expected skip: F0 images requires enforced Linux Landlock", "skipped")])]
    : name === "task-control-route-session-identity.test.ts" ? [routeRow("unswapped session"), routeRow("post-verification leaf swap", platform === "linux" ? "passed" : "skipped")]
    : [routeRow("required baseline row")],
})) });

const root = new URL("../", import.meta.url);
const manifest = JSON.parse(fs.readFileSync(new URL("package.json", root), "utf8"));
const workflowDirectory = fileURLToPath(new URL(".github/workflows/", root));
const workflows = fs.readdirSync(workflowDirectory).filter((name) => /\.ya?ml$/.test(name));

describe("C-1 routed CI report gate", () => {
  it("pins exactly the complete 26-suite workflow selection", async () => {
    const guard = await loadRouteGuard();
    const selected = fs.readdirSync(new URL("tests/", root)).filter(name => /^(?:task-control-.*|model-route.*)\.test\.ts$/.test(name) || name === "run-directory-mode.test.ts").sort();
    expect(guard.REQUIRED_ROUTE_SUITES).toHaveLength(26);
    expect([...guard.REQUIRED_ROUTE_SUITES].sort()).toEqual(selected);
  });
  for (const platform of ["linux", "darwin", "win32"]) {
    it(`${platform}: accepts the complete declared platform matrix`, async () => {
      const guard = await loadRouteGuard();
      expect(() => guard.assertRouteTestReport(routeReport(guard.REQUIRED_ROUTE_SUITES, platform), platform)).not.toThrow();
    });
    it.each(["omitted archive suite", "skipped archive row", "skipped platform row", "pending race row", "unlisted model skip", "omitted model suite"])(`${platform}: rejects %s`, async fault => {
      const guard = await loadRouteGuard(), report = routeReport(guard.REQUIRED_ROUTE_SUITES, platform);
      const suite = (file: string) => report.testResults.find(result => result.name === file)!;
      if (fault === "omitted archive suite") report.testResults = report.testResults.filter(result => result.name !== "task-control-archive-route.test.ts");
      if (fault === "omitted model suite") report.testResults = report.testResults.filter(result => result.name !== "model-route-owner.test.ts");
      if (fault === "skipped archive row") suite("task-control-archive-route.test.ts").assertionResults[0]!.status = "skipped";
      if (fault === "skipped platform row") suite("task-control-route-platform.test.ts").assertionResults[0]!.status = "skipped";
      if (fault === "pending race row") suite("task-control-route-race.test.ts").assertionResults[0]!.status = "pending";
      if (fault === "unlisted model skip") suite("model-route-hook.test.ts").assertionResults[0]!.status = "todo";
      expect(() => guard.assertRouteTestReport(report, platform)).toThrow();
    });
  }
  it("allows only existing named Windows POSIX skips and enforces their count bound", async () => {
    const guard = await loadRouteGuard(), report = routeReport(guard.REQUIRED_ROUTE_SUITES, "win32");
    const rows = report.testResults.find(result => result.name === "task-control-root.test.ts")!.assertionResults;
    const title = "lives under the validated state home, outside temp, and refuses temp placement";
    rows.push(routeRow(title, "skipped"));
    expect(() => guard.assertRouteTestReport(report, "win32")).not.toThrow();
    rows.push(routeRow("a new unreviewed POSIX test", "skipped"));
    expect(() => guard.assertRouteTestReport(report, "win32")).toThrow(/unlisted route skip/);
    rows.pop();
    while (rows.length < 19) rows.push(routeRow(title, "skipped"));
    expect(() => guard.assertRouteTestReport(report, "win32")).toThrow(/skip count exceeded/);
    expect(() => guard.assertRouteTestReport(report, "linux")).toThrow(/unlisted route skip/);
  });
  it("rejects a renamed/new selected suite until its inventory is reviewed", async () => {
    const guard = await loadRouteGuard(), report = routeReport(guard.REQUIRED_ROUTE_SUITES, "linux");
    report.testResults.push({ name: "task-control-new.test.ts", assertionResults: [routeRow("new row")] });
    expect(() => guard.assertRouteTestReport(report, "linux")).toThrow(/unlisted route suite/);
  });
});

describe("CI Node runtime prerequisites", () => {
  it("runs Python diagnostics with an explicit modern interpreter on both platforms", () => {
    const workflow = parse(fs.readFileSync(new URL(".github/workflows/test.yml", root), "utf8"));
    const { steps, strategy } = workflow.jobs.check;
    expect(strategy.matrix.os).toEqual(["ubuntu-latest", "windows-latest"]);
    const setup = steps.findIndex((step: Step) => step.uses?.startsWith("actions/setup-python@"));
    const diagnostics = steps.findIndex((step: Step) => step.run === "bunx vitest run tests/kernel-errors.test.ts");
    expect(setup).toBeGreaterThanOrEqual(0);
    expect(steps[setup].with["python-version"]).toBe("3.14");
    expect(steps[setup].if).toBeUndefined();
    expect(diagnostics).toBeGreaterThan(setup);
    expect(steps[diagnostics].if).toBeUndefined();
    const prerequisites = steps.find((step: Step) => step.run?.includes("choco install ripgrep"));
    expect(prerequisites.run).toContain("mkdir -p /tmp");
  });

  it("requires isolated routed-control coverage on all three platforms", () => {
    const workflow = parse(fs.readFileSync(new URL(".github/workflows/test.yml", root), "utf8"));
    for (const name of ["check", "route-control-macos"]) {
      const steps = workflow.jobs[name].steps as Step[];
      const at = steps.findIndex(step => step.run?.includes("tests/task-control-*.test.ts"));
      expect(at).toBeGreaterThan(steps.findIndex(step => step.run === "bun run build"));
      expect(steps[at]!.run).toContain("tests/model-route*.test.ts");
      expect(steps[at]!.run).toContain("tests/run-directory-mode.test.ts");
      expect(steps[at]!.run).toContain("XDG_STATE_HOME must be an existing absolute directory");
      expect(steps[at]!.run).toContain("XDG_STATE_HOME must be outside temp roots");
      expect(steps[at]!.run).toContain("scripts/assert-route-test-skips.mjs");
      expect(steps[at]!.env).toMatchObject({ XDG_STATE_HOME: "${{ github.workspace }}/.state", FABRIC_ROUTE_RESIDUAL_REQUIRED: "1" });
    }
    expect(workflow.jobs["route-control-macos"]["runs-on"]).toBe("macos-latest");
  });
  it("runs the real-Pi extension-hidden-tools acceptance on Linux after the build (smarty-dev#5492)", () => {
    const { steps } = parse(fs.readFileSync(new URL(".github/workflows/test.yml", root), "utf8")).jobs.check as { steps: Step[] };
    const build = steps.findIndex((step) => step.run === "bun run build");
    const realPi = steps.findIndex((step) => step.run === "bunx vitest run tests/extension-hidden-tools-real-pi.test.ts");
    expect(build).toBeGreaterThanOrEqual(0);
    expect(realPi).toBeGreaterThan(build);
    expect(steps[realPi]!.if).toBe("runner.os == 'Linux'");
    expect(steps[realPi]!.env).toEqual({ FABRIC_5492_REAL_PI: "1" });
  });

  it("runs the real-Pi sender-header wire proof on Linux after the build (smarty-dev#6207)", () => {
    const { steps } = parse(fs.readFileSync(new URL(".github/workflows/test.yml", root), "utf8")).jobs.check as { steps: Step[] };
    const build = steps.findIndex((step) => step.run === "bun run build");
    const realPi = steps.findIndex((step) => step.run === "bunx vitest run tests/sender-headers-real-pi.test.ts");
    expect(build).toBeGreaterThanOrEqual(0);
    expect(realPi).toBeGreaterThan(build);
    expect(steps[realPi]!.if).toBe("runner.os == 'Linux'");
    expect(steps[realPi]!.env).toEqual({ FABRIC_6207_REAL_PI: "1" });
  });

  // Our deliberately pinned jobs keep their own Node version (fork CI policy: our jobs stay as
  // they are). mesh-lock-bench measures dist/ built for node24 against a fixed baseline.
  const pinnedNodeJobs = new Set(["test.yml:mesh-lock-bench"]);

  it.each(workflows)("%s configures the declared Node runtime before install and execution", (file) => {
    const workflow = parse(fs.readFileSync(new URL(`.github/workflows/${file}`, root), "utf8")) as {
      jobs: Record<string, { steps?: Step[] }>;
    };
    for (const [job, { steps = [] }] of Object.entries(workflow.jobs)) {
      const executions = steps.flatMap((step, index) => /\b(?:node|bun|bunx)\s/.test(step.run ?? "") ? [index] : []);
      if (executions.length === 0) continue;
      const setup = steps.findIndex((step) => step.uses?.startsWith("actions/setup-node@"));
      expect(setup, `${file}:${job} must set up Node, not rely on the runner default`).toBeGreaterThanOrEqual(0);
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
      for (const index of executions) {
        expect(setup, `${file}:${job} must set up Node before ${steps[index]!.run}`).toBeLessThan(index);
      }
    }
  });
});
