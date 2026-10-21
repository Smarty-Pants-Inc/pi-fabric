import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync, execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";

const countsScript = fileURLToPath(new URL("../scripts/ci-test-counts.mjs", import.meta.url));
const affectedScript = fileURLToPath(new URL("../scripts/test-affected.mjs", import.meta.url));
const roots: string[] = [];
const temp = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "ci-test-counts-"));
  roots.push(root);
  return root;
};
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const json = (file: string, value: unknown) => fs.writeFileSync(file, JSON.stringify(value));
function receipt(root: string, index: number, phases = ["direct"]) {
  const directory = path.join(root, `affected-ubuntu-latest-${index}`);
  fs.mkdirSync(directory);
  json(path.join(directory, "selection.json"), {
    shard: `${index}/2`, platform: "linux", base: "same-base", direct: ["tests/example.test.ts"], related: [], phases,
  });
  for (const phase of phases) json(path.join(directory, `${phase}.json`), {
    success: true, numTotalTests: 3, numPassedTests: 2, numFailedTests: 0, numPendingTests: 1, numTodoTests: 0,
    testResults: [{ name: `/work/tests/example-${index}.test.ts` }],
  });
  return directory;
}
const aggregate = (root: string) => spawnSync(process.execPath, [countsScript, root, "ubuntu-latest", "2"], {
  encoding: "utf8", env: { ...process.env, GITHUB_STEP_SUMMARY: path.join(root, "summary.md") },
});

describe("affected-test count receipts", () => {
  it("prints each shard and sums counts including skips, keeping related/direct overlaps separate", () => {
    const root = temp();
    receipt(root, 1, ["related", "direct"]);
    receipt(root, 2, ["related", "direct"]);
    const result = aggregate(root);
    expect(result.status, result.stderr).toBe(0);
    expect(result.stdout).toContain("| 1/2 | direct | 1 | 3 | 2 | 0 | 1 | 0 | tests/example-1.test.ts |");
    expect(result.stdout).toContain("| **SUM** | **direct** | **2** | **6 | 4 | 0 | 2 | 0** | — |");
    expect(result.stdout).toContain("| **SUM** | **related** | **2** | **6 | 4 | 0 | 2 | 0** | — |");
    expect(result.stdout).toContain("Linux actor-manager test file present: false.");
    expect(fs.readFileSync(path.join(root, "summary.md"), "utf8")).toBe(result.stdout.trimEnd() + "\n");
  });

  it("rejects missing shards instead of counting an incomplete run as success", () => {
    const root = temp();
    receipt(root, 1);
    expect(aggregate(root).status).not.toBe(0);
  });

  it("rejects a failed phase and a missing phase report", () => {
    const root = temp();
    receipt(root, 1);
    const directory = receipt(root, 2);
    json(path.join(directory, "direct.json"), { success: false });
    expect(aggregate(root).status).not.toBe(0);
    fs.unlinkSync(path.join(directory, "direct.json"));
    expect(aggregate(root).status).not.toBe(0);
  });

  it("rejects mismatched selections and repeated files across shards", () => {
    const root = temp();
    receipt(root, 1);
    const directory = receipt(root, 2);
    const selection = JSON.parse(fs.readFileSync(path.join(directory, "selection.json"), "utf8"));
    json(path.join(directory, "selection.json"), { ...selection, base: "other-base" });
    expect(aggregate(root).status).not.toBe(0);
    json(path.join(directory, "selection.json"), selection);
    const report = JSON.parse(fs.readFileSync(path.join(directory, "direct.json"), "utf8"));
    json(path.join(directory, "direct.json"), { ...report, testResults: [{ name: "/work/tests/example-1.test.ts" }] });
    expect(aggregate(root).status).not.toBe(0);
  });

  it("reports zero selection explicitly rather than requiring an unrelated PR's test count", () => {
    const root = temp();
    receipt(root, 1, []);
    receipt(root, 2, []);
    const result = aggregate(root);
    expect(result.status, result.stderr).toBe(0);
    expect(result.stdout).toContain("| 2/2 | no selected tests | 0 | 0 | 0 | 0 | 0 | 0 | — |");
  });
});

describe("affected selector shard forwarding (dry run only)", () => {
  it("passes the same shard and JSON reporters to related AND direct selections", () => {
    const root = temp();
    const git = (...args: string[]) => execFileSync("git", args, { cwd: root, stdio: "pipe" });
    fs.mkdirSync(path.join(root, "src"));
    fs.mkdirSync(path.join(root, "tests"));
    fs.writeFileSync(path.join(root, "src/example.ts"), "export const value = 1;\n");
    fs.writeFileSync(path.join(root, "tests/example.test.ts"), "// initial test\n");
    git("init");
    git("add", ".");
    git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.test", "commit", "-m", "fixture");
    fs.appendFileSync(path.join(root, "src/example.ts"), "// changed\n");
    fs.appendFileSync(path.join(root, "tests/example.test.ts"), "// changed\n");
    const reports = path.join(root, "reports");
    const env: NodeJS.ProcessEnv = { ...process.env, PI_TEST_REPORT_DIR: reports };
    delete env.PI_TEST_BASE;
    const result = spawnSync(process.execPath, [affectedScript, "--dry-run", "--shard=1/10"], {
      cwd: root, env, encoding: "utf8",
    });
    expect(result.status, result.stderr).toBe(0);
    expect(result.stdout.match(/--shard=1\/10/g)).toHaveLength(2);
    expect(result.stdout).toContain("--reporter=./scripts/ci-retry-reporter.ts");
    expect(result.stdout).toContain("related.json");
    expect(result.stdout).toContain("--reporter=json");
    expect(result.stdout).toContain("--outputFile.json=");
    expect(result.stdout).toContain("direct.json");
    expect(JSON.parse(fs.readFileSync(path.join(reports, "selection.json"), "utf8")).phases).toEqual(["related", "direct"]);
    // No Vitest report means --dry-run did not run the suite.
    expect(fs.existsSync(path.join(reports, "direct.json"))).toBe(false);
  });
});
