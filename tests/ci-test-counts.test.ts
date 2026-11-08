import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync, execFileSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import { compareFileParity } from "../scripts/ci-test-counts.mjs";

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
const aggregate = (root: string) => {
  const selection = path.join(root, "unsharded.json");
  // Inject discovery results: these tests must not discover or run real suites.
  const files = ["tests/example-1.test.ts", "tests/example-2.test.ts"];
  const phases = JSON.parse(fs.readFileSync(path.join(root, "affected-ubuntu-latest-1", "selection.json"), "utf8")).phases;
  json(selection, { related: phases.includes("related") ? files : [], direct: phases.includes("direct") ? files : [] });
  return spawnSync(process.execPath, [countsScript, root, "ubuntu-latest", "2", selection], {
    encoding: "utf8", env: { ...process.env, GITHUB_STEP_SUMMARY: path.join(root, "summary.md") },
  });
};

describe("file-level shard parity (pure, injected unsharded selection)", () => {
  const a = "tests/a.test.ts";
  const b = "tests/b.test.ts";
  const expected = { related: [a, b], direct: [a] };

  it("accepts equal unions, including cross-phase overlap and empty shards", () => {
    const result = compareFileParity(expected, [
      { related: [b], direct: [a] }, { related: [a], direct: [] },
    ]);
    expect(result.related).toEqual({ expected: 2, actual: 2, missing: [], extra: [], duplicated: [], ok: true });
    expect(result.direct.ok).toBe(true);
    expect(compareFileParity({ related: [], direct: [] }, [{ related: [], direct: [] }]).direct.ok).toBe(true);
  });

  it("reports missing files independently per phase", () => {
    const result = compareFileParity(expected, [{ related: [a], direct: [] }]);
    expect(result.related.missing).toEqual([b]);
    expect(result.direct.missing).toEqual([a]);
    expect(result.related.ok).toBe(false);
    expect(result.direct.ok).toBe(false);
  });

  it("reports extra files, including an unexpectedly populated empty phase", () => {
    const result = compareFileParity({ related: [a], direct: [] }, [{ related: [a, b], direct: [b] }]);
    expect(result.related.extra).toEqual([b]);
    expect(result.direct.extra).toEqual([b]);
    expect(result.related.ok).toBe(false);
    expect(result.direct.ok).toBe(false);
  });

  it("rejects a duplicated file even when the union equals the selection", () => {
    const result = compareFileParity(expected, [
      { related: [a, b], direct: [a] }, { related: [b], direct: [a] },
    ]);
    expect(result.related.duplicated).toEqual([b]);
    expect(result.direct.duplicated).toEqual([a]);
    expect(result.related.missing).toEqual([]);
    expect(result.related.extra).toEqual([]);
    expect(result.related.ok).toBe(false);
    expect(result.direct.ok).toBe(false);
  });
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
    const result = aggregate(root);
    expect(result.status).not.toBe(0);
    expect(result.stdout).toContain("missing: tests/example-2.test.ts");
    expect(result.stdout).toContain("duplicated: tests/example-1.test.ts");
    expect(fs.readFileSync(path.join(root, "summary.md"), "utf8")).toContain("**direct: FAIL**");
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

describe("affected selector shard execution", () => {
  it("records an empty direct shard without running Vitest, but invokes Vitest for a nonempty shard", () => {
    const root = temp();
    const git = (...args: string[]) => execFileSync("git", args, { cwd: root, stdio: "pipe" });
    fs.mkdirSync(path.join(root, "src"));
    fs.mkdirSync(path.join(root, "tests"));
    fs.writeFileSync(path.join(root, "src/example.ts"), "export const value = 1;\n");
    git("init");
    git("add", ".");
    git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.test", "commit", "-m", "fixture");

    const preload = path.join(root, "record-bunx.mjs");
    const callsFile = path.join(root, "bunx-calls.jsonl");
    fs.writeFileSync(preload, `import fs from "node:fs";
import childProcess from "node:child_process";
import { syncBuiltinESMExports } from "node:module";
const originalExecFileSync = childProcess.execFileSync;
childProcess.execFileSync = function (file, args, options) {
  if (file === "bunx") {
    fs.appendFileSync(process.env.CI_TEST_BUNX_CALLS, JSON.stringify({ file, args }) + "\\n");
    return Buffer.alloc(0);
  }
  return originalExecFileSync.call(this, file, args, options);
};
syncBuiltinESMExports();
`);
    const runShard = (reports: string) => {
      const env: NodeJS.ProcessEnv = {
        ...process.env,
        PI_TEST_REPORT_DIR: reports,
        CI_TEST_BUNX_CALLS: callsFile,
        NODE_OPTIONS: [process.env.NODE_OPTIONS, `--import=${pathToFileURL(preload).href}`].filter(Boolean).join(" "),
      };
      delete env.PI_TEST_BASE;
      return spawnSync(process.execPath, [affectedScript, "--shard=7/10"], {
        cwd: root, env, encoding: "utf8",
      });
    };

    const directFiles = Array.from({ length: 10 }, (_, index) => `tests/direct-${index + 1}.test.ts`);
    fs.writeFileSync(path.join(root, directFiles[0]!), "// selected direct test\n");
    const skippedReports = path.join(root, "skipped-reports");
    const skipped = runShard(skippedReports);
    expect(skipped.status, skipped.stderr).toBe(0);
    expect(skipped.stdout).toContain("shard 7/10: no selected tests");
    expect(fs.existsSync(callsFile)).toBe(false);
    const skippedSelection = JSON.parse(fs.readFileSync(path.join(skippedReports, "selection.json"), "utf8"));
    expect(skippedSelection.phases).toEqual(["direct"]);
    expect(skippedSelection.direct).toEqual([directFiles[0]]);
    const skippedReport = JSON.parse(fs.readFileSync(path.join(skippedReports, "direct.json"), "utf8"));
    expect(skippedReport).toMatchObject({
      success: true,
      numTotalTests: 0,
      numPassedTests: 0,
      numFailedTests: 0,
      numPendingTests: 0,
      numTodoTests: 0,
      testResults: [],
    });

    const platform = process.platform === "win32" ? "windows-latest" : "ubuntu-latest";
    const shardRoot = path.join(root, "aggregate-shards");
    const unshardedSelection = path.join(root, "unsharded-selection.json");
    json(unshardedSelection, { related: [], direct: [directFiles[0]] });
    for (let index = 1; index <= 10; index++) {
      const directory = path.join(shardRoot, `affected-${platform}-${index}`);
      fs.mkdirSync(directory, { recursive: true });
      if (index === 7) {
        fs.copyFileSync(path.join(skippedReports, "selection.json"), path.join(directory, "selection.json"));
        fs.copyFileSync(path.join(skippedReports, "direct.json"), path.join(directory, "direct.json"));
      } else {
        json(path.join(directory, "selection.json"), { ...skippedSelection, shard: `${index}/10` });
        json(path.join(directory, "direct.json"), index === 1 ? {
          success: true,
          numTotalTests: 1,
          numPassedTests: 1,
          numFailedTests: 0,
          numPendingTests: 0,
          numTodoTests: 0,
          testResults: [{ name: `/fixture/${directFiles[0]}` }],
        } : skippedReport);
      }
    }
    const counts = spawnSync(process.execPath, [countsScript, shardRoot, platform, "10", unshardedSelection], {
      encoding: "utf8", env: { ...process.env, GITHUB_STEP_SUMMARY: path.join(root, "aggregate-summary.md") },
    });
    expect(counts.status, counts.stderr).toBe(0);
    expect(counts.stdout).toContain("**direct: PASS** — expected 1 files; shard union 1 files.");

    for (const file of directFiles.slice(1)) fs.writeFileSync(path.join(root, file), "// selected direct test\n");
    const nonemptyReports = path.join(root, "nonempty-reports");
    const nonempty = runShard(nonemptyReports);
    expect(nonempty.status, nonempty.stderr).toBe(0);
    const invocations = fs.readFileSync(callsFile, "utf8").trim().split("\n").map((line) => JSON.parse(line));
    expect(invocations).toHaveLength(1);
    expect(invocations[0].file).toBe("bunx");
    expect(invocations[0].args).toContain("vitest");
    expect(invocations[0].args).toContain("--shard=7/10");
    expect(invocations[0].args.filter((arg: string) => directFiles.includes(arg))).toHaveLength(10);
  });
});
