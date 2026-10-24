import { spawnSync } from "node:child_process";
import fs from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { describe, expect, it } from "vitest";

const normalized = (value: string) => process.platform === "win32" ? value.replaceAll("\\", "/") : value;

describe("Bun deletion guard preload propagation", () => {
  it("pins an explicit Bun child environment and refuses deletion outside the test temp", () => {
    const root = tmpdir();
    const outside = fs.mkdtempSync(join(dirname(root), "bun-guard-outside-"));
    const sentinel = join(outside, "keep");
    const script = join(root, `bun-guard-${process.pid}.mjs`);
    fs.writeFileSync(sentinel, "keep");
    fs.writeFileSync(script, `import fs from "node:fs";
const guard=process[Symbol.for("pi-fabric.test-deletion-guard")];
if(!guard || guard.report) process.exit(42);
let code;
try { fs.rmSync(${JSON.stringify(sentinel)}); } catch(error) { code=error.code; }
console.log(JSON.stringify({installed:!!guard,root:guard.root,code,only:process.env.ONLY}));`);
    const child = spawnSync("bun", [script], {
      env: { PATH: process.env.PATH, ONLY: "preserved", TMPDIR: root, TMP: root, TEMP: root, RUNNER_ENVIRONMENT: "", GITHUB_ACTIONS: "false" },
      encoding: "utf8", timeout: 10_000,
    });
    expect(child.error).toBeUndefined();
    expect(child.status, child.stderr).toBe(0);
    expect(JSON.parse(child.stdout)).toEqual({ installed: true, root, code: "ERR_TEST_DELETION_GUARD", only: "preserved" });
    expect(fs.readFileSync(sentinel, "utf8")).toBe("keep");
  });

  it("preserves caller Bun options and root when the guard itself is preloaded", () => {
    const root = tmpdir();
    const guard = join(process.cwd(), "tests", "test-deletion-guard.mjs");
    const script = `console.log(JSON.stringify({root:process.env.TEST_DELETION_GUARD_ROOT,options:process.env.BUN_OPTIONS}));`;
    const child = spawnSync("bun", ["-e", script], {
      env: { PATH: process.env.PATH, ONLY: "keep", TMPDIR: root, TMP: root, TEMP: root,
        BUN_OPTIONS: `--smol --preload=${guard}`, TEST_DELETION_GUARD_ROOT: root,
        RUNNER_ENVIRONMENT: "", GITHUB_ACTIONS: "false" },
      encoding: "utf8", timeout: 10_000,
    });
    expect(child.error).toBeUndefined();
    expect(child.status, child.stderr).toBe(0);
    const result = JSON.parse(child.stdout) as { root: string; options: string };
    expect(result.root).toBe(root);
    expect(result.options).toContain("--smol");
    expect(normalized(result.options)).toContain(normalized(guard));
    // Bun injects platform-standard variables; assert caller data and guard keys, not exact env equality.
    expect(result.options).toContain("--smol");
  });
});
