import { execFile, spawnSync } from "node:child_process";
import fs from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { describe, expect, it } from "vitest";

const normalized = (value: string) => process.platform === "win32" ? value.replaceAll("\\", "/") : value;

describe("Bun deletion guard preload propagation", () => {
  // Data-safety regression for pi-fabric#1204: real child syscalls, not a mocked fs.
  // Both sides of the child's boundary are inside the parent's temp, so teardown
  // can remove the sentinel without an outside-root exemption or leaked fixture.
  it.each(["direct", "node-relay", "bun-relay", "promisified"])("allows inside deletes, refuses outside deletes and preserves stderr with HOME inside the root (%s)", async launcher => {
    const fixture = fs.mkdtempSync(join(tmpdir(), "bun-guard-"));
    const root = join(fixture, "private");
    const home = join(root, "home");
    const inside = join(root, "remove");
    const sentinel = join(fixture, "keep");
    fs.mkdirSync(home, { recursive: true });
    fs.writeFileSync(inside, "remove");
    fs.writeFileSync(sentinel, "keep");
    const script = join(root, "child.mjs");
    fs.writeFileSync(script, `import fs from "node:fs";
const guard = process[Symbol.for("pi-fabric.test-deletion-guard")];
// RED-safe: a missing or report-only guard must never reach an outside delete.
if (!guard || guard.report) process.exit(42);
let insideCode = null, outsideCode = null;
try { fs.rmSync(${JSON.stringify(inside)}); } catch (error) { insideCode = error.code; }
try { fs.rmSync(${JSON.stringify(sentinel)}); } catch (error) { outsideCode = error.code; }
console.log(JSON.stringify({ root: guard.root, insideCode, outsideCode, only: process.env.ONLY }));`);
    const relay = join(root, "relay.mjs");
    // An unknown/intermediary executable may relay to Bun. Its explicit env must
    // retain the guard even though the immediate executable is not Bun.
    fs.writeFileSync(relay, launcher === "bun-relay"
      ? `const result = Bun.spawnSync(["bun", ${JSON.stringify(script)}], { env: { ...process.env }, stdout: "pipe", stderr: "pipe" });
process.stdout.write(result.stdout); process.stderr.write(result.stderr); process.exitCode = result.exitCode;`
      : `import cp from "node:child_process";
const result = cp.spawnSync("bun", [${JSON.stringify(script)}], { env: { ...process.env }, encoding: "utf8" });
process.stdout.write(result.stdout ?? ""); process.stderr.write(result.stderr ?? ""); process.exitCode = result.status ?? 1;`);
    try {
      const executable = launcher === "node-relay" ? process.execPath : "bun";
      const args = [launcher === "direct" || launcher === "promisified" ? script : relay];
      const options = {
        env: { PATH: process.env.PATH, ONLY: "preserved", HOME: home, USERPROFILE: home,
          TMPDIR: root, TMP: root, TEMP: root, TEST_DELETION_GUARD_ROOT: root,
          RUNNER_ENVIRONMENT: "", GITHUB_ACTIONS: "false" },
        encoding: "utf8" as const, timeout: 10_000,
      };
      const child = launcher === "promisified" ? await (async () => {
        const pending = promisify(execFile)(executable, args, options);
        const streams = await pending;
        expect(pending.child?.pid).toBeTypeOf("number");
        return { ...streams, status: 0, error: undefined };
      })() : spawnSync(executable, args, options);
      expect(child.error).toBeUndefined();
      expect(child.status, child.stderr).toBe(0);
      expect(JSON.parse(child.stdout)).toEqual({ root: fs.realpathSync(root), insideCode: null,
        outsideCode: "ERR_TEST_DELETION_GUARD", only: "preserved" });
      expect(fs.existsSync(inside)).toBe(false);
      expect(fs.readFileSync(sentinel, "utf8")).toBe("keep");
      expect(child.stderr).toBe("");
    } finally { fs.rmSync(fixture, { recursive: true, force: true }); }
  });

  it("keeps an inherited-environment Bun child that only deletes inside temp silent", () => {
    const fixture = fs.mkdtempSync(join(tmpdir(), "bun-guard-quiet-"));
    const inside = join(fixture, "remove");
    fs.writeFileSync(inside, "remove");
    const home = join(fixture, "home");
    fs.mkdirSync(home);
    try {
      const child = spawnSync("bun", ["-e", `import fs from "node:fs";
const guard = process[Symbol.for("pi-fabric.test-deletion-guard")];
if (!guard?.root) process.exit(42);
fs.rmSync(${JSON.stringify(inside)}); process.stdout.write("unchanged");`], {
        env: { ...process.env, HOME: home, USERPROFILE: home }, encoding: "utf8", timeout: 10_000,
      });
      expect(child.error).toBeUndefined();
      expect(child.status, child.stderr).toBe(0);
      expect(child.stdout).toBe("unchanged");
      expect(child.stderr).toBe("");
      expect(fs.existsSync(inside)).toBe(false);
    } finally { fs.rmSync(fixture, { recursive: true, force: true }); }
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
    expect(child.stderr).toBe("");
  });
});
