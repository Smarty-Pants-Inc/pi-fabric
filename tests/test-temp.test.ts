import { spawnSync } from "node:child_process";
import { existsSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename } from "node:path";
import { describe, expect, it } from "vitest";

describe("test temporary-storage isolation", () => {
  it("keeps workers and inherited child tools away from real session caches", () => {
    expect(tmpdir()).toBe(process.env.TMPDIR);
    expect(process.env.TMP).toBe(tmpdir());
    expect(process.env.TEMP).toBe(tmpdir());
    expect(basename(tmpdir())).toMatch(/^pi-(fabric|fovea|contour)-vitest-/);
    expect(statSync(tmpdir()).isDirectory()).toBe(true);
  });

  it.each([0, 7])("keeps a shared batch root alive until its owner exits with %i", (exitCode) => {
    const helper = new URL("../scripts/test-temp.mjs", import.meta.url).href;
    const childCode = `
      import { isolatedTestTemp } from ${JSON.stringify(helper)};
      process.stdout.write(JSON.stringify(isolatedTestTemp("pi-fabric-vitest-", true)));
      process.exit(Number(process.env.FIXTURE_EXIT));
    `;
    const result = spawnSync("bun", ["-e", `
      import { testTempEnvironment } from ${JSON.stringify(helper)};
      import { spawnSync } from "node:child_process";
      import { existsSync, writeFileSync } from "node:fs";
      import { join } from "node:path";
      const env = testTempEnvironment();
      writeFileSync(join(env.TMPDIR, "owner-evidence"), "keep through both children");
      const children = [0, ${exitCode}].map(status => {
        const child = spawnSync("bun", ["-e", ${JSON.stringify(childCode)}], {
          env: {...process.env, ...env, FIXTURE_EXIT:String(status)}, encoding:"utf8", timeout:5000
        });
        return {status:child.status, stderr:child.stderr, stdout:child.stdout,
          rootAlive:existsSync(env.TMPDIR), evidenceAlive:existsSync(join(env.TMPDIR,"owner-evidence"))};
      });
      process.stdout.write(JSON.stringify({env, children}));
      process.exit(${exitCode});
    `], {encoding:"utf8", timeout:15000,
      env:{...process.env, PI_TEST_TEMP_ROOT:undefined, PI_TEST_TEMP_CAPTURE:"0"}});
    expect(result.error).toBeUndefined();
    expect(result.stderr).toBe("");
    expect(result.status).toBe(exitCode);
    const {env, children} = JSON.parse(result.stdout);
    expect(children.map((child: {status:number}) => child.status)).toEqual([0, exitCode]);
    for (const child of children) {
      expect(child.stderr).toBe("");
      expect(child.rootAlive).toBe(true);
      expect(child.evidenceAlive).toBe(true);
      expect(JSON.parse(child.stdout)).toEqual({TMPDIR:env.TMPDIR, TMP:env.TMPDIR, TEMP:env.TMPDIR});
    }
    expect(existsSync(env.TMPDIR)).toBe(false);
  });

  it.each([0, 7])("removes private artifacts when the runner exits with %i", (exitCode) => {
    const helper = new URL("../scripts/test-temp.ts", import.meta.url).href;
    const result = spawnSync("bun", ["-e", `
      import { isolatedTestTemp } from ${JSON.stringify(helper)};
      import { writeFileSync } from "node:fs";
      import { join } from "node:path";
      const env = isolatedTestTemp("test-temp-probe-");
      writeFileSync(join(env.TMPDIR, "cache.json"), "temporary");
      process.stdout.write(JSON.stringify(env));
      process.exit(${exitCode});
    `], { encoding: "utf8", timeout: 10_000 });
    expect(result.error).toBeUndefined();
    expect(result.stderr).toBe("");
    expect(result.status).toBe(exitCode);
    const env = JSON.parse(result.stdout) as Record<string, string>;
    expect(env.TMPDIR).toBe(env.TMP);
    expect(env.TEMP).toBe(env.TMP);
    expect(existsSync(env.TMPDIR!)).toBe(false);
  });
});
