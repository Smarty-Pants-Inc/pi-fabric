import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { WINDOWS_NONREQUIRED_FULL_NAME, windowsNonrequiredPattern } from "../scripts/windows-nonrequired.mjs";

const hosted = { GITHUB_ACTIONS: "true", RUNNER_ENVIRONMENT: "github-hosted" };
const before = Date.parse("2026-11-28T23:59:59.999Z");
const expiry = Date.parse("2026-11-29T00:00:00Z");
const target = "live cleanup move-aside custody retainRuns leaves run and TMPDIR untouched but LIVE host still retains metadata and saved result";

// Keep reason: #20553/#20555's exact Windows failure path and UTC return-to-required boundary.
describe("temporary Windows non-required full-name selection", () => {
  it("excludes exactly the target, retaining every other row in the real suite", () => {
    expect(WINDOWS_NONREQUIRED_FULL_NAME).toBe(target);
    const pattern = new RegExp(windowsNonrequiredPattern("win32", hosted, before)!);
    expect(pattern.test(target)).toBe(false);
    const source = readFileSync(new URL("./residency-live-cleanup-move-aside.test.ts", import.meta.url), "utf8");
    const fullNames: string[] = [];
    let suite = "";
    for (const line of source.split("\n")) {
      const describeTitle = /^describe\("([^"]+)"/.exec(line);
      if (describeTitle) suite = describeTitle[1]!;
      const title = /^  it(?:\.\w+\([^)]*\))?\("([^"]+)"/.exec(line);
      if (title) fullNames.push(`${suite} ${title[1]}`);
    }
    expect(fullNames).toContain(target);
    expect(fullNames.length).toBeGreaterThan(1);
    expect(fullNames.filter((name) => !pattern.test(name))).toEqual([target]);
    for (const name of [target + " extra", "extra " + target, target + " ", target.replace("LIVE", "live"),
      target.slice("live cleanup move-aside custody ".length), "another suite " + target]) {
      expect(pattern.test(name), name).toBe(true);
    }
  });

  it.each([
    ["linux", hosted], ["darwin", hosted], ["win32", {}],
    ["win32", { ...hosted, GITHUB_ACTIONS: "false" }],
    ["win32", { ...hosted, GITHUB_ACTIONS: "1" }],
    ["win32", { RUNNER_ENVIRONMENT: "github-hosted" }],
    ["win32", { GITHUB_ACTIONS: "true" }],
    ["win32", { ...hosted, RUNNER_ENVIRONMENT: "self-hosted" }],
  ])("leaves %s / %j unchanged", (platform, env) => {
    expect(windowsNonrequiredPattern(platform, env, before)).toBeUndefined();
  });

  it("includes the last UTC date and returns to required at the exact next midnight", () => {
    expect(windowsNonrequiredPattern("win32", hosted, Date.parse("2026-11-28T00:00:00Z"))).toBeDefined();
    expect(windowsNonrequiredPattern("win32", hosted, before)).toBeDefined();
    expect(windowsNonrequiredPattern("win32", hosted, expiry)).toBeUndefined();
    expect(windowsNonrequiredPattern("win32", hosted, expiry + 1)).toBeUndefined();
    expect(windowsNonrequiredPattern("win32", { ...hosted, FABRIC_TEST_WINDOWS_NONREQUIRED: "0" }, before))
      .toBe(windowsNonrequiredPattern("win32", hosted, before));
  });

  it.each([before, expiry, expiry + 86_400_000])("selects only the dedicated row at %s regardless of expiry or host", (now) => {
    for (const platform of ["win32", "linux", "darwin"]) {
      const pattern = new RegExp(windowsNonrequiredPattern(platform, { FABRIC_TEST_WINDOWS_NONREQUIRED: "1" }, now)!);
      expect(pattern.test(target)).toBe(true);
      for (const other of ["other test", target + " extra", "extra " + target, target + " "]) {
        expect(pattern.test(other), other).toBe(false);
      }
    }
  });

  it("wires the filter into shared project configuration", () => {
    const config = readFileSync(new URL("../vitest.config.cts", import.meta.url), "utf8");
    expect(config).toContain('require("./scripts/windows-nonrequired.mjs")');
    expect(config).toContain("const selectedWindowsPattern = windowsNonrequiredPattern(process.platform, process.env, Date.now());");
    expect(config).toContain("...(selectedWindowsPattern === undefined ? {} : { testNamePattern: selectedWindowsPattern })");
  });
});
