import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  FABRIC_REPEAT_BLOCK,
  FabricRepeatGuard,
  fabricRepeatBlockText,
  fabricRepeatWarnText,
} from "../src/repeat-guard.js";

// Trust-boundary coverage: inspect guest code as data; never execute shell commands.
// Both configuration probes are mocked so no test touches a real disable file.
const fixture = vi.hoisted(() => ({
  disabled: false,
  home: vi.fn(() => "/fixture-home"),
  exists: vi.fn(),
}));
vi.mock("node:os", async original => ({
  ...await original<typeof import("node:os")>(), homedir: fixture.home,
}));
vi.mock("node:fs", async original => ({
  ...await original<typeof import("node:fs")>(), existsSync: fixture.exists,
}));

beforeEach(() => {
  vi.clearAllMocks();
  fixture.disabled = false;
  fixture.exists.mockImplementation(() => fixture.disabled);
  // Keep the original full-policy tests explicit and independent of the host env.
  vi.stubEnv("SMARTY_ALPHA", "0");
});
afterEach(() => vi.unstubAllEnvs());

describe("FabricRepeatGuard", () => {
  it("counts consecutive identical code and trips warn then block", () => {
    const g = new FabricRepeatGuard(3, 6);
    const code = "return 1";
    expect(g.observe(code)).toEqual({ count: 1, blocked: false, warn: false });
    expect(g.observe(code).count).toBe(2);
    expect(g.observe(code).warn).toBe(true);
    expect(g.observe(code).blocked).toBe(false);
    expect(g.observe(code).count).toBe(5);
    expect(g.observe(code).blocked).toBe(true);
    expect(g.observe(code).blocked).toBe(true);
  });

  it("resets on any differing code", () => {
    const g = new FabricRepeatGuard(3, 6);
    const code = "return 1";
    for (let i = 0; i < 5; i += 1) g.observe(code);
    expect(g.observe(code).blocked).toBe(true);
    expect(g.observe("return 2")).toEqual({ count: 1, blocked: false, warn: false });
  });

  it("ignores cosmetic variation: only the fingerprinted code matters", () => {
    // The 2026-09-15 incident: identical code executed 220+ times while the
    // display name incremented (noop1, noop2, ...). The fingerprint is the
    // code alone, so display churn does not reset the count.
    const g = new FabricRepeatGuard(3, 6);
    const loopCode = "const r = await pi.bash({ cmd: 'echo noop' }); return r?.output;";
    let verdict = { count: 0, blocked: false, warn: false };
    for (let i = 1; i <= 6; i += 1) {
      verdict = g.observe(loopCode);
      if (i >= 6) expect(verdict.blocked).toBe(true);
      if (i >= 3) expect(verdict.warn).toBe(true);
    }
    expect(verdict.count).toBe(6);
  });

  it("formats the warn and block copy the tool surfaces to the model", () => {
    expect(fabricRepeatWarnText(3, FABRIC_REPEAT_BLOCK)).toContain("at 6 identical runs will be blocked");
    expect(fabricRepeatBlockText(6)).toContain("executed 6 times in a row and was blocked");
  });
});

describe("FabricRepeatGuard under alpha", () => {
  it.each([undefined, "1"])("keeps counting and warning without blocking for env %s", value => {
    vi.stubEnv("SMARTY_ALPHA", value);
    const g = new FabricRepeatGuard(3, 6);
    for (let count = 1; count <= 12; count++) {
      expect(g.observe("return 1")).toEqual({ count, blocked: false, warn: count >= 3 });
    }
    expect(fixture.exists).toHaveBeenCalledTimes(12);
    expect(fabricRepeatWarnText(12, FABRIC_REPEAT_BLOCK)).toContain("warn-only under alpha mode");
    expect(fabricRepeatWarnText(12, FABRIC_REPEAT_BLOCK)).not.toContain("will be blocked");
    expect(g.observe("return 2")).toEqual({ count: 1, blocked: false, warn: false });
  });

  it("lets the disable file override exact zero for both verdict and warning", () => {
    fixture.disabled = true;
    const g = new FabricRepeatGuard(3, 6);
    for (let count = 1; count <= 8; count++) {
      expect(g.observe("return 1")).toEqual({ count, blocked: false, warn: count >= 3 });
    }
    expect(fixture.home).toHaveBeenCalledTimes(8);
    expect(fixture.exists).toHaveBeenLastCalledWith(join("/fixture-home", ".config/smarty/bash-guard.disabled"));
    expect(fabricRepeatWarnText(8, FABRIC_REPEAT_BLOCK)).toContain("warn-only under alpha mode");
    expect(fabricRepeatWarnText(8, FABRIC_REPEAT_BLOCK)).not.toContain("will be blocked");
  });

  it("switches modes on the same guard without discarding consecutive history", () => {
    vi.stubEnv("SMARTY_ALPHA", undefined);
    const g = new FabricRepeatGuard(3, 6);
    for (let count = 1; count <= 7; count++) {
      expect(g.observe("return 1")).toEqual({ count, blocked: false, warn: count >= 3 });
    }
    vi.stubEnv("SMARTY_ALPHA", "0");
    expect(g.observe("return 1")).toEqual({ count: 8, blocked: true, warn: true });
    fixture.disabled = true;
    expect(g.observe("return 1")).toEqual({ count: 9, blocked: false, warn: true });
    fixture.disabled = false;
    expect(g.observe("return 1")).toEqual({ count: 10, blocked: true, warn: true });
    vi.stubEnv("SMARTY_ALPHA", "1");
    expect(g.observe("return 1")).toEqual({ count: 11, blocked: false, warn: true });
    vi.stubEnv("SMARTY_ALPHA", undefined);
    expect(g.observe("return 1")).toEqual({ count: 12, blocked: false, warn: true });
    vi.stubEnv("SMARTY_ALPHA", "0");
    expect(g.observe("return 1")).toEqual({ count: 13, blocked: true, warn: true });
    expect(g.observe("return 2")).toEqual({ count: 1, blocked: false, warn: false });
    expect(fixture.home).toHaveBeenCalledTimes(14);
    expect(fixture.exists).toHaveBeenCalledTimes(14);
  });

  it("rechecks the mode whenever warning copy is formatted", () => {
    expect(fabricRepeatWarnText(3, FABRIC_REPEAT_BLOCK)).toContain("at 6 identical runs will be blocked");
    vi.stubEnv("SMARTY_ALPHA", "1");
    expect(fabricRepeatWarnText(3, FABRIC_REPEAT_BLOCK)).toContain("warn-only under alpha mode");
    vi.stubEnv("SMARTY_ALPHA", "0");
    fixture.disabled = true;
    expect(fabricRepeatWarnText(3, FABRIC_REPEAT_BLOCK)).toContain("warn-only under alpha mode");
    fixture.disabled = false;
    expect(fabricRepeatWarnText(3, FABRIC_REPEAT_BLOCK)).toContain("at 6 identical runs will be blocked");
    expect(fixture.exists).toHaveBeenCalledTimes(4);
  });
});
