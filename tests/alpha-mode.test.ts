import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { alphaModeEnabled } from "../src/guards/alpha-mode.js";

// Mock both home and the file probe; never inspect or change user configuration.
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

const importProbes = { home: fixture.home.mock.calls.length, exists: fixture.exists.mock.calls.length };

beforeEach(() => {
  vi.clearAllMocks();
  fixture.disabled = false;
  fixture.exists.mockImplementation(() => fixture.disabled);
  vi.stubEnv("SMARTY_ALPHA", undefined);
});
afterEach(() => vi.unstubAllEnvs());

describe("alphaModeEnabled", () => {
  it("has no configuration probes at import time", () => {
    expect(importProbes).toEqual({ home: 0, exists: 0 });
  });

  it("defaults to the current process environment", () => {
    expect(alphaModeEnabled()).toBe(true);
    vi.stubEnv("SMARTY_ALPHA", "0");
    expect(alphaModeEnabled()).toBe(false);
    vi.stubEnv("SMARTY_ALPHA", "1");
    expect(alphaModeEnabled()).toBe(true);
    vi.stubEnv("SMARTY_ALPHA", undefined);
    expect(alphaModeEnabled()).toBe(true);
    expect(fixture.exists).toHaveBeenCalledTimes(4);
  });

  it.each([undefined, "1", "", "true", "00", " 0", "0 "])("enables alpha for env %s", value => {
    const env: Readonly<Record<string, string | undefined>> = { SMARTY_ALPHA: value };
    expect(alphaModeEnabled(env)).toBe(true);
    expect(fixture.home).toHaveBeenCalledOnce();
    expect(fixture.exists).toHaveBeenCalledExactlyOnceWith(join("/fixture-home", ".config/smarty/bash-guard.disabled"));
  });

  it("only opts out for exact zero without the disable file", () => {
    expect(alphaModeEnabled({ SMARTY_ALPHA: "0" })).toBe(false);
    fixture.disabled = true;
    expect(alphaModeEnabled({ SMARTY_ALPHA: "0" })).toBe(true);
  });

  it("rechecks both a supplied environment and the disable file on every call", () => {
    const env: Record<string, string | undefined> = {};
    expect(alphaModeEnabled(env)).toBe(true);
    env.SMARTY_ALPHA = "0";
    expect(alphaModeEnabled(env)).toBe(false);
    fixture.disabled = true;
    expect(alphaModeEnabled(env)).toBe(true);
    fixture.disabled = false;
    expect(alphaModeEnabled(env)).toBe(false);
    env.SMARTY_ALPHA = "1";
    expect(alphaModeEnabled(env)).toBe(true);
    delete env.SMARTY_ALPHA;
    expect(alphaModeEnabled(env)).toBe(true);
    expect(fixture.home).toHaveBeenCalledTimes(6);
    expect(fixture.exists).toHaveBeenCalledTimes(6);
  });
});
