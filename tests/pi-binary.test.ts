import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { resolvePiBinary } from "../src/agents/pi-binary.js";

describe("resolvePiBinary", () => {
  it("prefers an explicit configured binary", () => {
    expect(resolvePiBinary("/custom/pi", {
      env: { PI_FABRIC_PI_BINARY: "/env/pi", LOCALTERM: "1" },
      homeDirectory: "/home/test",
      isExecutable: () => true,
    })).toBe("/custom/pi");
  });

  it("prefers PI_FABRIC_PI_BINARY over LocalTerm discovery", () => {
    expect(resolvePiBinary(undefined, {
      env: { PI_FABRIC_PI_BINARY: "/env/pi", LOCALTERM: "1" },
      homeDirectory: "/home/test",
      isExecutable: () => true,
    })).toBe("/env/pi");
  });

  it("uses the LocalTerm shim by absolute path inside LocalTerm", () => {
    const isExecutable = vi.fn(() => true);
    const binary = resolvePiBinary(undefined, {
      env: { LOCALTERM: "1" },
      homeDirectory: "/home/test",
      isExecutable,
    });

    const expected = path.join("/home/test", ".localterm", "shims", "pi");
    expect(binary).toBe(expected);
    expect(isExecutable).toHaveBeenCalledWith(expected);
  });

  it("falls back to PATH lookup when the LocalTerm shim is unavailable", () => {
    expect(resolvePiBinary(undefined, {
      env: { LOCALTERM: "1" },
      homeDirectory: "/home/test",
      isExecutable: () => false,
    })).toBe("pi");
  });

  it("resolves the PATH launcher to an absolute path before a transport changes PATH", () => {
    const expected = path.resolve("/opt/second", "pi");
    const isExecutable = (candidate: string) => candidate === expected;
    expect(resolvePiBinary(undefined, {
      env: { PATH: ["/opt/first", "/opt/second"].join(path.delimiter) },
      isExecutable,
    })).toBe(expected);
  });

  it("uses PATH lookup outside LocalTerm", () => {
    const isExecutable = vi.fn(() => true);
    expect(resolvePiBinary(undefined, { env: {}, isExecutable })).toBe("pi");
    expect(isExecutable).not.toHaveBeenCalled();
  });
});
