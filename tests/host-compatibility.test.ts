import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  compareVersions,
  fixturePiHostSupported,
  MINIMUM_FIXTURE_PI_HOST_VERSION,
  detectPiHostVersion,
  MINIMUM_PI_HOST_VERSION,
  piHostCompatibilityWarning,
} from "../src/host-compatibility.js";

const roots: string[] = [];

const fakeHost = (version: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-host-version-"));
  roots.push(root);
  const dist = path.join(root, "dist");
  fs.mkdirSync(dist);
  fs.writeFileSync(
    path.join(root, "package.json"),
    JSON.stringify({ name: "@earendil-works/pi-coding-agent", version }),
  );
  const cli = path.join(dist, "cli.js");
  fs.writeFileSync(cli, "");
  return cli;
};

afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("Pi host compatibility", () => {
  it("compares release and prerelease versions", () => {
    expect(compareVersions("0.80.5", MINIMUM_PI_HOST_VERSION)).toBeLessThan(0);
    expect(compareVersions("1.0.0", MINIMUM_PI_HOST_VERSION)).toBe(0);
    expect(compareVersions("1.0.1", MINIMUM_PI_HOST_VERSION)).toBeGreaterThan(0);
    expect(compareVersions("1.0.0-beta.1", MINIMUM_PI_HOST_VERSION)).toBeLessThan(0);
    expect(compareVersions("invalid", MINIMUM_PI_HOST_VERSION)).toBeUndefined();
  });

  it.each([
    ["0.80.6", false], ["0.85.1", false], ["0.86.0-beta.1", false],
    [undefined, false], ["invalid", false], ["0.87.0garbage", false],
    ["0.87.0-beta.1", false], ["0.087.0", false], ["0.87.0+bad..build", false],
    ["0.86.0", true], ["0.87.0", true], ["0.87.1+fixture.1", true],
  ])("requires a known shell-safe fixture host %j: %j", (version, supported) => {
    expect(MINIMUM_FIXTURE_PI_HOST_VERSION).toBe("0.86.0");
    expect(fixturePiHostSupported(version)).toBe(supported);
    expect(MINIMUM_PI_HOST_VERSION).toBe("0.80.6");
  });

  it("detects the host package from the CLI path", () => {
    expect(detectPiHostVersion(fakeHost("1.0.1"))).toBe("1.0.1");
    expect(detectPiHostVersion("/does/not/exist")).toBeUndefined();
  });

  it("warns only for a detected unsupported host", () => {
    expect(piHostCompatibilityWarning("0.80.5")).toContain("requires Pi >= 1.0.0");
    expect(piHostCompatibilityWarning("1.0.0")).toBeUndefined();
    expect(piHostCompatibilityWarning(undefined)).toBeUndefined();
  });
});
