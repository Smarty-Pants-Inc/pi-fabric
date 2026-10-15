import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { windowsMeshLocalAppData } from "./windows-mesh-cache-fixture.js";
import { holdMeshFenceSync } from "../src/mesh/fence-lock.js";
import { windowsHelperBootstrap } from "../src/storage/verified-remove.js";
import {
  forgetWindowsMeshCapability, prepareWindowsMeshCapability, windowsMeshReleaseTestHooks, type WindowsMeshReleaseChildSample,
} from "../src/storage/windows-mesh-release.js";

// Hosted Windows only: real filesystem aliases and the production acquisition preflight, no platform/filesystem mocks.
// Keep: retained-capability lifetime trust boundary (CR-A-02). Value-import verified-remove deliberately keeps this
// real native/deleting fixture out of local deletion-detector admission. Off-platform skip is not hosted evidence.
const hosted = process.platform === "win32" && process.env.GITHUB_ACTIONS === "true";
const fixtures: string[] = [];
afterEach(() => {
  vi.unstubAllEnvs();
  forgetWindowsMeshCapability();
  for (const key of Object.keys(windowsMeshReleaseTestHooks)) delete (windowsMeshReleaseTestHooks as Record<string, unknown>)[key];
  for (const dir of fixtures.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

describe.skipIf(!hosted)("Windows production retained-cache temp policy (hosted Windows CI only)", () => {
  it("default preparation and public release use the actual LOCALAPPDATA per-build cache without a cacheRoot override", () => {
    const boot = windowsHelperBootstrap();
    expect(boot, "hosted build must supply the trusted digest-pinned helper").toBeDefined();
    expect(windowsMeshReleaseTestHooks.cacheRoot).toBeUndefined();
    const local = windowsMeshLocalAppData();
    expect(fs.realpathSync.native(process.env.LOCALAPPDATA!)).toBe(local); // no profile/temp environment overrides
    forgetWindowsMeshCapability();
    const capability = prepareWindowsMeshCapability();
    const expected = path.join(local, "pi-fabric", "native", boot!.digest, "fabric-win-remove.exe");
    expect(capability).toEqual({ absolutePath: expected, digest: boot!.digest, meshAbi: 1 });
    const identity = (file: string) => {
      const stat = fs.lstatSync(file, { bigint: true });
      return { dev: stat.dev, ino: stat.ino };
    };
    const retained = [path.dirname(expected), expected];
    const before = retained.map(identity);
    expect(createHash("sha256").update(fs.readFileSync(expected)).digest("hex")).toBe(boot!.digest);
    forgetWindowsMeshCapability(); // cold preparation must reuse, not replace, this build's retained directory/file
    expect(prepareWindowsMeshCapability()).toEqual(capability);
    expect(retained.map(identity)).toEqual(before);

    const root = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fmrd-")));
    fixtures.push(root); // ONLY this test-owned mesh root is deleted; the default production cache is retained.
    const samples: WindowsMeshReleaseChildSample[] = [];
    windowsMeshReleaseTestHooks.afterChild = sample => { samples.push(sample); };
    let entered = 0;
    expect(holdMeshFenceSync(root, 0, () => { entered++; return "profile cache executed"; })).toBe("profile cache executed");
    expect(entered).toBe(1);
    expect(samples).toHaveLength(2); // both public fence locks executed the actual retained native helper
    for (const sample of samples) expect(sample).toMatchObject({ absolutePath: expected, digest: boot!.digest, outcome: "released", attempt: 1 });
    expect(fs.readdirSync(root)).toEqual([]);
    expect(retained.map(identity)).toEqual(before);
    expect(createHash("sha256").update(fs.readFileSync(expected)).digest("hex")).toBe(boot!.digest);
    expect(windowsMeshReleaseTestHooks.cacheRoot).toBeUndefined();
  });

  it.each(["direct", "junction"] as const)("CR-A-02: real %s temp-backed LOCALAPPDATA refuses acquisition before cache/owner publication", kind => {
    expect(windowsHelperBootstrap(), "hosted build must supply the trusted digest-pinned helper").toBeDefined();
    forgetWindowsMeshCapability();
    expect(windowsMeshReleaseTestHooks.cacheRoot).toBeUndefined();
    const fixture = fs.realpathSync.native(fs.mkdtempSync(path.join(os.tmpdir(), "fmrc-")));
    fixtures.push(fixture);
    const temp = path.join(fixture, "effective-temp");
    fs.mkdirSync(temp);
    const local = kind === "direct" ? temp : path.join(fixture, "local-alias");
    if (kind === "junction") fs.symlinkSync(temp, local, "junction");
    // The alias is outside the effective temp path by spelling, but resolves to it. This is intentionally a TEMP
    // refusal fixture, not a usable cache: policy must refuse before checking the NETWORK SERVICE-owned ancestry.
    vi.stubEnv("TEMP", temp);
    vi.stubEnv("TMP", temp);
    vi.stubEnv("LOCALAPPDATA", local);
    const root = path.join(fixture, "mesh");
    let entered = 0;
    expect(() => holdMeshFenceSync(root, 0, () => { entered++; })).toThrow(/must not be in a temporary location/);
    expect(entered).toBe(0);
    expect(fs.existsSync(path.join(temp, "pi-fabric"))).toBe(false);
    expect(fs.existsSync(path.join(root, "custody.lock"))).toBe(false);
    expect(fs.existsSync(path.join(root, "custody.lock", "owner"))).toBe(false);
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
    expect(fs.existsSync(path.join(root, ".lock", "owner"))).toBe(false);
    expect(fs.readdirSync(root)).toEqual([]); // no unpublished lock staging either
  });
});
