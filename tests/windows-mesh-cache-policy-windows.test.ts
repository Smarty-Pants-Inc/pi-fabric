import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { holdMeshFenceSync } from "../src/mesh/fence-lock.js";
import { windowsHelperBootstrap } from "../src/storage/verified-remove.js";
import { forgetWindowsMeshCapability, windowsMeshReleaseTestHooks } from "../src/storage/windows-mesh-release.js";

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
    // The alias is outside the effective temp path by spelling, but resolves to it. Temp inherits this user's private
    // runner directory ACL; policy refusal must happen before native privacy/publication, not because of a shared ACL.
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
