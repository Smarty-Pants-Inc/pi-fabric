import { afterEach, describe, expect, it } from "vitest";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const temporary: string[] = [];
afterEach(() => { for (const dir of temporary.splice(0)) fs.rmSync(dir, { recursive: true, force: true }); });

const fixture = (): string => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-build-artifacts-"));
  temporary.push(dir);
  fs.cpSync(path.join(root, "dist"), path.join(dir, "dist"), { recursive: true });
  fs.cpSync(path.join(root, "bin"), path.join(dir, "bin"), { recursive: true });
  for (const file of ["package.json", "config/landlock-roles.json", "scripts/assert-build-artifacts.mjs", "src/verified/generated/manifest.json"]) {
    fs.mkdirSync(path.dirname(path.join(dir, file)), { recursive: true });
    fs.copyFileSync(path.join(root, file), path.join(dir, file));
  }
  return dir;
};
const assertBuild = (dir: string, env: NodeJS.ProcessEnv = process.env) =>
  spawnSync(process.execPath, [path.join(dir, "scripts/assert-build-artifacts.mjs")], { encoding: "utf8", timeout: 20_000, env });
const rejected = (dir: string, reason: string, env?: NodeJS.ProcessEnv): void => {
  const result = assertBuild(dir, env);
  expect(result.error).toBeUndefined();
  expect(result.status).toBe(1);
  expect(result.stderr).toContain(reason);
};

describe("published build artifact guards", () => {
  it("rejects a public export omitted from the compiled tree", () => {
    const dir = fixture();
    const file = path.join(dir, "package.json");
    const manifest = JSON.parse(fs.readFileSync(file, "utf8"));
    manifest.exports["./missing"] = { import: "./dist/missing.js" };
    fs.writeFileSync(file, JSON.stringify(manifest));
    rejected(dir, "Missing or unpackaged public entrypoint: ./dist/missing.js");
  });
  it("rejects a missing lazy process-tree owner entry", () => {
    const dir = fixture();
    fs.rmSync(path.join(dir, "dist/residency/launcher-owner.js"));
    rejected(dir, "residency/launcher-owner.js");
  });
  it("rejects a missing lazy Bend grammar entry", () => {
    const dir = fixture();
    fs.rmSync(path.join(dir, "dist/ui/languages/bend.js"));
    rejected(dir, "ui/languages/bend.js");
  });
  it("rejects a missing generated ABI declaration", () => {
    const dir = fixture();
    fs.rmSync(path.join(dir, "dist/verified/generated/storage-kernel.d.ts"));
    rejected(dir, "storage-kernel.d.ts");
  });
  it("rejects a modified generated library in the bundle", () => {
    const dir = fixture();
    fs.appendFileSync(path.join(dir, "dist/verified/generated/storage-kernel.js"), "\n// changed\n");
    rejected(dir, "Bundled verified artifact differs");
  });
  // smarty-dev#8121: CI and release builds ship the helpers, so they must come from the pinned toolchain.
  it.runIf(process.platform === "linux")("rejects unpinned native helpers under CI or FABRIC_NATIVE_CC_PINNED=1, and only warns locally", () => {
    const dir = fixture();
    const file = path.join(dir, "dist/native/manifest.json");
    fs.writeFileSync(file, JSON.stringify({ ...JSON.parse(fs.readFileSync(file, "utf8")),
      compiler: "gcc version 15.2.0 (Ubuntu 15.2.0-16ubuntu1)", toolchain: "unpinned" }));
    const { CI: _ci, FABRIC_NATIVE_CC_PINNED: _pinned, ...local } = process.env;
    rejected(dir, "Native helpers are unpinned", { ...local, CI: "true" });
    rejected(dir, "Native helpers are unpinned", { ...local, FABRIC_NATIVE_CC_PINNED: "1" });
    // Locally it only warns and goes on (this copied tree then stops later, at its missing node_modules).
    const result = assertBuild(dir, local);
    expect(result.stderr).toContain("native helpers are UNPINNED");
    expect(result.stderr).not.toContain("Native helpers are unpinned");
  });
  it("rejects a stale bundled receipt", () => {
    const dir = fixture();
    fs.appendFileSync(path.join(dir, "dist/verified/generated/manifest.json"), "\n");
    rejected(dir, "Bundled verified artifact receipt differs from source");
  });
});
