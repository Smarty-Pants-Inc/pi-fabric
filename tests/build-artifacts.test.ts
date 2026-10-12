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
  it("uses host-only wildcard peers and exact Pi 1.0 development dependencies", () => {
    const manifest = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));
    expect(manifest.dependencies["pi-fabric-worker-sdk"]).toBe(`npm:@earendil-works/pi-coding-agent@${manifest.devDependencies["@earendil-works/pi-coding-agent"]}`);
    for (const name of ["@earendil-works/pi-ai", "@earendil-works/pi-agent-core", "@earendil-works/pi-coding-agent", "@earendil-works/pi-tui", "typebox"]) {
      expect(manifest.dependencies[name]).toBeUndefined();
      expect(manifest.peerDependencies[name]).toBe("*");
      expect(manifest.devDependencies[name]).toBe(name === "typebox" ? "1.3.27" : "1.1.0");
      expect(manifest.overrides?.[name]).toBeUndefined();
      expect(manifest.resolutions?.[name]).toBeUndefined();
    }
  });
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
  it.each(["yaml", "pi-fabric-worker-sdk"])("rejects eager %s in the actual extension entry", (dependency) => {
    const dir = fixture();
    const manifest = JSON.parse(fs.readFileSync(path.join(dir, "package.json"), "utf8"));
    expect(manifest.pi.extensions).toEqual(["./dist/index.js"]);
    fs.appendFileSync(path.join(dir, manifest.pi.extensions[0]), `\nimport "${dependency}";\n`);
    rejected(dir, `Startup eagerly imports optional dependency ${dependency}`);
  });
  it("rejects a missing standalone worker validator", () => {
    const dir = fixture();
    fs.rmSync(path.join(dir, "dist/worker/result.js"));
    rejected(dir, "worker/result.js");
  });
  it.each(["worker.js", "durable/worker.js"])("rejects external dependencies in %s bootstrap", (entry) => {
    const dir = fixture();
    fs.appendFileSync(path.join(dir, "dist", entry), '\nimport "typebox/value";\n');
    rejected(dir, "Worker bootstrap imports an external package: typebox/value");
  });
  it("rejects a validator that relies on host modules", () => {
    const dir = fixture();
    fs.appendFileSync(path.join(dir, "dist/worker/result.js"), '\nimport "typebox/value";\n');
    rejected(dir, "Worker validator must be self-contained");
  });
  it("rejects a missing lazy Bend grammar entry", () => {
    const dir = fixture();
    fs.rmSync(path.join(dir, "dist/ui/languages/bend.js"));
    rejected(dir, "ui/languages/bend.js");
  });
  it("rejects a missing stable image-overlay entry", () => {
    const dir = fixture();
    fs.rmSync(path.join(dir, "dist/ui/image-overlays.js"));
    rejected(dir, "ui/image-overlays.js");
  });
  it.each(["native-discovery.js", "memory/extractive-history.js", "durable/worker.js", "durable/worker-host.js"])("requires stable optional entry %s", (entry) => {
    const dir = fixture();
    fs.rmSync(path.join(dir, "dist", entry));
    rejected(dir, entry);
  });
  it("rejects an eager extractive engine implementation", () => {
    const dir = fixture();
    // The entry Pi loads (package.json pi.extensions), not a fixed file (pi-fabric#982).
    const manifest = JSON.parse(fs.readFileSync(path.join(dir, "package.json"), "utf8"));
    fs.appendFileSync(path.join(dir, manifest.pi.extensions[0]), "\n// src/memory/extractive-index.ts\n");
    rejected(dir, "Startup static graph contains lazy module marker: src/memory/extractive-index.ts");
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
  // smarty-dev#8145: the lease probe trusts dist/ only when no other user can write any of it.
  it.skipIf(process.platform !== "linux")("rejects a group- or other-writable dist/, or a symlink inside it", () => {
    const dir = fixture();
    fs.chmodSync(path.join(dir, "dist"), 0o775);
    rejected(dir, "dist must be owned by uid");
    fs.chmodSync(path.join(dir, "dist"), 0o755);
    fs.chmodSync(path.join(dir, "dist/mesh/mesh-backend-cli.js"), 0o646);
    rejected(dir, "dist/mesh/mesh-backend-cli.js must be owned by uid");
    fs.chmodSync(path.join(dir, "dist/mesh/mesh-backend-cli.js"), 0o644);
    fs.symlinkSync(os.tmpdir(), path.join(dir, "dist/native/elsewhere"));
    rejected(dir, "dist/native/elsewhere must not be a symlink");
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
