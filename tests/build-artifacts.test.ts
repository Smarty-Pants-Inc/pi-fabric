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
const rejected = (dir: string, reason: string): void => {
  const result = spawnSync(process.execPath, [path.join(dir, "scripts/assert-build-artifacts.mjs")], { encoding: "utf8", timeout: 20_000 });
  expect(result.error).toBeUndefined();
  expect(result.status).toBe(1);
  expect(result.stderr).toContain(reason);
};

describe("published build artifact guards", () => {
  it("uses host-only wildcard peers and exact Pi 1.0 development dependencies", () => {
    const manifest = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));
    for (const name of ["@earendil-works/pi-ai", "@earendil-works/pi-agent-core", "@earendil-works/pi-coding-agent", "@earendil-works/pi-tui", "typebox"]) {
      expect(manifest.dependencies[name]).toBeUndefined();
      expect(manifest.peerDependencies[name]).toBe("*");
      expect(manifest.devDependencies[name]).toBe(name === "typebox" ? "1.3.27" : "1.0.0");
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
  it("checks optional dependencies in the manifest's actual extension entry", () => {
    const dir = fixture();
    const manifest = JSON.parse(fs.readFileSync(path.join(dir, "package.json"), "utf8"));
    expect(manifest.pi.extensions).toEqual(["./dist/extension-bootstrap.js"]);
    fs.appendFileSync(path.join(dir, manifest.pi.extensions[0]), '\nimport "yaml";\n');
    rejected(dir, "Startup eagerly imports optional dependency yaml");
  });
  it("rejects a missing standalone worker validator", () => {
    const dir = fixture();
    fs.rmSync(path.join(dir, "dist/worker/result.js"));
    rejected(dir, "worker/result.js");
  });
  it("rejects an omitted private worker first-use entry", () => {
    const dir = fixture();
    fs.rmSync(path.join(dir, "dist/worker/tool-call-stream-guard.js"));
    rejected(dir, "Missing worker first-use entry: ./worker/tool-call-stream-guard.js");
  });
  it("rejects external dependencies in the worker bootstrap", () => {
    const dir = fixture();
    fs.appendFileSync(path.join(dir, "dist/worker.js"), '\nimport "typebox/value";\n');
    rejected(dir, "Worker bootstrap imports an external package: typebox/value");
  });
  it("rejects a validator that relies on host modules", () => {
    const dir = fixture();
    fs.appendFileSync(path.join(dir, "dist/worker/result.js"), '\nimport "typebox/value";\n');
    rejected(dir, "Worker validator must be self-contained");
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
  it("rejects a missing stable image-overlay entry", () => {
    const dir = fixture();
    fs.rmSync(path.join(dir, "dist/ui/image-overlays.js"));
    rejected(dir, "ui/image-overlays.js");
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
  it("rejects a stale bundled receipt", () => {
    const dir = fixture();
    fs.appendFileSync(path.join(dir, "dist/verified/generated/manifest.json"), "\n");
    rejected(dir, "Bundled verified artifact receipt differs from source");
  });
});
