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
  for (const file of ["package.json", "scripts/assert-build-artifacts.mjs", "src/verified/generated/manifest.json"]) {
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
  it("rejects a public export omitted from the compiled tree", () => {
    const dir = fixture();
    const file = path.join(dir, "package.json");
    const manifest = JSON.parse(fs.readFileSync(file, "utf8"));
    manifest.exports["./missing"] = { import: "./dist/missing.js" };
    fs.writeFileSync(file, JSON.stringify(manifest));
    rejected(dir, "Missing or unpackaged public entrypoint: ./dist/missing.js");
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
