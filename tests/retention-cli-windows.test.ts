import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";

// smarty-dev#7858 (Windows proof): the real retention CLI refuses --apply as policy, before it reads the mesh
// (pi-fabric#1014, until smarty-dev#10678). Windows only: Linux covers the CLI in tests/retention-cli.test.ts.
const repo = fileURLToPath(new URL("..", import.meta.url));
const cli = path.join(repo, "src", "storage", "retention-cli.ts");
// The same Jiti child bootstrap as tests/retention-cli.test.ts: the real CLI entry, not a mock.
const runner = `const { createJiti } = require("jiti"); const { pathToFileURL } = require("node:url");
process.argv = [process.argv[0], ${JSON.stringify(cli)}, ...process.argv.slice(1)];
createJiti(pathToFileURL(process.cwd() + "/index.js").href).import(${JSON.stringify(cli)}).catch(error => { console.error(error); process.exitCode = 1; });`;
const DISABLED = "retention-cli: --apply is disabled until smarty-dev#10678 (owner-attributed callback fencing) lands; nothing was changed. Use --dry-run.";

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const snapshot = (root: string): unknown[] => fs.readdirSync(root).sort().map(name => {
  const file = path.join(root, name); const stat = fs.lstatSync(file);
  return [name, stat.size, stat.mtimeMs, stat.isDirectory() ? snapshot(file) : fs.readFileSync(file, "utf8")];
});

describe.skipIf(process.platform !== "win32")("retention CLI --apply on Windows (smarty-dev#7858)", () => {
  // Each mesh root is unusable: had the CLI read it, the sweep would fail with "Mesh root must be an owned
  // directory" (exit 1) instead of the policy refusal (exit 2).
  it.each([
    ["a regular guard file", "mesh-guard"],
    ["a missing path", path.join("absent", "mesh")],
  ])("refuses before reading the mesh when the mesh root is %s, and creates no report", (_kind, relative) => {
    const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-retention-win-")); roots.push(fixture);
    const guard = path.join(fixture, "mesh-guard");
    fs.writeFileSync(guard, "not a mesh: the CLI must not read this\n");
    fs.writeFileSync(path.join(fixture, "sentinel.txt"), "keep");
    const meshRoot = path.join(fixture, relative);
    const report = path.join(fixture, "report.json");
    const before = snapshot(fixture);

    const result = spawnSync(process.execPath, ["-e", runner, meshRoot, "--apply", "--runs-older-than", String(7 * 86400000), "--report", report],
      { cwd: repo, encoding: "utf8", timeout: 60_000 });

    expect(result.error).toBeUndefined();
    expect(result.status, result.stderr).toBe(2);
    expect(result.stderr.trim()).toBe(DISABLED);
    expect(result.stdout).toBe("");
    // A policy refusal, not a filesystem or usage error from touching the mesh.
    expect(result.stderr).not.toMatch(/Mesh root must be|ENOENT|EACCES|EPERM|EBUSY|Usage:|Unsafe/);
    // Nothing created or changed: no report, no report temp file, the fixture byte-for-byte as before.
    expect(fs.existsSync(report)).toBe(false);
    expect(fs.readdirSync(fixture).filter(name => name.includes("report"))).toEqual([]);
    expect(snapshot(fixture)).toEqual(before);
    expect(fs.readFileSync(guard, "utf8")).toBe("not a mesh: the CLI must not read this\n");
    expect(fs.existsSync(fixture)).toBe(true);
  }, 60_000);
});
