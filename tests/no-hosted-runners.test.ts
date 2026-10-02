import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";

const guard = fileURLToPath(new URL("../scripts/ci/no-hosted-runners.sh", import.meta.url));
const temporaryDirectories: string[] = [];

// Git Bash accepts forward-slash paths on Windows as well as POSIX paths.
const bashPath = (value: string) => value.replace(/\\/g, "/");
const runGuard = (files: Record<string, string>) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-runner-guard-"));
  temporaryDirectories.push(directory);
  for (const [name, contents] of Object.entries(files)) fs.writeFileSync(path.join(directory, name), contents);
  return spawnSync("bash", [bashPath(guard), bashPath(directory)], { encoding: "utf8", timeout: 5_000 });
};

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

describe("no hosted runners guard", () => {
  it.each([
    ["pending approval", "ubuntu-latest"],
    ["unapproved", "windows-latest"],
    ["approved by the workflow author", "macos-latest"],
  ])("rejects hosted-exception markers claiming %s", (approval, runner) => {
    const result = runGuard({
      "test.yml": `jobs:\n  check:\n    # hosted-exception: ${approval}\n    runs-on: ${runner}\n`,
    });
    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(result.stderr).toContain(`test.yml:4: unapproved hosted runner:     runs-on: ${runner}`);
  });

  it.each([
    "runs-on: ubuntu-latest",
    "runs-on: 'windows-2025'",
    'runs-on: [self-hosted, "macos-15"]',
    "runner: ubuntu-24.04",
    "os: [ubuntu-latest, windows-latest]",
    "- macos-latest",
  ])("rejects hosted scalar and matrix/list values: %s", (definition) => {
    const result = runGuard({ "test.yaml": `jobs:\n  check:\n    ${definition}\n` });
    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(result.stderr).toContain("test.yaml:3: unapproved hosted runner:");
  });

  it("accepts owned runner labels without mistaking preserved check names or comments for hosted runners", () => {
    const result = runGuard({
      "test.yml": "jobs:\n  check:\n    name: check (ubuntu-latest)\n    runs-on: [self-hosted, smarty-linux-x64]\n    # ubuntu-latest and windows-latest remain required check names.\n",
      "windows.yaml": "jobs:\n  check:\n    name: check (windows-latest)\n    runs-on: [self-hosted, Windows, X64, smarty-ci-windows-x64]\n",
    });
    expect(result.error).toBeUndefined();
    expect(result.status).toBe(0);
    expect(result.stderr).toBe("");
  });

  it.each(["pending approval", "unapproved", "approved by the workflow author"])("rejects a %s exception marker even without a hosted definition", (approval) => {
    const result = runGuard({ "test.yml": `# hosted-exception: ${approval}\njobs:\n  check:\n    runs-on: smarty-linux-x64\n` });
    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(result.stderr).toContain("test.yml:1: hosted-exception marker forbidden: no hosted exceptions are approved");
  });

  it("rejects hosted definitions in any workflow even if another file is safe", () => {
    const result = runGuard({
      "owned.yml": "jobs:\n  check:\n    runs-on: smarty-linux-x64\n",
      "hosted.yaml": "jobs:\n  check:\n    # hosted-exception: pending approval\n    runs-on: windows-latest\n",
    });
    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(result.stderr).toContain("hosted.yaml:4: unapproved hosted runner:");
  });

  it("fails closed when there are no workflows", () => {
    const result = runGuard({});
    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(result.stderr).toContain("No workflows found");
  });
});
