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
const workflow = (definition: string) => `name: Probe\non: push\njobs:\n  check:\n${definition}\n    steps:\n      - run: echo probe\n`;

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

describe("no hosted runners guard", () => {
  it.each([
    ["pending approval", "ubuntu-latest"],
    ["unapproved", "windows-latest"],
    ["approved by the workflow author", "macos-latest"],
  ])("rejects hosted-exception markers claiming %s", (approval, runner) => {
    const result = runGuard({ "test.yml": workflow(`    # hosted-exception: ${approval}\n    runs-on: ${runner}`) });
    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(result.stderr).toContain("hosted-exception marker forbidden: no hosted exceptions are approved");
    expect(result.stderr).toContain(`unapproved hosted runner: ${runner}`);
  });

  it.each([
    ["plain scalar", "    runs-on: ubuntu-latest"],
    ["quoted scalar", "    runs-on: 'windows-2025'"],
    ["folded scalar", "    runs-on: >-\n      ubuntu-latest"],
    ["literal scalar", "    runs-on: |\n      windows-latest"],
    ["indented plain scalar", "    runs-on:\n      macos-latest"],
    ["flow list", '    runs-on: [self-hosted, "macos-15"]'],
    ["multiline flow list", "    runs-on: [\n      self-hosted,\n      ubuntu-latest\n    ]"],
    ["block sequence", "    runs-on:\n      - self-hosted\n      - >-\n        windows-latest"],
    ["labels object", "    runs-on:\n      group: owned\n      labels: >-\n        ubuntu-latest"],
    ["flow labels object", "    runs-on: {group: owned, labels: [self-hosted, macos-latest]}"],
    ["aliased labels", "    env: {images: &images [self-hosted, ubuntu-latest]}\n    runs-on: *images"],
    ["labels expression", "    strategy:\n      matrix:\n        image: [ubuntu-latest]\n    runs-on: {group: owned, labels: '${{ matrix.image }}'}"],
    ["sequence expression", "    strategy:\n      matrix:\n        image: [ubuntu-latest]\n    runs-on: [self-hosted, '${{ matrix.image }}']"],
    ["matrix axis expression", "    strategy:\n      matrix:\n        image: [ubuntu-latest, smarty-linux-x64]\n    runs-on: ${{ matrix.image }}"],
    ["matrix include expression", "    strategy:\n      matrix:\n        include:\n          - image: >-\n              windows-latest\n    runs-on: ${{ matrix.image }}"],
    ["matrix object expression", "    strategy:\n      matrix:\n        include:\n          - target: {group: owned, labels: [self-hosted, macos-latest]}\n    runs-on: ${{ matrix.target }}"],
    ["matrix property expression", "    strategy:\n      matrix:\n        include:\n          - target: {label: ubuntu-latest}\n    runs-on: ${{ matrix.target.label }}"],
    ["constant expression", "    runs-on: ${{ 'ubuntu-latest' }}"],
  ])("rejects hosted runner in %s", (_name, definition) => {
    const result = runGuard({ "test.yaml": workflow(definition) });
    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(result.stderr).toMatch(/test\.yaml:\d+: unapproved hosted runner:/);
  });

  it.each([
    ["conditional expression", "    runs-on: ${{ github.ref == 'refs/heads/main' && 'ubuntu-latest' || 'smarty-linux-x64' }}"],
    ["dynamic context", "    runs-on: ${{ inputs.runner }}"],
    ["dynamic fromJSON", "    runs-on: ${{ fromJSON(vars.RUNNERS) }}"],
    ["string interpolation", "    runs-on: smarty-${{ inputs.platform }}"],
    ["dynamic matrix", "    strategy:\n      matrix: ${{ fromJSON(needs.setup.outputs.matrix) }}\n    runs-on: ${{ matrix.runner }}"],
    ["undefined matrix property", "    strategy:\n      matrix:\n        include: [{runner: smarty-linux-x64}, {other: owned}]\n    runs-on: ${{ matrix.runner }}"],
    ["unknown selector key", "    runs-on: {label: ubuntu-latest}"],
    ["non-string label", "    runs-on: [self-hosted, 42]"],
    ["null runner", "    runs-on: null"],
    ["empty labels", "    runs-on: []"],
    ["empty object", "    runs-on: {}"],
    ["nested list", "    runs-on: [[self-hosted]]"],
    ["missing runs-on", "    name: missing runner"],
    ["malformed YAML", "    runs-on: [ubuntu-latest"],
    ["duplicate runs-on", "    runs-on: ubuntu-latest\n    runs-on: smarty-linux-x64"],
    ["unknown YAML tag", "    runs-on: !runner smarty-linux-x64"],
    ["YAML merge key", "    <<: {runs-on: ubuntu-latest}\n    runs-on: smarty-linux-x64"],
    ["nested list expression", "    strategy:\n      matrix:\n        image: [[self-hosted, smarty-linux-x64]]\n    runs-on: ['${{ matrix.image }}']"],
    ["recursive alias", "    runs-on: &runner [*runner]"],
    ["multiple YAML documents", "    runs-on: smarty-linux-x64\n---\njobs: {check: {runs-on: ubuntu-latest}}"],
  ])("fails closed for %s", (_name, definition) => {
    const result = runGuard({ "test.yaml": workflow(definition) });
    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(result.stderr).not.toBe("");
  });

  it.each([
    "    runs-on: [self-hosted, smarty-linux-x64]",
    "    runs-on: >-\n      smarty-linux-x64",
    "    runs-on: |\n      smarty-linux-x64",
    "    runs-on:\n      smarty-linux-x64",
    "    runs-on:\n      - self-hosted\n      - smarty-linux-x64",
    "    runs-on: {group: Smarty Windows CI, labels: [self-hosted, Windows, X64, smarty-ci-windows-x64]}",
    "    runs-on: {group: Smarty Windows CI}",
    "    env: {images: &images [self-hosted, smarty-linux-x64]}\n    runs-on: *images",
    "    strategy:\n      matrix:\n        image: [smarty-linux-x64]\n    runs-on: {group: owned, labels: '${{ matrix.image }}'}",
    "    strategy:\n      matrix:\n        image: [smarty-linux-x64]\n    runs-on: [self-hosted, '${{ matrix.image }}']",
    "    runs-on: ${{ 'smarty-linux-x64' }}",
    "    strategy:\n      matrix:\n        image: [smarty-linux-x64, smarty-ci-windows-x64]\n    runs-on: ${{ matrix.image }}",
    "    strategy:\n      matrix:\n        include:\n          - runner: [self-hosted, smarty-linux-x64]\n          - runner: [self-hosted, Windows, X64, smarty-ci-windows-x64]\n    runs-on: ${{ matrix.runner }}",
    "    strategy:\n      matrix:\n        platform: [linux, windows]\n        include:\n          - platform: linux\n            runner: smarty-linux-x64\n          - platform: windows\n            runner: smarty-ci-windows-x64\n    runs-on: ${{ matrix.runner }}",
    "    strategy:\n      matrix:\n        image: [ubuntu-latest, smarty-linux-x64]\n        exclude:\n          - image: ubuntu-latest\n    runs-on: ${{ matrix.image }}",
  ])("accepts statically owned selectors: %s", (definition) => {
    const result = runGuard({ "test.yml": workflow(`    name: check (ubuntu-latest)\n${definition}\n    # windows-latest remains a required check name.`) });
    expect(result.error).toBeUndefined();
    expect(result.status).toBe(0);
    expect(result.stderr).toBe("");
  });

  it.each(["test.yml", "entropy.yml"])("accepts actual owned-runner workflow %s", (name) => {
    const contents = fs.readFileSync(fileURLToPath(new URL(`../.github/workflows/${name}`, import.meta.url)), "utf8");
    const result = runGuard({ [name]: contents });
    expect(result.error).toBeUndefined();
    expect(result.status).toBe(0);
    expect(result.stderr).toBe("");
  });

  // Parity with the canonical factory detective rule, smarty-dev setup/factory/actions_minutes.py HOSTED
  // (smarty-dev#1246): the same runs-on labels count as hosted in both checks.
  it.each([
    ["ubuntu-24.04", 1], ["macos-15-large", 1], ["windows-2025", 1], ["ubuntu-latest", 1],
    ["smarty-linux-x64", 0], ["self.ubuntu-like", 0], ["corp-ubuntu-pool", 0],
  ] as const)("classifies runs-on %s like the canonical detective check", (label, status) => {
    const result = runGuard({ "test.yml": workflow(`    runs-on: ${label}`) });
    expect(result.error).toBeUndefined();
    expect(result.status).toBe(status);
  });

  it("accepts reusable workflow jobs that do not select a runner", () => {
    const result = runGuard({ "test.yml": "jobs:\n  reuse:\n    uses: ./.github/workflows/test.yml\n" });
    expect(result.status).toBe(0);
  });

  it.each(["pending approval", "unapproved", "approved by the workflow author"])("rejects a %s exception marker even without a hosted definition", (approval) => {
    const result = runGuard({ "test.yml": workflow(`    # hosted-exception: ${approval}\n    runs-on: smarty-linux-x64`) });
    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(result.stderr).toContain("hosted-exception marker forbidden: no hosted exceptions are approved");
  });

  it("rejects hosted definitions in any workflow even if another file is safe", () => {
    const result = runGuard({
      "owned.yml": workflow("    runs-on: smarty-linux-x64"),
      "hosted.yaml": workflow("    runs-on: >-\n      windows-latest"),
    });
    expect(result.status).toBe(1);
    expect(result.stderr).toMatch(/hosted\.yaml:\d+: unapproved hosted runner:/);
  });

  it.each(["", "jobs: {}\n", "jobs: []\n", "jobs: {check: owned}\n"])("fails closed for invalid workflow structure %j", (contents) => {
    const result = runGuard({ "test.yml": contents });
    expect(result.status).toBe(1);
    expect(result.stderr).not.toBe("");
  });

  it("fails closed when there are no workflows", () => {
    const result = runGuard({});
    expect(result.error).toBeUndefined();
    expect(result.status).toBe(1);
    expect(result.stderr).toContain("No workflows found");
  });
});
