import { execFileSync, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { assertMainProofCaller, assertWorkHostRoutes, linkProofModels } from "../scripts/lib/native-placement-proof.mjs";

const aliases = { ryzen2: "ryzen2-agent", ryzen3: "forge-agent", ryzen4: "ryzen4-agent", ryzen5: "ryzen5-agent" };
const roots: string[] = [];
const root = () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "native-placement-offline-"));
  roots.push(directory);
  return directory;
};
afterEach(() => {
  vi.restoreAllMocks();
  for (const directory of roots.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

describe("native placement proof caller guard (offline)", () => {
  it.each(["dev1.smartypants.ai", "dev1", "ryzen1", "ryzen1.smartypants.ai"])("accepts approved Main %s", hostname => {
    expect(() => assertMainProofCaller(hostname, aliases)).not.toThrow();
  });
  it.each(["ryzen2", "ryzen3", "forge", "ryzen4", "ryzen5", ...Object.values(aliases)])("rejects work-host route %s", hostname => {
    expect(() => assertMainProofCaller(hostname, aliases)).toThrow();
    expect(() => assertMainProofCaller(`${hostname}.smartypants.ai`, aliases)).toThrow();
  });
  it.each(["other", "dev1.unapproved.example", "ryzen1.unapproved.example"])("rejects unapproved caller %s", hostname => {
    expect(() => assertMainProofCaller(hostname, aliases)).toThrow();
  });
  it.each(["dev1", "ryzen1", "dev1.smartypants.ai", "ryzen1.smartypants.ai", "dev1-agent", "ryzen1-agent"])("rejects Main route %s as a work-host key or alias", route => {
    for (const map of [{ ...aliases, [route]: "work-agent" }, { ...aliases, ryzen2: route }]) {
      expect(() => assertWorkHostRoutes(map)).toThrow("must not appear as a work-host route");
      expect(() => assertMainProofCaller("dev1.smartypants.ai", map)).toThrow("must not appear as a work-host route");
      expect(() => assertMainProofCaller("ryzen1", map)).toThrow("must not appear as a work-host route");
    }
  });
});

describe("isolated proof model providers (offline fake metadata only)", () => {
  it("is opt-in, symlinks without reading/copying, and fails closed for a missing file", () => {
    const directory = root();
    const profile = path.join(directory, "isolated");
    const hostProfile = path.join(directory, "host");
    fs.mkdirSync(profile); fs.mkdirSync(hostProfile);
    expect(linkProofModels(profile, hostProfile, false)).toBeNull();
    expect(() => linkProofModels(profile, hostProfile, true)).toThrow("requires a host profile models.json");
    const source = path.join(hostProfile, "models.json");
    fs.writeFileSync(source, '{"publicFixture":"not-real-provider-data"}');
    const read = vi.spyOn(fs, "readFileSync");
    const copy = vi.spyOn(fs, "copyFileSync");
    const destination = linkProofModels(profile, hostProfile, true)!;
    expect(fs.lstatSync(destination).isSymbolicLink()).toBe(true);
    expect(fs.readlinkSync(destination)).toBe(source);
    expect(read).not.toHaveBeenCalled(); expect(copy).not.toHaveBeenCalled();
    fs.rmSync(profile, { recursive: true });
    expect(fs.existsSync(source)).toBe(true);
  });

  it.each(["linked", "linked-error", "default"])("keeps model filename/contents out of actual proof evidence and argv (%s)", mode => {
    // Execute the real proof script against an inert SDK fixture. No launcher,
    // inference, credential store, SSH or real host models file is accessed.
    const directory = root();
    const cwd = path.join(directory, "repo");
    const sdk = path.join(directory, "sdk");
    const hostProfile = path.join(directory, "host-profile");
    const output = path.join(directory, "evidence");
    const temp = path.join(directory, "temp");
    for (const folder of [path.join(cwd, "dist"), path.join(sdk, "dist/core"), hostProfile, temp]) fs.mkdirSync(folder, { recursive: true });
    fs.writeFileSync(path.join(cwd, "dist/index.js"), "// inert extension fixture\n");
    execFileSync("git", ["init", "--quiet", cwd]);
    execFileSync("git", ["-C", cwd, "-c", "user.name=Offline Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--quiet", "--allow-empty", "-m", "offline fixture"]);
    const modelContents = '{"publicFixture":"UNIQUE_FAKE_MODELS_SENTINEL_499"}';
    const hostModels = path.join(hostProfile, "models.json");
    fs.writeFileSync(hostModels, modelContents);
    fs.writeFileSync(path.join(sdk, "package.json"), '{"type":"module"}');
    fs.writeFileSync(path.join(sdk, "dist/core/auth-storage.js"), "export const AuthStorage = { inMemory: () => ({}) };\n");
    fs.writeFileSync(path.join(sdk, "dist/index.js"), `
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
export const ModelRuntime = { create: async options => {
  assert.equal(options.allowModelNetwork, false);
  assert.equal(options.refreshOnCreate, false);
  if (process.env.PROOF_MODELS_FROM_PROFILE === "1") {
    assert.equal(options.modelsPath, path.join(process.env.PI_CODING_AGENT_DIR, "models.json"));
    assert(fs.lstatSync(options.modelsPath).isSymbolicLink());
    assert.equal(fs.readlinkSync(options.modelsPath), path.join(process.env.FIXTURE_HOST_PROFILE, "models.json"));
    const fixture = fs.readFileSync(options.modelsPath, "utf8");
    assert.equal(fixture, ${JSON.stringify(modelContents)});
    if (process.env.FIXTURE_MODEL_ERROR === "1") throw new Error(fixture);
  } else assert.equal(options.modelsPath, null);
  return { registerProvider() { assert.equal(options.modelsPath, null); }, getModel: () => ({}) };
} };
export const SettingsManager = { inMemory: () => ({}) };
export const SessionManager = { inMemory: () => ({}) };
export class DefaultResourceLoader {
  async reload() {}
  getExtensions() { return { errors: [] }; }
}
export async function createAgentSession() {
  return { session: {
    bindExtensions: async () => {}, dispose() {}, extensionRunner: { emit: async () => {} },
    agent: { state: { tools: [{ name: "fabric_exec", execute: async () => {
      const run = process.env.PI_FABRIC_RUNS_ROOT;
      fs.mkdirSync(run, { recursive: true });
      fs.writeFileSync(path.join(run, "placement.json"), JSON.stringify({ id: "offline-id", output: "RYZEN2_TASK_ACCEPTED offline-id on ryzen2" }));
      fs.writeFileSync(path.join(run, "events.jsonl"), '{"type":"placement.remote"}\\n{"type":"placement.result"}\\n');
      // Sensitive-named files in scratch must NOT be swept into evidence.
      fs.writeFileSync(path.join(run, "models.json"), ${JSON.stringify(modelContents)});
      return { content: [{ type: "text", text: JSON.stringify({ handle: { id: "offline-id" }, result: { status: "completed", exitCode: 0, text: "ryzen2.smartypants.ai" } }) }] };
    } }] } }
  } };
}
`);
    const preload = path.join(directory, "hostname.mjs");
    fs.writeFileSync(preload, 'import os from "node:os"; os.hostname = () => "dev1.smartypants.ai";\n');
    const map = path.join(directory, "work-hosts.json");
    fs.writeFileSync(map, JSON.stringify(aliases));
    const probe = path.resolve("scripts/probe-native-process-placement.mjs");
    const result = spawnSync(process.execPath, ["--import", preload, probe, sdk, output, process.execPath, "ryzen2", map, "ssh"], {
      cwd, encoding: "utf8", timeout: 20_000,
      env: { ...process.env, TMPDIR: temp, TMP: temp, TEMP: temp, PI_CODING_AGENT_DIR: hostProfile,
        PROOF_MODELS_FROM_PROFILE: mode === "default" ? "0" : "1", FIXTURE_HOST_PROFILE: hostProfile,
        FIXTURE_MODEL_ERROR: mode === "linked-error" ? "1" : "0" },
    });
    expect(result.error).toBeUndefined();
    expect(result.status, result.stderr).toBe(mode === "linked-error" ? 1 : 0);
    const evidence = JSON.parse(fs.readFileSync(path.join(output, "evidence.json"), "utf8"));
    expect(evidence.passed).toBe(mode !== "linked-error");
    expect(fs.existsSync(evidence.isolatedAgentDir)).toBe(false);
    const scan = (folder: string): string => fs.readdirSync(folder, { withFileTypes: true }).map(entry => {
      expect(entry.name).not.toBe("models.json");
      const file = path.join(folder, entry.name);
      return entry.isDirectory() ? scan(file) : fs.readFileSync(file, "utf8");
    }).join("\n");
    const saved = scan(output);
    for (const text of [saved, result.stdout, result.stderr]) {
      expect(text).not.toContain(modelContents);
      expect(text).not.toContain("UNIQUE_FAKE_MODELS_SENTINEL_499");
      expect(text).not.toContain(hostModels);
    }
    expect(fs.readFileSync(hostModels, "utf8")).toBe(modelContents);
    expect(fs.readdirSync(temp)).toEqual([]);
  });
});
