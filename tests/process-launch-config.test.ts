import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  DEFAULT_FABRIC_CONFIG,
  loadFabricConfig,
  loadFabricConfigForScope,
  loadGlobalFabricConfig,
  normalizeFabricConfig,
  type FabricAgentConfig,
  type FabricProcessLaunchConfig,
} from "../src/config.js";
import { CURRENT_FABRIC_CONFIG_VERSION } from "../src/config-migrations.js";

// Keep reason: real config-load trust boundary for extension execution and child env injection.
const roots: string[] = [];
const location = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-process-launch-config-"));
  roots.push(root);
  const cwd = path.join(root, "project");
  const agentDir = path.join(root, "selected-agent-profile");
  fs.mkdirSync(path.join(cwd, ".pi"), { recursive: true });
  fs.mkdirSync(agentDir);
  return { cwd, agentDir, projectTrusted: true };
};
const writeConfig = (file: string, agents: Record<string, unknown>): string => {
  const source = JSON.stringify({ configVersion: CURRENT_FABRIC_CONFIG_VERSION, agents });
  fs.writeFileSync(file, source);
  return source;
};
type Location = ReturnType<typeof location>;
const loaders = (options: Location): Array<() => ReturnType<typeof loadFabricConfig>> => [
  () => loadFabricConfig(options),
  () => loadGlobalFabricConfig(options.agentDir),
  () => loadFabricConfigForScope(options, "global"),
  ...(options.projectTrusted ? [() => loadFabricConfigForScope(options, "project")] : []),
];

beforeEach(() => {
  vi.stubEnv("PI_FABRIC_KERNEL", undefined);
  vi.stubEnv("PI_FABRIC_PYTHON_RUNTIME", undefined);
  vi.stubEnv("PI_FABRIC_THINKING_BOUNDS", undefined);
  vi.stubEnv("PI_FABRIC_COMPACTION_ENGINE", undefined);
});
afterEach(() => {
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("host-only agents.process launch configuration", () => {
  it("leaves absent policy omitted from defaults, normalization and every disk loader", () => {
    const options = location();
    const agents = [DEFAULT_FABRIC_CONFIG.agents, normalizeFabricConfig({}).agents,
      ...loaders(options).map(load => load().agents)];
    for (const config of agents) expect(Object.hasOwn(config, "process")).toBe(false);
    // Loading an unconfigured profile creates neither a config nor a launch/session store.
    expect(fs.readdirSync(options.agentDir)).toEqual([]);
    expect(fs.readdirSync(path.join(options.cwd, ".pi"))).toEqual([]);
  });

  it("accepts the selected host profile without root policy or launch-time expansion", () => {
    const options = location();
    vi.stubEnv("PROCESS_CONFIG_TEST_SOURCE", "parent-value-must-not-be-expanded");
    vi.stubEnv("PROCESS_CONFIG_TEST_MISSING", undefined);
    const processConfig: FabricProcessLaunchConfig = {
      extensions: [path.join(options.agentDir, "first extension.ts"), path.join(options.agentDir, "second.ts")],
      env: {
        LITERAL: "literal text, $HOME and braces {stay literal}",
        EMPTY: "",
        TEMPLATE: "prefix/${env:PROCESS_CONFIG_TEST_SOURCE}/${env:PROCESS_CONFIG_TEST_MISSING}",
        ADJACENT: "${env:PROCESS_CONFIG_TEST_SOURCE}${env:PROCESS_CONFIG_TEST_SOURCE}",
        SEPARATED: "$${env:PROCESS_CONFIG_TEST_SOURCE}{",
        _PORTABLE_2: "line one\nline two",
      },
    };
    const typedAgents: Pick<FabricAgentConfig, "process"> = { process: processConfig };
    const file = path.join(options.agentDir, "fabric.json");
    const source = writeConfig(file, typedAgents);
    for (const load of loaders(options)) expect(load().agents.process).toEqual(processConfig);
    expect(fs.readFileSync(file, "utf8")).toBe(source);
    expect(fs.readdirSync(options.agentDir)).toEqual(["fabric.json"]);
    expect(fs.readdirSync(path.join(options.cwd, ".pi"))).toEqual([]);
  });

  it.each([
    {},
    { extensions: [] },
    { env: {} },
    { extensions: [], env: {} },
    { env: { ONLY_ENV: "value" } },
  ])("preserves optional/empty launch fields without adding defaults: %j", processConfig => {
    const options = location();
    writeConfig(path.join(options.agentDir, "fabric.json"), { process: processConfig });
    expect(loadFabricConfig(options).agents.process).toEqual(processConfig);
  });

  it.each([true, false])("project cannot replace, clear or merge host policy (trusted=%s)", projectTrusted => {
    const options = { ...location(), projectTrusted };
    const host: FabricProcessLaunchConfig = {
      extensions: [path.join(options.agentDir, "host.ts")],
      env: { HOST: "host", SHARED: "host-wins" },
    };
    writeConfig(path.join(options.agentDir, "fabric.json"), { process: host });
    for (const processConfig of [
      { extensions: [path.join(options.cwd, "workspace.ts")], env: { SHARED: "project", EXTRA: "injected" } },
      { extensions: [], env: {} },
      { env: { EXTRA: "injected" } },
      null,
      { extensions: ["relative.ts"], env: { BAD: 123 } },
    ]) {
      writeConfig(path.join(options.cwd, ".pi", "fabric.json"), { process: processConfig, model: "provider/project" });
      for (const load of loaders(options)) expect(load().agents.process).toEqual(host);
      // A trusted project's ordinary settings still merge; only the policy field is stripped.
      expect(loadFabricConfig(options).agents.model).toBe(projectTrusted ? "provider/project" : undefined);
    }
  });

  it.each([true, false])("project cannot enable launch policy without a host opt-in (trusted=%s)", projectTrusted => {
    const options = { ...location(), projectTrusted };
    writeConfig(path.join(options.cwd, ".pi", "fabric.json"), {
      process: { extensions: [path.join(options.cwd, "injected.ts")], env: { INJECTED: "yes" } },
    });
    for (const load of loaders(options)) expect(Object.hasOwn(load().agents, "process")).toBe(false);
  });

  it("does not borrow process policy from another agent directory", () => {
    const options = location();
    writeConfig(path.join(options.agentDir, "fabric.json"), { process: { env: { HOST: "selected-only" } } });
    const alternate = location();
    expect(loadFabricConfig({ ...options, agentDir: alternate.agentDir }).agents.process).toBeUndefined();
    expect(loadFabricConfig(options).agents.process?.env).toEqual({ HOST: "selected-only" });
  });

  const invalid: Array<[string, unknown, string]> = [
    ["null policy", null, "agents.process must be an object"],
    ["array policy", [], "agents.process must be an object"],
    ["string policy", "launch", "agents.process must be an object"],
    ["boolean policy", false, "agents.process must be an object"],
    ["null extensions", { extensions: null }, "agents.process.extensions"],
    ["non-array extensions", { extensions: "absolute.ts" }, "agents.process.extensions"],
    ...["relative.ts", "./extension.ts", "../extension.ts", "~/extension.ts", "", null, 123, false,
      `${path.parse(process.cwd()).root}bad\0extension.ts`]
      .map((entry): [string, unknown, string] => ["invalid extension entry", { extensions: [entry] }, "agents.process.extensions[0]"]),
    ...[null, [], "not-an-object", false]
      .map((env): [string, unknown, string] => ["invalid env map", { env }, "agents.process.env must be an object"]),
    ...[null, 12, true, [], { secret: "do-not-print-value" }]
      .map((VALUE): [string, unknown, string] => ["non-string env value", { env: { VALUE } }, "agents.process.env.VALUE must be a string"]),
    ["NUL env value", { env: { VALUE: "do-not-print-value\0" } }, "agents.process.env.VALUE must not contain NUL"],
    ...["", "1BAD", "BAD=NAME", "BAD-NAME", "BAD.NAME", "BAD NAME", "NUL\0NAME", "NAME\n", "NAME\r", "NÁME"]
      .map((name): [string, unknown, string] => ["invalid env name", { env: { [name]: "value" } }, "agents.process.env keys"]),
    ...["${env:}", "${env:1NAME}", "${env:BAD-NAME}", "${env:NAME", "${env:NAME:-fallback}",
      "${ENV:NAME}", "${NAME}", "${env:NAME\n}", "${", "${env:${env:NAME}}", "${env:GOOD} ${broken}"]
      .map((VALUE): [string, unknown, string] => ["malformed env template", { env: { VALUE } }, "agents.process.env.VALUE has an invalid template"]),
  ];
  it.each(invalid)("fails closed at config load: %s (%j)", (_label, processConfig, message) => {
    const options = location();
    const file = path.join(options.agentDir, "fabric.json");
    const source = writeConfig(file, { process: processConfig });
    // None of the public loaders may silently drop malformed host execution policy.
    for (const load of loaders(options)) expect(load).toThrow(message);
    expect(fs.readFileSync(file, "utf8")).toBe(source);
  });

  it("reports the offending extension index without discarding valid entries", () => {
    const options = location();
    writeConfig(path.join(options.agentDir, "fabric.json"), {
      process: { extensions: [path.join(options.agentDir, "valid.ts"), "invalid.ts"] },
    });
    expect(() => loadFabricConfig(options)).toThrow("agents.process.extensions[1]");
  });

  it("preserves prototype-like valid env names as own string-valued properties", () => {
    const options = location();
    const env = Object.fromEntries([["__proto__", "literal"], ["constructor", "literal"], ["prototype", "literal"]]);
    writeConfig(path.join(options.agentDir, "fabric.json"), { process: { env } });
    const loaded = loadFabricConfig(options).agents.process!.env!;
    expect(Object.keys(loaded).sort()).toEqual(["__proto__", "constructor", "prototype"]);
    expect(loaded).toEqual(env);
    expect(Object.getPrototypeOf(loaded)).toBe(Object.prototype);
    expect(Object.hasOwn(Object.prototype, "literal")).toBe(false);
  });

  it("never includes env values in validation errors", () => {
    const options = location();
    writeConfig(path.join(options.agentDir, "fabric.json"), {
      process: { env: { VALUE: "do-not-print-value ${invalid}" } },
    });
    let error: unknown;
    try { loadFabricConfig(options); } catch (caught) { error = caught; }
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toContain("agents.process.env.VALUE");
    expect((error as Error).message).not.toContain("do-not-print-value");
  });
});
