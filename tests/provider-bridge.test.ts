import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { ModelRuntime } from "@earendil-works/pi-coding-agent";
import { afterAll, afterEach, describe, expect, it, vi } from "vitest";
import fabricProviderBridge, {
  parseModelKey,
  ProviderNotFoundError,
  PROVIDER_BRIDGE_MODEL_ENV,
  resolveExtensionProviders,
} from "../src/agents/provider-bridge.js";

const roots: string[] = [];
afterAll(() => {
  for (const root of roots) fs.rmSync(root, { recursive: true, force: true });
});
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); });

/** An extension that records its own load and registers one config provider. */
const extensionSource = (marker: string, provider?: string) => `
import fs from "node:fs";
fs.appendFileSync(${JSON.stringify(marker + ".module")}, "evaluated\\n");
export default function (pi) {
  fs.appendFileSync(${JSON.stringify(marker)}, "loaded\\n");
  pi.registerTool({ name: "leaked_tool", label: "Leaked", description: "must not bind", parameters: { type: "object", properties: {} },
    async execute() { return { content: [{ type: "text", text: "leaked" }], details: {} }; } });
  pi.registerCommand("leaked-command", { description: "must not bind", handler: async () => {} });
  pi.on("session_start", () => fs.appendFileSync(${JSON.stringify(marker)}, "session_start\\n"));
  ${provider ? `pi.registerProvider(${JSON.stringify(provider)}, {
    baseUrl: "https://${provider}.invalid/v1", apiKey: "$FIXTURE_PROVIDER_KEY", api: "openai-completions",
    models: [{ id: "m1", name: "M1", reasoning: false, input: ["text"],
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 8192, maxTokens: 1024 }],
  });` : ""}
}
`;

function fixture(options: {
  extensions: Array<{ dir: string; provider?: string; project?: boolean }>;
  settings?: Record<string, unknown>;
  models?: Record<string, unknown>;
}) {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-provider-bridge-")));
  roots.push(root);
  const cwd = path.join(root, "project");
  const agentDir = path.join(root, "agent");
  fs.mkdirSync(cwd, { recursive: true });
  fs.mkdirSync(agentDir, { recursive: true });
  const markers: Record<string, string> = {};
  const globalPaths: string[] = [];
  for (const extension of options.extensions) {
    const base = extension.project ? path.join(cwd, ".pi", "extensions") : path.join(root, "packages");
    const file = path.join(base, extension.dir, "index.ts");
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const marker = path.join(root, `${extension.dir}.marker`);
    markers[extension.dir] = marker;
    fs.writeFileSync(file, extensionSource(marker, extension.provider));
    if (!extension.project) globalPaths.push(file);
  }
  fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ extensions: globalPaths, ...options.settings }));
  if (options.models) fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify(options.models));
  const loads = (dir: string) => fs.existsSync(markers[dir]!) ? fs.readFileSync(markers[dir]!, "utf8").trim().split("\n") : [];
  const moduleLoaded = (dir: string) => fs.existsSync(markers[dir]! + ".module");
  return { root, cwd, agentDir, markers, loads, moduleLoaded };
}

describe("extension provider bridge", () => {
  it("parses canonical model keys at the first slash", () => {
    expect(parseModelKey("coralbricks/glm-5.3-fast")).toEqual({ provider: "coralbricks", id: "glm-5.3-fast" });
    expect(parseModelKey("openrouter/anthropic/claude")).toEqual({ provider: "openrouter", id: "anthropic/claude" });
    expect(parseModelKey("bare")).toBeUndefined();
    expect(parseModelKey("/id")).toBeUndefined();
    expect(parseModelKey("provider/")).toBeUndefined();
  });

  it("does not infer registration authority from an extension's provider-matching path", async () => {
    const { cwd, agentDir, loads, moduleLoaded } = fixture({ extensions: [
      { dir: "pi-unrelated" },
      { dir: "pi-acme-provider", provider: "acme" },
    ] });
    await expect(resolveExtensionProviders({ model: "acme/m1", cwd, agentDir })).rejects.toMatchObject({
      name: "ProviderNotFoundError", code: "PROVIDER_NOT_FOUND", model: "acme/m1", provider: "acme",
    });
    for (const dir of ["pi-acme-provider", "pi-unrelated"]) {
      expect(loads(dir)).toEqual([]);
      expect(moduleLoaded(dir)).toBe(false);
    }
  });

  it("does not execute provider-named misses or unrelated extensions when extensions are disabled", async () => {
    const { cwd, agentDir, loads, moduleLoaded } = fixture({ extensions: [
      { dir: "pi-zeta-provider", provider: "wrong" },
      { dir: "pi-unrelated" },
      { dir: "pi-misc-tools", provider: "zeta" },
    ] });
    await expect(resolveExtensionProviders({ model: "zeta/m1", cwd, agentDir })).rejects.toBeInstanceOf(ProviderNotFoundError);
    for (const dir of ["pi-zeta-provider", "pi-unrelated", "pi-misc-tools"]) {
      expect(loads(dir)).toEqual([]);
      expect(moduleLoaded(dir)).toBe(false);
    }
  });

  it("leaves models Pi already knows untouched and loads no extension", async () => {
    const builtin = (await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false })).getModels()[0]!;
    const { cwd, agentDir, loads } = fixture({
      extensions: [{ dir: `pi-${builtin.provider}-override`, provider: builtin.provider }, { dir: "pi-local-provider", provider: "local" }],
      models: { providers: { local: { baseUrl: "http://127.0.0.1:1/v1", apiKey: "none", api: "openai-completions",
        models: [{ id: "m1", name: "Local", reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 8192, maxTokens: 1024 }] } } },
    });
    await expect(resolveExtensionProviders({ model: `${builtin.provider}/${builtin.id}`, cwd, agentDir })).resolves.toBeUndefined();
    await expect(resolveExtensionProviders({ model: "local/m1", cwd, agentDir })).resolves.toBeUndefined();
    expect(loads(`pi-${builtin.provider}-override`)).toEqual([]);
    expect(loads("pi-local-provider")).toEqual([]);
  });

  it("fails closed with a typed actionable error when no declarative model exists", async () => {
    const { cwd, agentDir } = fixture({ extensions: [{ dir: "pi-acme-provider", provider: "acme" }] });
    await expect(resolveExtensionProviders({ model: "absent/m1", cwd, agentDir }))
      .rejects.toThrow(/needs provider "absent"[\s\S]*models\.json[\s\S]*extensions: true/);
    await expect(resolveExtensionProviders({ model: "absent/m1", cwd, agentDir }))
      .rejects.toMatchObject({ code: "PROVIDER_NOT_FOUND", provider: "absent" });
  });

  it.each(["ask", "always", "never"])("keeps disabled project provider modules disabled even with project trust %s", async defaultProjectTrust => {
    const { cwd, agentDir, loads, moduleLoaded } = fixture({
      extensions: [{ dir: "pi-proj-provider", provider: "proj", project: true }],
      settings: { defaultProjectTrust },
    });
    await expect(resolveExtensionProviders({ model: "proj/m1", cwd, agentDir })).rejects.toBeInstanceOf(ProviderNotFoundError);
    expect(loads("pi-proj-provider")).toEqual([]);
    expect(moduleLoaded("pi-proj-provider")).toBe(false);
  });

  it("keeps the legacy explicit bridge entry fail-closed instead of executing registration", async () => {
    const { cwd, agentDir, loads, moduleLoaded } = fixture({ extensions: [{ dir: "pi-acme-provider", provider: "acme" }] });
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    vi.spyOn(process, "cwd").mockReturnValue(cwd);
    const registerProvider = vi.fn();
    const pi = { registerProvider } as unknown as Parameters<typeof fabricProviderBridge>[0];
    vi.stubEnv(PROVIDER_BRIDGE_MODEL_ENV, "");
    await fabricProviderBridge(pi);
    expect(registerProvider).not.toHaveBeenCalled();
    vi.stubEnv(PROVIDER_BRIDGE_MODEL_ENV, "acme/m1");
    await expect(fabricProviderBridge(pi)).rejects.toBeInstanceOf(ProviderNotFoundError);
    expect(registerProvider).not.toHaveBeenCalled();
    expect(loads("pi-acme-provider")).toEqual([]);
    expect(moduleLoaded("pi-acme-provider")).toBe(false);
  });
});
