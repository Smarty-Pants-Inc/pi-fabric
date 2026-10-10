import {
  createSyntheticSourceInfo,
  defineTool,
  type ExtensionAPI,
  type ExtensionRunner,
} from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog, type CapturedToolEntry } from "../src/capture/catalog.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { coreOverridePromptGuidance } from "../src/core/core-override-guidance.js";
import { PI_CORE_TOOL_NAMES } from "../src/core/pi-tools.js";
import { extensionToolRosterGuidance } from "../src/core/system-guidance.js";
import { FabricToolOwnership, hostToolVisibility } from "../src/core/tool-ownership.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { CapturedToolsProvider } from "../src/providers/captured-tools-provider.js";
import { emitBeforeAgentStart } from "./helpers/emit-before-agent-start.js";

// smarty-dev#5492: an extension hides a tool by dropping it from Pi's active
// set (pi.setActiveTools). Fabric itself also removes captured tools from that
// set in full code mode, so "inactive" alone is not "hidden": only tools that
// are inactive and were not removed by Fabric's ownership are hidden.

const context = { cwd: process.cwd(), signal: new AbortController().signal } as FabricInvocationContext;
afterEach(() => vi.unstubAllEnvs());

const tool = (name: string, extra: { promptSnippet?: string; promptGuidelines?: string[] } = {}) => defineTool({
  name,
  label: name,
  description: `${name} description`,
  ...extra,
  parameters: Type.Object({}),
  async execute() {
    return { content: [{ type: "text" as const, text: `${name} ran` }], details: {} };
  },
});

const openai = createSyntheticSourceInfo("/ext/pi-better-openai/index.ts", { source: "npm:@monotykamary/pi-better-openai" });
const fovea = createSyntheticSourceInfo("/ext/pi-fovea/index.ts", { source: "npm:pi-fovea" });
const organon = createSyntheticSourceInfo("/ext/organon/index.ts", { source: "npm:organon" });

const setup = () => {
  let active = ["read", "bash", "fovea_focus", "openai_decide", "openai_image", "openai_websearch", "fabric_exec"];
  const host = {
    getActiveTools: () => [...active],
    setActiveTools: (names: string[]) => { active = [...names]; },
  };
  const runner = {
    createContext: () => ({ cwd: process.cwd() }),
    getActiveTools: () => host.getActiveTools(),
  } as unknown as ExtensionRunner;
  const catalog = new CapturedToolCatalog();
  catalog.replace(
    [
      { definition: tool("openai_decide"), sourceInfo: openai },
      { definition: tool("openai_image"), sourceInfo: openai },
      { definition: tool("openai_websearch"), sourceInfo: openai },
      { definition: tool("fovea_focus"), sourceInfo: fovea },
      { definition: tool("read", { promptSnippet: "structure-aware reads" }), sourceInfo: organon },
    ],
    runner,
    DEFAULT_FABRIC_CONFIG.capture,
    "/ext/pi-fabric/index.ts",
  );
  const ownership = new FabricToolOwnership(host);
  catalog.setHostVisibility(hostToolVisibility(host, ownership));
  // Fabric's full-code ownership pass, as index.ts runs it.
  const applyOwnership = () => ownership.apply(
    true,
    new Set(catalog.listRegistered().map((entry) => entry.name)),
  );
  return { host, catalog, ownership, applyOwnership };
};

const roster = (catalog: CapturedToolCatalog) =>
  extensionToolRosterGuidance(catalog.list(), new Set(PI_CORE_TOOL_NAMES)) ?? "";

describe("extension-hidden tools (smarty-dev#5492)", () => {
  it("omits hidden tools from the catalog, tools.catalog provider and guidance while visible tools stay", async () => {
    vi.stubEnv("PI_FABRIC_TOOL_ALLOWLIST", undefined);
    const { host, catalog, applyOwnership } = setup();
    // pi-better-openai with image/decisions disabled: Pi no longer offers them.
    host.setActiveTools(host.getActiveTools().filter((name) => name !== "openai_image" && name !== "openai_decide"));
    applyOwnership();
    // Fabric routed the visible captured tools (and core read) through fabric_exec.
    expect(host.getActiveTools()).toEqual(["fabric_exec"]);

    expect(catalog.list().map((entry) => entry.name)).toEqual(["fovea_focus", "openai_websearch", "read"]);
    expect(catalog.size).toBe(3);
    expect(catalog.get("openai_image")).toBeUndefined();
    expect(catalog.get("openai_websearch")).toBeDefined();
    expect(() => catalog.require("openai_image")).toThrow(/hidden by its extension/);
    // Fabric's bookkeeping still sees the registration.
    expect(catalog.getRegistered("openai_image")).toBeDefined();
    expect(catalog.listRegistered()).toHaveLength(5);

    const provider = new CapturedToolsProvider(catalog);
    const listed = (await provider.list({}, context)).map((entry) => entry.name);
    expect(listed).toEqual(["fovea_focus", "openai_websearch", "read"]);
    expect((await provider.list({ query: "openai" }, context)).map((entry) => entry.name)).toEqual(["openai_websearch"]);
    expect(await provider.describe("openai_image", context)).toBeUndefined();
    expect(await provider.describe("openai_websearch", context)).toBeDefined();
    await expect(provider.invoke("openai_image", {}, context)).rejects.toThrow(/hidden by its extension/);

    const guidance = roster(catalog);
    expect(guidance).toContain("- @monotykamary/pi-better-openai: openai_websearch");
    expect(guidance).toContain("- pi-fovea: fovea_focus");
    expect(guidance).not.toContain("openai_image");
    expect(guidance).not.toContain("openai_decide");
    expect(coreOverridePromptGuidance(catalog)).toContain("structure-aware reads");
  });

  it("drops the whole extension line when all its tools are hidden, and restores it when re-offered", () => {
    const { host, catalog, applyOwnership } = setup();
    host.setActiveTools(host.getActiveTools().filter((name) => !name.startsWith("openai_")));
    applyOwnership();
    expect(catalog.revalidateHostVisibility()).toBe(false); // first read only records the baseline
    expect(roster(catalog)).not.toContain("pi-better-openai");
    expect(roster(catalog)).toContain("- pi-fovea: fovea_focus");

    const listener = vi.fn();
    catalog.subscribe(listener);
    // The user re-enables image generation: the extension offers the tool again.
    host.setActiveTools([...host.getActiveTools(), "openai_image"]);
    expect(catalog.revalidateHostVisibility()).toBe(true);
    expect(listener).toHaveBeenCalledTimes(1);
    applyOwnership();
    expect(host.getActiveTools()).toEqual(["fabric_exec"]);
    expect(catalog.list().map((entry) => entry.name)).toContain("openai_image");
    expect(roster(catalog)).toContain("- @monotykamary/pi-better-openai: openai_image");
    expect(catalog.revalidateHostVisibility()).toBe(false);
  });

  it("keeps every tool listed when the host has not initialized its active set", () => {
    const catalog = new CapturedToolCatalog();
    catalog.replace(
      [{ definition: tool("fovea_focus"), sourceInfo: fovea }],
      { createContext: () => ({}), getActiveTools: () => [] } as unknown as ExtensionRunner,
      DEFAULT_FABRIC_CONFIG.capture,
      "/ext/pi-fabric/index.ts",
    );
    catalog.setHostVisibility(() => { throw new Error("not initialized"); });
    expect(catalog.list().map((entry) => entry.name)).toEqual(["fovea_focus"]);
  });

  it("leaves extension-hidden tools out of the fabric_exec system prompt section", async () => {
    const handlers = new Map<string, Array<(event: unknown, context: unknown) => unknown>>();
    const pi = {
      sendMessage: vi.fn(),
      events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
      // pi-better-openai disabled openai_image; fovea_focus is still offered.
      getActiveTools: vi.fn(() => ["fabric_exec", "fovea_focus", "openai_websearch"]),
      getAllTools: vi.fn(() => []),
      on: vi.fn((event: string, handler: (event: unknown, context: unknown) => unknown) => {
        const list = handlers.get(event) ?? [];
        list.push(handler);
        handlers.set(event, list);
      }),
      registerCommand: vi.fn(),
      registerMessageRenderer: vi.fn(),
      registerTool: vi.fn(),
      setActiveTools: vi.fn(),
    } as unknown as ExtensionAPI;
    const entries = [
      { name: "fovea_focus", definition: tool("fovea_focus"), sourceInfo: fovea },
      { name: "openai_image", definition: tool("openai_image"), sourceInfo: openai },
      { name: "openai_websearch", definition: tool("openai_websearch"), sourceInfo: openai },
    ] as unknown as CapturedToolEntry[];
    vi.spyOn(CapturedToolCatalog.prototype, "listRegistered").mockReturnValue(entries);
    const { default: piFabric } = await import("../src/index.js");
    await piFabric(pi);
    const result = await emitBeforeAgentStart(handlers, {
      systemPrompt: "base system",
      prompt: "generate an image",
      systemPromptOptions: { skills: [] },
    }, {});
    const section = result.systemPromptOptions.sections.fabric_execution ?? "";
    expect(section).toContain("- pi-fovea: fovea_focus");
    expect(section).toContain("- @monotykamary/pi-better-openai: openai_websearch");
    expect(section).not.toContain("openai_image");
    expect(result.systemPrompt).not.toContain("openai_image");
  });
});
