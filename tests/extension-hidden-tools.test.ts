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
import { FabricState } from "../src/fabric-state.js";
import { coreOverridePromptGuidance } from "../src/core/core-override-guidance.js";
import { PI_CORE_TOOL_NAMES } from "../src/core/pi-tools.js";
import { extensionToolRosterGuidance } from "../src/core/system-guidance.js";
import { FabricToolOwnership, hostToolVisibility } from "../src/core/tool-ownership.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { CapturedToolsProvider } from "../src/providers/captured-tools-provider.js";
import { emitBeforeAgentStart } from "./helpers/emit-before-agent-start.js";

// smarty-dev#5492: an extension hides a tool by dropping it from Pi's active
// set (pi.setActiveTools). Fabric never removes tools from that set (native
// prepareLoadout hides declarations and keeps tools active), so an inactive
// registered tool is extension- or host-hidden, never Fabric-hidden, and must
// stay hidden from listing and invocation even while Fabric owns the model's
// tools (PR #567 review P2).

const context = {
  cwd: process.cwd(),
  signal: new AbortController().signal,
  nestedToolCallId: "fabric-nested-1",
  update: () => {},
  extensionContext: {},
} as unknown as FabricInvocationContext;
afterEach(() => {
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

const executed: string[] = [];
const tool = (name: string, extra: { promptSnippet?: string; promptGuidelines?: string[] } = {}) => defineTool({
  name,
  label: name,
  description: `${name} description`,
  ...extra,
  parameters: Type.Object({}),
  async execute() {
    executed.push(name);
    return { content: [{ type: "text" as const, text: `${name} ran` }], details: {} };
  },
});

const openai = createSyntheticSourceInfo("/ext/pi-better-openai/index.ts", { source: "npm:@monotykamary/pi-better-openai" });
const fovea = createSyntheticSourceInfo("/ext/pi-fovea/index.ts", { source: "npm:pi-fovea" });
const organon = createSyntheticSourceInfo("/ext/organon/index.ts", { source: "npm:organon" });

const ALL = ["read", "bash", "fovea_focus", "openai_decide", "openai_image", "openai_websearch", "fabric_exec"];

const setup = () => {
  let active = [...ALL];
  const host = {
    getActiveTools: () => [...active],
    setActiveTools: (names: string[]) => { active = [...names]; },
  };
  const runner = {
    createContext: () => ({ cwd: process.cwd() }),
    getActiveTools: () => host.getActiveTools(),
    emit: async () => undefined,
    emitToolCall: async () => undefined,
    emitToolResult: async () => undefined,
    hasHandlers: () => false,
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
  catalog.setHostVisibility(hostToolVisibility(host));
  // Fabric's full-code ownership pass, as extension.ts runs it: every captured
  // tool is routed through fabric_exec.
  const applyOwnership = () => ownership.apply(
    true,
    new Set(catalog.listRegistered().map((entry) => entry.name)),
  );
  const hide = (...names: string[]) =>
    host.setActiveTools(host.getActiveTools().filter((name) => !names.includes(name)));
  return { host, catalog, ownership, applyOwnership, hide };
};

const roster = (catalog: CapturedToolCatalog) =>
  extensionToolRosterGuidance(catalog.list(), new Set(PI_CORE_TOOL_NAMES)) ?? "";

const names = (catalog: CapturedToolCatalog) => catalog.list().map((entry) => entry.name);

describe("extension-hidden tools (smarty-dev#5492)", () => {
  it("omits hidden tools from the catalog, tools.catalog provider and guidance while visible tools stay", async () => {
    vi.stubEnv("PI_FABRIC_TOOL_ALLOWLIST", undefined);
    const { catalog, applyOwnership, hide } = setup();
    // pi-better-openai with image/decisions disabled: Pi no longer offers them.
    hide("openai_image", "openai_decide");
    applyOwnership();

    expect(names(catalog)).toEqual(["fovea_focus", "openai_websearch", "read"]);
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

  it("Fabric's ownership never removes a tool, so an inactive tool is never mistaken for one Fabric routes", () => {
    const { host, ownership, applyOwnership } = setup();
    host.setActiveTools(ALL.filter((name) => name !== "fabric_exec"));
    expect(applyOwnership()).toBe(true);
    // Every captured tool Fabric owns is still active: only fabric_exec was added.
    expect(host.getActiveTools()).toEqual([...ALL.filter((name) => name !== "fabric_exec"), "fabric_exec"]);
    expect(applyOwnership()).toBe(false);
    expect(ownership.release()).toBe(false);
    expect(host.getActiveTools()).toContain("openai_image");
  });

  it("keeps a tool its extension hid after Fabric took ownership hidden from listing and invocation (review P2)", async () => {
    vi.stubEnv("PI_FABRIC_TOOL_ALLOWLIST", undefined);
    const { catalog, ownership, applyOwnership, hide } = setup();
    // Full code mode: Fabric owns the model's tool surface and routes every
    // captured tool, openai_image included, through fabric_exec.
    applyOwnership();
    expect(names(catalog)).toContain("openai_image");
    expect(catalog.revalidateHostVisibility()).toBe(false); // baseline
    const listener = vi.fn();
    catalog.subscribe(listener);

    // The extension then disables image generation while Fabric still owns the set.
    hide("openai_image");
    expect(catalog.revalidateHostVisibility()).toBe(true);
    expect(listener).toHaveBeenCalledTimes(1);
    // Fabric's per-turn reassertion must neither re-offer nor re-list it.
    applyOwnership();
    ownership.release();

    const provider = new CapturedToolsProvider(catalog);
    expect(names(catalog)).not.toContain("openai_image");
    expect((await provider.list({}, context)).map((entry) => entry.name)).not.toContain("openai_image");
    expect(await provider.describe("openai_image", context)).toBeUndefined();
    expect(roster(catalog)).not.toContain("openai_image");
    executed.length = 0;
    expect(() => provider.prepareArguments("openai_image", {})).toThrow(/hidden by its extension/);
    await expect(provider.invoke("openai_image", {}, context)).rejects.toThrow(/hidden by its extension/);
    expect(executed).toEqual([]);
    // A tool the extension still offers keeps working through fabric_exec.
    await expect(provider.invoke("openai_websearch", {}, context)).resolves.toBeDefined();
    expect(executed).toEqual(["openai_websearch"]);
  });

  it("drops the whole extension line when all its tools are hidden, and restores it when re-offered", () => {
    const { host, catalog, applyOwnership, hide } = setup();
    hide("openai_decide", "openai_image", "openai_websearch");
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
    expect(names(catalog)).toContain("openai_image");
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
    expect(names(catalog)).toEqual(["fovea_focus"]);
  });

  // PR #567 review (Astra): after the first good read a throwing source must
  // fail closed, never turn into "no filter".
  const flakySource = (host: { getActiveTools: () => string[] }) => {
    const control = { failing: false };
    const source = hostToolVisibility({
      getActiveTools: () => {
        if (control.failing) throw Object.assign(new Error("secret host detail /srv/x"), { code: "EHOSTDOWN" });
        return host.getActiveTools();
      },
    });
    return { control, source };
  };

  it("fails closed when the active-tool read throws after a good read: hidden stay hidden and refused", async () => {
    vi.stubEnv("PI_FABRIC_TOOL_ALLOWLIST", undefined);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const { host, catalog, hide } = setup();
    const { control, source } = flakySource(host);
    catalog.setHostVisibility(source);
    hide("openai_image", "openai_decide");
    expect(names(catalog)).toEqual(["fovea_focus", "openai_websearch", "read"]); // good read

    control.failing = true;
    expect(names(catalog)).toEqual(["fovea_focus", "openai_websearch", "read"]);
    expect(catalog.get("openai_image")).toBeUndefined();
    expect(catalog.get("openai_websearch")).toBeDefined();
    expect(() => catalog.require("openai_image")).toThrow(/hidden by its extension/);
    const provider = new CapturedToolsProvider(catalog);
    executed.length = 0;
    await expect(provider.invoke("openai_image", {}, context)).rejects.toThrow(/hidden by its extension/);
    await expect(provider.invoke("openai_websearch", {}, context)).resolves.toBeDefined();
    expect(executed).toEqual(["openai_websearch"]);
    expect(roster(catalog)).not.toContain("openai_image");

    // Warned once, with the error code only.
    expect(warn).toHaveBeenCalledTimes(1);
    expect(String(warn.mock.calls[0]?.[0])).toContain("EHOSTDOWN");
    expect(String(warn.mock.calls[0]?.[0])).not.toContain("secret host detail");

    // No known-good set for a replacement source that throws: hide every tool.
    catalog.setHostVisibility(() => { throw new Error("broken"); });
    expect(names(catalog)).toEqual([]);
    expect(() => catalog.require("openai_websearch")).toThrow(/hidden by its extension/);
    expect(warn).toHaveBeenCalledTimes(1);
  });

  it("keeps today's no-filter behaviour while the source throws before any good read (startup)", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const { host, catalog, hide } = setup();
    const { control, source } = flakySource(host);
    control.failing = true;
    catalog.setHostVisibility(source);
    hide("openai_image");
    expect(names(catalog)).toEqual(["fovea_focus", "openai_decide", "openai_image", "openai_websearch", "read"]);
    expect(catalog.get("openai_image")).toBeDefined();
    expect(warn).not.toHaveBeenCalled();
    // Once Pi's runtime is ready its active set filters as usual.
    control.failing = false;
    expect(names(catalog)).not.toContain("openai_image");
  });

  it("uses the fresh active set again once the failing source recovers", () => {
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const { host, catalog, hide } = setup();
    const { control, source } = flakySource(host);
    catalog.setHostVisibility(source);
    hide("openai_image");
    expect(names(catalog)).not.toContain("openai_image");
    control.failing = true;
    expect(names(catalog)).not.toContain("openai_image");

    // Recovery: the extension re-offers openai_image and hides openai_websearch.
    host.setActiveTools([...host.getActiveTools().filter((name) => name !== "openai_websearch"), "openai_image"]);
    control.failing = false;
    expect(names(catalog)).toEqual(["fovea_focus", "openai_decide", "openai_image", "read"]);
    expect(catalog.get("openai_websearch")).toBeUndefined();
    // A later failure falls back to that fresh set, not the older one.
    control.failing = true;
    expect(names(catalog)).toEqual(["fovea_focus", "openai_decide", "openai_image", "read"]);
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
    // The roster reads the live config once captured tools exist (post-bootstrap in Pi).
    vi.spyOn(FabricState.prototype, "config", "get").mockReturnValue(DEFAULT_FABRIC_CONFIG);
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
