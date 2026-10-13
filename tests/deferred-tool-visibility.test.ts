import { createSyntheticSourceInfo, defineTool, type ExtensionRunner, type ToolInfo } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { hostToolVisibility } from "../src/core/tool-ownership.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { CapturedToolsProvider } from "../src/providers/captured-tools-provider.js";

// Pi's ExtensionAPI contract: tools with `codemode` or `deferred` exposure stay
// callable from codemode scripts whether active or not; `direct` tools are only
// callable while active and `hidden` tools never are. hostToolVisibility reads
// the optional host getAllTools() exposure metadata alongside getActiveTools().
// Plain active-set hiding (direct tools) is covered by extension-hidden-tools.test.ts.

type Exposure = ToolInfo["exposure"];
type Meta = Pick<ToolInfo, "name" | "exposure">;

const context = {
  cwd: process.cwd(), signal: new AbortController().signal, nestedToolCallId: "fabric-nested-1",
  update: () => {}, extensionContext: {},
} as unknown as FabricInvocationContext;
const source = createSyntheticSourceInfo("/ext/pi-lazy/index.ts", { source: "npm:pi-lazy" });
const executed: string[] = [];
const tool = (name: string, exposure: Exposure) => defineTool({
  name, label: name, description: name, exposure, parameters: Type.Object({}),
  async execute() {
    executed.push(name);
    return { content: [{ type: "text" as const, text: `${name} ran` }], details: {} };
  },
});
const runner = {
  createContext: () => ({ cwd: process.cwd() }), emit: async () => undefined, emitToolCall: async () => undefined,
  emitToolResult: async () => undefined, hasHandlers: () => false,
} as unknown as ExtensionRunner;

const INITIAL: Record<string, Exposure> = {
  deferred: "deferred", codemode: "codemode", direct: "direct", hidden: "hidden", modelOnly: "model-only",
};

const setup = (opts: { metadata?: boolean } = {}) => {
  let exposures: Record<string, Exposure> = { ...INITIAL };
  let active: string[] = ["hidden", "modelOnly"]; // non-callable exposures, even if active
  let fail: Error | undefined;
  const host: { getActiveTools(): string[]; getAllTools?(): Meta[] } = { getActiveTools: () => [...active] };
  if (opts.metadata !== false) host.getAllTools = () => {
    if (fail) throw fail;
    return Object.entries(exposures).map(([name, exposure]) => ({ name, exposure }));
  };
  const catalog = new CapturedToolCatalog();
  const replace = () => catalog.replace(
    Object.entries(exposures).map(([name, exposure]) => ({ definition: tool(name, exposure), sourceInfo: source })),
    runner, DEFAULT_FABRIC_CONFIG.capture, "/ext/pi-fabric/index.ts",
  );
  replace();
  catalog.setHostVisibility(hostToolVisibility(host));
  return {
    catalog, replace,
    setExposure: (name: string, exposure: Exposure) => { exposures = { ...exposures, [name]: exposure }; },
    setActive: (names: string[]) => { active = [...names]; },
    setFail: (error: Error | undefined) => { fail = error; },
  };
};
const names = (catalog: CapturedToolCatalog) => catalog.list().map((entry) => entry.name);
const denied = (catalog: CapturedToolCatalog, name: string) => {
  expect(catalog.get(name)).toBeUndefined();
  expect(() => catalog.require(name)).toThrow();
};

afterEach(() => { vi.restoreAllMocks(); executed.length = 0; });

describe("deferred/codemode tool visibility from host getAllTools metadata", () => {
  it("inactive deferred and codemode tools are callable; inactive direct and non-callable exposures are denied", async () => {
    const { catalog } = setup();
    expect(names(catalog)).toEqual(["codemode", "deferred"]);
    for (const name of ["deferred", "codemode"]) {
      expect(catalog.get(name)?.name).toBe(name);
      expect(catalog.require(name).name).toBe(name);
    }
    denied(catalog, "direct");
    denied(catalog, "hidden");
    denied(catalog, "modelOnly");
    const provider = new CapturedToolsProvider(catalog);
    await provider.invoke("deferred", {}, context);
    await expect(provider.invoke("direct", {}, context)).rejects.toThrow();
    await expect(provider.invoke("hidden", {}, context)).rejects.toThrow();
    await expect(provider.invoke("modelOnly", {}, context)).rejects.toThrow();
    expect(executed).toEqual(["deferred"]);
  });

  it("a legacy host without getAllTools stays active-set only", () => {
    const { catalog, setActive } = setup({ metadata: false });
    setActive([]);
    expect(names(catalog)).toEqual([]);
    setActive(["direct", "hidden"]);
    expect(names(catalog)).toEqual(["direct"]);
    denied(catalog, "deferred");
    denied(catalog, "hidden");
  });

  it("metadata withdrawal or re-registration as hidden removes visibility", () => {
    const { catalog, replace, setExposure } = setup();
    setExposure("deferred", "direct"); // withdrawn to direct while inactive
    expect(names(catalog)).toEqual(["codemode"]);
    denied(catalog, "deferred");
    setExposure("codemode", "hidden"); // re-registered hidden
    replace();
    expect(names(catalog)).toEqual([]);
    denied(catalog, "codemode");
  });

  it("fails closed to last-good visibility without leaking host detail, then recovers fresh metadata", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const { catalog, setExposure, setFail } = setup();
    expect(names(catalog)).toEqual(["codemode", "deferred"]);
    setFail(new Error("SECRET /home/host/token=abc123"));
    setExposure("direct", "deferred"); // unseen while failing
    expect(names(catalog)).toEqual(["codemode", "deferred"]);
    denied(catalog, "direct");
    denied(catalog, "hidden");
    let message = "";
    try { catalog.require("direct"); } catch (error) { message = String(error); }
    for (const text of [message, ...warn.mock.calls.flat().map(String)]) {
      expect(text).not.toContain("SECRET");
      expect(text).not.toContain("abc123");
    }
    setFail(undefined);
    setExposure("deferred", "direct");
    expect(names(catalog)).toEqual(["codemode", "direct"]);
    denied(catalog, "deferred");
  });
});
