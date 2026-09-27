import { beforeEach, describe, expect, it, vi } from "vitest";

const loads = vi.hoisted(() => vi.fn());
const bendLoads = vi.hoisted(() => vi.fn());
vi.mock("../src/ui/languages/bend.js", async original => {
  bendLoads();
  return original<typeof import("../src/ui/languages/bend.js")>();
});
vi.mock("node:module", async original => {
  const actual = await original<typeof import("node:module")>();
  return { ...actual, createRequire: (base: string | URL) => {
    const require = actual.createRequire(base);
    return Object.assign((id: string) => { loads(id); return require(id); }, require);
  } };
});
beforeEach(() => { vi.resetModules(); loads.mockClear(); bendLoads.mockClear(); });

describe("optional preview startup", () => {
  it("keeps catalogs, serialization, and Python parsing out of import and configuration", async () => {
    const highlight = await import("../src/ui/highlight.js");
    const structured = await import("../src/ui/structured.js");
    const parser = await import("../src/ui/fabric-code-parser.js");
    highlight.configureHighlighting("auto", true);
    expect(structured.formatFabricValue("plain", "auto").text).toBe("plain");
    expect(parser.fabricExecTitleHint('return "hello";', "typescript")).toBeUndefined();
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(loads).not.toHaveBeenCalled();
    expect(bendLoads).not.toHaveBeenCalled();
  });

  it("loads Bend only on its first preview, not during generic highlighting initialization", async () => {
    const highlight = await import("../src/ui/highlight.js");
    await highlight.initHighlighting("dark-plus", true);
    expect(bendLoads).not.toHaveBeenCalled();
    const invalidate = vi.fn();
    expect(highlight.highlightCode("law add_zero:", "bend", invalidate)).toBeNull();
    await vi.waitFor(() => expect(invalidate).toHaveBeenCalled(), { timeout: 15_000 });
    expect(highlight.highlightCode("law add_zero:", "bend")).not.toBeNull();
    expect(highlight.highlightCode("def add_zero(x):", "bend")).not.toBeNull();
    expect(bendLoads).toHaveBeenCalledTimes(1);
    highlight.configureHighlighting("dark-plus", false);
  });

  it("loads synchronous preview dependencies once, on the first relevant use", async () => {
    const highlight = await import("../src/ui/highlight.js");
    const structured = await import("../src/ui/structured.js");
    const parser = await import("../src/ui/fabric-code-parser.js");
    expect(highlight.languageFromPath("main.py")).toBe("python");
    expect(highlight.languageFromPath("main.ts")).toBe("typescript");
    expect(highlight.languageFromPath("LAWS.bend")).toBe("bend");
    expect(bendLoads).not.toHaveBeenCalled();
    highlight.configureHighlighting("github-light", true);
    expect(highlight.effectiveShikiThemeIsLight()).toBe(true);
    expect(highlight.effectiveShikiThemeIsLight()).toBe(true);
    expect(structured.formatJsonAsYaml({ ok: true })).toBe("ok: true");
    expect(structured.formatJsonAsYaml({ ok: false })).toBe("ok: false");
    expect(parser.fabricExecTitleHint('await pi.read({"path": "example.py"})', "python")).toBeTruthy();
    parser.fabricExecTitleHint('await pi.read({"path": "other.py"})', "python");
    expect(loads.mock.calls.map(([id]) => id).sort()).toEqual(["@lezer/python", "shiki/langs", "shiki/themes", "yaml"]);
  });
});
