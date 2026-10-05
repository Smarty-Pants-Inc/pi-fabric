import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext, MessageRenderer, Theme } from "@earendil-works/pi-coding-agent";
import { convertToLlm, CustomMessageComponent, initTheme } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { loadFabricConfig, normalizeFabricConfig, type FabricPrincipalViewMode } from "../src/config.js";
import type { FabricState } from "../src/fabric-state.js";
import { registerIncomingMessageRenderers } from "../src/ui/incoming-messages.js";
import { PRINCIPAL_VIEW_SHORTCUT, principalViewEnabled, principalViewIncomingMode, registerPrincipalView } from "../src/ui/principal-view.js";

const roots: string[] = [];
afterEach(() => { vi.unstubAllEnvs(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const harness = (mode: FabricPrincipalViewMode = "off", expanded = false, trusted = true, uiMode = "tui") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "principal-view-test-")); roots.push(root);
  const agentDir = path.join(root, "agent"); const cwd = path.join(root, "workspace");
  fs.mkdirSync(agentDir); fs.mkdirSync(cwd);
  vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
  vi.stubEnv("PI_FABRIC_ROLE", undefined); vi.stubEnv("SMARTY_ROLE", "org-agent@abc123456789");
  const initialFile = trusted ? path.join(cwd, ".pi", "fabric.json") : path.join(agentDir, "fabric.json");
  fs.mkdirSync(path.dirname(initialFile), { recursive: true });
  fs.writeFileSync(initialFile, JSON.stringify({ ui: { principalView: mode }, prewalk: { enabled: false } }));
  let config = loadFabricConfig({ cwd, agentDir, projectTrusted: trusted });
  const commands = new Map<string, any>(); const shortcuts = new Map<string, any>();
  const pi = { registerCommand: vi.fn((name, command) => commands.set(name, command)),
    registerShortcut: vi.fn((key, shortcut) => shortcuts.set(key, shortcut)), on: vi.fn(),
    registerMessageRenderer: vi.fn(), registerMarkdownTransformer: vi.fn(), sendMessage: vi.fn(),
    setThinkingLevel: vi.fn(), setActiveTools: vi.fn() } as unknown as ExtensionAPI;
  const ui = { getToolsExpanded: vi.fn(() => expanded), setToolsExpanded: vi.fn((next: boolean) => { expanded = next; }),
    setStatus: vi.fn(), notify: vi.fn() };
  const context = { mode: uiMode, hasUI: uiMode === "tui" || uiMode === "rpc", cwd, ui,
    isProjectTrusted: () => trusted, sessionManager: { getBranch: () => [] } } as unknown as ExtensionContext;
  const state = { bootstrapped: true, provisionalConfig: () => config, bootstrap: vi.fn(async () => { state.bootstrapped = true; }),
    reloadConfig: vi.fn(() => { config = loadFabricConfig({ cwd, agentDir, projectTrusted: trusted }); }), ensure: vi.fn() };
  const controller = registerPrincipalView(pi, state as unknown as FabricState);
  const command = (arg = "") => commands.get("principal-view").handler(arg, context);
  return { controller, command, context, ui, pi, state, commands, shortcuts, cwd, agentDir,
    config: () => config, expanded: () => expanded, setMode: (next: FabricPrincipalViewMode) => { config.ui.principalView = next; } };
};

describe("principal view", () => {
  it.each([
    [{}, false], [{ SMARTY_ROLE: "org-agent@stamp" }, true], [{ SMARTY_ROLE: "org-agent" }, true],
    [{ SMARTY_ROLE: "org@stamp" }, true], [{ PI_FABRIC_ROLE: "org", SMARTY_ROLE: "task-agent" }, true],
    [{ PI_FABRIC_ROLE: "project", SMARTY_ROLE: "org-agent@stamp" }, false],
    [{ SMARTY_ROLE: "project-agent" }, false], [{ SMARTY_ROLE: "org-kate" }, false], [{ PI_SMARTY_ROLE: "org" }, false],
  ])("defaults by exact fleet role and precedence: %j", (env, expected) => {
    expect(principalViewEnabled("auto", env)).toBe(expected);
    expect(principalViewEnabled("on", env)).toBe(true);
    expect(principalViewEnabled("off", env)).toBe(false);
  });

  it("registers one command and one shortcut, with no context/user/reply transformations", () => {
    const h = harness();
    expect([...h.commands.keys()]).toEqual(["principal-view"]);
    expect([...h.shortcuts.keys()]).toEqual([PRINCIPAL_VIEW_SHORTCUT]);
    expect(h.commands.get("principal-view").getArgumentCompletions("o")).toEqual([
      { value: "on", label: "on" }, { value: "off", label: "off" },
    ]);
    for (const prefix of ["", "on", "off", "auto", "off ", "invalid"]) {
      expect(h.commands.get("principal-view").getArgumentCompletions(prefix)).toBeNull();
    }
    expect(h.pi.on).not.toHaveBeenCalled(); expect(h.pi.registerMarkdownTransformer).not.toHaveBeenCalled();
    expect(h.pi.registerMessageRenderer).not.toHaveBeenCalled(); expect(h.pi.sendMessage).not.toHaveBeenCalled();
    expect(h.pi.setThinkingLevel).not.toHaveBeenCalled(); expect(h.pi.setActiveTools).not.toHaveBeenCalled();
  });

  it.each([false, true])("persists the toggle and restores prior tool expansion=%s", async expanded => {
    const h = harness("off", expanded);
    h.controller.start(h.context); expect(h.ui.setToolsExpanded).not.toHaveBeenCalled();
    await h.command(); expect(h.config().ui.principalView).toBe("on"); expect(h.expanded()).toBe(false);
    const saved = JSON.parse(fs.readFileSync(path.join(h.cwd, ".pi", "fabric.json"), "utf8"));
    expect(saved.ui.principalView).toBe("on"); expect(saved.prewalk.enabled).toBe(false);
    // A repeated on command must not overwrite the pre-principal expansion snapshot.
    await h.command("on"); await h.command("off"); expect(h.expanded()).toBe(expanded);
    expect(h.config().ui.principalView).toBe("off");
    expect(h.state.ensure).not.toHaveBeenCalled(); expect(h.state.bootstrap).not.toHaveBeenCalled();
    expect(h.ui.setStatus).toHaveBeenLastCalledWith("fabric-principal-view", undefined);
  });

  it("enables org auto at session start, permits native expansion, and resets across sessions", async () => {
    const h = harness("auto", true); h.controller.start(h.context); expect(h.expanded()).toBe(false);
    h.ui.setToolsExpanded(true); // Native Ctrl+O remains available for inspection.
    expect(h.expanded()).toBe(true);
    await h.shortcuts.get(PRINCIPAL_VIEW_SHORTCUT).handler(h.context);
    expect(h.config().ui.principalView).toBe("off"); expect(h.expanded()).toBe(true);
    h.setMode("on"); h.ui.setToolsExpanded(false); h.controller.start(h.context);
    await h.command("off"); expect(h.expanded()).toBe(false); // No old session snapshot leaked.
  });

  it("saves globally for untrusted projects, handles auto, and only bootstraps config", async () => {
    const h = harness("off", false, false); h.state.bootstrapped = false;
    await h.command("auto"); expect(h.state.bootstrap).toHaveBeenCalledOnce();
    expect(h.config().ui.principalView).toBe("auto"); expect(h.expanded()).toBe(false);
    expect(JSON.parse(fs.readFileSync(path.join(h.agentDir, "fabric.json"), "utf8")).ui.principalView).toBe("auto");
    expect(fs.existsSync(path.join(h.cwd, ".pi"))).toBe(false);
    expect(h.state.ensure).not.toHaveBeenCalled();
  });

  it.each(["rpc", "json", "print"])("persists without terminal effects in %s mode", async mode => {
    const h = harness("off", true, false, mode); h.controller.start(h.context); await h.command("on");
    expect(h.config().ui.principalView).toBe("on"); expect(h.ui.getToolsExpanded).not.toHaveBeenCalled();
    expect(h.ui.setToolsExpanded).not.toHaveBeenCalled(); expect(h.ui.setStatus).not.toHaveBeenCalled();
    await h.shortcuts.get(PRINCIPAL_VIEW_SHORTCUT).handler(h.context);
    expect(h.config().ui.principalView).toBe("on");
  });

  it("rejects invalid input without writes or runtime work", async () => {
    const h = harness(); await h.command("on off");
    expect(h.state.reloadConfig).not.toHaveBeenCalled(); expect(h.state.bootstrap).not.toHaveBeenCalled();
    expect(h.ui.setToolsExpanded).not.toHaveBeenCalled(); expect(h.ui.notify).toHaveBeenCalledWith(expect.stringContaining("Usage:"), "warning");
    expect(h.config().ui.principalView).toBe("off");
  });

  it("reports a failed save without changing the active view", async () => {
    const h = harness("off", true, false);
    fs.mkdirSync(path.join(h.cwd, ".pi")); fs.writeFileSync(path.join(h.cwd, ".pi", "fabric.json"), "not json");
    // The global scope is writable, so block that exact isolated file with a directory.
    fs.renameSync(path.join(h.agentDir, "fabric.json"), path.join(h.agentDir, "original.json"));
    fs.mkdirSync(path.join(h.agentDir, "fabric.json")); await h.command("on");
    expect(h.config().ui.principalView).toBe("off"); expect(h.expanded()).toBe(true);
    expect(h.ui.notify).toHaveBeenCalledWith(expect.stringContaining("Failed to save principal view:"), "error");
    expect(h.state.reloadConfig).not.toHaveBeenCalled();
  });

  it("normalizes and loads legacy preferences per layer, with explicit new settings winning", () => {
    expect(normalizeFabricConfig({}).ui.principalView).toBe("auto");
    expect(normalizeFabricConfig({ ui: { principalView: "invalid" } }).ui.principalView).toBe("auto");
    expect(normalizeFabricConfig({ ui: { incomingMessages: "collapsed" } }).ui.principalView).toBe("on");
    expect(normalizeFabricConfig({ ui: { incomingMessages: "expanded", principalView: "auto" } }).ui.principalView).toBe("auto");
    const h = harness("off");
    fs.writeFileSync(path.join(h.agentDir, "fabric.json"), JSON.stringify({ ui: { principalView: "on" } }));
    fs.writeFileSync(path.join(h.cwd, ".pi", "fabric.json"), JSON.stringify({ ui: { incomingMessages: "expanded" } }));
    const load = (trusted = true) => loadFabricConfig({ cwd: h.cwd, agentDir: h.agentDir, projectTrusted: trusted }).ui.principalView;
    expect(load()).toBe("off"); expect(load(false)).toBe("on");
    fs.writeFileSync(path.join(h.cwd, ".pi", "fabric.json"), JSON.stringify({ ui: { incomingMessages: "collapsed", principalView: "auto" } }));
    expect(load()).toBe("auto");
  });

  it("renders both modes and keeps the complete principal/assistant/custom model context byte-identical", async () => {
    initTheme("dark", false);
    const h = harness("on"); const renderers = new Map<string, MessageRenderer>();
    const incomingPi = { on: vi.fn(), registerMessageRenderer: (type: string, render: MessageRenderer) => renderers.set(type, render) } as unknown as ExtensionAPI;
    registerIncomingMessageRenderers(incomingPi, () => principalViewIncomingMode(h.config().ui.principalView));
    const custom = { role: "custom" as const, customType: "pi-fabric-agent-message", display: true, timestamp: 1,
      content: '<fabric-agent-message from_name="Builder">Visible summary\nFULL_BODY_5049</fabric-agent-message>', details: {} };
    const principal = { role: "user" as const, content: [{ type: "text" as const, text: "Paul: original user bytes\nincluding blank lines\n\nand emoji 😀" }], timestamp: 2 };
    const assistant = { role: "assistant" as const, content: [{ type: "text" as const, text: "Original assistant reply in full." }],
      api: "test", model: "test", provider: "test", timestamp: 3, stopReason: "stop" as const,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
    const all = [principal, custom, assistant]; const carrier = JSON.stringify(all); const bytes = JSON.stringify(convertToLlm(all));
    const actual = new CustomMessageComponent(custom, renderers.get(custom.customType));
    const native = new CustomMessageComponent(custom);
    const plain = (text: string) => text.replace(/\x1b\[[0-?]*[ -/]*[@-~]/g, "");
    expect(plain(actual.render(120).join("\n"))).toContain("↳ Builder: Visible summary FULL_BODY_5049");
    await h.command("off"); actual.setExpanded(true); actual.setExpanded(false);
    expect(actual.render(120)).toEqual(native.render(120));
    await h.command("on"); actual.setExpanded(true); actual.setExpanded(false);
    expect(plain(actual.render(120).join("\n"))).toContain("↳ Builder:");
    expect(JSON.stringify(all)).toBe(carrier); expect(JSON.stringify(convertToLlm(all))).toBe(bytes);
    expect(h.pi.registerMarkdownTransformer).not.toHaveBeenCalled();
  });
});
