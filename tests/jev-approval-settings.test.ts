import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext, Theme } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SettingsList } from "@earendil-works/pi-tui";
import type { CapturedToolCatalog } from "../src/capture/catalog.js";
import { loadFabricConfig, normalizeFabricConfig } from "../src/config.js";
import { isJevApprovalModel } from "../src/jev/model-key.js";
import type { FabricState } from "../src/fabric-state.js";
import { FabricModelSelector } from "../src/ui/fabric-model-selector.js";
import { buildFabricSettingsItems, openFabricSettings } from "../src/ui/settings.js";
import { ProbabilityInputSubmenu, SectionSubmenu } from "../src/ui/settings-submenus.js";

const theme = { fg: (_: string, text: string) => text, bg: (_: string, text: string) => text, bold: (text: string) => text } as unknown as Theme;
const thresholdId = "jev.autoApprovalThreshold";
const fixture = (model?: string) => {
  const config = normalizeFabricConfig({ approvals: { model } });
  const apply = vi.fn((id: string, value: unknown) => {
    if (id === "approvals.model") {
      if (value) config.approvals.model = String(value);
      else delete config.approvals.model;
    } else if (id === thresholdId) config.jev.autoApprovalThreshold = value as number;
  });
  const items = buildFabricSettingsItems(theme, config, apply, { keepVisibleCandidates: [], modelSource: { models: [], lastUsed: {} } });
  const open = () => items.find(item => item.id === "approvals")!.submenu!("", () => {}) as SectionSubmenu;
  return { config, apply, open };
};
const activate = <T>(section: SectionSubmenu, id: string): T => {
  section.settingsList.selectItem(id);
  section.handleInput("\r");
  return (section.settingsList as unknown as { submenuComponent: T }).submenuComponent;
};
afterEach(() => vi.unstubAllEnvs());

describe("Jev approval probability settings", () => {
  it("offers every route alias without duplicate or legacy picker entries", () => {
    const { open } = fixture("jev/jev-latest");
    const picker = activate<FabricModelSelector>(open(), "approvals.model");
    expect(picker.rpcChoices().map(choice => choice.value)).toEqual([
      "Inherit",
      "pi-fabric/typesafe/jev-latest",
      "pi-fabric/openrouter/jev-1.13", "pi-fabric/openrouter/jev-latest",
      "pi-fabric/typesafe/jev-1.13", "pi-fabric/typesafe/jev-1.13.0", "pi-fabric/typesafe/jev-preview",
      "pi-fabric/vercel-ai-gateway/jev-latest",
    ]);
    expect(picker.rpcChoices().find(choice => choice.current)?.value).toBe("pi-fabric/typesafe/jev-latest");
  });
  it.each([undefined, "anthropic/chat", "pi-fabric/typesafe/jev-latest", "pi-fabric/openrouter/jev-latest", "pi-fabric/vercel-ai-gateway/jev-latest", "pi-fabric/typesafe/pinned"])("shows the setting only for a Jev override: %s", model => {
    const { config, open } = fixture(model);
    expect(config.jev.autoApprovalThreshold).toBe(0.5);
    const row = open().items.find(item => item.id === thresholdId);
    expect(Boolean(row)).toBe(Boolean(model && isJevApprovalModel(model)));
    if (row) expect(row.currentValue).toBe("0.5");
  });

  it("refreshes immediately on model selection and retains exact values when hidden or reopened", () => {
    const { config, apply, open } = fixture();
    const section = open();
    const rows = section.items;
    activate<FabricModelSelector>(section, "approvals.model").selectRpc("pi-fabric/typesafe/jev-latest");
    expect(section.items).toBe(rows);
    expect(rows.some(item => item.id === thresholdId)).toBe(true);
    activate<ProbabilityInputSubmenu>(section, thresholdId).submitRpc("0.975");
    expect(apply).toHaveBeenLastCalledWith(thresholdId, 0.975);
    expect(config.jev.autoApprovalThreshold).toBe(0.975);
    expect(section.render(100).join("\n")).toContain("0.975");
    activate<FabricModelSelector>(section, "approvals.model").selectRpc("Inherit");
    expect(rows.some(item => item.id === thresholdId)).toBe(false);
    expect(config.jev.autoApprovalThreshold).toBe(0.975);
    activate<FabricModelSelector>(section, "approvals.model").selectRpc("pi-fabric/typesafe/jev-latest");
    expect(open().items.find(item => item.id === thresholdId)?.currentValue).toBe("0.975");
  });

  it("refreshes with older host widgets that lack cursor restoration", () => {
    const { open } = fixture();
    const section = open();
    const original = vi.spyOn(SettingsList.prototype, "selectItem");
    Object.defineProperty(SettingsList.prototype, "selectItem", { value: undefined });
    try {
      section.applyChange("approvals.model", "pi-fabric/typesafe/jev-latest");
      expect(section.items.some(item => item.id === thresholdId)).toBe(true);
      expect(section.render(100).join("\n")).toContain("Jev minimum probability");
    } finally { original.mockRestore(); }
  });

  it.each(["", " ", "NaN", "Infinity", "-0.01", "1.001", "text", "50%"])("rejects invalid input %j and cancels without saving", value => {
    const { config, apply, open } = fixture("pi-fabric/typesafe/jev-latest");
    const input = activate<ProbabilityInputSubmenu>(open(), thresholdId);
    input.input.setValue(value);
    input.handleInput("\r");
    expect(input.render(90).join("\n")).toContain("Enter a probability between 0 and 1.");
    input.handleInput("\x1b");
    expect(apply).not.toHaveBeenCalled();
    expect(config.jev.autoApprovalThreshold).toBe(0.5);
  });

  it.each(["0", "0.50", ".955", "1"])("accepts probability %s through the terminal input", value => {
    const { config, open } = fixture("pi-fabric/typesafe/jev-latest");
    const input = activate<ProbabilityInputSubmenu>(open(), thresholdId);
    input.input.setValue(value);
    input.handleInput("\r");
    expect(config.jev.autoApprovalThreshold).toBe(Number(value));
  });

  it.each(["project", "global"])("persists model and threshold in the %s scope through RPC dialogs", async scope => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-jev-settings-"));
    const cwd = path.join(root, "project");
    const agentDir = path.join(root, "agent");
    fs.mkdirSync(cwd, { recursive: true });
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    try {
      const config = normalizeFabricConfig({});
      const state = {
        config, ensure: vi.fn(async () => {}),
        reloadConfig: vi.fn(() => Object.assign(config, loadFabricConfig({ cwd, agentDir, projectTrusted: true }))),
        agents: { claudeModels: vi.fn(async () => []) },
      } as unknown as FabricState;
      let switched = false;
      let opened = false;
      let edits = 0;
      const notify = vi.fn();
      const select = vi.fn(async (title: string, options: string[]) => {
        if (title.startsWith("Fabric settings › Approvals › Auto model")) return options.find(option => option.startsWith("typesafe/jev-latest"));
        if (title.startsWith("Fabric settings › Approvals")) {
          if (edits++ === 0) {
            expect(options.some(option => option.startsWith("Jev minimum probability"))).toBe(false);
            return options.find(option => option.startsWith("Auto model"));
          }
          if (edits === 2) return options.find(option => option.startsWith("Jev minimum probability · 0.5"));
          expect(options.some(option => option.startsWith("Jev minimum probability · 0.975"))).toBe(true);
          return "← Back";
        }
        if (scope === "global" && !switched) {
          switched = true;
          return options.find(option => option.startsWith("Switch save scope"));
        }
        if (!opened) {
          opened = true;
          return options.find(option => option.startsWith("Approvals"));
        }
        return "Done";
      });
      const input = vi.fn().mockResolvedValueOnce("1.1").mockResolvedValueOnce("0.975");
      const context = {
        mode: "rpc", cwd, isProjectTrusted: () => true,
        modelRegistry: { getAvailable: () => [] },
        ui: { theme, select, input, notify },
      } as unknown as ExtensionContext;
      await openFabricSettings(context, { state, applyFabricMode() {}, capturedTools: { list: () => [] } as unknown as CapturedToolCatalog });
      const file = scope === "project" ? path.join(cwd, ".pi", "fabric.json") : path.join(agentDir, "fabric.json");
      expect(JSON.parse(fs.readFileSync(file, "utf8"))).toMatchObject({ approvals: { model: "pi-fabric/typesafe/jev-latest" }, jev: { autoApprovalThreshold: 0.975 } });
      expect(loadFabricConfig({ cwd, agentDir, projectTrusted: true }).jev.autoApprovalThreshold).toBe(0.975);
      expect(config.jev.autoApprovalThreshold).toBe(0.975);
      expect(input).toHaveBeenCalledTimes(2);
      expect(notify).toHaveBeenCalledWith("Enter a probability between 0 and 1.", "warning");
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });
});
