import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext, Theme } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_EXTRACTIVE_CONFIG, DEFAULT_FABRIC_CONFIG, loadFabricConfig, normalizeFabricConfig, saveFabricConfig } from "../src/config.js";
import type { FabricState } from "../src/fabric-state.js";
import type { CapturedToolCatalog } from "../src/capture/catalog.js";
import { openFabricSettings } from "../src/ui/settings.js";
import { buildExtractiveSection } from "../src/ui/settings-sections-lifecycle.js";
import { SectionSubmenu, SelectSubmenu } from "../src/ui/settings-submenus.js";
import { EXTRACTIVE_CONSENT, EXTRACTIVE_LOCAL, EXTRACTIVE_OFF, buildPartial } from "../src/ui/settings-values.js";

const theme = { fg: (_color: string, text: string) => text, bg: (_color: string, text: string) => text, bold: (text: string) => text } as unknown as Theme;
afterEach(() => vi.unstubAllEnvs());

describe("extractive settings", () => {
  it("defaults OFF, normalizes finite limits, and survives JSON worker/runtime round trips", () => {
    expect(normalizeFabricConfig({}).memory.extractive).toEqual(DEFAULT_EXTRACTIVE_CONFIG);
    const config = normalizeFabricConfig({ memory: { extractive: { enabled: true, provider: "local", model: "org/classifier", maxEvaluationsPerTurn: 999, timeoutMs: Infinity, maxCandidates: 999, maxViewBytes: 999999, maxSourceChars: 9999999 } } });
    expect(config.memory.extractive).toEqual({ enabled: true, provider: "local", model: "org/classifier", maxEvaluationsPerTurn: 1, timeoutMs: 3000, maxCandidates: 256, maxViewBytes: 32768, maxSourceChars: 100000 });
    expect(normalizeFabricConfig(JSON.parse(JSON.stringify(config))).memory.extractive).toEqual(config.memory.extractive);
    expect(normalizeFabricConfig({ memory: { extractive: { enabled: "true", maxEvaluationsPerTurn: -1 } } }).memory.extractive).toMatchObject({ enabled: false, maxEvaluationsPerTurn: 0 });
  });

  it("requires explicit charged-text opt-in and offers all native classifiers rather than chat picks", () => {
    const item = buildExtractiveSection({ config: DEFAULT_FABRIC_CONFIG, theme, persist: vi.fn(), options: { classifierModels: ["local/custom", "remote/org/classifier"], availableClassifierModels: ["local/custom"] } });
    expect(item.label).toBe("Extractive history");
    expect(item.description).toContain("selected native classifier");
    expect(item.description).toContain("API charges");
    expect(item.description).toContain("No secret scanning");
    const section = item.submenu!("", () => {}) as SectionSubmenu;
    const mode = section.items.find((row) => row.id === "memory.extractive.mode")!;
    const modes = mode.submenu!(mode.currentValue, () => {}) as SelectSubmenu;
    expect(modes.options.map((o) => o.value)).toEqual([EXTRACTIVE_OFF, EXTRACTIVE_LOCAL, EXTRACTIVE_CONSENT]);
    expect(EXTRACTIVE_CONSENT).toContain("API charges");
    const classifier = section.items.find((row) => row.id === "memory.extractive.classifier")!;
    const picker = classifier.submenu!(classifier.currentValue, () => {}) as SelectSubmenu;
    expect(picker.options.map((o) => o.value)).toContain("remote/org/classifier");
    expect(classifier.description).toContain("Available now: local/custom");
    expect(buildPartial("memory.extractive.classifier", "remote/org/classifier")).toEqual({ memory: { extractive: { provider: "remote", model: "org/classifier" } } });
    expect(buildPartial("memory.extractive.mode", EXTRACTIVE_LOCAL)).toEqual({ memory: { extractive: { enabled: true, maxEvaluationsPerTurn: 0 } } });
  });

  it("persists UI model and mode, reloads runtime config and emits cancellation on disable", async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), "extractive-settings-"));
    const agentDir = path.join(cwd, "agent");
    fs.mkdirSync(agentDir);
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    const location = { cwd, agentDir, projectTrusted: true };
    const config = loadFabricConfig(location);
    const emit = vi.fn();
    const state = { config, pi: { events: { emit } }, ensure: vi.fn(), reloadConfig: vi.fn(() => Object.assign(config, loadFabricConfig(location))), agents: { claudeModels: async () => [] } } as unknown as FabricState;
    const getModelsOfType = vi.fn(() => [{ provider: "custom", id: "org/native" }]);
    const getAvailableOfType = vi.fn(async () => [{ provider: "custom", id: "org/native" }]);
    const context = { cwd, mode: "tui", isProjectTrusted: () => true,
      modelRegistry: { getAvailable: () => [], getModelsOfType, getAvailableOfType },
      ui: { notify: vi.fn(), custom: vi.fn(async (factory: any) => {
        const component = factory({ requestRender: vi.fn() }, theme, {}, () => {});
        const item = component.settingsList.items.find((row: { id: string }) => row.id === "memory");
        const section = item.submenu("", () => {}) as SectionSubmenu;
        const classifier = section.items.find((row) => row.id === "memory.extractive.classifier")!;
        expect((classifier.submenu!(classifier.currentValue, () => {}) as SelectSubmenu).options.map((o) => o.value)).toContain("custom/org/native");
        section.applyChange("memory.extractive.classifier", "custom/org/native");
        section.applyChange("memory.extractive.mode", EXTRACTIVE_CONSENT);
        expect(config.memory.extractive).toMatchObject({ enabled: true, provider: "custom", model: "org/native", maxEvaluationsPerTurn: 1 });
        section.applyChange("memory.extractive.mode", EXTRACTIVE_OFF);
      }) },
    } as unknown as ExtensionContext;
    try {
      await openFabricSettings(context, { state, applyFabricMode: vi.fn(), capturedTools: { list: () => [] } as unknown as CapturedToolCatalog });
      expect(getModelsOfType).toHaveBeenCalledWith("classifier");
      expect(getAvailableOfType).toHaveBeenCalledWith("classifier", undefined, expect.objectContaining({ signal: expect.any(AbortSignal) }));
      expect(emit).toHaveBeenCalledTimes(3);
      expect(emit).toHaveBeenCalledWith("pi-fabric:extractive-config-changed", {});
      expect(loadFabricConfig(location).memory.extractive).toMatchObject({ enabled: false, provider: "custom", model: "org/native", maxEvaluationsPerTurn: 0 });
      // Manual JSON opt-in is explicit consent too; no separate hidden TUI bit.
      saveFabricConfig({ ...location, scope: "project" }, { memory: { extractive: { enabled: true, maxEvaluationsPerTurn: 1 } } });
      expect(loadFabricConfig(location).memory.extractive).toMatchObject({ enabled: true, maxEvaluationsPerTurn: 1 });
    } finally { fs.rmSync(cwd, { recursive: true, force: true }); }
  });
});
