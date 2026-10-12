import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext, Theme } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { CapturedToolCatalog } from "../src/capture/catalog.js";
import { loadFabricConfig, loadFabricConfigForScope, normalizeFabricConfig, saveFabricConfig } from "../src/config.js";
import type { FabricState } from "../src/fabric-state.js";
import { buildFabricSettingsItems, FabricSettingsComponent, openFabricSettings } from "../src/ui/settings.js";
import { SectionSubmenu, SelectSubmenu } from "../src/ui/settings-submenus.js";
import { buildPartial, coerceValue, summaryFor } from "../src/ui/settings-values.js";
import {
  DECISION_DOCUMENT_DEFAULT as DEFAULT,
  DECISION_PROFILE_SETTING_ID as ID,
  decisionProfileView,
  decisionTargetInfo,
} from "../src/ui/settings-decision-profiles.js";

const theme = { fg: (_: string, text: string) => text, bg: (_: string, text: string) => text, bold: (text: string) => text } as unknown as Theme;
const profiles = {
  version: 1 as const, defaultProfile: "native",
  profiles: {
    native: { provider: "typesafe", model: "jev-latest", api: "system-one" as const },
    generated: { provider: "anthropic", model: "configured-model", api: "anthropic" as const, allowGenerated: true },
    "default": { provider: "openrouter" },
  },
};
const fixture = (jev: Record<string, unknown> = { decisionProfiles: profiles }) => {
  const config = normalizeFabricConfig({ approvals: { model: "approval/model", execute: "ask" }, jev: { model: "jev-latest" } });
  // Deliberately allow malformed editor data so the UI's own boundary is tested.
  Object.assign(config.jev, jev);
  const apply = vi.fn((id: string, value: unknown) => {
    if (id === ID) {
      if (value === null) delete config.jev.decisionProfile;
      else config.jev.decisionProfile = String(value);
    }
  });
  const modelSource = vi.fn(() => ({ models: [], lastUsed: {} }));
  const items = buildFabricSettingsItems(theme, config, apply, { keepVisibleCandidates: [], get modelSource() { return modelSource(); } });
  const open = () => items.find(item => item.id === "decisions")!.submenu!("", () => {}) as SectionSubmenu;
  return { config, apply, items, open, modelSource };
};
const activate = <T>(section: SectionSubmenu | FabricSettingsComponent, id: string): T => {
  section.settingsList.selectItem(id);
  section.handleInput("\r");
  return (section.settingsList as unknown as { submenuComponent: T }).submenuComponent;
};
afterEach(() => vi.unstubAllEnvs());

describe("decision profile settings", () => {
  it("builds a separate Decisions section without model discovery or approval mutations", () => {
    const { items, open, config, modelSource } = fixture();
    const approvals = structuredClone(config.approvals);
    expect(items.find(item => item.id === "decisions")?.currentValue).toBe("Document default · native");
    const section = open();
    expect(section.items.find(item => item.id === "decisions.provider")?.currentValue).toBe("typesafe");
    expect(section.items.find(item => item.id === "decisions.model")?.currentValue).toBe("jev-latest");
    expect(section.items.find(item => item.id === "decisions.probability")?.currentValue).toContain("Native");
    expect(section.items.find(item => item.id === "decisions.images")?.currentValue).toContain("not verified");
    expect(section.items.some(item => item.id.startsWith("approvals."))).toBe(false);
    expect(config.approvals).toEqual(approvals);
    expect(modelSource).not.toHaveBeenCalled();
    const approvalSection = items.find(item => item.id === "approvals")!.submenu!("", () => {}) as SectionSubmenu;
    expect(approvalSection.items.some(item => item.id === ID)).toBe(false);
  });

  it("uses built-in terminal selection, refreshes informational rows and returns to document default", () => {
    const { config, apply, open } = fixture();
    const original = structuredClone(config.jev.decisionProfiles);
    const section = open();
    const picker = activate<SelectSubmenu>(section, ID);
    expect(picker.options.map(item => item.value)).toEqual([DEFAULT, "default", "generated", "native"]);
    picker.selectList.setSelectedIndex(2);
    picker.handleInput("\r");
    expect(apply).toHaveBeenLastCalledWith(ID, "generated");
    expect(section.items.find(item => item.id === "decisions.provider")?.currentValue).toBe("anthropic");
    expect(section.items.find(item => item.id === "decisions.probability")?.currentValue).toContain("Generated");
    expect(summaryFor("decisions", config)).toBe("generated");
    activate<SelectSubmenu>(section, ID).selectRpc(DEFAULT);
    expect(apply).toHaveBeenLastCalledWith(ID, null);
    expect(section.items.find(item => item.id === "decisions.provider")?.currentValue).toBe("typesafe");
    expect(config.jev.decisionProfiles).toEqual(original);
    expect(config.approvals.model).toBe("approval/model");
    expect(config.jev.model).toBe("jev-latest");
  });

  it("keeps literal default as a valid profile and cancels without saving", () => {
    const { apply, open } = fixture();
    const section = open();
    activate<SelectSubmenu>(section, ID).handleInput("\x1b");
    expect(apply).not.toHaveBeenCalled();
    activate<SelectSubmenu>(section, ID).selectRpc("default");
    expect(apply).toHaveBeenCalledExactlyOnceWith(ID, "default");
  });

  it.each(["", " ", "missing", "native\n", "../native", "é", "x".repeat(129), "__proto__", "constructor"])("rejects an invalid or missing profile %j, never falling back", selection => {
    const { config, apply, open } = fixture({ decisionProfiles: profiles, decisionProfile: selection });
    expect(decisionProfileView(config.jev).error).toBeDefined();
    const section = open();
    expect(section.items.some(item => item.id === "decisions.provider")).toBe(false);
    const picker = activate<SelectSubmenu>(section, ID);
    expect(picker.selectRpc(selection)).toBe(false);
    section.applyChange(ID, selection);
    expect(apply).not.toHaveBeenCalled();
    expect(() => coerceValue(ID, selection, config)).toThrow();
    picker.selectRpc(DEFAULT);
    expect(apply).toHaveBeenCalledExactlyOnceWith(ID, null);
  });

  it.each([null, {}, { ...profiles, version: 2 }, { version: 1, profiles: { bad: { model: "missing-provider" } } }, { version: 1, profiles: { "bad name": { provider: "typesafe" } } }, { version: 1, profiles: { "native\n": { provider: "typesafe" } } }])("fails closed on a malformed profile document %j", document => {
    const { config, apply, open } = fixture({ decisionProfiles: document });
    const section = open();
    expect(section.items.find(item => item.id === ID)?.submenu).toBeUndefined();
    expect(section.items.find(item => item.id === "decisions.validation")?.description).toContain("Invalid profile document");
    section.applyChange(ID, DEFAULT);
    expect(apply).not.toHaveBeenCalled();
    expect(() => coerceValue(ID, DEFAULT, config)).toThrow();
    expect(config.jev.decisionProfiles).toEqual(document);
  });

  it("offers configuration instructions without inventing a configured profile", () => {
    const { apply, open } = fixture({});
    const section = open();
    expect(section.items.find(item => item.id === "decisions.configure")?.description).toContain("jev.decisionProfiles");
    const picker = activate<SelectSubmenu>(section, ID);
    expect(picker.options.map(item => item.value)).toEqual([DEFAULT]);
    picker.handleInput("\x1b");
    expect(apply).not.toHaveBeenCalled();
    expect(buildPartial(ID, null)).toEqual({ jev: { decisionProfile: null } });
  });

  it("accepts bounded ASCII names literally, without treating dots as config paths", () => {
    for (const name of ["a", "local.fast_v1-2", "x".repeat(128)]) {
      const { config, apply, open } = fixture({ decisionProfiles: { version: 1, profiles: { [name]: { provider: "custom", model: "configured" } } } });
      activate<SelectSubmenu>(open(), ID).selectRpc(name);
      expect(apply).toHaveBeenCalledExactlyOnceWith(ID, name);
      expect(buildPartial(ID, coerceValue(ID, name, config))).toEqual({ jev: { decisionProfile: name } });
    }
  });

  it("does not infer availability or probability calibration from configured targets", () => {
    expect(decisionTargetInfo({ provider: "custom", api: "google" }).probability).toContain("requires allowGenerated");
    expect(decisionTargetInfo({ provider: "custom", api: "llama-cpp" }).probability).toContain("Token log-probabilities");
    expect(decisionTargetInfo({ provider: "unknown" }).images).toContain("not verified");
    expect(decisionTargetInfo({ provider: "openrouter" }).images).toContain("Unsupported");
    expect(decisionTargetInfo({ provider: "typesafe" }).probability).toContain("Native");
    expect(decisionTargetInfo({ provider: "anthropic", allowGenerated: true }).probability).toContain("Generated");
  });

  it("rebuilds Decisions from the chosen terminal save scope", () => {
    const project = fixture({ decisionProfiles: profiles, decisionProfile: "generated" });
    const global = fixture();
    const scope = vi.fn();
    const component = new FabricSettingsComponent(theme, project.items, vi.fn(), vi.fn(), {
      initialSaveScope: "project", onSaveScopeChange: scope,
      itemsForSaveScope: value => value === "global" ? global.items : project.items,
    });
    expect(activate<SectionSubmenu>(component, "decisions").items.find(item => item.id === ID)?.currentValue).toBe("generated");
    component.handleInput("\x07");
    expect(scope).toHaveBeenCalledExactlyOnceWith("global");
    expect(activate<SectionSubmenu>(component, "decisions").items.find(item => item.id === ID)?.currentValue).toBe(DEFAULT);
  });

  it("persists a document-default tombstone over an inherited selector without copying the profile document", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-decision-layers-"));
    const location = { cwd: path.join(root, "project"), agentDir: path.join(root, "agent"), projectTrusted: true };
    fs.mkdirSync(location.cwd, { recursive: true });
    try {
      saveFabricConfig({ ...location, scope: "global" }, { jev: { decisionProfiles: profiles, decisionProfile: "generated" } });
      const config = loadFabricConfigForScope(location, "project");
      const items = buildFabricSettingsItems(theme, config, (id, value) => {
        saveFabricConfig({ ...location, scope: "project" }, buildPartial(id, value));
        Object.assign(config, loadFabricConfigForScope(location, "project"));
      }, { keepVisibleCandidates: [], modelSource: { models: [], lastUsed: {} } });
      const section = items.find(item => item.id === "decisions")!.submenu!("", () => {}) as SectionSubmenu;
      expect(section.items.find(item => item.id === ID)?.currentValue).toBe("generated");
      activate<SelectSubmenu>(section, ID).selectRpc(DEFAULT);
      expect(decisionProfileView(loadFabricConfig(location).jev).selected).toBe("native");
      const saved = JSON.parse(fs.readFileSync(path.join(location.cwd, ".pi", "fabric.json"), "utf8"));
      expect(saved.jev).toEqual({ decisionProfile: null });
      expect(loadFabricConfigForScope(location, "global").jev.decisionProfile).toBe("generated");
      expect(section.items.find(item => item.id === "decisions.provider")?.currentValue).toBe("typesafe");
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.each(["project", "global"] as const)("persists and resets only the %s selector through RPC without discovery or policy reload", async scope => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-decision-ui-"));
    const cwd = path.join(root, "project");
    const agentDir = path.join(root, "agent");
    fs.mkdirSync(path.join(cwd, ".pi"), { recursive: true });
    fs.mkdirSync(agentDir, { recursive: true });
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    const globalFile = path.join(agentDir, "fabric.json");
    const projectFile = path.join(cwd, ".pi", "fabric.json");
    const original = { jev: { decisionProfiles: profiles, autoApprovalThreshold: 0.9 }, approvals: { model: "approval/model", execute: "ask" } };
    fs.writeFileSync(globalFile, JSON.stringify(original));
    fs.writeFileSync(projectFile, JSON.stringify({ ui: { widget: "hidden" } }));
    try {
      const config = loadFabricConfig({ cwd, agentDir, projectTrusted: true });
      // Deliberately diverge live policy: a selector save must not reload disk policy.
      config.approvals.execute = "deny";
      const reloadConfig = vi.fn();
      const claudeModels = vi.fn(async () => []);
      const setDecisionProfile = vi.fn((profile: string | null | undefined) => {
        if (profile === undefined) delete config.jev.decisionProfile;
        else config.jev.decisionProfile = profile;
      });
      const state = { config, ensure: vi.fn(async () => {}), reloadConfig, setDecisionProfile, agents: { claudeModels }, kernelReloadRequired: true } as unknown as FabricState;
      const applyFabricMode = vi.fn();
      const reloadResources = vi.fn();
      const registry = { getAvailable: vi.fn(() => []), getModelsOfType: vi.fn(() => []), getAvailableOfType: vi.fn(async () => []), getProviderAuth: vi.fn(), getProviderAuthStatus: vi.fn() };
      let switched = false;
      let opened = false;
      let edits = 0;
      const select = vi.fn(async (title: string, options: string[]): Promise<string | undefined> => {
        if (title.startsWith("Fabric settings › Decisions › Decision profile")) {
          return options.find(option => option.startsWith(edits === 1 ? "generated" : DEFAULT));
        }
        if (title.startsWith("Fabric settings › Decisions")) {
          if (edits === 1) {
            const file = scope === "project" ? projectFile : globalFile;
            expect(JSON.parse(fs.readFileSync(file, "utf8")).jev.decisionProfile).toBe("generated");
            expect(config.jev.decisionProfile).toBe("generated");
            expect(options.some(option => option.startsWith("Selected provider · anthropic"))).toBe(true);
          }
          if (edits++ < 2) return options.find(option => option.startsWith("Decision profile"));
          expect(options.some(option => option.startsWith("Selected provider · typesafe"))).toBe(true);
          return "← Back";
        }
        if (scope === "global" && !switched) { switched = true; return options.find(option => option.startsWith("Switch save scope")); }
        if (!opened) { opened = true; return options.find(option => option.startsWith("Decisions")); }
        return "Done";
      });
      const notify = vi.fn();
      const context = { mode: "rpc", cwd, isProjectTrusted: () => true, modelRegistry: registry, ui: { theme, select, notify, input: vi.fn() } } as unknown as ExtensionContext;
      await openFabricSettings(context, { state, applyFabricMode, reloadResources, capturedTools: { list: () => [] } as unknown as CapturedToolCatalog });
      const savedGlobal = JSON.parse(fs.readFileSync(globalFile, "utf8"));
      const savedProject = JSON.parse(fs.readFileSync(projectFile, "utf8"));
      expect(savedGlobal.jev.decisionProfiles).toEqual(profiles);
      expect(savedGlobal.approvals).toEqual(original.approvals);
      expect((scope === "project" ? savedProject : savedGlobal).jev.decisionProfile).toBeNull();
      expect(savedProject.jev?.decisionProfiles).toBeUndefined();
      expect(loadFabricConfig({ cwd, agentDir, projectTrusted: true }).jev.decisionProfile ?? null).toBeNull();
      expect(config.approvals.execute).toBe("deny");
      expect(config.approvals.model).toBe("approval/model");
      expect(config.jev.autoApprovalThreshold).toBe(0.9);
      expect(setDecisionProfile.mock.calls).toEqual([["generated"], [null]]);
      expect(reloadConfig).not.toHaveBeenCalled();
      expect(reloadResources).not.toHaveBeenCalled();
      expect(applyFabricMode).not.toHaveBeenCalled();
      expect(claudeModels).not.toHaveBeenCalled();
      for (const fn of Object.values(registry)) expect(fn).not.toHaveBeenCalled();
      expect(notify).toHaveBeenCalledWith("Decision profile saved. Approvals and chat model unchanged.", "info");
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });
});
