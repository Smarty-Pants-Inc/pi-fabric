import type { SettingItem } from "@earendil-works/pi-tui";
import type { SettingsSectionContext } from "./settings-section-context.js";
import { sectionSubmenu, SelectSubmenu, setting } from "./settings-submenus.js";
import {
  DECISION_DOCUMENT_DEFAULT,
  DECISION_PROFILE_SETTING_ID,
  decisionProfileSelection,
  decisionProfileSummary,
  decisionProfileView,
  decisionTargetInfo,
} from "./settings-decision-profiles.js";

const disclosure = "Routing for jev.decide only. Does not change Approvals, legacy Jev evaluation, or the default chat model. Configuration only: credentials, availability and image support are not probed.";
const configure = "Configure jev.decisionProfiles in fabric.json: {version:1, defaultProfile?:name, profiles:{name:{provider, model?, api?}}}. Names: ASCII letters, digits, _, . or - (1–128). jev.models is available programmatically.";

export const buildDecisionsSection = (
  { config, theme, persist }: Pick<SettingsSectionContext, "config" | "theme" | "persist">,
): SettingItem => setting("decisions", "Decisions", decisionProfileSummary(config.jev), {
  description: disclosure,
  submenu: sectionSubmenu(theme, "Decisions", disclosure, () => {
    const view = decisionProfileView(config.jev);
    const names = Object.keys(view.document?.profiles ?? {}).sort();
    const current = config.jev.decisionProfile == null ? DECISION_DOCUMENT_DEFAULT
      : view.error ? "Invalid selection" : view.selected!;
    const rows: SettingItem[] = [setting(DECISION_PROFILE_SETTING_ID, "Decision profile", current, {
      description: "Overrides the profile document's default in this save scope. Document default clears only this selector, not the profiles. Project overrides can remain active when editing global defaults.",
      ...(config.jev.decisionProfiles !== undefined && !view.document ? {} : {
        submenu: (_current: string, done: (value?: string) => void) => new SelectSubmenu(
          theme, "Decision profile", disclosure,
          [
            { value: DECISION_DOCUMENT_DEFAULT, label: DECISION_DOCUMENT_DEFAULT,
              description: view.document?.defaultProfile ? `Use ${view.document.defaultProfile}` : "No document default; use the decision API's default route" },
            ...names.map(name => ({ value: name, label: name,
              description: `${view.document!.profiles[name]!.provider} / ${view.document!.profiles[name]!.model ?? "provider default model"} · configured, not verified` })),
          ], current, value => done(value), () => done(),
        ),
      }),
    })];
    if (view.error) rows.push(setting("decisions.validation", "Configuration", "Needs attention", { description: view.error }));
    if (!names.length) rows.push(setting("decisions.configure", "Configure profiles", "No profiles configured", { description: configure }));
    if (view.target) {
      const info = decisionTargetInfo(view.target);
      rows.push(
        setting("decisions.provider", "Selected provider", view.target.provider, { description: "Configured target only; no availability or credential check." }),
        setting("decisions.model", "Selected model", view.target.model ?? "Provider default", { description: "Configured model, not a verified live model." }),
        setting("decisions.api", "Decision API", view.target.api ?? "Provider default"),
        setting("decisions.probability", "Probability mode", info.probability),
        setting("decisions.images", "Images", info.images, { description: "Images may be rejected by the selected provider, model or API. Selection does not validate image support." }),
      );
    }
    return rows;
  }, (id, value) => {
    if (id !== DECISION_PROFILE_SETTING_ID) return;
    try { decisionProfileSelection(config.jev, value); } catch { return; }
    persist(id, value);
  }),
});
