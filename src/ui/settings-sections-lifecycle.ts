import type { SettingItem } from "@earendil-works/pi-tui";
import { DEFAULT_EXTRACTIVE_CONFIG } from "../config.js";
import type { SettingsSectionContext } from "./settings-section-context.js";
import {
  setting,
  sectionSubmenu,
  compactionThresholdSubmenu,
  numericSubmenu,
  stringOptionsSubmenu,
} from "./settings-submenus.js";
import {
  summaryFor,
  EXTRACTIVE_OFF, EXTRACTIVE_LOCAL, EXTRACTIVE_CONSENT,
  COMPACTION_THRESHOLD_SETTING_ID,
  formatCompactionThreshold,
  COMPACTION_ENGINES,
  COMPACTION_TARGET_RATIOS,
  formatRetention,
  BOOLEANS,
  ACTOR_SCOPES,
  formatMs,
} from "./settings-values.js";

export const buildCompactionSection = (
  { config, theme, options, persist }: Pick<SettingsSectionContext<"activeModelKey">, "config" | "theme" | "options" | "persist">,
): SettingItem => {
  return setting("compaction", "Compaction", summaryFor("compaction", config), {
    description: "Compaction engine used at session compaction boundaries.",
    submenu: sectionSubmenu(
      theme,
      "Compaction",
      "Choose Fabric deterministic compaction or Pi core model-driven compaction.",
      [
        ...(options.activeModelKey
          ? [setting(
              COMPACTION_THRESHOLD_SETTING_ID,
              "Threshold",
              formatCompactionThreshold(config, options.activeModelKey),
              {
                description:
                  `Context usage that triggers compaction for ${options.activeModelKey}, as a percent of its window or an exact token count.`,
                submenu: compactionThresholdSubmenu(theme),
              },
            )]
          : []),
        setting("compaction.engine", "Engine", config.compaction.engine, {
          description:
            "Fabric uses deterministic branch summaries; Pi delegates compaction to Pi core.",
          values: COMPACTION_ENGINES,
        }),
        setting(
          "compaction.targetContextRatio",
          "Max occupancy",
          String(config.compaction.targetContextRatio),
          {
            description:
              "Hard post-compaction occupancy ceiling; Fabric normally keeps Pi's bounded recent-token tail instead.",
            values: COMPACTION_TARGET_RATIOS,
          },
        ),
      ],
      persist,
    ),
  });
};

export const buildExtractiveSection = (
  { config, theme, persist, options }: Pick<SettingsSectionContext<"classifierModels" | "availableClassifierModels">, "config" | "theme" | "persist" | "options">,
): SettingItem => {
  const extractive = config.memory.extractive ?? DEFAULT_EXTRACTIVE_CONFIG;
  const mode = !extractive.enabled ? EXTRACTIVE_OFF : extractive.maxEvaluationsPerTurn === 0 ? EXTRACTIVE_LOCAL : EXTRACTIVE_CONSENT;
  const disclosure = "Opt-in: sends bounded current-session active-branch user/assistant text to the selected native classifier (API charges). No thinking/tool output. No secret scanning. Scores are salience, not truth. Raw history and compaction are unchanged.";
  return setting("memory", "Classifier-assisted extractive history (Jev supported)", summaryFor("memory", config), {
    description: disclosure,
    submenu: sectionSubmenu(theme, "Extractive history", disclosure, [
      setting("memory.extractive.mode", "Consent / mode", mode, {
        description: disclosure,
        submenu: stringOptionsSubmenu(theme, [EXTRACTIVE_OFF, EXTRACTIVE_LOCAL, EXTRACTIVE_CONSENT], "Explicit classifier opt-in", disclosure),
      }),
      setting("memory.extractive.classifier", "Native classifier", `${extractive.provider}/${extractive.model}`, {
        description: `Native registry classifiers only. Available now: ${options.availableClassifierModels?.join(", ") || "none (deterministic fallback; configure credentials)"}.`,
        submenu: stringOptionsSubmenu(theme, options.classifierModels ?? [], "Select native classifier", "Any registered classifier; unavailable credentials produce deterministic fallback. Selection does not enable inference."),
      }),
      ...([
        ["maxViewBytes", "View bytes", [1024, 4096, 8192, 16384, 32768]],
        ["maxCandidates", "Candidate bundles", [16, 32, 64, 128, 256]],
        ["maxSourceChars", "Source character budget", [4000, 12000, 24000, 48000]],
        ["timeoutMs", "Classifier timeout ms", [1000, 2000, 3000, 5000, 10000]],
      ] as const).map(([key, label, values]) => setting(`memory.extractive.${key}`, label, String(extractive[key]), {
        submenu: numericSubmenu(theme, values, String, label, "Whole bundles or omission with source addresses; never clipped evidence."),
      })),
    ], persist),
  });
};

export const buildRetentionSection = (
  { config, theme, persist }: Pick<SettingsSectionContext, "config" | "theme" | "persist">,
): SettingItem => {
  return setting("retention", "Retention", summaryFor("retention", config), {
    description: "Age-based cleanup for inactive Fabric run artifacts.",
    submenu: sectionSubmenu(
      theme,
      "Retention",
      "Cleanup removes dead temporary roots and terminal run artifacts, and bounds older terminal event logs. Active runs, latest actor runs and actor session.jsonl files are never modified.",
      [
        setting(
          "retention.orphanedTempRunMs",
          "Orphaned temp runs",
          formatRetention(config.retention.orphanedTempRunMs),
          {
            description: "Remove temporary run roots this long after their owner process dies.",
            submenu: numericSubmenu(
              theme,
              [3_600_000, 3 * 3_600_000, 6 * 3_600_000, 12 * 3_600_000, 24 * 3_600_000],
              formatRetention,
              "Orphaned temp runs",
              "Remove temporary run roots this long after their owner process dies.",
            ),
          },
        ),
        setting(
          "retention.oneShotRunMs",
          "One-shot runs",
          formatRetention(config.retention.oneShotRunMs),
          {
            description: "Retain completed one-shot agent run artifacts for this duration.",
            submenu: numericSubmenu(
              theme,
              [6 * 3_600_000, 12 * 3_600_000, 24 * 3_600_000, 2 * 86_400_000, 3 * 86_400_000, 7 * 86_400_000],
              formatRetention,
              "One-shot runs",
              "Retain completed one-shot agent run artifacts for this duration.",
            ),
          },
        ),
        setting(
          "retention.actorRunArchiveMs",
          "Actor run archives",
          formatRetention(config.retention.actorRunArchiveMs),
          {
            description: "Retain terminal actor run archives for this duration; the latest run is always preserved.",
            submenu: numericSubmenu(
              theme,
              [86_400_000, 3 * 86_400_000, 7 * 86_400_000, 14 * 86_400_000, 30 * 86_400_000, 90 * 86_400_000],
              formatRetention,
              "Actor run archives",
              "Retain terminal actor run archives for this duration; the latest run is always preserved.",
            ),
          },
        ),
        setting(
          "retention.terminalRunEventsAgeMs",
          "Terminal event log age",
          formatRetention(config.retention.terminalRunEventsAgeMs),
          {
            description: "Bound older terminal event logs; live and latest actor runs are exempt.",
            submenu: numericSubmenu(theme, [3_600_000, 6 * 3_600_000, 86_400_000, 3 * 86_400_000, 7 * 86_400_000],
              formatRetention, "Terminal event log age", "Bound terminal event logs after this age."),
          },
        ),
        setting(
          "retention.terminalRunEventsMaxBytes",
          "Terminal event log tail",
          `${config.retention.terminalRunEventsMaxBytes / 1024} KiB`,
          {
            description: "Retain complete trailing event lines within this byte cap, including a truncation marker.",
            submenu: numericSubmenu(theme, [64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024],
              (bytes) => `${bytes / 1024} KiB`, "Terminal event log tail", "Maximum retained terminal events.jsonl bytes."),
          },
        ),
      ],
      persist,
    ),
  });
};

export const buildMeshSection = (
  { config, theme, persist }: Pick<SettingsSectionContext, "config" | "theme" | "persist">,
): SettingItem => {
  return setting("mesh", "Mesh", summaryFor("mesh", config), {
    description: "Durable mesh coordination store and actors.",
    submenu: sectionSubmenu(
      theme,
      "Mesh",
      "Durable mesh coordination store and actors.",
      [
        setting("mesh.enabled", "Enabled", config.mesh.enabled ? "true" : "false", {
          description: "Enable the durable mesh store and actor providers.",
          values: BOOLEANS,
        }),
        setting("mesh.actorScope", "Actor scope", config.mesh.actorScope, {
          description:
            'Default storage for newly created actors. Each agents.create call may choose project or session independently; project actors are shared, while session actors follow the root Pi session and its participant agents.',
          values: ACTOR_SCOPES,
        }),
        setting("mesh.maxReadEvents", "Max read events", String(config.mesh.maxReadEvents), {
          description: "Maximum events returned by a single mesh read.",
          submenu: numericSubmenu(
            theme,
            [100, 200, 500, 1000, 5000],
            String,
            "Max read events",
            "Maximum events returned by a single mesh read.",
          ),
        }),
        setting("mesh.actorPollMs", "Actor poll fallback", formatMs(config.mesh.actorPollMs), {
          description: "Fallback polling interval when mesh filesystem notifications are unavailable.",
          submenu: numericSubmenu(
            theme,
            [50, 100, 250, 500, 1000],
            formatMs,
            "Actor poll fallback",
            "Fallback polling interval when mesh filesystem notifications are unavailable.",
          ),
        }),
        setting("mesh.actorQueueLimit", "Actor queue limit", String(config.mesh.actorQueueLimit), {
          description: "Maximum messages queued per actor mailbox.",
          submenu: numericSubmenu(
            theme,
            [4, 8, 16, 32, 64, 128],
            String,
            "Actor queue limit",
            "Maximum messages queued per actor mailbox.",
          ),
        }),
        setting("mesh.actorContextEntries", "Actor context entries", String(config.mesh.actorContextEntries), {
          description: "Transcript entries forwarded to actors as context.",
          submenu: numericSubmenu(
            theme,
            [3, 5, 10, 14, 20, 50],
            String,
            "Actor context entries",
            "Transcript entries forwarded to actors as context.",
          ),
        }),
        setting("mesh.eventContextChars", "Event context chars", config.mesh.eventContextChars.toLocaleString(), {
          description: "Character cap applied to host events dispatched to actors.",
          submenu: numericSubmenu(
            theme,
            [10_000, 20_000, 40_000, 80_000, 160_000],
            (n) => n.toLocaleString(),
            "Event context chars",
            "Character cap applied to host events dispatched to actors.",
          ),
        }),
      ],
      persist,
    ),
  });
};
