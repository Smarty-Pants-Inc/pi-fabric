import { normalizeDecisionProfiles } from "../jev/decision-profiles.js";
import type { DecisionProfiles, DecisionTarget } from "../jev/decision-types.js";

export const DECISION_PROFILE_SETTING_ID = "jev.decisionProfile";
// Spaces deliberately distinguish this action from every legal profile name.
export const DECISION_DOCUMENT_DEFAULT = "Document default";

type DecisionSettings = { decisionProfiles?: unknown; decisionProfile?: unknown };
const profileName = (value: unknown): value is string =>
  typeof value === "string" && value.length >= 1 && value.length <= 128 && !/[^A-Za-z0-9_.-]/.test(value);
export type DecisionProfileView = {
  document?: DecisionProfiles | undefined;
  selected?: string;
  target?: DecisionTarget | undefined;
  error?: string;
};

/** Inspect routing data only: no registry, credentials, commands or inference. */
export const decisionProfileView = (config: DecisionSettings): DecisionProfileView => {
  let document: DecisionProfiles | undefined;
  try {
    if (config.decisionProfiles !== undefined) {
      document = normalizeDecisionProfiles(config.decisionProfiles);
      if (Object.entries(document.profiles).some(([name, target]) => !profileName(name) || !profileName(target.provider))) {
        throw new Error("Invalid profile name or provider");
      }
    }
  } catch {
    return { error: "Invalid profile document; fix jev.decisionProfiles in fabric.json." };
  }
  const selector = config.decisionProfile;
  if (selector !== undefined && selector !== null &&
      !profileName(selector)) {
    return { document, error: "Invalid profile selection; choose a configured profile or Document default." };
  }
  const selected = typeof selector === "string" ? selector : document?.defaultProfile;
  if (selected === undefined) return { document };
  if (!document || !Object.hasOwn(document.profiles, selected)) {
    return { document, error: "Selected profile is missing; choose a configured profile or Document default." };
  }
  return { document, selected, target: document.profiles[selected] };
};

export const decisionProfileSummary = (config: DecisionSettings): string => {
  const view = decisionProfileView(config);
  if (view.error) return "Invalid configuration";
  if (config.decisionProfile != null) return view.selected!;
  return view.selected ? `${DECISION_DOCUMENT_DEFAULT} · ${view.selected}` : DECISION_DOCUMENT_DEFAULT;
};

/** Only the explicit reset action can clear the scalar; never rewrite the document. */
export const decisionProfileSelection = (config: DecisionSettings, value: string): string | null => {
  const view = decisionProfileView(config);
  if (config.decisionProfiles !== undefined && !view.document) throw new Error("Invalid decision profile document");
  if (value === DECISION_DOCUMENT_DEFAULT) return null;
  if (!profileName(value) || !view.document || !Object.hasOwn(view.document.profiles, value)) {
    throw new Error("Choose a configured decision profile or Document default");
  }
  return value;
};

export const decisionTargetInfo = (target: DecisionTarget): { probability: string; images: string } => {
  // Provider-family hints are informational, not runtime target resolution.
  const familyApi: Record<string, DecisionTarget["api"]> = {
    typesafe: "system-one", openrouter: "system-one",
    vercel: "system-one", "vercel-ai-gateway": "system-one",
    "vercel-evaluate": "vercel-evaluate", "vercel-decisions": "openai-decisions",
    openai: "openai-decisions", "cloudflare-workers-ai": "cloudflare",
    "llama.cpp": "llama-cpp", "llama-system-one": "llama-system-one",
    anthropic: "anthropic", google: "google",
  };
  const api = target.api ?? (Object.hasOwn(familyApi, target.provider) ? familyApi[target.provider] : undefined);
  const images = target.provider === "openrouter" || api === "llama-cpp"
    ? "Unsupported by this decision adapter"
    : "Provider/model/API-dependent; not verified";
  switch (api) {
    case "anthropic":
    case "google":
      return { probability: target.allowGenerated ? "Generated (opted in; not calibrated)" : "Generated (requires allowGenerated)", images };
    case "llama-cpp":
      return { probability: "Token log-probabilities (calibration not verified)", images };
    case "system-one":
    case "llama-system-one":
    case "cloudflare":
    case "openai-decisions":
    case "vercel-evaluate":
      return { probability: "Native decisions (calibration not verified)", images };
    default:
      return { probability: `Provider/API-dependent; generated ${target.allowGenerated ? "allowed" : "not opted in"}`, images };
  }
};
