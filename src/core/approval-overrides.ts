/** Per-action approval override: replaces the risk-class mode for matching refs. */
export type FabricActionApprovalMode = "allow" | "ask" | "deny";

const MAX_APPROVAL_ACTION_OVERRIDES = 256;

const PROVIDER = "[a-z][a-z0-9_-]*";
// Exact refs are provider.action (actions may contain further dots, e.g. MCP).
const EXACT_ACTION_REF = new RegExp(`^${PROVIDER}\\.[^\\s*]{1,256}$`);
const PROVIDER_WILDCARD = new RegExp(`^${PROVIDER}\\.\\*$`);

const isMode = (value: unknown): value is FabricActionApprovalMode =>
  value === "allow" || value === "ask" || value === "deny";

/**
 * Parse `approvals.actions`. Keys are exact refs (`delegate.dispatch`) or a
 * provider wildcard (`delegate.*`); every other form is a hard config error,
 * because a silently dropped `deny` would widen authority.
 */
export const approvalActionOverridesValue = (
  value: unknown,
): Record<string, FabricActionApprovalMode> | undefined => {
  if (value === undefined) return undefined;
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("approvals.actions must be an object of action ref → allow | ask | deny");
  }
  const entries = Object.entries(value as Record<string, unknown>);
  if (entries.length > MAX_APPROVAL_ACTION_OVERRIDES) {
    throw new Error(`approvals.actions supports at most ${MAX_APPROVAL_ACTION_OVERRIDES} entries`);
  }
  const overrides: Record<string, FabricActionApprovalMode> = {};
  for (const [key, mode] of entries) {
    if (!PROVIDER_WILDCARD.test(key) && (key.includes("*") || !EXACT_ACTION_REF.test(key))) {
      throw new Error(
        `Invalid approvals.actions key ${JSON.stringify(key)}: use an exact provider.action ref or provider.*`,
      );
    }
    if (!isMode(mode)) {
      throw new Error(
        `Invalid approvals.actions[${JSON.stringify(key)}]: expected "allow", "ask", or "deny"`,
      );
    }
    overrides[key] = mode;
  }
  return entries.length > 0 ? overrides : undefined;
};

/** Exact ref beats provider wildcard; undefined falls back to the risk-class mode. */
export const actionApprovalOverride = (
  overrides: Readonly<Record<string, FabricActionApprovalMode>> | undefined,
  ref: string,
): FabricActionApprovalMode | undefined => {
  if (!overrides) return undefined;
  if (Object.hasOwn(overrides, ref)) return overrides[ref];
  const dot = ref.indexOf(".");
  if (dot <= 0) return undefined;
  const wildcard = `${ref.slice(0, dot)}.*`;
  return Object.hasOwn(overrides, wildcard) ? overrides[wildcard] : undefined;
};
