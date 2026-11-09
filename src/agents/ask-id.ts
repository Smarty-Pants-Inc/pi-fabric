/** Opaque ledger provenance. Optional on old records, always resolved for new spawns. */
export interface AgentAskIdentity {
  askId: string | null;
  askIdState: "explicit" | "inherited" | "missing";
}

/** Validate only the whole Ask-ID carrier; never parse, trim, or rewrite it. */
export const normalizeAgentAskId = (value: unknown): string | undefined => {
  if (value === undefined) return undefined;
  if (typeof value !== "string" || !value.length || Buffer.byteLength(value, "utf8") > 512 || /[\p{Cc}\u2028\u2029]/u.test(value)) {
    throw new Error("Invalid askId: expected a non-empty string of at most 512 UTF-8 bytes without newlines or control characters");
  }
  return value;
};

export const resolveAgentAskIdentity = (value: unknown, parent: string | null | undefined): AgentAskIdentity => {
  const explicit = normalizeAgentAskId(value);
  if (explicit !== undefined) return { askId: explicit, askIdState: "explicit" };
  const inherited = normalizeAgentAskId(parent ?? undefined);
  return inherited === undefined ? { askId: null, askIdState: "missing" } : { askId: inherited, askIdState: "inherited" };
};

export const taskWithAskId = (task: string, askId: string | null): string =>
  askId === null ? task : `Ask-ID: ${askId}\n${task}`;
