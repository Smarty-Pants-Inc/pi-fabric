/** One merge rule for state and file liveness. A lease never grants identity/ownership. */
export interface Liveness {
  updatedAt: number;
  expiresAt: number;
}

export const effectiveLiveness = (stored: Liveness | undefined, lease: Liveness | undefined): Liveness => {
  // File-only publication gaps have no stored timestamp to merge.
  const base = stored ?? lease ?? { updatedAt: 0, expiresAt: 0 };
  return {
    updatedAt: Math.max(base.updatedAt, lease?.updatedAt ?? base.updatedAt),
    expiresAt: Math.max(base.expiresAt, lease?.expiresAt ?? base.expiresAt),
  };
};
