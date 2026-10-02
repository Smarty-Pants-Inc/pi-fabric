// Only release callbacks, no runtime loader or engines. The native process outlives modules;
// a reload replacement may remain lazy and still must release its old claim on retirement.
const RETAINED_ROOTS = Symbol.for("pi-fabric.retained-root-registrations");
type RetainedRoots = Map<string, Map<string, () => Promise<void>>>;
const retainedRoots = (): RetainedRoots => {
  const globals = globalThis as typeof globalThis & { [RETAINED_ROOTS]?: RetainedRoots };
  return globals[RETAINED_ROOTS] ??= new Map();
};
export const retainRootRegistration = (sessionId: string, key: string, release: () => Promise<void>): void => {
  const roots = retainedRoots();
  const session = roots.get(sessionId) ?? new Map<string, () => Promise<void>>();
  session.set(key, release);
  roots.set(sessionId, session);
};
export const forgetRetainedRootRegistration = (sessionId: string, key: string): void => {
  const roots = retainedRoots();
  const session = roots.get(sessionId);
  session?.delete(key);
  if (session?.size === 0) roots.delete(sessionId);
};
export const releaseRetainedRootRegistrations = async (sessionId: string): Promise<void> => {
  const session = retainedRoots().get(sessionId);
  for (const [key, release] of [...session?.entries() ?? []]) {
    if (session?.get(key) !== release) continue;
    await release();
    if (session?.get(key) === release) forgetRetainedRootRegistration(sessionId, key);
  }
};
