// The real tool promise can outlive its caller's abort race and a Pi reload.
// Share only session IDs/counts/listeners across release generations, never ctx.
interface SessionWork { pending: number; listeners: Set<() => void> }
const key = Symbol.for("pi-fabric.resident-process-work.v1");
const globals = globalThis as typeof globalThis & { [key]?: Map<string, SessionWork> };
const sessions = globals[key] ??= new Map<string, SessionWork>();
const stateFor = (id: string): SessionWork => {
  let state = sessions.get(id);
  if (!state) sessions.set(id, state = { pending: 0, listeners: new Set() });
  return state;
};
const changed = (id: string, state: SessionWork): void => {
  for (const listener of state.listeners) {
    try { listener(); } catch { /* advisory scheduling must not change tool settlement */ }
  }
  if (!state.pending && !state.listeners.size) sessions.delete(id);
};

export const residentProcessWorkPending = (sessionId: string): boolean =>
  (sessions.get(sessionId)?.pending ?? 0) > 0;

export const subscribeResidentProcessWork = (sessionId: string, listener: () => void): (() => void) => {
  const state = stateFor(sessionId);
  state.listeners.add(listener);
  return () => {
    state.listeners.delete(listener);
    if (!state.pending && !state.listeners.size) sessions.delete(sessionId);
  };
};

export const retainResidentProcessWork = (sessionId: string | undefined): (() => void) => {
  if (!sessionId) return () => {};
  const state = stateFor(sessionId);
  state.pending++;
  changed(sessionId, state);
  let held = true;
  return () => {
    if (!held) return;
    held = false;
    state.pending--;
    changed(sessionId, state);
  };
};

export const trackResidentProcessWork = async <T>(sessionId: string | undefined,
  operation: () => T | PromiseLike<T>): Promise<T> => {
  const release = retainResidentProcessWork(sessionId);
  try { return await operation(); } finally { release(); }
};
