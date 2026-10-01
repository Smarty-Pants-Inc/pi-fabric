import type { ExtensionAPI, SessionBeforeCompactEvent } from "@earendil-works/pi-coding-agent";

// Pi stops session_before_compact dispatch at the first veto. Main's later handler
// may never see the operation signal, so the hook carries it to the failure event.
// Per-extension-runtime weak ownership avoids state leaking across real /reload.
const declines = new WeakMap<ExtensionAPI, Pick<SessionBeforeCompactEvent, "reason" | "signal">>();

export const recordCompactionDecline = (pi: ExtensionAPI, event: SessionBeforeCompactEvent): void => {
  declines.set(pi, { reason: event.reason, signal: event.signal });
};

export const takeCompactionDecline = (pi: ExtensionAPI): Pick<SessionBeforeCompactEvent, "reason" | "signal"> | undefined => {
  const decline = declines.get(pi);
  declines.delete(pi);
  return decline;
};
