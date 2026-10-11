import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { MeshEvent } from "../mesh/store.js";
import { fabricProvenanceOptions, fabricProvenanceSupported, fabricTurnProvenance, fabricWakeCause, fabricWakeMessage } from "../fabric-provenance.js";
import { rootInboxBudget, rootInboxMessage, type RootInboxEvent } from "./root-inbox-shared.js";

/** Retained/mixed-version events need positive, recorded admission evidence. */
const eventProvenance = (event: MeshEvent) => event.verification === "mesh" || event.verification === "bridge"
  ? fabricTurnProvenance(event.from, "followUp", event.verification, event.principal) : undefined;

/** Pi injection stays separate from the mesh inbox's storage and pure message shaping. */
export const deliverRootInbox = (
  pi: ExtensionAPI, events: readonly RootInboxEvent[],
  options: Parameters<ExtensionAPI["sendMessage"]>[1] = { deliverAs: "followUp", triggerTurn: true },
): void => {
  if (!events.length) return;
  const seen = new Set<string>();
  events = events.filter(event => {
    const key = JSON.stringify([event.from.kind, event.from.id, event.topic, event.id]);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
  // One injection stays within the inbox budget across all its messages (R-token-waste): the
  // overflow is one summary line in the last message, never more full event bodies.
  const { shown, overflow } = rootInboxBudget(events);
  events = shown;
  let start = 0;
  while (start < events.length) {
    const first = events[start]!;
    const provenance = eventProvenance(first);
    const capable = fabricProvenanceSupported(pi);
    const key = JSON.stringify(provenance);
    let end = start + 1;
    // Late actionable work is passive context: never in one message with work that wakes.
    while (end < events.length && Boolean(events[end]!.deliveredLate) === Boolean(first.deliveredLate) &&
      (!capable || JSON.stringify(eventProvenance(events[end]!)) === key)) end++;
    const batch = events.slice(start, end);
    const extra: readonly RootInboxEvent[] = end === events.length ? overflow : [];
    // A passive followUp records a receipt now and joins the next natural inference.
    const delivery = first.deliveredLate && extra.every((event) => event.deliveredLate) ? { ...options, triggerTurn: false } : options;
    const message = rootInboxMessage(batch, extra);
    const wakeCauses = [...batch, ...extra].map((event) => fabricWakeCause(event.from, "mesh", event.topic, event.id));
    // One native message can admit several sources. Keep the whole FIFO cause
    // list, including on legacy hosts that batch events from different senders.
    pi.sendMessage(fabricWakeMessage(pi, message, delivery, wakeCauses),
      provenance ? fabricProvenanceOptions(pi, delivery, provenance) : delivery);
    start = end;
  }
};
