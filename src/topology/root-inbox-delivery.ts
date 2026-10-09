import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { MeshEvent } from "../mesh/store.js";
import { fabricProvenanceOptions, fabricProvenanceSupported, fabricTurnProvenance } from "../fabric-provenance.js";
import { rootInboxMessage, type RootInboxEvent } from "./root-inbox.js";

/** Retained/mixed-version events need positive, recorded admission evidence. */
const eventProvenance = (event: MeshEvent) => event.verification === "mesh" || event.verification === "bridge"
  ? fabricTurnProvenance(event.from, "followUp", event.verification, event.principal) : undefined;

/** Pi injection stays separate from the mesh inbox's storage and pure message shaping. */
export const deliverRootInbox = (
  pi: ExtensionAPI, events: readonly RootInboxEvent[],
  options: Parameters<ExtensionAPI["sendMessage"]>[1] = { deliverAs: "followUp", triggerTurn: true },
): void => {
  if (!events.length) return;
  let start = 0;
  while (start < events.length) {
    const first = events[start]!;
    const provenance = eventProvenance(first);
    const capable = fabricProvenanceSupported(pi);
    const key = JSON.stringify(provenance);
    let end = start + 1;
    while (end < events.length && Boolean(events[end]!.deliveredLate) === Boolean(first.deliveredLate) &&
      (!capable || JSON.stringify(eventProvenance(events[end]!)) === key)) end++;
    // ponytail: passive followUp records a receipt now and joins the next natural inference.
    // Pi's nextTurn queue has no receipt yet, so pre-queuing here would duplicate at turn start.
    const delivery = first.deliveredLate ? { ...options, triggerTurn: false } : options;
    pi.sendMessage(rootInboxMessage(events.slice(start, end)), provenance ? fabricProvenanceOptions(pi, delivery, provenance) : delivery);
    start = end;
  }
};
