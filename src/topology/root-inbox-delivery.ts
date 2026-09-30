import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { MeshEvent } from "../mesh/store.js";
import { fabricProvenanceOptions, fabricProvenanceSupported, fabricTurnProvenance } from "../fabric-provenance.js";
import { rootInboxMessage } from "./root-inbox.js";

/** Retained/mixed-version events need positive, recorded admission evidence. */
const eventProvenance = (event: MeshEvent) => event.verification === "mesh" || event.verification === "bridge"
  ? fabricTurnProvenance(event.from, "followUp", event.verification) : undefined;

/** Pi injection stays separate from the mesh inbox's storage and pure message shaping. */
export const deliverRootInbox = (
  pi: ExtensionAPI, events: readonly MeshEvent[],
  options: Parameters<ExtensionAPI["sendMessage"]>[1] = { deliverAs: "followUp", triggerTurn: true },
): void => {
  if (!events.length) return;
  let start = 0;
  while (start < events.length) {
    const first = events[start]!;
    const provenance = eventProvenance(first);
    let end = events.length;
    if (fabricProvenanceSupported(pi)) {
      const key = JSON.stringify(provenance);
      end = start + 1;
      while (end < events.length && JSON.stringify(eventProvenance(events[end]!)) === key) end++;
    }
    pi.sendMessage(rootInboxMessage(events.slice(start, end)), provenance ? fabricProvenanceOptions(pi, options, provenance) : options);
    start = end;
  }
};
