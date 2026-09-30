import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { MeshEvent } from "../mesh/store.js";
import { fabricProvenanceSupported, fabricTurnProvenance, sendFabricMessage } from "../fabric-provenance.js";
import { rootInboxMessage } from "./root-inbox.js";

/** Pi injection stays separate from the mesh inbox's storage and pure message shaping. */
export const deliverRootInbox = (pi: ExtensionAPI, events: readonly MeshEvent[]): void => {
  if (!events.length) return;
  let start = 0;
  while (start < events.length) {
    const first = events[start]!;
    let end = events.length;
    if (fabricProvenanceSupported(pi)) {
      const key = JSON.stringify(fabricTurnProvenance(first.from, "followUp"));
      end = start + 1;
      while (end < events.length && JSON.stringify(fabricTurnProvenance(events[end]!.from, "followUp")) === key) end++;
    }
    sendFabricMessage(pi, rootInboxMessage(events.slice(start, end)), { deliverAs: "followUp", triggerTurn: true }, first.from, "followUp");
    start = end;
  }
};
