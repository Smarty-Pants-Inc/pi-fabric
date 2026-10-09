import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { MeshEvent } from "../mesh/store.js";
import { fabricProvenanceOptions, fabricProvenanceSupported, fabricTurnProvenance, fabricWakeCause, fabricWakeMessage } from "../fabric-provenance.js";
import { rootInboxMessage } from "./root-inbox.js";

/** Retained/mixed-version events need positive, recorded admission evidence. */
const eventProvenance = (event: MeshEvent) => event.verification === "mesh" || event.verification === "bridge"
  ? fabricTurnProvenance(event.from, "followUp", event.verification, event.principal) : undefined;

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
    const batch = events.slice(start, end);
    const message = rootInboxMessage(batch);
    const wakeCauses = batch.map((event) => fabricWakeCause(event.from, "mesh", event.topic, event.id));
    // The first FIFO event explains the batch wake. Retain all event causes too:
    // legacy hosts still receive one batch even when its senders differ.
    pi.sendMessage(fabricWakeMessage(pi, { ...message, details: { ...message.details, wakeCauses } }, options, wakeCauses[0]!),
      provenance ? fabricProvenanceOptions(pi, options, provenance) : options);
    start = end;
  }
};
