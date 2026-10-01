import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { MeshIdentity } from "../mesh/store.js";
import type { FabricActorDeliveryRequest } from "./types.js";
import { actorDeliveryNotice } from "./delivery-policy.js";
import { sendFabricMessage } from "../fabric-provenance.js";

const escapeXmlText = (text: string): string => text.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");

/** The production actor/lifecycle output call site, shared with the runtime's ActorDirectory. */
export const deliverActorToMain = (
  pi: ExtensionAPI,
  host: MeshIdentity,
  { actor, message, delivery, triggerTurn }: FabricActorDeliveryRequest,
): void => {
  const text = message.text ?? "";
  if (!text) return;
  const deliveryNotice = actorDeliveryNotice(delivery, triggerTurn);
  // Failure alarms are emitted by Fabric's host, not by the failing actor.
  const from: MeshIdentity = message.source === "fabric-host" ? host : { id: actor.id, name: actor.name, kind: "actor" };
  sendFabricMessage(pi, {
    customType: "pi-fabric-actor",
    content: [
      `<fabric-actor name=${JSON.stringify(actor.name)} id=${JSON.stringify(actor.id)}>\n${escapeXmlText(text)}\n</fabric-actor>`,
      deliveryNotice,
    ].filter((line): line is string => Boolean(line)).join("\n"),
    display: true,
    details: { actor, message, delivery: { mode: delivery, triggerTurn, passive: Boolean(deliveryNotice) } },
  }, { deliverAs: delivery, triggerTurn }, from, "actor", "mesh");
};
