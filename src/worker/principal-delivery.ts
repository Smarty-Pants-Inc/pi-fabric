import fs from "node:fs";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { copyFabricProvenance, fabricProvenanceOptions } from "../fabric-provenance.js";
import { followUpFile, followUpState, settleFollowUp } from "../agents/follow-up-delivery.js";

/** Worker-owned files, not RPC/prompt fields, carry admission into the trusted extension. */
export default function principalDelivery(pi: ExtensionAPI): void {
  const directory = process.env.PI_FABRIC_DELIVERY_DIR;
  if (!directory) return;
  const held = new Map<string, () => void>();
  const flush = (event: { outcome?: string }, ctx: ExtensionContext): void => {
    if (ctx.signal?.aborted || event.outcome === "aborted" || event.outcome === "error") return;
    for (const [id, deliver] of held) { deliver(); held.delete(id); }
  };
  let listening = false;
  pi.registerCommand("fabric-delivery", {
    description: "Consume one private Fabric worker delivery (host-only)",
    handler: async (id, ctx) => {
      if (!/^[0-9a-f-]{36}$/.test(id)) throw new Error("Invalid Fabric delivery id");
      const file = path.join(directory, id + ".json");
      // No files supplied by the model are read: the directory is pinned by the launching worker.
      const item = JSON.parse(fs.readFileSync(file, "utf8")) as { message?: unknown; images?: unknown; provenance?: unknown; delivery?: unknown; followUpId?: unknown };
      const provenance = copyFabricProvenance(item.provenance);
      const tracked = item.followUpId === id && item.delivery === "followUp";
      if (!provenance || typeof item.message !== "string" ||
        (item.delivery !== "steer" && item.delivery !== "followUp")) throw new Error("Invalid Fabric worker delivery");
      const content = Array.isArray(item.images) && item.images.length
        ? [{ type: "text", text: item.message }, ...item.images] : item.message;
      const send = (delivery: "steer" | "followUp"): void => {
        pi.sendUserMessage(content as Parameters<ExtensionAPI["sendUserMessage"]>[0],
          fabricProvenanceOptions(pi, { deliverAs: delivery }, provenance));
      };
      if (tracked) {
        if (!listening) {
          pi.on("turn_end", flush);
          pi.on("agent_before_settle", flush);
          listening = true;
        }
        const receipt = followUpFile(path.dirname(directory), id);
        // Validate the private admission before accepting an unprovenanced tracked input.
        const admission = JSON.parse(fs.readFileSync(receipt, "utf8"));
        if (admission.messageId !== id || !Number.isSafeInteger(admission.deadlineAt)) throw new Error("Invalid follow-up admission");
        const deliver = (): void => { settleFollowUp(receipt, "delivered", () => send(ctx.isIdle() ? "followUp" : "steer")); };
        if (followUpState(receipt) === "queued") {
          if (ctx.isIdle()) deliver();
          else held.set(id, deliver); // Expiry never dequeues it; only a boundary or cancellation does.
        }
      } else send(item.delivery);
      fs.unlinkSync(file);
    },
  });
}
