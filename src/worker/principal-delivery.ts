import fs from "node:fs";
import path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { copyFabricProvenance, fabricProvenanceOptions } from "../fabric-provenance.js";

/** Worker-owned files, not RPC/prompt fields, carry admission into the trusted extension. */
export default function principalDelivery(pi: ExtensionAPI): void {
  const directory = process.env.PI_FABRIC_DELIVERY_DIR;
  if (!directory) return;
  pi.registerCommand("fabric-delivery", {
    description: "Consume one private Fabric worker delivery (host-only)",
    handler: async (id) => {
      if (!/^[0-9a-f-]{36}$/.test(id)) throw new Error("Invalid Fabric delivery id");
      const file = path.join(directory, id + ".json");
      // No files supplied by the model are read: the directory is pinned by the launching worker.
      const item = JSON.parse(fs.readFileSync(file, "utf8")) as { message?: unknown; images?: unknown; provenance?: unknown; delivery?: unknown };
      const provenance = copyFabricProvenance(item.provenance);
      if (!provenance || typeof item.message !== "string" ||
        (item.delivery !== "steer" && item.delivery !== "followUp")) throw new Error("Invalid Fabric worker delivery");
      const content = Array.isArray(item.images) && item.images.length
        ? [{ type: "text", text: item.message }, ...item.images] : item.message;
      pi.sendUserMessage(content as Parameters<ExtensionAPI["sendUserMessage"]>[0],
        fabricProvenanceOptions(pi, { deliverAs: item.delivery }, provenance));
      fs.unlinkSync(file);
    },
  });
}
