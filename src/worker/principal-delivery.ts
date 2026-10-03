import fs from "node:fs";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { copyFabricProvenance, fabricProvenanceOptions } from "../fabric-provenance.js";
import { followUpFile, followUpState, settleFollowUp, followUpMessage, followUpMessageId, releaseFollowUpPayload } from "../agents/follow-up-delivery.js";

/** Worker-owned files, not RPC/prompt fields, carry admission into the trusted extension. */
export default function principalDelivery(pi: ExtensionAPI): void {
  const directory = process.env.PI_FABRIC_DELIVERY_DIR;
  if (!directory) return;
  const runDirectory = path.dirname(directory);
  const held = new Map<string, () => void>();
  const submitted = new Set<string>();
  const flush = (event: { outcome?: string }, ctx: ExtensionContext): void => {
    if (ctx.signal?.aborted || event.outcome === "aborted" || event.outcome === "error") return;
    for (const [id, deliver] of held) { deliver(); held.delete(id); }
  };
  let listening = false;
  const listen = (): void => {
    if (listening) return;
    listening = true;
    pi.on("turn_end", flush);
    pi.on("agent_before_settle", flush);
    // sendUserMessage returns void before asynchronous input/preflight. Even
    // native queue admission is not consumption. Cancellation can win at either
    // gate, including after a message has entered a one-at-a-time native queue.
    pi.on("input", event => {
      const id = event.source === "extension" ? followUpMessageId(event.text) : undefined;
      if (id && followUpState(followUpFile(runDirectory, id)) !== "queued") return { action: "handled" };
    });
    pi.on("context", event => {
      const seen = new Set<string>();
      return { messages: event.messages.flatMap(message => {
        if (message.role !== "user" || !Array.isArray(message.content)) return [message];
        const first = message.content[0];
        if (first?.type !== "text") return [message];
        const id = followUpMessageId(first.text);
        if (!id) return [message];
        const file = followUpFile(runDirectory, id);
        if (!fs.existsSync(file)) return [message];
        if (seen.has(id)) return [];
        seen.add(id);
        // Resume can include a persisted but not yet consumed identified input.
        // Never invent an admission or expose a malformed/uncertain payload.
        try {
          const admission = JSON.parse(fs.readFileSync(file, "utf8"));
          if (admission.messageId !== id || !Number.isSafeInteger(admission.deadlineAt)) return [];
        } catch { return []; }
        // Only receiver context consumption races cancellation for the final
        // receipt. Submission holds no filesystem claim and disables no alarm.
        const state = settleFollowUp(file, "delivered");
        releaseFollowUpPayload(file);
        if (state !== "delivered") return [];
        return [{ ...message, content: [{ ...first, text: first.text.slice(followUpMessage(id, "").length) }, ...message.content.slice(1)] }];
      }) };
    });
  };
  // Already delivered inputs still carry transport identities in native history.
  // Restore the context gate on exact-session resume even without an envelope.
  pi.on("session_start", () => { if (fs.existsSync(path.join(runDirectory, "follow-ups"))) listen(); });
  pi.registerCommand("fabric-delivery", {
    description: "Consume one private Fabric worker delivery (host-only)",
    handler: async (id, ctx) => {
      if (!/^[0-9a-f-]{36}$/.test(id)) throw new Error("Invalid Fabric delivery id");
      const file = path.join(directory, id + ".json");
      const receipt = followUpFile(runDirectory, id);
      // Cancellation may release an envelope before its command is ingested.
      if (!fs.existsSync(file) && ["delivered", "cancelled"].includes(followUpState(receipt))) return;
      // No files supplied by the model are read: the directory is pinned by the launching worker.
      const item = JSON.parse(fs.readFileSync(file, "utf8")) as { message?: unknown; images?: unknown; provenance?: unknown; delivery?: unknown; followUpId?: unknown };
      const provenance = copyFabricProvenance(item.provenance);
      const tracked = item.followUpId === id && item.delivery === "followUp";
      if (!provenance || typeof item.message !== "string" ||
        (item.delivery !== "steer" && item.delivery !== "followUp")) throw new Error("Invalid Fabric worker delivery");
      const content = Array.isArray(item.images) && item.images.length
        ? [{ type: "text", text: item.message }, ...item.images] : item.message;
      if (tracked) {
        listen();
        const admission = JSON.parse(fs.readFileSync(receipt, "utf8"));
        if (admission.messageId !== id || !Number.isSafeInteger(admission.deadlineAt)) throw new Error("Invalid follow-up admission");
        const deliver = (): void => {
          if (submitted.has(id) || followUpState(receipt) !== "queued") return;
          submitted.add(id);
          const identified = typeof content === "string" ? followUpMessage(id, content)
            : [{ type: "text", text: followUpMessage(id, item.message as string) }, ...content.slice(1)];
          // Tracking changes receipt ownership, not the native message class.
          // Even at a busy turn boundary, use the follow-up queue so batching
          // honours followUpMode and waits for completion, never steeringMode.
          pi.sendUserMessage(identified as Parameters<ExtensionAPI["sendUserMessage"]>[0],
            fabricProvenanceOptions(pi, { deliverAs: "followUp" }, provenance));
        };
        if (followUpState(receipt) === "queued") {
          if (ctx.isIdle()) deliver();
          else held.set(id, deliver); // Alarm expiry never dequeues it.
        }
        // Held/native-queued payloads remain worker-owned across replacement.
        releaseFollowUpPayload(receipt);
      } else {
        pi.sendUserMessage(content as Parameters<ExtensionAPI["sendUserMessage"]>[0],
          fabricProvenanceOptions(pi, { deliverAs: item.delivery }, provenance));
        fs.unlinkSync(file);
      }
    },
  });
}
