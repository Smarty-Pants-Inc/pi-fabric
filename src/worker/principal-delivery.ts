import fs from "node:fs";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { copyFabricProvenance, fabricProvenanceOptions } from "../fabric-provenance.js";
import { followUpFile, followUpState, settleFollowUp, followUpMessage, followUpMessageId, releaseFollowUpPayload } from "../agents/follow-up-delivery.js";
import { readFinalAnswerReceipt } from "./terminal-answer.js";
import { readTerminalControl, settleTerminalControl, terminalControlMessage, terminalControlMessageId } from "./terminal-controls.js";

/** Worker-owned files, not RPC/prompt fields, carry admission into the trusted extension. */
export default function principalDelivery(pi: ExtensionAPI): void {
  const directory = process.env.PI_FABRIC_DELIVERY_DIR;
  if (!directory) return;
  const runDirectory = path.dirname(directory);
  const terminalTask = process.env.PI_FABRIC_TERMINAL_TASK === "1";
  const terminal = (): boolean => terminalTask && Boolean(readFinalAnswerReceipt(runDirectory, process.env.PI_FABRIC_PARENT_RUN ?? ""));
  const held = new Map<string, () => void>();
  const submitted = new Set<string>();
  const flush = (event: { outcome?: string }, ctx: ExtensionContext): void => {
    if (terminal()) { held.clear(); return; }
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
      if (terminal()) return { action: "handled" };
      const id = event.source === "extension" ? followUpMessageId(event.text) : undefined;
      if (id && followUpState(followUpFile(runDirectory, id)) !== "queued") return { action: "handled" };
    });
    pi.on("message_end", event => {
      if (!terminalTask || event.message.role !== "user" || !Array.isArray(event.message.content)) return;
      const first = event.message.content[0];
      if (first?.type !== "text") return;
      const id = terminalControlMessageId(first.text);
      if (!id) return;
      const text = first.text.slice(terminalControlMessage(id, "").length);
      // Untracked controls are consumed when the native loop appends the user
      // input. Strip their private envelope before persistence, not just in the
      // provider context. Tracked follow-ups keep their separate context-only
      // consumption/cancellation gate and never receive an early receipt here.
      if (followUpMessageId(text)) return;
      settleTerminalControl(runDirectory, id, "delivered");
      return { message: { ...event.message, content: [{ ...first, text }, ...event.message.content.slice(1)] } };
    });
    pi.on("context", event => {
      const seen = new Set<string>();
      return { messages: event.messages.flatMap(message => {
        if (message.role !== "user" || !Array.isArray(message.content)) return [message];
        const first = message.content[0];
        if (first?.type !== "text") return [message];
        const controlId = terminalTask ? terminalControlMessageId(first.text) : undefined;
        if (controlId) {
          if (terminal() || readTerminalControl(runDirectory, controlId)?.state === "refused") return [];
          message = { ...message, content: [{ ...first, text: first.text.slice(terminalControlMessage(controlId, "").length) }, ...message.content.slice(1)] };
        }
        const content = message.content as typeof message.content & Array<{ type: string; text: string }>;
        const identified = content[0]!;
        const id = followUpMessageId(identified.text);
        if (!id) {
          if (controlId) settleTerminalControl(runDirectory, controlId, "delivered");
          return [message];
        }
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
        if (controlId) settleTerminalControl(runDirectory, controlId, "delivered");
        return [{ ...message, content: [{ ...identified, text: identified.text.slice(followUpMessage(id, "").length) }, ...content.slice(1)] }];
      }) };
    });
  };
  // Already delivered inputs still carry transport identities in native history.
  // Restore the context gate on exact-session resume even without an envelope.
  pi.on("session_start", () => { if (terminalTask || fs.existsSync(path.join(runDirectory, "follow-ups"))) listen(); });
  pi.registerCommand("fabric-delivery", {
    description: "Consume one private Fabric worker delivery (host-only)",
    handler: async (id, ctx) => {
      if (!/^[0-9a-f-]{36}$/.test(id)) throw new Error("Invalid Fabric delivery id");
      if (terminal()) return;
      const file = path.join(directory, id + ".json");
      const receipt = followUpFile(runDirectory, id);
      // Cancellation may release an envelope before its command is ingested.
      if (!fs.existsSync(file) && ["delivered", "cancelled"].includes(followUpState(receipt))) return;
      // No files supplied by the model are read: the directory is pinned by the launching worker.
      const item = JSON.parse(fs.readFileSync(file, "utf8")) as { message?: unknown; images?: unknown; provenance?: unknown; delivery?: unknown; followUpId?: unknown; controlId?: unknown };
      const provenance = copyFabricProvenance(item.provenance);
      const tracked = item.followUpId === id && item.delivery === "followUp";
      const control = terminalTask && item.controlId === id ? readTerminalControl(runDirectory, id) : undefined;
      if ((!provenance && !control) || typeof item.message !== "string" ||
        (item.delivery !== "steer" && item.delivery !== "followUp")) throw new Error("Invalid Fabric worker delivery");
      const message = control ? terminalControlMessage(id, item.message) : item.message;
      const content = Array.isArray(item.images) && item.images.length
        ? [{ type: "text", text: message }, ...item.images] : message;
      if (tracked) {
        listen();
        const admission = JSON.parse(fs.readFileSync(receipt, "utf8"));
        if (admission.messageId !== id || !Number.isSafeInteger(admission.deadlineAt)) throw new Error("Invalid follow-up admission");
        const deliver = (): void => {
          if (terminal() || submitted.has(id) || followUpState(receipt) !== "queued") return;
          submitted.add(id);
          const trackedText = followUpMessage(id, item.message as string);
          const identifiedText = control ? terminalControlMessage(id, trackedText) : trackedText;
          const identified = typeof content === "string" ? identifiedText
            : [{ type: "text", text: identifiedText }, ...content.slice(1)];
          // Tracking changes receipt ownership, not the native message class.
          // Even at a busy turn boundary, use the follow-up queue so batching
          // honours followUpMode and waits for completion, never steeringMode.
          pi.sendUserMessage(identified as Parameters<ExtensionAPI["sendUserMessage"]>[0],
            provenance ? fabricProvenanceOptions(pi, { deliverAs: "followUp" }, provenance) : { deliverAs: "followUp" });
        };
        if (followUpState(receipt) === "queued") {
          if (ctx.isIdle()) deliver();
          else held.set(id, deliver); // Alarm expiry never dequeues it.
        }
        // Held/native-queued payloads remain worker-owned across replacement.
        releaseFollowUpPayload(receipt);
      } else {
        pi.sendUserMessage(content as Parameters<ExtensionAPI["sendUserMessage"]>[0],
          provenance ? fabricProvenanceOptions(pi, { deliverAs: item.delivery }, provenance) : { deliverAs: item.delivery });
        fs.unlinkSync(file);
      }
    },
  });
}
