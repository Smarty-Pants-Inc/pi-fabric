import fs from "node:fs";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { PI_CORE_TOOL_NAME_SET } from "../core/pi-tools.js";
import { assertNativeRolePair, assertNativeRoleTools, assertNativeRoleParticipant, attestNativeRoleParticipant,
  installNativeRoleBinding, snapshotNativeRoleBinding } from "../agents/native-role-binding.js";

/** Explicit worker hook; fd 3 is a worker-created read-only launch snapshot, not an env selector. */
export default function nativeRoleActivation(pi: ExtensionAPI): void {
  const packet = JSON.parse(fs.readFileSync(3, "utf8"));
  fs.closeSync(3);
  if (typeof packet.runId !== "string" || typeof packet.nonce !== "string" ||
      (packet.kind !== "actor" && packet.kind !== "agent") || typeof packet.id !== "string") {
    throw new Error("NATIVE_ROLE_BINDING_MISMATCH: invalid worker activation snapshot");
  }
  const binding = snapshotNativeRoleBinding(packet.binding);
  installNativeRoleBinding(pi.events, binding);
  fs.writeSync(1, `${JSON.stringify({ type: "fabric_native_role_ready", runId: packet.runId, nonce: packet.nonce, protocol: 1 })}\n`);
  pi.registerCommand("fabric-role-admit", {
    description: "Attest native role worker admission (host-only)",
    handler: async (nonce, ctx) => {
      const frame: Record<string, unknown> = { type: "fabric_native_role_admitted", runId: packet.runId, nonce };
      try {
        if (nonce !== packet.nonce || ctx.mode !== "rpc" || !ctx.isIdle()) throw new Error("invalid role admission request");
        const model = ctx.model ? `${ctx.model.provider}/${ctx.model.id}` : undefined;
        const thinking = pi.getThinkingLevel();
        const tools = pi.getActiveTools().filter(tool => PI_CORE_TOOL_NAME_SET.has(tool));
        assertNativeRolePair(binding, model, thinking);
        assertNativeRoleTools(binding, tools, "delivered");
        // Publish the actual native pair, then reread the very directory used by agents.self.
        const participant = await attestNativeRoleParticipant(pi.events, model!, thinking);
        assertNativeRoleParticipant(binding, participant, packet.id, packet.kind);
        Object.assign(frame, { model, thinking, tools, participant });
      } catch (error) {
        frame.error = error instanceof Error ? error.message : String(error);
      }
      fs.writeSync(1, `${JSON.stringify(frame)}\n`);
    },
  });
}
