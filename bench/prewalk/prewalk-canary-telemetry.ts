// Passive, opt-in telemetry recorder for Prewalk live-canary runs.
//
// Loaded explicitly via `pi -e bench/prewalk/prewalk-canary-telemetry.ts` with
// PREWALK_CANARY_TELEMETRY pointing at the output JSONL. It only records:
// never mutates context, session, provider, or model state, and no handler
// returns a replacement message. The turn cap is the single deliberate
// non-passive act: it aborts a runaway paid run; every other handler only
// records.
//
// Two Prewalk-message projections are captured with explicit source labels:
// * live — message_end events with role "custom" and a message-level
//   customType, observed while the session streams;
// * session — persisted `custom_message` entries with a top-level
//   customType, read at session_shutdown.
// The original canary recorder looked for message-shaped session entries and
// always found none; this recorder reads both shapes.

import { createHash } from "node:crypto";
import fs from "node:fs";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import {
  PREWALK_MESSAGE_PREFIX,
  parseRequestContract,
  persistedPrewalkMessages,
  scanRequestMessages,
} from "./lib/prewalk-live-evidence.mjs";

const DEFAULT_MAX_TURNS = 16;

// The turn cap aborts a runaway paid run. It is configurable so a longer
// recovery scenario can raise it without patching a copy of this recorder;
// the chosen cap is recorded in session_start for the evidence trail.
const readMaxTurns = (): number => {
  const raw = process.env.PREWALK_CANARY_MAX_TURNS;
  if (raw === undefined || raw === "") return DEFAULT_MAX_TURNS;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`PREWALK_CANARY_MAX_TURNS must be a positive integer, got ${JSON.stringify(raw)}`);
  }
  return parsed;
};

interface LivePrewalkRecord {
  customType: string;
  details: unknown;
}

export default function (pi: ExtensionAPI): void {
  const out = process.env.PREWALK_CANARY_TELEMETRY;
  if (!out) {
    throw new Error("Missing canary telemetry destination (set PREWALK_CANARY_TELEMETRY)");
  }
  const emit = (type: string, data: Record<string, unknown> = {}): void => {
    fs.appendFileSync(out, JSON.stringify({ type, at: Date.now(), ...data }) + "\n");
  };
  const modelKey = (context: ExtensionContext): string | null =>
    context.model ? `${context.model.provider}/${context.model.id}` : null;

  // Opt-in request evidence. The contract and its hash are frozen at load so
  // every record can be checked against the exact probe configuration.
  const contractPath = process.env.PREWALK_CANARY_REQUEST_CONTRACT;
  const contractText = contractPath ? fs.readFileSync(contractPath, "utf8") : null;
  const requestContract = contractText === null ? null : parseRequestContract(contractText, contractPath ?? "request contract");
  const requestContractSha256 = contractText === null ? null : createHash("sha256").update(contractText).digest("hex");
  let requestIndex = 0;

  const maxTurns = readMaxTurns();
  let turns = 0;
  const livePrewalk: LivePrewalkRecord[] = [];

  pi.on("session_start", (_event, context) => {
    emit("session_start", {
      sessionId: context.sessionManager.getSessionId(),
      cwd: context.cwd,
      model: modelKey(context),
      thinking: context.thinkingLevel ?? null,
      activeTools: pi.getActiveTools(),
      turnCap: maxTurns,
      requestContract: requestContract === null ? null : { path: contractPath, sha256: requestContractSha256 },
    });
  });

  pi.on("before_agent_start", () => emit("before_agent_start"));

  pi.on("turn_start", (_event, context) => {
    turns += 1;
    emit("turn_start", { turn: turns, model: modelKey(context) });
    if (turns > maxTurns) {
      emit("turn_limit");
      context.abort();
    }
  });

  pi.on("model_select", (event) => {
    emit("model_select", {
      model: `${event.model.provider}/${event.model.id}`,
      previous: event.previousModel
        ? `${event.previousModel.provider}/${event.previousModel.id}`
        : null,
    });
  });

  pi.on("before_provider_request", (event, context) => {
    emit("provider_request", {
      model: modelKey(context),
      payloadBytes: Buffer.byteLength(JSON.stringify(event.payload)),
    });
  });

  // Opt-in request evidence: the context hook is provider-agnostic (custom API
  // providers never fire before_provider_request) and runs after Fabric's
  // filter/injection in extension load order. Without a contract it records
  // nothing, so default telemetry stays unchanged.
  pi.on("context", (event, context) => {
    if (requestContract === null) return;
    requestIndex += 1;
    const scan = scanRequestMessages(event.messages, requestContract.markers);
    emit("request_context", {
      requestIndex,
      model: modelKey(context),
      messages: event.messages.length,
      requestEvidence: {
        layout: "context",
        truncated: scan.truncated,
        matches: scan.matches,
        contractSha256: requestContractSha256,
      },
    });
  });

  pi.on("after_provider_response", (event) => {
    emit("provider_response", { status: event.status });
  });

  pi.on("message_end", (event) => {
    const message = event.message;
    if (message.role === "assistant") {
      emit("message_end", {
        role: "assistant",
        provider: message.provider,
        model: message.model,
        usage: message.usage,
        stopReason: message.stopReason,
        ...(message.errorMessage !== undefined ? { errorMessage: message.errorMessage } : {}),
      });
      return;
    }
    if (message.role === "toolResult") {
      emit("message_end", {
        role: "toolResult",
        toolName: message.toolName,
        isError: message.isError,
        ...(message.usage !== undefined ? { usage: message.usage } : {}),
      });
      return;
    }
    if (message.role === "custom" && message.customType.startsWith(PREWALK_MESSAGE_PREFIX)) {
      // Observed while streaming: keeps evidence even if shutdown never fires.
      const record: LivePrewalkRecord = {
        customType: message.customType,
        details: message.details ?? null,
      };
      livePrewalk.push(record);
      emit("prewalk_message", { source: "live", ...record });
    }
  });

  // Effective host settings are visible only if preparation reaches this hook.
  // Small-session skips happen earlier; an earlier extension's cancel can also
  // short-circuit delivery. Terminal failure metadata is recorded separately.
  pi.on("session_before_compact", (event, context) => {
    emit("compaction_attempt", {
      model: modelKey(context),
      reason: event.reason,
      willRetry: event.willRetry ?? null,
      tokensBefore: event.preparation?.tokensBefore ?? null,
      settings: {
        reserveTokens: event.preparation?.settings?.reserveTokens ?? null,
        keepRecentTokens: event.preparation?.settings?.keepRecentTokens ?? null,
      },
    });
  });

  pi.on("session_compact", (event) =>
    emit("compaction", {
      reason: event.reason,
      willRetry: event.willRetry ?? null,
      fromExtension: event.fromExtension ?? null,
      tokensBefore: event.compactionEntry?.tokensBefore ?? null,
    }));

  pi.on("session_compact_failed", (event) => {
    emit("compaction_failed", {
      reason: event.reason,
      ...(event.errorMessage !== undefined ? { error: event.errorMessage } : {}),
      aborted: event.aborted ?? null,
      willRetry: event.willRetry ?? null,
      fromExtension: event.fromExtension ?? null,
    });
  });

  pi.on("agent_settled", () => emit("agent_settled"));

  pi.on("session_shutdown", (_event, context) => {
    // Persisted durable shape: `custom_message` entries carry a top-level
    // customType (session-format.md). The shared normalizer performs the
    // projection so the recorder and the analyzer cannot drift.
    const persisted = persistedPrewalkMessages(context.sessionManager.getEntries()).map(
      (message) => ({ id: message.id, customType: message.customType, details: message.details }),
    );
    emit("session_shutdown", {
      model: modelKey(context),
      prewalkLive: livePrewalk,
      prewalkPersisted: persisted,
    });
  });
}
