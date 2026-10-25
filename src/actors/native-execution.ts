// INTERNAL native execution utilities. No callback registry, ingress mint, model or module loading.
import path from "node:path";
import type { NativeActorBinding } from "./native-handler.js";
import type { FabricActorMessage } from "./types.js";

export const hasNativeActorMarker = (value: unknown): boolean =>
  value !== null && typeof value === "object" &&
  (Object.hasOwn(value, "execution") || (value as { runner?: unknown }).runner === "native");

export class NativeActorUnavailableError extends Error {
  readonly code = "FABRIC_NATIVE_ACTOR_UNAVAILABLE";
  constructor() { super("Native actor is unavailable: exact pinned execution authority is required"); this.name = "NativeActorUnavailableError"; }
}
export class NativeActorUnsupportedOperationError extends Error {
  readonly code = "FABRIC_NATIVE_ACTOR_UNSUPPORTED_OPERATION";
  constructor() { super("Native actor supports admitted bridge asks and lifecycle controls only"); this.name = "NativeActorUnsupportedOperationError"; }
}
export class NativeActorBusyError extends Error {
  readonly code = "FABRIC_NATIVE_ACTOR_BUSY";
  constructor() { super("Native callback has not settled; execution custody retained"); this.name = "NativeActorBusyError"; }
}

const freeze = <T>(value: T): T => {
  if (value !== null && typeof value === "object") {
    Object.freeze(value);
    for (const child of Object.values(value)) freeze(child);
  }
  return value;
};
// Strict snapshot, not JSON.stringify's lossy undefined/accessor/toJSON coercion.
export const snapshotNativeJson = <T>(value: T, maxBytes: number): T => {
  const seen = new Set<object>();
  const copy = (input: unknown, depth: number): unknown => {
    if (depth > 128) throw new NativeActorUnavailableError();
    if (input === null || typeof input === "string" || typeof input === "boolean" ||
        (typeof input === "number" && Number.isFinite(input))) return input;
    if (typeof input !== "object" || input === null || seen.has(input) ||
        (!Array.isArray(input) && Object.getPrototypeOf(input) !== Object.prototype && Object.getPrototypeOf(input) !== null) ||
        Object.getOwnPropertySymbols(input).length) throw new NativeActorUnavailableError();
    seen.add(input);
    const descriptors = Object.getOwnPropertyDescriptors(input);
    const keys = Object.keys(input);
    if (Array.isArray(input) && (keys.length !== input.length || keys.some((key, index) => key !== String(index)))) {
      throw new NativeActorUnavailableError();
    }
    const output: unknown[] | Record<string, unknown> = Array.isArray(input) ? [] : Object.create(null);
    for (const key of keys) {
      const descriptor = descriptors[key];
      if (!descriptor || !("value" in descriptor)) throw new NativeActorUnavailableError();
      Object.defineProperty(output, key, { value: copy(descriptor.value, depth + 1), enumerable: true, configurable: true, writable: true });
    }
    seen.delete(input);
    return output;
  };
  const copied = copy(value, 0);
  if (Buffer.byteLength(JSON.stringify(copied), "utf8") > maxBytes) throw new NativeActorUnavailableError();
  // Return ordinary JSON objects, including an own __proto__ key, never caller-owned objects.
  return freeze(JSON.parse(JSON.stringify(copied)) as T);
};

export const copyNativeBinding = (input: NativeActorBinding): NativeActorBinding => {
  const b = snapshotNativeJson(input, 16 * 1024);
  const text = (value: unknown): value is string => typeof value === "string" && !!value.trim() && !value.includes("\0");
  if (!/^[0-9a-f]{32}$/.test(b.actorId) || !/^session:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(b.rootId) ||
      b.scope !== "session" || !text(b.name) || !text(b.handlerKey) || !text(b.residentModule) || !path.isAbsolute(b.residentModule) ||
      !b.ingress || !text(b.ingress.link) || !text(b.ingress.ledgerDir) || !path.isAbsolute(b.ingress.ledgerDir)) throw new NativeActorUnavailableError();
  return b;
};
export const sameNativeBinding = (a: NativeActorBinding, b: NativeActorBinding): boolean =>
  a.actorId === b.actorId && a.rootId === b.rootId && a.scope === b.scope && a.name === b.name &&
  a.handlerKey === b.handlerKey && a.residentModule === b.residentModule &&
  a.ingress.link === b.ingress.link && a.ingress.ledgerDir === b.ingress.ledgerDir;

export const nativeRowMatches = (row: Record<string, unknown>, binding: NativeActorBinding): boolean => {
  const execution = row.execution as Record<string, unknown> | null | undefined;
  return row.id === binding.actorId && row.rootId === binding.rootId && row.name === binding.name &&
    (row.scope === undefined || row.scope === "session") && row.runner === "native" && row.instructions === null &&
    row.instructionsFile === undefined && row.residency === "durable" && row.delivery === "mailbox" && row.responseMode === "text" &&
    row.triggerTurn === false && row.coalesce === false && Array.isArray(row.events) && row.events.length === 0 &&
    Array.isArray(row.topics) && row.topics.length === 0 && !!execution && typeof execution === "object" && !Array.isArray(execution) &&
    Object.keys(execution).length === 3 && execution.kind === "native" && execution.version === 1 && execution.handlerKey === binding.handlerKey &&
    typeof row.createdAt === "number" && Number.isFinite(row.createdAt) &&
    ["model", "thinking", "tools", "routeClass", "kernel", "pythonRuntime", "validWhile", "records", "coalesceKey", "dedupeKey", "activation",
      "activationFilter", "batchWindowMs", "batchMax", "batchMaxBytes", "resolvedBinding", "runnerSessionId", "adoptedAt", "adoptedFrom", "supervisorFor",
      "principalScope", "inferenceContext", "transport", "modelReason", "modelPolicyOriginalModel"].every(key => row[key] === undefined) &&
    (row.requirements === undefined || (Array.isArray(row.requirements) && row.requirements.length === 0));
};

/** Apply BEFORE both the bounded active ring and the unbounded pending archive prefix. */
export const nativeMessageMetadata = (message: FabricActorMessage): FabricActorMessage => ({
  id: message.id, actorId: message.actorId, actorName: message.actorName,
  direction: message.direction, source: "direct", createdAt: message.createdAt,
  reason: message.direction === "in" ? "native ask accepted" : message.error ? "native ask failed" : "native ask settled",
});
