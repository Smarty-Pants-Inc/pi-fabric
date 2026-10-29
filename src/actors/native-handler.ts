import { isAbsolute, parse } from "node:path";

/** Host-supplied execution pins. Registration never materializes an actor. */
export interface NativeActorBinding {
  readonly actorId: string;
  readonly rootId: string;
  readonly scope: "session";
  readonly name: string;
  readonly handlerKey: string;
  /** Absolute host path; filesystem and module checks belong to the loader. */
  readonly residentModule: string;
  readonly ingress: {
    readonly link: string;
    readonly ledgerDir: string;
  };
}

export interface NativeActorRequest {
  readonly message: string;
  /** Opaque application input, not identity or authority. */
  readonly data: unknown;
}

/** A callback read view, not an admission capability or a context constructor. */
export interface NativeActorContext {
  readonly actorId: string;
  readonly source: "nats-bridge";
  readonly sender: {
    readonly id: string;
    readonly kind: "main";
    readonly sessionId: string;
  };
  readonly sourceMachine: string;
  readonly link: string;
  readonly commandId: string;
  readonly sourceCommandId: string;
  readonly sourceEventId: string;
  readonly deadlineAt: number;
  readonly signal: AbortSignal;
}

export type NativeActorHandler = (
  request: Readonly<NativeActorRequest>,
  context: Readonly<NativeActorContext>,
) => Promise<{ data: unknown; text?: string }>;

/** Internal only: a sealed declaration plus its exact process-local callback. */
export interface NativeActorRegistration {
  readonly binding: NativeActorBinding;
  readonly handler: NativeActorHandler;
}

// Like runner-registry.ts, share registrations between duplicate installed
// module copies. This is trusted host state, not a guest authority boundary.
const REGISTRY_SYMBOL = Symbol.for("pi-fabric.nativeActorHandlers.v1");
const registryHost = globalThis as unknown as Record<symbol, Map<string, NativeActorRegistration> | undefined>;
const registrations: Map<string, NativeActorRegistration> =
  registryHost[REGISTRY_SYMBOL] ?? (registryHost[REGISTRY_SYMBOL] = new Map());

const requireRecord = (value: unknown, field: string): Record<string, unknown> => {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`Invalid native actor binding: ${field} must be an object`);
  }
  return value as Record<string, unknown>;
};

const requireText = (value: unknown, field: string): string => {
  if (typeof value !== "string" || !value.trim() || value.includes("\0")) {
    throw new Error(`Invalid native actor binding: ${field} must be a nonempty string without NUL`);
  }
  return value;
};

const requireRoot = (value: unknown): string => {
  if (
    typeof value !== "string" || value.length !== 44 ||
    !/^session:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value)
  ) {
    throw new Error("Invalid native actor binding: rootId must be canonical session:<lowercase UUID>");
  }
  return value;
};

const requireScope = (value: unknown): "session" => {
  if (value !== "session") throw new Error('Invalid native actor binding: scope must be "session"');
  return value;
};

const requireAbsolutePath = (value: unknown, field: string): string => {
  const checked = requireText(value, field);
  // Windows root-relative paths depend on the current drive; unlike fully
  // qualified drive/UNC paths, they are not immutable host pins.
  if (!isAbsolute(checked) || (process.platform === "win32" && parse(checked).root.length <= 1)) {
    throw new Error(`Invalid native actor binding: ${field} must be an absolute host path`);
  }
  return checked;
};

const copyBinding = (value: unknown): NativeActorBinding => {
  const source = requireRecord(value, "binding");
  // Read each caller-owned field once. Validate and store only our own copies,
  // never the input object (or its ingress object) as registry identity.
  const actorId = source.actorId;
  if (typeof actorId !== "string" || actorId.length !== 32 || !/^[0-9a-f]{32}$/.test(actorId)) {
    throw new Error("Invalid native actor binding: actorId must be exactly 32 lowercase hex characters");
  }
  const rootId = requireRoot(source.rootId);
  const scope = requireScope(source.scope);
  const name = requireText(source.name, "name");
  const handlerKey = requireText(source.handlerKey, "handlerKey");
  const residentModule = requireAbsolutePath(source.residentModule, "residentModule");
  const ingress = requireRecord(source.ingress, "ingress");
  return Object.freeze({
    actorId, rootId, scope, name, handlerKey, residentModule,
    ingress: Object.freeze({
      link: requireText(ingress.link, "ingress.link"),
      ledgerDir: requireAbsolutePath(ingress.ledgerDir, "ingress.ledgerDir"),
    }),
  });
};

const registrationKey = (binding: NativeActorBinding): string =>
  JSON.stringify([binding.rootId, binding.scope, binding.actorId, binding.handlerKey]);

/**
 * Declare one exact native actor handler for this process, before its owner
 * captures a snapshot. No loading, actor creation, model lookup or background
 * registration occurs. Every duplicate/conflicting binding fails closed.
 *
 * Unregister affects future snapshots only. Existing managers retain their
 * sealed snapshot; replacing a live owner requires a quiescent owner restart.
 */
export const registerNativeActorHandler = (
  binding: NativeActorBinding,
  handler: NativeActorHandler,
): (() => void) => {
  if (typeof handler !== "function") throw new Error("Native actor handler must be a function");
  const copied = copyBinding(binding);
  const key = registrationKey(copied);
  if (registrations.has(key)) throw new Error("Native actor handler tuple already registered");
  for (const { binding: existing } of registrations.values()) {
    if (existing.rootId === copied.rootId && existing.scope === copied.scope && existing.actorId === copied.actorId) {
      throw new Error("Conflicting native actor binding already registered for actor/root/scope");
    }
  }
  const registered: NativeActorRegistration = Object.freeze({ binding: copied, handler });
  registrations.set(key, registered);
  return () => {
    // A stale disposer must never remove a later registration of this tuple.
    if (registrations.get(key) === registered) registrations.delete(key);
  };
};

/**
 * INTERNAL host composition seam; deliberately not re-exported by native-actors.
 * Capture once for a manager's lifetime. The array, entries and declarations are
 * frozen, with no live Map exposed; later disposal/registration cannot alter it.
 */
export const snapshotNativeActorHandlers = (
  rootId: string,
  scope: NativeActorBinding["scope"],
): readonly NativeActorRegistration[] => {
  const checkedRoot = requireRoot(rootId);
  const checkedScope = requireScope(scope);
  return Object.freeze([...registrations.values()].filter(
    ({ binding }) => binding.rootId === checkedRoot && binding.scope === checkedScope,
  ));
};
