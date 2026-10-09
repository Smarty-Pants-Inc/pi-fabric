import type { MeshEvent, MeshIdentity, MeshPublishInput } from "../mesh/store.js";

/**
 * Caller-keyed idempotent publish (smarty-dev#5138).
 *
 * An actor that retries `mesh.publish` after an uncertain outcome (the publish took
 * effect and then raised, or the process died before it saved its receipt) passes the
 * same `key`. Fabric maps it onto the mesh store's public keyed publish (`dedupeKey`),
 * so the retry returns the original event (same id and sequence) and appends nothing.
 * The actor manager already drops a repeat of one event id per actor, so delivery is once.
 *
 * Safety:
 * - The key is namespaced by the publisher's own identity (kind plus id), JSON-encoded so
 *   no delimiter in a key or an id can forge another publisher's namespace. Participants
 *   cannot collide with or suppress each other, nor host keys (which never use this prefix).
 * - The key is bounded to {@link CALLER_PUBLISH_KEY_MAX_LENGTH} characters.
 * - A key is honored for {@link CALLER_PUBLISH_KEY_RETENTION_MS}. The store keeps receipts
 *   without expiry, so the window is applied here: a receipt older than the window is
 *   superseded by the key's next generation, and the same key then publishes once more.
 * - Reusing a live key for a different topic, kind, or recipient is refused.
 */
export const CALLER_PUBLISH_KEY_MAX_LENGTH = 200;
/** Retries within 7 days (at least the 24 h the issue requires) return the original event. */
export const CALLER_PUBLISH_KEY_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
/** Bounded generations per key; past this, choose a new key. */
export const CALLER_PUBLISH_KEY_MAX_GENERATIONS = 256;
const NAMESPACE = "caller-key:v1:";

export class CallerPublishKeyError extends Error {
  override readonly name = "CallerPublishKeyError";
}

/** Validate a caller-supplied publish key; undefined means an ordinary publish. */
export const callerPublishKey = (value: unknown): string | undefined => {
  if (value === undefined) return undefined;
  if (typeof value !== "string" || !value.trim()) {
    throw new CallerPublishKeyError("mesh.publish key must be a non-empty string");
  }
  if (value.length > CALLER_PUBLISH_KEY_MAX_LENGTH) {
    throw new CallerPublishKeyError(`mesh.publish key exceeds ${CALLER_PUBLISH_KEY_MAX_LENGTH} characters (${value.length})`);
  }
  return value;
};

/** The store dedupe key for one publisher, caller key, and generation. */
export const namespacedPublishKey = (identity: Pick<MeshIdentity, "id" | "kind">, key: string, generation = 0): string =>
  NAMESPACE + JSON.stringify([identity.kind, identity.id, key, generation]);

const sameRoute = (event: MeshEvent, input: MeshPublishInput): boolean =>
  event.topic === input.topic &&
  event.kind === (input.kind?.trim() || "message") &&
  (event.to ?? undefined) === (input.to || undefined);

/**
 * Publish once per (publisher, key) within the retention window, through the store's
 * public keyed publish. Returns the original event on a retry.
 */
export async function publishWithCallerKey(
  publish: (input: MeshPublishInput) => Promise<MeshEvent>,
  input: Omit<MeshPublishInput, "dedupeKey">,
  key: string,
  options: { now?: () => number; retentionMs?: number } = {},
): Promise<MeshEvent> {
  const now = options.now ?? Date.now;
  const retentionMs = options.retentionMs ?? CALLER_PUBLISH_KEY_RETENTION_MS;
  const startedAt = now();
  for (let generation = 0; generation < CALLER_PUBLISH_KEY_MAX_GENERATIONS; generation++) {
    const event = await publish({ ...input, dedupeKey: namespacedPublishKey(input.from, key, generation) });
    // A fresh event, or a prior receipt still inside the window.
    if (event.createdAt > startedAt - retentionMs) {
      if (!sameRoute(event, input)) {
        throw new CallerPublishKeyError(
          `mesh.publish key was already used within its retention window for topic ${event.topic}` +
          `${event.to ? ` to ${event.to}` : ""} (kind ${event.kind}); use a new key for a different publication`,
        );
      }
      return event;
    }
  }
  throw new CallerPublishKeyError(`mesh.publish key was reused across ${CALLER_PUBLISH_KEY_MAX_GENERATIONS} retention windows; use a new key`);
}
