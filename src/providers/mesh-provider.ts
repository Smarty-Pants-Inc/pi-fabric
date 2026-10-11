import type {
  FabricActionDescriptor,
  FabricInvocationContext,
  FabricProvider,
  FabricProviderListRequest,
} from "../protocol.js";
import { fabricHostCallerId, invocationFabricPrincipal, snapshotFabricInvocation } from "../fabric-provenance.js";
import { MeshStore, type MeshIdentity } from "../mesh/store.js";
import type { FabricParticipantSource } from "../topology/types.js";
import { FABRIC_PARTICIPANT_LIFECYCLE_TOPIC } from "../lifecycle/types.js";
import { actionArgNormalizer } from "./arg-normalization.js";
import { deliverWithMessageNotice, outgoingMessageNotice } from "./message-id-notice.js";

const emptySchema = { type: "object", properties: {}, additionalProperties: false };
const INTERNAL_STATE_PREFIXES = ["topology/", "sessions/", "actors/", "residency/"];
const PRIVATE_STATE_PREFIXES = ["residency/"];
const INTERNAL_CONTROL_PREFIX = "fabric.control.";
const INTERNAL_HOST_EVENT_TOPIC = "fabric.actor.host-event";

const assertPublicStateKey = (key: string): void => {
  if (INTERNAL_STATE_PREFIXES.some((prefix) => key.startsWith(prefix))) {
    throw new Error(`Fabric mesh key is reserved for host coordination: ${key}`);
  }
};

const assertReadableStateKey = (key: string): void => {
  if (PRIVATE_STATE_PREFIXES.some((prefix) => key.startsWith(prefix))) {
    throw new Error(`Fabric mesh key is private host state: ${key}`);
  }
};

const descriptors: FabricActionDescriptor[] = [
  {
    name: "self",
    description: "Return this Fabric participant's mesh identity",
    inputSchema: emptySchema,
    risk: "read",
    namespace: "coordination",
  },
  {
    name: "publish",
    description: "Append a durable event to a mesh topic, optionally addressed to one actor",
    inputSchema: {
      type: "object",
      properties: {
        topic: { type: "string" },
        kind: { type: "string" },
        to: { type: "string" },
        text: { type: "string" },
        data: {},
      },
      required: ["topic"],
      additionalProperties: false,
    },
    risk: "agent",
    namespace: "coordination",
  },
  {
    name: "read",
    description: "Read durable mesh events after a sequence cursor",
    inputSchema: {
      type: "object",
      properties: {
        after: { type: "number", minimum: 0 },
        topic: { type: "string" },
        to: { type: "string" },
        limit: { type: "number", minimum: 1 },
      },
      additionalProperties: false,
    },
    risk: "read",
    namespace: "coordination",
  },
  {
    name: "members",
    description: "List roots, agents, and actors in the unified project participant directory",
    inputSchema: {
      type: "object",
      properties: {
        scope: { type: "string", enum: ["local", "lineage", "project"] },
        kinds: {
          type: "array",
          items: { type: "string", enum: ["root", "agent", "actor"] },
        },
        includeStale: { type: "boolean" },
        limit: { type: "number", minimum: 1 },
      },
      additionalProperties: false,
    },
    risk: "read",
    namespace: "coordination",
  },
  {
    name: "get",
    description: "Read a versioned value from shared mesh state",
    inputSchema: {
      type: "object",
      properties: { key: { type: "string" } },
      required: ["key"],
      additionalProperties: false,
    },
    risk: "read",
    namespace: "coordination",
  },
  {
    name: "list",
    description: "List shared mesh state by key prefix",
    inputSchema: {
      type: "object",
      properties: {
        prefix: { type: "string" },
        limit: { type: "number", minimum: 1 },
      },
      additionalProperties: false,
    },
    risk: "read",
    namespace: "coordination",
  },
  {
    name: "put",
    description: "Write shared mesh state, optionally with compare-and-swap version checking",
    inputSchema: {
      type: "object",
      properties: {
        key: { type: "string" },
        value: {},
        ifVersion: { type: "number", minimum: 0 },
      },
      required: ["key", "value"],
      additionalProperties: false,
    },
    risk: "agent",
    namespace: "coordination",
  },
  {
    name: "delete",
    description: "Delete shared mesh state, optionally with compare-and-swap version checking",
    inputSchema: {
      type: "object",
      properties: {
        key: { type: "string" },
        ifVersion: { type: "number", minimum: 0 },
      },
      required: ["key"],
      additionalProperties: false,
    },
    risk: "agent",
    namespace: "coordination",
  },
];


/** Fixed typed refusal: caller identity is captured by the host, never read from arguments. */
export class MeshHostPublishError extends Error {
  readonly code = "FABRIC_MESH_HOST_PUBLISH_REQUIRED";
  readonly retryable = false;
  constructor() {
    super("Mesh dedupeKey, publishBatch and sync require a trusted host component publisher");
    this.name = "MeshHostPublishError";
  }
}

const assertHostPublisher = (context: FabricInvocationContext): string => {
  const caller = fabricHostCallerId(context);
  if (caller === undefined) throw new MeshHostPublishError();
  return caller;
};

const assertDedupeKey = (args: Record<string, unknown>, context: FabricInvocationContext): void => {
  if (!Object.hasOwn(args, "dedupeKey")) return;
  assertHostPublisher(context);
  if (typeof args.dedupeKey !== "string" || Buffer.byteLength(args.dedupeKey, "utf8") < 1 ||
      Buffer.byteLength(args.dedupeKey, "utf8") > 512) {
    throw new Error("Mesh dedupeKey must be a string of 1..512 UTF-8 bytes");
  }
};

/** Upper bound of a host `mesh.sync` budget: a barrier that cannot confirm within it reports failure. */
const MAX_SYNC_BUDGET_MS = 60_000;

const assertPublishArguments = (actionName: string, args: Record<string, unknown>, context: FabricInvocationContext): void => {
  if (actionName === "publish") assertDedupeKey(args, context);
  if (actionName === "sync") {
    // Same trust class as dedupeKey and publishBatch (smarty-dev#9313): checked before any argument.
    assertHostPublisher(context);
    if (Object.hasOwn(args, "budgetMs") && (typeof args.budgetMs !== "number" || !Number.isFinite(args.budgetMs) ||
        args.budgetMs < 0 || args.budgetMs > MAX_SYNC_BUDGET_MS)) {
      throw new Error(`Mesh sync budgetMs must be a number in 0..${MAX_SYNC_BUDGET_MS}`);
    }
  }
  if (actionName === "publishBatch") {
    assertHostPublisher(context);
    if (!Array.isArray(args.events) || !args.events.length || args.events.length > 256) {
      throw new Error("Mesh publish batch must contain 1..256 events");
    }
    for (const event of args.events) {
      if (!event || typeof event !== "object" || Array.isArray(event)) throw new Error("Mesh batch events must be objects");
      assertDedupeKey(event as Record<string, unknown>, context);
    }
  }
};

const namespacedDedupeKey = (args: Record<string, unknown>, context: FabricInvocationContext): string | undefined => {
  if (!Object.hasOwn(args, "dedupeKey")) return undefined;
  const caller = assertHostPublisher(context);
  // Length-frame the component id so delimiter-containing ids/keys cannot collide. The
  // 512-byte limit applies to the caller's raw key, not this host-owned receipt namespace.
  return `component:${Buffer.byteLength(caller, "utf8")}:${caller}:${args.dedupeKey as string}`;
};

const assertPublishTopic = (topic: string): void => {
  if (topic.startsWith(INTERNAL_CONTROL_PREFIX) || topic === INTERNAL_HOST_EVENT_TOPIC ||
      topic === FABRIC_PARTICIPANT_LIFECYCLE_TOPIC) {
    throw new Error(`Fabric mesh topic is reserved for host coordination: ${topic}`);
  }
};

const publicPublish = descriptors.find(descriptor => descriptor.name === "publish")!;
const publicPublishSchema = publicPublish.inputSchema as { properties: Record<string, unknown> };
const hostPublishSchema = {
  ...publicPublish.inputSchema as object,
  properties: {
    ...publicPublishSchema.properties,
    dedupeKey: { type: "string", minLength: 1, maxLength: 512, description: "Host-only key: 1..512 UTF-8 bytes, scoped to this component" },
  },
};
const hostDescriptors: FabricActionDescriptor[] = descriptors.map(descriptor =>
  descriptor.name === "publish" ? { ...descriptor, inputSchema: hostPublishSchema } : descriptor);
hostDescriptors.push({
  name: "publishBatch",
  description: "Publish a bounded durable prefix in order; retry keyed events to recover their original identities",
  inputSchema: {
    type: "object",
    properties: { events: { type: "array", minItems: 1, maxItems: 256, items: hostPublishSchema } },
    required: ["events"],
    additionalProperties: false,
  },
  risk: "agent",
  namespace: "coordination",
});
hostDescriptors.push({
  name: "sync",
  description: "Durability barrier: resolve once every mesh event and state write made before the call is on disk; " +
    "await it before fsyncing an external cursor or delivered mark that covers them. Returns { synced, through }: " +
    "advance an event cursor only to through, the highest sequence committed to the log that the barrier made durable",
  inputSchema: {
    type: "object",
    properties: { budgetMs: { type: "number", minimum: 0, maximum: MAX_SYNC_BUDGET_MS, description: "SQLite state checkpoint budget, default 1000" } },
    additionalProperties: false,
  },
  risk: "agent",
  namespace: "coordination",
});
const meshDescriptors = (context: FabricInvocationContext): FabricActionDescriptor[] =>
  fabricHostCallerId(context) === undefined ? descriptors : hostDescriptors;
const normalizeHostMeshArgs = actionArgNormalizer(() => hostDescriptors);

// Argument repair derives from the action schemas plus the shared synonym
// lexicon; no mesh-specific table remains.
export const normalizeMeshArgs = actionArgNormalizer(() => descriptors);

export class MeshProvider implements FabricProvider {
  readonly name = "mesh";
  readonly description =
    "Durable topics and compare-and-swap shared state for emergent agent coordination";

  constructor(
    readonly store: MeshStore,
    readonly identity: MeshIdentity,
    readonly participants: FabricParticipantSource,
  ) {}

  async list(
    request: FabricProviderListRequest,
    context: FabricInvocationContext,
  ): Promise<FabricActionDescriptor[]> {
    const descriptors = meshDescriptors(context);
    const query = request.query?.toLowerCase();
    return query
      ? descriptors.filter((descriptor) =>
          `${descriptor.name} ${descriptor.description}`.toLowerCase().includes(query),
        )
      : descriptors;
  }

  async describe(
    actionName: string,
    context: FabricInvocationContext,
  ): Promise<FabricActionDescriptor | undefined> {
    if (actionName === "publishBatch" || actionName === "sync") assertHostPublisher(context);
    return meshDescriptors(context).find((descriptor) => descriptor.name === actionName);
  }

  guardArguments(
    actionName: string,
    args: Record<string, unknown>,
    context: FabricInvocationContext,
  ): Record<string, unknown> {
    // Before generic repair or nullish-option stripping: a forbidden key is never ignored.
    assertPublishArguments(actionName, args, context);
    return args;
  }

  prepareArguments(
    actionName: string,
    args: Record<string, unknown>,
    context: FabricInvocationContext,
  ): Record<string, unknown> {
    this.guardArguments(actionName, args, context);
    const normalized = (fabricHostCallerId(context) === undefined ? normalizeMeshArgs : normalizeHostMeshArgs)(actionName, args);
    this.guardArguments(actionName, normalized, context);
    return normalized;
  }

  async invoke(
    actionName: string,
    args: Record<string, unknown>,
    context: FabricInvocationContext,
  ): Promise<unknown> {
    context = snapshotFabricInvocation(context);
    switch (actionName) {
      case "self":
        return this.identity;
      case "publish": {
        assertPublishArguments(actionName, args, context);
        const topic = String(args.topic);
        assertPublishTopic(topic);
        const dedupeKey = namespacedDedupeKey(args, context);
        const checked = typeof args.text === "string" ? await outgoingMessageNotice(args.text, context, this.identity.id) : undefined;
        const publish = (text?: string) => {
          context.signal?.throwIfAborted();
          return this.store.publish({
            topic,
            from: this.identity,
            principal: invocationFabricPrincipal(context),
            signal: context.signal,
            ...(dedupeKey === undefined ? {} : { dedupeKey }),
            ...(typeof args.kind === "string" ? { kind: args.kind } : {}),
            ...(typeof args.to === "string" ? { to: args.to } : {}),
            ...(text === undefined ? {} : { text }),
            ...(args.data !== undefined ? { data: args.data } : {}),
          });
        };
        const event = checked
          ? await deliverWithMessageNotice(args.text as string, checked, publish, "mesh.publish")
          : await publish();
        return checked?.notice ? { ...event, notice: checked.notice } : event;
      }
      case "publishBatch": {
        assertPublishArguments(actionName, args, context);
        const events = args.events as Record<string, unknown>[];
        // Capture/validate the entire request before any append. The store owns per-event
        // receipts and bounded-prefix admission; never wrap an uncertain batch in a retry.
        const inputs = await Promise.all(events.map(async event => {
          const topic = String(event.topic);
          assertPublishTopic(topic);
          const checked = typeof event.text === "string" ? await outgoingMessageNotice(event.text, context, this.identity.id) : undefined;
          const dedupeKey = namespacedDedupeKey(event, context);
          return {
            topic, from: this.identity, principal: invocationFabricPrincipal(context), signal: context.signal,
            ...(dedupeKey === undefined ? {} : { dedupeKey }),
            ...(typeof event.kind === "string" ? { kind: event.kind } : {}),
            ...(typeof event.to === "string" ? { to: event.to } : {}),
            ...(checked ? { text: checked.text } : {}),
            ...(event.data !== undefined ? { data: event.data } : {}),
          };
        }));
        context.signal?.throwIfAborted();
        return this.store.publishBatch(inputs);
      }
      case "sync": {
        assertPublishArguments(actionName, args, context);
        context.signal?.throwIfAborted();
        // `through` is the highest sequence committed to the log before the barrier's fsync, never the
        // reservation counter (latestSequence), which runs ahead of an append (pi-fabric#894).
        const { through } = await this.store.sync(typeof args.budgetMs === "number" ? args.budgetMs : undefined);
        return { synced: true, through };
      }
      case "read":
        return this.store.read({
          ...(typeof args.after === "number" ? { after: args.after } : {}),
          ...(typeof args.topic === "string" ? { topic: args.topic } : {}),
          ...(typeof args.to === "string" ? { to: args.to } : {}),
          ...(typeof args.limit === "number" ? { limit: args.limit } : {}),
        });
      case "members": {
        const kinds = Array.isArray(args.kinds)
          ? args.kinds.filter(
              (kind): kind is "root" | "agent" | "actor" =>
                kind === "root" || kind === "agent" || kind === "actor",
            )
          : undefined;
        const scope =
          args.scope === "local" || args.scope === "lineage" || args.scope === "project"
            ? args.scope
            : "project";
        // Every member unless the caller asks for fewer: a silent default cap dropped live roots
        // once the fleet passed 100 participants (smarty-dev#1241). The list is already in memory.
        const members = this.participants.list({
          scope,
          ...(kinds ? { kinds } : {}),
          ...(args.includeStale === true ? { includeStale: true } : {}),
        });
        return typeof args.limit === "number" ? members.slice(0, Math.max(1, Math.floor(args.limit))) : members;
      }
      case "get": {
        const key = String(args.key);
        assertReadableStateKey(key);
        // Guest code pairs get with compare-and-swap writes: read the current file.
        return this.store.get(key, { fresh: true }) ?? null;
      }
      case "list": {
        const prefix = typeof args.prefix === "string" ? args.prefix : "";
        assertReadableStateKey(prefix);
        const limit = Math.max(
          1,
          Math.min(
            Math.floor(typeof args.limit === "number" ? args.limit : 100),
            this.store.maxReadEvents,
          ),
        );
        return this.store
          .listAll(prefix, { fresh: true })
          .filter(
            (entry) =>
              !PRIVATE_STATE_PREFIXES.some((privatePrefix) =>
                entry.key.startsWith(privatePrefix),
              ),
          )
          .slice(0, limit);
      }
      case "put": {
        const key = String(args.key);
        assertPublicStateKey(key);
        return this.store.put({
          key,
          value: args.value,
          identity: this.identity,
          ...(typeof args.ifVersion === "number" ? { ifVersion: args.ifVersion } : {}),
        });
      }
      case "delete": {
        const key = String(args.key);
        assertPublicStateKey(key);
        return this.store.delete({
          key,
          ...(typeof args.ifVersion === "number" ? { ifVersion: args.ifVersion } : {}),
        });
      }
      default:
        throw new Error(`Unknown mesh action: ${actionName}`);
    }
  }
}
