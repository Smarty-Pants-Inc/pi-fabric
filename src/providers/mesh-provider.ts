import type {
  FabricActionDescriptor,
  FabricInvocationContext,
  FabricProvider,
  FabricProviderListRequest,
} from "../protocol.js";
import { invocationFabricPrincipal, snapshotFabricInvocation } from "../fabric-provenance.js";
import { MeshStore, type MeshIdentity } from "../mesh/store.js";
import type { FabricParticipantSource } from "../topology/types.js";
import type { AsyncMeshStateStore, AsyncMeshStateStoreOptions } from "../mesh/state-async.js";

/** Audited independent single-key state only; host/typed state never moves with this opt-in. */
const ASYNC_STATE_PREFIX = "shared/";
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



// Argument repair derives from the action schemas plus the shared synonym
// lexicon; no mesh-specific table remains.
export const normalizeMeshArgs = actionArgNormalizer(() => descriptors);

export class MeshProvider implements FabricProvider {
  readonly name = "mesh";
  readonly description =
    "Durable topics and compare-and-swap shared state for emergent agent coordination";

  #asyncState: AsyncMeshStateStore | undefined;
  #closed = false;
  #closing: Promise<void> | undefined;

  /** Explicit experimental selector. No startup registration, config/env override or fallback. */
  static async withStateBackend(
    store: MeshStore,
    identity: MeshIdentity,
    participants: FabricParticipantSource,
    options: Extract<AsyncMeshStateStoreOptions, { backend: "nats-kv" }>,
  ): Promise<MeshProvider> {
    if (options.backend !== "nats-kv" || options.nats?.experimentalNatsKv !== true) {
      throw new Error("Async mesh tools require backend: nats-kv and experimentalNatsKv: true");
    }
    // Optional network/client work starts only after this explicitly awaited factory call.
    const { openAsyncMeshStateStore } = await import("../mesh/state-async.js");
    const state = await openAsyncMeshStateStore(store.root, options);
    const provider = new MeshProvider(store, identity, participants);
    provider.#asyncState = state;
    return provider;
  }

  #usesAsyncState(key: string): boolean {
    return this.#asyncState !== undefined && key.startsWith(ASYNC_STATE_PREFIX);
  }

  async #listState(prefix: string) {
    if (this.#asyncState && prefix.startsWith(ASYNC_STATE_PREFIX)) {
      return (await this.#asyncState.listAll(prefix))
        .filter(entry => entry.key.startsWith(ASYNC_STATE_PREFIX) && entry.key.startsWith(prefix))
        .sort((a, b) => a.key.localeCompare(b.key));
    }
    const local = this.store.listAll(prefix, { fresh: true });
    if (!this.#asyncState) return local;
    const selected = local.filter(entry => !entry.key.startsWith(ASYNC_STATE_PREFIX));
    if (prefix.startsWith(ASYNC_STATE_PREFIX) || ASYNC_STATE_PREFIX.startsWith(prefix)) {
      const remote = await this.#asyncState.listAll(prefix.length < ASYNC_STATE_PREFIX.length ? ASYNC_STATE_PREFIX : prefix);
      selected.push(...remote.filter(entry => entry.key.startsWith(ASYNC_STATE_PREFIX) && entry.key.startsWith(prefix)));
    }
    // Mixed listings are not atomic cross-backend snapshots; remote values cannot shadow host state.
    return selected.sort((a, b) => a.key.localeCompare(b.key));
  }

  /** Provider owns only its explicit async handle; the supplied MeshStore remains caller-owned. */
  close(): Promise<void> {
    if (this.#closing) return this.#closing;
    this.#closed = true;
    this.#closing = (async () => { await this.#asyncState?.close(); })();
    return this.#closing;
  }

  constructor(
    readonly store: MeshStore,
    readonly identity: MeshIdentity,
    readonly participants: FabricParticipantSource,
  ) {}

  async list(
    request: FabricProviderListRequest,
    _context: FabricInvocationContext,
  ): Promise<FabricActionDescriptor[]> {
    const query = request.query?.toLowerCase();
    return query
      ? descriptors.filter((descriptor) =>
          `${descriptor.name} ${descriptor.description}`.toLowerCase().includes(query),
        )
      : descriptors;
  }

  async describe(
    actionName: string,
    _context: FabricInvocationContext,
  ): Promise<FabricActionDescriptor | undefined> {
    return descriptors.find((descriptor) => descriptor.name === actionName);
  }

  prepareArguments(
    actionName: string,
    args: Record<string, unknown>,
  ): Record<string, unknown> {
    return normalizeMeshArgs(actionName, args);
  }

  async invoke(
    actionName: string,
    args: Record<string, unknown>,
    context: FabricInvocationContext,
  ): Promise<unknown> {
    if (this.#closed) throw new Error("MeshProvider is closed");
    context = snapshotFabricInvocation(context);
    switch (actionName) {
      case "self":
        return this.identity;
      case "publish": {
        const topic = String(args.topic);
        if (
          topic.startsWith(INTERNAL_CONTROL_PREFIX) ||
          topic === INTERNAL_HOST_EVENT_TOPIC ||
          topic === FABRIC_PARTICIPANT_LIFECYCLE_TOPIC
        ) {
          throw new Error(`Fabric mesh topic is reserved for host coordination: ${topic}`);
        }
        const checked = typeof args.text === "string" ? await outgoingMessageNotice(args.text, context, this.identity.id) : undefined;
        const publish = (text?: string) => {
          context.signal?.throwIfAborted();
          return this.store.publish({
            topic,
            from: this.identity,
            principal: invocationFabricPrincipal(context),
            signal: context.signal,
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
        // Guest code pairs get with CAS: await remote leader authority, never a watch cache.
        return (this.#usesAsyncState(key)
          ? await this.#asyncState!.get(key)
          : this.store.get(key, { fresh: true })) ?? null;
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
        return (await this.#listState(prefix))
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
        return (this.#usesAsyncState(key) ? this.#asyncState! : this.store).put({
          key,
          value: args.value,
          identity: this.identity,
          ...(typeof args.ifVersion === "number" ? { ifVersion: args.ifVersion } : {}),
        });
      }
      case "delete": {
        const key = String(args.key);
        assertPublicStateKey(key);
        return (this.#usesAsyncState(key) ? this.#asyncState! : this.store).delete({
          key,
          ...(typeof args.ifVersion === "number" ? { ifVersion: args.ifVersion } : {}),
        });
      }
      default:
        throw new Error(`Unknown mesh action: ${actionName}`);
    }
  }
}
