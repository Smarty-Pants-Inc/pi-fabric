import type {
  FabricActionDescriptor,
  FabricInvocationContext,
  FabricProvider,
  FabricProviderListRequest,
} from "../protocol.js";
<<<<<<< HEAD
import { invocationFabricPrincipal, snapshotFabricInvocation } from "../fabric-provenance.js";
import { MeshStore, type MeshIdentity } from "../mesh/store.js";
=======
import {
  MESH_MAX_PENDING_SCHEDULES,
  MESH_MAX_SCHEDULE_AHEAD_MS,
  MeshStore,
  type MeshIdentity,
} from "../mesh/store.js";
import {
  createMeshGrant,
  listMeshGrants,
  meshCliArgv,
  meshPostCommand,
  MESH_GRANT_MAX_TTL_MS,
  MESH_GRANT_MAX_USES,
  MESH_GRANT_MIN_TTL_MS,
  revokeMeshGrant,
} from "../mesh/grants.js";
>>>>>>> upstream-v0.105.0
import type { FabricParticipantSource } from "../topology/types.js";
import { FABRIC_PARTICIPANT_LIFECYCLE_TOPIC } from "../lifecycle/types.js";
import { actionArgNormalizer } from "./arg-normalization.js";
import { deliverWithMessageNotice, outgoingMessageNotice } from "./message-id-notice.js";

const emptySchema = { type: "object", properties: {}, additionalProperties: false };
const INTERNAL_STATE_PREFIXES = ["topology/", "sessions/", "actors/", "residency/", "decisions/"];
const PRIVATE_STATE_PREFIXES = ["residency/"];
const INTERNAL_CONTROL_PREFIX = "fabric.control.";
const INTERNAL_HOST_EVENT_TOPIC = "fabric.actor.host-event";

/** Topics reserved for host coordination; user code, schedules and grants may not target them. */
export const assertPublicMeshTopic = (topic: string): string => {
  if (
    topic.startsWith(INTERNAL_CONTROL_PREFIX) ||
    topic === INTERNAL_HOST_EVENT_TOPIC ||
    topic === FABRIC_PARTICIPANT_LIFECYCLE_TOPIC
  ) {
    throw new Error(`Fabric mesh topic is reserved for host coordination: ${topic}`);
  }
  return topic;
};

/** Resolves `notBefore` (epoch ms or ISO 8601) or `afterMs` to an absolute due time. */
export const meshScheduleDueAt = (args: Record<string, unknown>, now = Date.now()): number | undefined => {
  if (args.notBefore !== undefined && args.afterMs !== undefined) {
    throw new Error("mesh.publish accepts notBefore or afterMs, not both");
  }
  if (args.afterMs !== undefined) {
    const afterMs = args.afterMs;
    if (typeof afterMs !== "number" || !Number.isFinite(afterMs) || afterMs < 0) {
      throw new Error("mesh.publish afterMs must be a nonnegative number of milliseconds");
    }
    return now + Math.ceil(afterMs);
  }
  if (args.notBefore === undefined) return undefined;
  const dueAt = typeof args.notBefore === "number"
    ? args.notBefore
    : typeof args.notBefore === "string" && /^\d{4}-\d{2}-\d{2}T/.test(args.notBefore)
      ? Date.parse(args.notBefore)
      : Number.NaN;
  if (!Number.isFinite(dueAt) || dueAt < 0) {
    throw new Error("mesh.publish notBefore must be epoch milliseconds or an ISO 8601 date-time");
  }
  return Math.ceil(dueAt);
};

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
    description:
      "Append a durable event to a mesh topic, optionally addressed to one actor. notBefore (epoch ms or ISO) or afterMs schedules it instead; key makes the schedule replaceable",
    inputSchema: {
      type: "object",
      properties: {
        topic: { type: "string" },
        kind: { type: "string" },
        to: { type: "string" },
        text: { type: "string" },
        data: {},
        notBefore: { anyOf: [{ type: "number", minimum: 0 }, { type: "string", maxLength: 64 }] },
        afterMs: { type: "number", minimum: 0, maximum: MESH_MAX_SCHEDULE_AHEAD_MS },
        key: { type: "string", minLength: 1, maxLength: 128 },
      },
      required: ["topic"],
      additionalProperties: false,
    },
    risk: "agent",
    namespace: "coordination",
  },
  {
    name: "scheduled",
    description: "List pending scheduled mesh events by due time",
    inputSchema: {
      type: "object",
      properties: {
        topic: { type: "string" },
        limit: { type: "number", minimum: 1, maximum: MESH_MAX_PENDING_SCHEDULES },
      },
      additionalProperties: false,
    },
    risk: "read",
    namespace: "coordination",
  },
  {
    name: "unschedule",
    description: "Cancel a pending scheduled mesh event by key",
    inputSchema: {
      type: "object",
      properties: { key: { type: "string", minLength: 1, maxLength: 128 } },
      required: ["key"],
      additionalProperties: false,
    },
    risk: "agent",
    namespace: "coordination",
  },
  {
    name: "grant",
    description:
      "Mint a scoped token that lets an outside process post untrusted events to one topic through `pi-fabric mesh post`",
    inputSchema: {
      type: "object",
      properties: {
        topic: { type: "string" },
        ttlMs: { type: "number", minimum: MESH_GRANT_MIN_TTL_MS, maximum: MESH_GRANT_MAX_TTL_MS },
        uses: { type: "number", minimum: 1, maximum: MESH_GRANT_MAX_USES },
        kind: { type: "string", minLength: 1, maxLength: 128 },
      },
      required: ["topic", "ttlMs"],
      additionalProperties: false,
    },
    // Opens an ingress for principals outside this session: the most conservative fitting class.
    risk: "network",
    namespace: "coordination",
  },
  {
    name: "revoke",
    description: "Revoke a scoped external mesh grant",
    inputSchema: {
      type: "object",
      properties: { grantId: { type: "string", minLength: 1, maxLength: 64 } },
      required: ["grantId"],
      additionalProperties: false,
    },
    risk: "write",
    namespace: "coordination",
  },
  {
    name: "grants",
    description: "List unexpired external mesh grants (never tokens)",
    inputSchema: emptySchema,
    risk: "read",
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
    context = snapshotFabricInvocation(context);
    switch (actionName) {
      case "self":
        return this.identity;
      case "publish": {
<<<<<<< HEAD
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
=======
        const topic = assertPublicMeshTopic(String(args.topic));
        const message = {
          topic,
          from: this.identity,
          ...(typeof args.kind === "string" ? { kind: args.kind } : {}),
          ...(typeof args.to === "string" ? { to: args.to } : {}),
          ...(typeof args.text === "string" ? { text: args.text } : {}),
          ...(args.data !== undefined ? { data: args.data } : {}),
        };
        const dueAt = meshScheduleDueAt(args);
        if (dueAt === undefined) {
          if (args.key !== undefined) throw new Error("mesh.publish key requires notBefore or afterMs");
          return this.store.publish(message);
        }
        const schedule = await this.store.schedule({
          ...message,
          dueAt,
          ...(typeof args.key === "string" ? { key: args.key } : {}),
        });
        return { scheduled: true, ...schedule };
>>>>>>> upstream-v0.105.0
      }
      case "scheduled":
        return this.store.scheduled({
          ...(typeof args.topic === "string" ? { topic: args.topic } : {}),
          ...(typeof args.limit === "number" ? { limit: args.limit } : {}),
        });
      case "unschedule":
        return this.store.unschedule(String(args.key));
      case "grant": {
        const topic = assertPublicMeshTopic(String(args.topic));
        const { grant, token } = await createMeshGrant(this.store, {
          topic,
          ttlMs: Number(args.ttlMs),
          ...(typeof args.uses === "number" ? { uses: args.uses } : {}),
          ...(typeof args.kind === "string" ? { kind: args.kind } : {}),
          createdBy: this.identity,
        });
        return {
          ...grant,
          token,
          command: meshPostCommand(meshCliArgv(), {
            root: this.store.root,
            token,
            ...(grant.kind !== undefined ? { kind: grant.kind } : {}),
          }),
        };
      }
      case "revoke":
        return revokeMeshGrant(this.store, String(args.grantId));
      case "grants":
        return listMeshGrants(this.store);
      case "read":
        // Reading the mesh is a touch: release schedules that fell due while no host ran.
        await this.store.releaseDueSchedules().catch(() => undefined);
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
