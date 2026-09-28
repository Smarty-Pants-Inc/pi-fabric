import type { FabricActionDescriptor, FabricInvocationContext, FabricProvider, FabricProviderListRequest } from "../protocol.js";
import { RECORD_KINDS } from "../records/kinds.js";

/** The slice of the records service the provider calls; the service is opened at first use. */
export interface RecordsProviderService {
  principal(): import("../records/store.js").RecordsPrincipal;
  readonly backend: import("../records/store.js").RecordsBackend;
  status(): Promise<unknown>;
}

const kind = { type: "string", enum: [...RECORD_KINDS] };
const cursor = { type: "integer", minimum: 0 };
const limit = { type: "integer", minimum: 1, maximum: 500 };

const descriptors: FabricActionDescriptor[] = [
  {
    name: "append",
    description: "Append one record (issue, status, comment, decision, ask, answer, handoff, link, close, reopen) to the org's record on its Node. Returns {id, sequence, origin, topic, ref, key, createdAt} only after the commit. key is required: retry with the same key after any error or timeout; an identical retry returns the original receipt, and a different payload under the key is refused. The author is the calling participant. Past the archive lag bound it refuses with a retryable RECORD_ARCHIVE_LAGGING error.",
    inputSchema: {
      type: "object", required: ["kind", "key"], additionalProperties: false,
      properties: {
        ref: { type: "string", description: "Owner/repo#123, or a Node-native Owner/repo#L12" },
        repo: { type: "string", description: "Owner/repo: creates an issue (kind issue, no ref) and allocates its Node-native ref" },
        kind, key: { type: "string", minLength: 1, maxLength: 256 }, text: { type: "string" },
        data: { type: "object", description: "The kind's fields (see the records reference)" },
        supersedes: { type: "string", description: "The id of the record this one replaces (same ref and kind)" },
        author: { type: "string", description: "Importer role only, with data.via" },
      },
    },
    risk: "write", namespace: "coordination",
    effect: { kind: "emission", resources: ["records"], ordering: "ordered" },
  },
  {
    name: "read",
    description: "Page records in commit order after a processing cursor, up to the committed frontier: {records, next, frontier, origin}. Consumers that must not miss a record read by cursor and advance it only after acting.",
    inputSchema: { type: "object", additionalProperties: false, properties: { after: cursor, limit, origin: { type: "string" }, ref: { type: "string" }, kind, to: { type: "string" } } },
    risk: "read", namespace: "coordination",
  },
  {
    name: "get",
    description: "One ref's fold (title, owner, open, each author's current status, open asks, decisions, links, mirror state) plus its history, oldest first, paged by sequence.",
    inputSchema: { type: "object", required: ["ref"], additionalProperties: false, properties: { ref: { type: "string" }, after: cursor, limit } },
    risk: "read", namespace: "coordination",
  },
  {
    name: "list",
    description: "A view query over current issues (for boards and alarms, never a delivery path): items with statuses per author and open asks, newest update first.",
    inputSchema: {
      type: "object", additionalProperties: false,
      properties: { org: { type: "string" }, repo: { type: "string" }, open: { type: "boolean" }, owner: { type: "string" }, hasOpenAsk: { type: "boolean" }, updatedSince: cursor, limit, after: { type: "string" } },
    },
    risk: "read", namespace: "coordination",
  },
  {
    name: "status",
    description: "The record layer's state on this Node: org, origin, committed frontier, unpublished nudges, and C2 archive admission.",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
    risk: "read", namespace: "coordination",
  },
];

/** Stop waiting for a shared promise (the service opening) when the caller is gone; it keeps opening. */
const untilAborted = <T>(promise: Promise<T>, signal: AbortSignal | undefined): Promise<T> => {
  if (!signal) return promise;
  signal.throwIfAborted();
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(resolve, reject).finally(() => signal.removeEventListener("abort", onAbort));
  });
};

export class RecordsProvider implements FabricProvider {
  readonly name = "records";
  readonly description = "The org's durable record on its Node (PostgreSQL): append, read by cursor, get a ref's fold, list views";

  constructor(readonly service: () => Promise<RecordsProviderService>) {}

  async list(request: FabricProviderListRequest): Promise<FabricActionDescriptor[]> {
    const query = request.query?.toLowerCase();
    return descriptors.filter((action) => !query || `${action.name} ${action.description}`.toLowerCase().includes(query));
  }

  async describe(name: string): Promise<FabricActionDescriptor | undefined> {
    return descriptors.find((action) => action.name === name);
  }

  async invoke(name: string, args: Record<string, unknown>, context: FabricInvocationContext): Promise<unknown> {
    if (!descriptors.some((action) => action.name === name)) throw new Error(`Unknown records action: ${name}`);
    // The call's lifetime reaches the store: a cancelled or revoked call starts no later step and commits nothing.
    const signal = context.signal;
    const service = await untilAborted(this.service(), signal);
    signal?.throwIfAborted();
    if (name === "status") return service.status();
    const principal = service.principal();
    // The caller's lifetime; closing the service disconnects its client, which cancels every call on the service.
    const options = signal ? { signal } : {};
    switch (name) {
      case "append": return service.backend.append(principal, args, options);
      case "read": return service.backend.read(principal, args, options);
      case "get": return service.backend.get(principal, args, options);
      default: return service.backend.list(principal, args, options);
    }
  }
}
