import type { FabricActionDescriptor, FabricInvocationContext, FabricProvider, FabricProviderListRequest } from "../protocol.js";
import type { FabricShellJobStore } from "../core/shell-jobs.js";
import { validationMessage } from "../core/action-arguments.js";

const idSchema = { type: "object", properties: { id: { type: "string", minLength: 1 } }, required: ["id"], additionalProperties: false };
const waitSchema = { ...idSchema, properties: { ...idSchema.properties,
  timeoutMs: { type: "integer", minimum: 1, maximum: 300000, description: "Observation ceiling in milliseconds, not a delay or process deadline. Timeout never cancels the task." },
} };
const watchSchema = { ...waitSchema, properties: { ...waitSchema.properties,
  after: { type: "integer", minimum: 0, maximum: Number.MAX_SAFE_INTEGER, description: "Previously returned nextCursor; default 0. Missing retained events are disclosed as omitted." },
} };
const descriptors: FabricActionDescriptor[] = [
  { name: "list", description: "List this session's tracked shell tasks and monitors, with IDs, command, cwd, state, timestamps, and bounded monitor events. No output polling is needed: detached completions notify the owning agent.", inputSchema: { type: "object", properties: {}, additionalProperties: false }, risk: "read", effect: { kind: "none", ordering: "commutative" } },
  { name: "get", description: "Inspect one shell task by ID, returning metadata and a bounded output tail. Acknowledges its pending notification after a successful read. Full retained output is at logPath (bounded, not an archive).", inputSchema: idSchema, risk: "read", effect: { kind: "emission", ordering: "ordered" } },
  { name: "wait", description: "Wait without polling for a session-owned shell task to finish, default 30 seconds. Returns task metadata, a bounded output tail and timedOut. Inspect status and exitCode; an exited task is not goal verification. Timeout or cancellation stops only this wait.", inputSchema: waitSchema, risk: "read", effect: { kind: "emission", ordering: "ordered" } },
  { name: "watch", description: "Wait without polling for the next bounded monitor batch, task exit, or timeout (default 5 seconds). Start pi.bash with monitor.delivery ui and an optional literal monitor.match first. Returns reason, lines, omitted, nextCursor and task metadata; pass nextCursor as after. No inference, wakeup, renewal or cancellation of the task. Previews are not a lossless RPC stream.", inputSchema: watchSchema, risk: "read", effect: { kind: "none", ordering: "commutative" } },
  { name: "stop", description: "Stop one session-owned shell task or monitor by ID using its existing abort controller, not an arbitrary PID. Cancellation does not wake the owning agent.", inputSchema: idSchema, risk: "execute", effect: { kind: "emission", ordering: "ordered" } },
];

export class TasksProvider implements FabricProvider {
  readonly name = "tasks";
  readonly description = "Session-owned shell orchestration with bounded wait/watch and explicit stop";
  constructor(readonly jobs: FabricShellJobStore) {}
  async list(request: FabricProviderListRequest): Promise<FabricActionDescriptor[]> {
    const query = request.query?.toLowerCase();
    return query ? descriptors.filter(d => `${d.name} ${d.description}`.toLowerCase().includes(query)) : descriptors;
  }
  async describe(name: string): Promise<FabricActionDescriptor | undefined> { return descriptors.find(d => d.name === name); }
  async invoke(name: string, args: Record<string, unknown>, context: FabricInvocationContext): Promise<unknown> {
    const descriptor = await this.describe(name);
    if (!descriptor) throw new Error(`Unknown tasks action: ${name}`);
    const invalid = validationMessage(descriptor.inputSchema, args);
    if (invalid) throw new Error(`Invalid tasks.${name} arguments: ${invalid}`);
    if (name === "list") return this.jobs.list();
    const id = args.id as string;
    const job = this.jobs.get(id);
    if (!job) throw new Error(`Unknown shell task: ${id}`);
    if (name === "stop") return { stopped: this.jobs.stop(id), task: job.info() };
    if (name === "watch") {
      if (!job.options.monitor) throw new Error("tasks.watch requires a task started with monitor; use tasks.wait for ordinary tasks");
      const after = (args.after as number | undefined) ?? 0;
      const { task, timedOut } = await this.jobs.waitFor(id, { after, timeoutMs: (args.timeoutMs as number | undefined) ?? 5000, signal: context.signal });
      const count = task.eventCount - after;
      const retained = task.lastEvent?.lines ?? [];
      const lines = count > 0 ? retained.slice(-Math.min(count, retained.length)) : [];
      return { task, reason: count > 0 ? "event" : timedOut ? "timeout" : "finished", lines,
        omitted: Math.max(0, count - lines.length), nextCursor: task.eventCount };
    }
    const waited = name === "wait"
      ? await this.jobs.waitFor(id, { timeoutMs: (args.timeoutMs as number | undefined) ?? 30000, signal: context.signal })
      : undefined;
    const before = job.info();
    const output = await job.outputText();
    const after = job.info();
    // A new event during the async read was not necessarily consumed.
    if (!waited?.timedOut && before.eventCount === after.eventCount && before.finishedAt === after.finishedAt) this.jobs.acknowledge(id);
    return { task: job.info(), output, ...(waited ? { timedOut: waited.timedOut } : {}) };
  }
}
