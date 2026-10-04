import type { FabricActionDescriptor, FabricInvocationContext, FabricProvider, FabricProviderListRequest } from "../protocol.js";
import { StringDecoder } from "node:string_decoder";
import { SHELL_READ_MAX_BYTES, type FabricShellJobStore } from "../core/shell-jobs.js";
import { validationMessage } from "../core/action-arguments.js";

const idSchema = { type: "object", properties: { id: { type: "string", minLength: 1 } }, required: ["id"], additionalProperties: false };
const waitSchema = { ...idSchema, properties: { ...idSchema.properties,
  timeoutMs: { type: "integer", minimum: 1, maximum: 300000, description: "Observation ceiling in milliseconds, not a delay or process deadline. Timeout never cancels the task." },
} };
const watchSchema = { ...waitSchema, properties: { ...waitSchema.properties,
  after: { type: "integer", minimum: 0, maximum: Number.MAX_SAFE_INTEGER, description: "Previously returned nextCursor; default 0. With match it is a byte offset; otherwise a monitor line cursor. Lost positions are disclosed." },
  match: { type: "string", minLength: 1, maxLength: 256, description: "Case-sensitive literal chosen at watch time; works on any background task, like jev-fabric watch <id> <literal>. Omit to read a launch-time monitor." },
} };
const readSchema = { ...idSchema, properties: { ...idSchema.properties,
  offset: { type: "integer", minimum: 0, maximum: Number.MAX_SAFE_INTEGER, description: "Byte offset into the task's combined output since launch; default 0. Pass the returned next." },
  max: { type: "integer", minimum: 1, maximum: SHELL_READ_MAX_BYTES, description: "Largest page in bytes; default 65536." },
  waitMs: { type: "integer", minimum: 1, maximum: 300000, description: "Long-poll ceiling: return as soon as bytes past offset exist or the task ends. Never stops the task." },
  encoding: { type: "string", enum: ["text", "base64"], description: "text (default) never splits a UTF-8 character; base64 returns byte-exact data." },
} };
const WATCH_LINES = 64;
const WATCH_LINE_CHARS = 2048;
const clean = (line: string): string => line.replace(/[\u0000-\u001f\u007f-\u009f]/g, " ").trim();
const descriptors: FabricActionDescriptor[] = [
  { name: "list", description: "List this session's tracked shell tasks and monitors, with IDs, command, cwd, state, timestamps, and bounded monitor events. No output polling is needed: detached completions notify the owning agent.", inputSchema: { type: "object", properties: {}, additionalProperties: false }, risk: "read", effect: { kind: "none", ordering: "commutative" } },
  { name: "get", description: "Inspect one shell task by ID, returning metadata and a bounded output tail. Acknowledges its pending notification after a successful read. Full retained output is at logPath (bounded, not an archive).", inputSchema: idSchema, risk: "read", effect: { kind: "emission", ordering: "ordered" } },
  { name: "wait", description: "Wait without polling for a session-owned shell task to finish, default 30 seconds. Returns task metadata, a bounded output tail and timedOut. Inspect status and exitCode; an exited task is not goal verification. Timeout or cancellation stops only this wait.", inputSchema: waitSchema, risk: "read", effect: { kind: "emission", ordering: "ordered" } },
  { name: "read", description: "Read a background task's combined output by byte offset, in jev-fabric's read record shape: offset, bytes, omittedBytes, text (or base64 data), next, eof, state. Optional waitMs long-polls for new bytes. The live window is 1 MiB; 32 KiB stays readable after exit. Never consumes or stops anything.", inputSchema: readSchema, risk: "read", effect: { kind: "none", ordering: "commutative" } },
  { name: "watch", description: "Wait without polling for matching lines, task exit, or timeout (default 5 seconds). With match (any background task, chosen now): returns complete output lines containing that literal after the byte cursor, omittedBytes, more and nextCursor. Without match: reads a launch-time monitor (pi.bash monitor with delivery ui). Returns reason, up to 64 lines after the cursor, losses (burst/evicted cursor ranges), omitted, more and nextCursor with task metadata; pass nextCursor as after, immediately again while more is true. The latest 256 positions are replayable. No inference, wakeup, renewal or cancellation of the task. Not a lossless RPC stream.", inputSchema: watchSchema, risk: "read", effect: { kind: "none", ordering: "commutative" } },
  { name: "stop", description: "Stop one session-owned shell task or monitor by ID using its existing abort controller, not an arbitrary PID. A durable task is stopped through jev-fabric. Cancellation does not wake the owning agent.", inputSchema: idSchema, risk: "execute", effect: { kind: "emission", ordering: "ordered" } },
  { name: "external", description: "List jev-fabric jobs in this session's durable store that no task here tracks, for example dev servers started by another harness or an earlier session. Returns id, state, label and start time; read-only.", inputSchema: { type: "object", properties: {}, additionalProperties: false }, risk: "read", effect: { kind: "none", ordering: "commutative" } },
  { name: "adopt", description: "Attach an existing jev-fabric job from tasks.external to this session as a durable task: its output, completion notification, wait/stop and inspector row. Adopting does not restart or signal the process.", inputSchema: { type: "object", properties: { jobId: { type: "string", minLength: 1, maxLength: 128 }, description: { type: "string", minLength: 1, maxLength: 120 } }, required: ["jobId"], additionalProperties: false }, risk: "read", effect: { kind: "emission", ordering: "ordered" } },
];
const durableOnly = new Set(["external", "adopt"]);

export class TasksProvider implements FabricProvider {
  readonly name = "tasks";
  readonly description = "Session-owned shell orchestration with bounded wait/watch and explicit stop";
  constructor(readonly jobs: FabricShellJobStore) {}
  async list(request: FabricProviderListRequest): Promise<FabricActionDescriptor[]> {
    const query = request.query?.toLowerCase();
    const available = this.jobs.durable ? descriptors : descriptors.filter(d => !durableOnly.has(d.name));
    return query ? available.filter(d => `${d.name} ${d.description}`.toLowerCase().includes(query)) : available;
  }
  async describe(name: string): Promise<FabricActionDescriptor | undefined> {
    return this.jobs.durable || !durableOnly.has(name) ? descriptors.find(d => d.name === name) : undefined;
  }
  async invoke(name: string, args: Record<string, unknown>, context: FabricInvocationContext): Promise<unknown> {
    const descriptor = await this.describe(name);
    if (!descriptor) throw new Error(`Unknown tasks action: ${name}`);
    const invalid = validationMessage(descriptor.inputSchema, args);
    if (invalid) throw new Error(`Invalid tasks.${name} arguments: ${invalid}`);
    // Reattach this session's durable tasks before answering about them.
    await this.jobs.durable?.resume();
    if (name === "list") return this.jobs.list();
    if (name === "external") return await this.jobs.durable!.external();
    if (name === "adopt") return { task: await this.jobs.durable!.adopt(args.jobId as string, args.description as string | undefined) };
    const id = args.id as string;
    const job = this.jobs.get(id);
    if (!job) throw new Error(`Unknown shell task: ${id}`);
    if (name === "stop") return { stopped: this.jobs.stop(id), task: job.info() };
    if (name === "read") {
      const offset = (args.offset as number | undefined) ?? 0;
      if (offset > job.written) throw new Error("tasks.read offset must not be past the task's output");
      if (args.waitMs !== undefined) await job.whenOutput(offset, args.waitMs as number, context.signal);
      return job.read(offset, (args.max as number | undefined) ?? SHELL_READ_MAX_BYTES, (args.encoding as "text" | "base64" | undefined) ?? "text");
    }
    if (name === "watch" && typeof args.match === "string") {
      return this.#watchOutput(job, args.match, (args.after as number | undefined) ?? 0, (args.timeoutMs as number | undefined) ?? 5000, context.signal);
    }
    if (name === "watch") {
      if (!job.options.monitor) throw new Error("tasks.watch needs a match literal, or a task started with monitor");
      const after = (args.after as number | undefined) ?? 0;
      const { task, timedOut } = await this.jobs.waitFor(id, { after, timeoutMs: (args.timeoutMs as number | undefined) ?? 5000, signal: context.signal });
      const page = job.replay(after);
      return { task, reason: page.nextCursor > after ? "event" : timedOut ? "timeout" : "finished", ...page };
    }
    const waited = name === "wait"
      ? await this.jobs.waitFor(id, { timeoutMs: (args.timeoutMs as number | undefined) ?? 30000, signal: context.signal })
      : undefined;
    const before = job.info();
    const output = await job.outputText();
    const after = job.info();
    // A new event during the async read was not necessarily consumed. Recheck
    // again at delivery: publication/encoding/transport can reject this result,
    // or a newer event/finish can arrive while its response is queued.
    if (!waited?.timedOut && before.eventCount === after.eventCount && before.finishedAt === after.finishedAt) {
      const acknowledge = (): void => {
        const current = job.info();
        if (current.unread && current.eventCount === after.eventCount && current.finishedAt === after.finishedAt) this.jobs.acknowledge(id);
      };
      if (context.deferResultConsumption) context.deferResultConsumption(acknowledge);
      else acknowledge();
    }
    return { task: job.info(), output, ...(waited ? { timedOut: waited.timedOut } : {}) };
  }

  /** Literal line filter over the byte stream, like jev-fabric watch; the cursor stops at line boundaries. */
  async #watchOutput(job: NonNullable<ReturnType<FabricShellJobStore["get"]>>, match: string, after: number, timeoutMs: number, signal: AbortSignal | undefined) {
    if (after > job.written) throw new Error("tasks.watch after must not be past the task's output");
    const deadline = Date.now() + timeoutMs;
    const lines: string[] = [];
    const decoder = new StringDecoder("utf8");
    let readCursor = after;
    let cursor = after;
    let lineStart = after;
    let pending = "";
    let omittedBytes = 0;
    const result = (reason: "event" | "finished" | "timeout") =>
      ({ task: job.info(), reason, lines, omittedBytes, more: cursor < job.written, nextCursor: cursor });
    for (;;) {
      signal?.throwIfAborted();
      const previousReadCursor = readCursor;
      const page = job.read(readCursor, SHELL_READ_MAX_BYTES, "base64");
      omittedBytes += page.omittedBytes;
      if (page.offset > lineStart && !pending) {
        lineStart = page.offset;
        cursor = page.offset;
      }
      readCursor = page.next;
      pending += decoder.write(Buffer.from(page.data ?? "", "base64"));
      for (;;) {
        const end = pending.indexOf("\n");
        if (end < 0 || lines.length >= WATCH_LINES) break;
        const line = pending.slice(0, end);
        pending = pending.slice(end + 1);
        lineStart += Buffer.byteLength(line + "\n");
        cursor = lineStart;
        if (line.includes(match)) lines.push(clean(line).slice(0, WATCH_LINE_CHARS));
      }
      if (lines.length) return result("event");
      if (page.eof) {
        pending += decoder.end();
        if (pending && lines.length < WATCH_LINES) {
          if (pending.includes(match)) lines.push(clean(pending).slice(0, WATCH_LINE_CHARS));
          cursor = readCursor;
        }
        return result("finished");
      }
      // Bound an unterminated line and, crucially, advance the read cursor even
      // when a page ends in an incomplete UTF-8 sequence.
      if (pending && readCursor - lineStart >= SHELL_READ_MAX_BYTES) {
        if (pending.includes(match)) lines.push(clean(pending).slice(0, WATCH_LINE_CHARS));
        pending = "";
        lineStart = readCursor;
        cursor = readCursor;
        if (lines.length) return result("event");
      }
      signal?.throwIfAborted();
      const remaining = deadline - Date.now();
      if (remaining <= 0) return result("timeout");
      if (readCursor > previousReadCursor && job.written > readCursor) continue;
      // A shortened/empty read must wait beyond the raw tail, not rescan it.
      await job.whenOutput(job.written, remaining, signal);
    }
  }
}
