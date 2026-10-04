import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { writePolicyDenial, type FabricWritePolicy } from "../agents/write-guard.js";
import type { FabricActionDescriptor, FabricInvocationContext, FabricProvider, FabricProviderListRequest } from "../protocol.js";
import { validationMessage } from "../core/action-arguments.js";
import type { DurableShellBridge } from "../jev-fabric/bridge.js";
import type { JevFabricServe } from "../jev-fabric/serve.js";

const DAY_MS = 24 * 3_600_000;
const id = { type: "string", minLength: 1, maxLength: 128, description: "Session ID from sessions.open (`s-…` for session lifetime) or a durable jev-fabric job ID." };
const idOnly = { type: "object", properties: { id }, required: ["id"], additionalProperties: false };
const waitMs = { type: "integer", minimum: 1, maximum: 300000, description: "Long-poll ceiling; ready evidence returns at once. Never stops the child." };

const descriptors: FabricActionDescriptor[] = [
  {
    name: "open",
    description: "Start an interactive child with its stdin kept open, owned by jev-fabric (macOS/Linux). Lifetime session (default) ends with this Pi session, or with the Jev program that opened it; durable: true keeps it in the jev-fabric store after Pi exits. Give argv (literal, no shell) or cmd (run by bash -c). Returns id, lifetime and state. Drive it with write/read; stop it explicitly.",
    inputSchema: { type: "object", additionalProperties: false, properties: {
      argv: { type: "array", minItems: 1, maxItems: 57, items: { type: "string", minLength: 1, maxLength: 4096 }, description: "Literal argv; argv[0] is the executable." },
      cmd: { type: "string", minLength: 1, maxLength: 65536, description: "Shell source for bash -c, when shell syntax is needed." },
      cwd: { type: "string", minLength: 1, description: "Working directory; default this session's cwd." },
      timeoutMs: { type: "integer", minimum: 1000, maximum: DAY_MS, description: "Child lifetime ceiling; default 1 hour, at most 24 hours." },
      label: { type: "string", minLength: 1, maxLength: 120, description: "Short human-readable purpose." },
      durable: { type: "boolean", description: "Keep the child in the jev-fabric store beyond this session; input goes through its queue (about one 25 ms tick of latency)." },
    } },
    risk: "execute", effect: { kind: "emission", ordering: "ordered" },
  },
  {
    name: "write",
    description: "Append text to an interactive child's stdin, in order. Returns written bytes. A finished child or closed stdin is an error, never a silent drop.",
    inputSchema: { type: "object", properties: { id, text: { type: "string", maxLength: 65536 } }, required: ["id", "text"], additionalProperties: false },
    risk: "execute", effect: { kind: "emission", ordering: "ordered" },
  },
  { name: "closeInput", description: "Send EOF to an interactive child's stdin. Idempotent.", inputSchema: idOnly, risk: "execute", effect: { kind: "emission", ordering: "ordered" } },
  {
    name: "read",
    description: "Read stdout or stderr bytes from an offset: {offset, bytes, omittedBytes, text | data, next, eof, state}. Offsets never reset; at least the newest 1 MiB per stream stays readable and older bytes are disclosed as omittedBytes. With waitMs it returns as soon as bytes past offset exist. Pass next as the following offset.",
    inputSchema: { type: "object", additionalProperties: false, required: ["id"], properties: {
      id,
      stream: { type: "string", enum: ["stdout", "stderr"], description: "Default stdout." },
      offset: { type: "integer", minimum: 0, maximum: Number.MAX_SAFE_INTEGER },
      max: { type: "integer", minimum: 1, maximum: 65536 },
      waitMs,
      encoding: { type: "string", enum: ["text", "base64"] },
    } },
    risk: "read", effect: { kind: "none", ordering: "commutative" },
  },
  { name: "status", description: "Current state, or the final receipt.", inputSchema: idOnly, risk: "read", effect: { kind: "none", ordering: "commutative" } },
  {
    name: "wait",
    description: "Wait for the final receipt, or return the running state at the ceiling (default 30 s). Waiting never stops the child.",
    inputSchema: { type: "object", properties: { id, timeoutMs: { type: "integer", minimum: 1, maximum: 300000 } }, required: ["id"], additionalProperties: false },
    risk: "read", effect: { kind: "none", ordering: "commutative" },
  },
  {
    name: "events",
    description: "Retained jev-fabric events after a sequence cursor, optionally long-polled.",
    inputSchema: { type: "object", properties: { id, after: { type: "integer", minimum: 0 }, waitMs }, required: ["id"], additionalProperties: false },
    risk: "read", effect: { kind: "none", ordering: "commutative" },
  },
  { name: "stop", description: "Stop a child by ID (process group, then force), never by PID. Idempotent.", inputSchema: idOnly, risk: "execute", effect: { kind: "emission", ordering: "ordered" } },
  { name: "list", description: "Interactive children opened by this Pi session, with lifetime and state.", inputSchema: { type: "object", properties: {}, additionalProperties: false }, risk: "read", effect: { kind: "none", ordering: "commutative" } },
];

type Opened = { id: string; lifetime: "session" | "durable"; label?: string; owner?: string; stopRequired?: boolean; custodyFile?: string };
type PendingLaunch = { owner: string; lifetime: "session" | "durable"; ended: boolean; pending: Promise<unknown> };

/**
 * Interactive children through jev-fabric's serve protocol: the same verbs,
 * records and lifetimes as the jev-fabric CLI and clients (docs/composition.md
 * in jev-fabric). One serve connection per Pi session owns every `session`
 * child; a Jev program's session children end with that program.
 */
export class SessionsProvider implements FabricProvider {
  readonly name = "sessions";
  readonly description = "Interactive jev-fabric children: open, write, read, wait and stop";
  #serve: Promise<JevFabricServe> | undefined;
  readonly #opened = new Map<string, Opened>();
  #closed = false;
  #closePromise: Promise<void> | undefined;
  readonly #launches = new Set<PendingLaunch>();

  constructor(
    readonly bridge: DurableShellBridge,
    readonly options: {
      cwd: string;
      shellOverride: () => boolean;
      /** Session runners cannot preserve Pi's child write policy. */
      writePolicy?: () => FabricWritePolicy | undefined;
      /** Durable jev-fabric cannot run inside the host Landlock sandbox. */
      landlockEnforced?: () => boolean;
      /** Explicit root-session authority to control jobs not opened here; IDs are not capabilities. */
      trustedExternalControl?: () => boolean;
    },
  ) {}

  async list(request: FabricProviderListRequest): Promise<FabricActionDescriptor[]> {
    const query = request.query?.toLowerCase();
    return query ? descriptors.filter(d => `${d.name} ${d.description}`.toLowerCase().includes(query)) : descriptors;
  }

  async describe(name: string): Promise<FabricActionDescriptor | undefined> { return descriptors.find(d => d.name === name); }

  #connect(): Promise<JevFabricServe> {
    if (this.#closed) return Promise.reject(new Error("Sessions provider is closed"));
    this.#serve ??= (async () => {
      const [resolution, { JevFabricServe }] = await Promise.all([this.bridge.resolve("sessions"), import("../jev-fabric/serve.js")]);
      const serve = await JevFabricServe.open(resolution.path, { home: this.bridge.home, cwd: this.options.cwd, timeoutMs: DAY_MS });
      // A lost connection took its session children with it; the next call reconnects.
      void serve.exited.then(() => {
        for (const [key, opened] of this.#opened) if (opened.lifetime === "session") this.#opened.delete(key);
        this.#serve = undefined;
      });
      return serve;
    })();
    this.#serve.catch(() => { this.#serve = undefined; });
    return this.#serve;
  }

  async invoke(name: string, args: Record<string, unknown>, context: FabricInvocationContext): Promise<unknown> {
    const descriptor = await this.describe(name);
    if (!descriptor) throw new Error(`Unknown sessions action: ${name}`);
    const invalid = validationMessage(descriptor.inputSchema, args);
    if (invalid) throw new Error(`Invalid sessions.${name} arguments: ${invalid}`);
    if (name === "list") return [...this.#opened.values()].map(opened => ({ ...opened }));
    if (name === "open") return this.#open(args, context);
    const job = args.id as string;
    if (name === "write" || name === "closeInput") {
      this.#assertShellControl(name);
      if (!this.#opened.has(job) && !this.options.trustedExternalControl?.()) {
        throw new Error("Interactive job control requires explicit trusted external control authority; a store ID is not authority");
      }
    }
    const serve = await this.#connect();
    switch (name) {
      case "write": return serve.request("write", { job, text: args.text }, context.signal);
      case "closeInput": return serve.request("closeInput", { job }, context.signal);
      case "read": return serve.request("read", { job, stream: args.stream ?? "stdout",
        ...pick(args, ["offset", "max", "waitMs", "encoding"]) }, context.signal);
      case "status": return serve.request("status", { job }, context.signal);
      case "wait": return serve.request("wait", { job, ...pick(args, ["timeoutMs"]) }, context.signal);
      case "events": return serve.request("events", { job, ...pick(args, ["after", "waitMs"]) }, context.signal);
      case "stop": {
        const opened = this.#opened.get(job);
        return opened?.stopRequired ? this.#stopOpened(serve, opened) : serve.request("stop", { job }, context.signal);
      }
    }
    throw new Error(`Unknown sessions action: ${name}`);
  }

  #assertShellControl(action: string): void {
    // Sessions run outside pi.bash: never let them bypass an extension's shell gate.
    if (this.options.shellOverride()) throw new Error("Interactive sessions are unavailable while an extension overrides bash; they would bypass its shell protection");
    if (this.options.landlockEnforced?.()) throw new Error("Interactive sessions are unavailable while Landlock enforce is active; the jev-fabric runner cannot preserve the sandbox");
    const policy = this.options.writePolicy?.();
    if (policy) {
      // The external runner cannot apply Pi's effective roots and shell hook.
      // Refuse both argv and cmd forms rather than delegate an unrestricted child.
      const denial = writePolicyDenial(policy, "bash", { command: `sessions.${action}` }, this.options.cwd);
      throw new Error(denial ?? "Interactive sessions are unavailable while a child write policy is active; the runner cannot preserve it");
    }
  }

  #open(args: Record<string, unknown>, context: FabricInvocationContext): Promise<unknown> {
    const launch: PendingLaunch = { owner: context.parentToolCallId, lifetime: args.durable === true ? "durable" : "session",
      ended: false, pending: Promise.resolve() };
    this.#launches.add(launch);
    launch.pending = this.#launch(args, context, launch).finally(() => this.#launches.delete(launch));
    return launch.pending;
  }

  async #launch(args: Record<string, unknown>, context: FabricInvocationContext, launch: PendingLaunch): Promise<unknown> {
    this.#assertShellControl("open");
    if ((args.argv === undefined) === (args.cmd === undefined)) throw new Error("sessions.open needs exactly one of argv or cmd");
    const cwd = path.resolve(this.options.cwd, typeof args.cwd === "string" ? args.cwd : ".");
    if (!fs.statSync(cwd, { throwIfNoEntry: false })?.isDirectory()) throw new Error(`Working directory does not exist: ${cwd}`);
    const argv = Array.isArray(args.argv) ? args.argv as string[] : [fs.existsSync("/bin/bash") ? "/bin/bash" : "bash", "-c", args.cmd as string];
    const durable = args.durable === true;
    context.signal?.throwIfAborted();
    const serve = await this.#connect();
    context.signal?.throwIfAborted();
    if (launch.ended || this.#closed) throw new Error("Interactive launch owner ended before submission");
    const key = randomUUID();
    const custodyFile = path.join(this.bridge.home, ".fabric-launch-custody", `${key}.json`);
    const fields = { argv, cwd, ...pick(args, ["timeoutMs", "label"]), label: args.label ?? `fabric-launch:${key}` };
    const custody = { version: 1, key, owner: launch.owner, lifetime: launch.lifetime, label: fields.label, submittedAt: Date.now() };
    // A lost acknowledgement cannot erase an effectful launch. Retain a durable
    // uncertain-launch obligation even if the serve connection dies without a receipt.
    writeJsonAtomic(custodyFile, { ...custody, state: "awaiting-receipt" }, { durable: true });
    let opened: Opened | undefined;
    try {
      // Abort only the observation, not launch-result custody. Always collect the
      // acknowledgement and compensate before this provider-owned work settles.
      const result = await serve.request<Record<string, unknown>>(durable ? "start" : "spawn", durable ? { ...fields, input: "pipe" } : fields);
      if (typeof result.id !== "string" || !result.id || result.id.length > 128) throw new Error("Interactive launch returned no valid child receipt");
      opened = {
        id: result.id, lifetime: launch.lifetime,
        ...(typeof args.label === "string" ? { label: args.label } : {}),
        // A Jev program's session children end with the program; fabric_exec ones stay with Pi.
        ...(!durable && context.parentToolCallId.startsWith("jev:") ? { owner: context.parentToolCallId } : {}),
      };
      this.#opened.set(opened.id, opened);
      context.signal?.throwIfAborted();
      if (launch.ended || this.#closed) throw new Error("Interactive launch owner ended before acknowledgement");
      fs.unlinkSync(custodyFile); // Custody now belongs to #opened and the acknowledged caller.
      return { ...result, lifetime: opened.lifetime };
    } catch (error) {
      if (opened) {
        opened.stopRequired = true;
        opened.custodyFile = custodyFile;
        try { writeJsonAtomic(custodyFile, { ...custody, state: "stop-required", id: opened.id }, { durable: true }); }
        catch { /* The original durable obligation and in-memory receipt still retain custody. */ }
        try { await this.#stopOpened(serve, opened); }
        catch (stopError) { throw new Error(`Interactive launch stop is unconfirmed for ${opened.id}; custody retained at ${custodyFile}: ${String(stopError)}`); }
      } else {
        throw new Error(`Interactive launch receipt is uncertain; custody retained at ${custodyFile}: ${String(error)}`);
      }
      throw error;
    }
  }

  async #stopOpened(serve: JevFabricServe, opened: Opened): Promise<Record<string, unknown>> {
    opened.stopRequired = true;
    const result = await serve.request<Record<string, unknown>>("stop", { job: opened.id });
    if (result.id !== opened.id || !["cancelled", "timed_out", "exited", "failed"].includes(String(result.state))) {
      throw new Error("jev-fabric did not confirm a terminal stop receipt");
    }
    this.#opened.delete(opened.id);
    if (opened.custodyFile) fs.unlinkSync(opened.custodyFile);
    return result;
  }

  async invocationEnded(parentToolCallId: string): Promise<void> {
    if (!parentToolCallId.startsWith("jev:")) return;
    const launching = [...this.#launches].filter(launch => launch.owner === parentToolCallId && launch.lifetime === "session");
    for (const launch of launching) launch.ended = true;
    await Promise.allSettled(launching.map(launch => launch.pending));
    const owned = [...this.#opened.values()].filter(opened => opened.owner === parentToolCallId);
    if (!owned.length || !this.#serve) return;
    const serve = await this.#serve.catch(() => undefined);
    await Promise.allSettled(owned.map(async opened => {
      if (serve) await this.#stopOpened(serve, opened);
    }));
  }

  close(): Promise<void> {
    this.#closed = true;
    for (const launch of this.#launches) launch.ended = true;
    return this.#closePromise ??= this.#close();
  }

  async #close(): Promise<void> {
    // Join launch acknowledgements and compensating stops before ending the
    // connection; durable children do not die merely because serve disconnects.
    const connection = this.#serve;
    let forcedClose: Promise<void> | undefined;
    // A silent/lost serve acknowledgement cannot hang host teardown forever.
    // Closing the connection rejects pending receipts; durable uncertainty stays
    // in the write-ahead custody record, while session children die with serve.
    const timer = this.#launches.size ? setTimeout(() => {
      forcedClose = connection?.then(serve => serve.close()).catch(() => undefined);
    }, 30_000) : undefined;
    try { await Promise.allSettled([...this.#launches].map(launch => launch.pending)); }
    finally { clearTimeout(timer); }
    await forcedClose;
    const serve = await connection?.catch(() => undefined);
    if (serve) await Promise.allSettled([...this.#opened.values()].filter(opened => opened.stopRequired).map(opened => this.#stopOpened(serve, opened)));
    this.#opened.clear();
    // Ending the connection stops its session children; durable jobs stay in their store.
    await serve?.close();
  }
}

const pick = (args: Record<string, unknown>, keys: string[]): Record<string, unknown> =>
  Object.fromEntries(keys.filter(key => args[key] !== undefined).map(key => [key, args[key]]));
