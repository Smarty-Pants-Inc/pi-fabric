import fs from "node:fs";
import path from "node:path";
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

type Opened = { id: string; lifetime: "session" | "durable"; label?: string; owner?: string };

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

  constructor(
    readonly bridge: DurableShellBridge,
    readonly options: {
      cwd: string;
      shellOverride: () => boolean;
      /** Session runners cannot preserve Pi's child write policy. */
      writePolicy?: () => FabricWritePolicy | undefined;
      /** Durable jev-fabric cannot run inside the host Landlock sandbox. */
      landlockEnforced?: () => boolean;
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
    const serve = await this.#connect();
    const job = args.id as string;
    switch (name) {
      case "write": return serve.request("write", { job, text: args.text }, context.signal);
      case "closeInput": return serve.request("closeInput", { job }, context.signal);
      case "read": return serve.request("read", { job, stream: args.stream ?? "stdout",
        ...pick(args, ["offset", "max", "waitMs", "encoding"]) }, context.signal);
      case "status": return serve.request("status", { job }, context.signal);
      case "wait": return serve.request("wait", { job, ...pick(args, ["timeoutMs"]) }, context.signal);
      case "events": return serve.request("events", { job, ...pick(args, ["after", "waitMs"]) }, context.signal);
      case "stop": return serve.request("stop", { job }, context.signal);
    }
    throw new Error(`Unknown sessions action: ${name}`);
  }

  async #open(args: Record<string, unknown>, context: FabricInvocationContext): Promise<unknown> {
    // Sessions run outside pi.bash: never let them bypass an extension's shell gate.
    if (this.options.shellOverride()) throw new Error("Interactive sessions are unavailable while an extension overrides bash; they would bypass its shell protection");
    if (this.options.landlockEnforced?.()) throw new Error("Interactive sessions are unavailable while Landlock enforce is active; the jev-fabric runner cannot preserve the sandbox");
    const policy = this.options.writePolicy?.();
    if (policy) {
      // The external runner cannot apply Pi's effective roots and shell hook.
      // Refuse both argv and cmd forms rather than delegate an unrestricted child.
      const denial = writePolicyDenial(policy, "bash", { command: "sessions.open" }, this.options.cwd);
      throw new Error(denial ?? "Interactive sessions are unavailable while a child write policy is active; the runner cannot preserve it");
    }
    if ((args.argv === undefined) === (args.cmd === undefined)) throw new Error("sessions.open needs exactly one of argv or cmd");
    const cwd = path.resolve(this.options.cwd, typeof args.cwd === "string" ? args.cwd : ".");
    if (!fs.statSync(cwd, { throwIfNoEntry: false })?.isDirectory()) throw new Error(`Working directory does not exist: ${cwd}`);
    const argv = Array.isArray(args.argv) ? args.argv as string[] : [fs.existsSync("/bin/bash") ? "/bin/bash" : "bash", "-c", args.cmd as string];
    const durable = args.durable === true;
    const serve = await this.#connect();
    const fields = { argv, cwd, ...pick(args, ["timeoutMs", "label"]) };
    const result = await serve.request<Record<string, unknown>>(durable ? "start" : "spawn", durable ? { ...fields, input: "pipe" } : fields, context.signal);
    const opened: Opened = {
      id: String(result.id), lifetime: durable ? "durable" : "session",
      ...(typeof args.label === "string" ? { label: args.label } : {}),
      // A Jev program's session children end with the program; fabric_exec ones stay with Pi.
      ...(!durable && context.parentToolCallId.startsWith("jev:") ? { owner: context.parentToolCallId } : {}),
    };
    this.#opened.set(opened.id, opened);
    return { ...result, lifetime: opened.lifetime };
  }

  async invocationEnded(parentToolCallId: string): Promise<void> {
    if (!parentToolCallId.startsWith("jev:")) return;
    const owned = [...this.#opened.values()].filter(opened => opened.owner === parentToolCallId);
    if (!owned.length || !this.#serve) return;
    const serve = await this.#serve.catch(() => undefined);
    await Promise.allSettled(owned.map(async opened => {
      this.#opened.delete(opened.id);
      await serve?.request("stop", { job: opened.id });
    }));
  }

  async close(): Promise<void> {
    this.#closed = true;
    const serve = await this.#serve?.catch(() => undefined);
    this.#opened.clear();
    // Ending the connection stops its session children; durable jobs stay in their store.
    await serve?.close();
  }
}

const pick = (args: Record<string, unknown>, keys: string[]): Record<string, unknown> =>
  Object.fromEntries(keys.filter(key => args[key] !== undefined).map(key => [key, args[key]]));
