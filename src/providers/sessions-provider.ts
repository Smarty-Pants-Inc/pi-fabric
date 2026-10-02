import fs from "node:fs";
import path from "node:path";
import type { FabricActionDescriptor, FabricInvocationContext, FabricProvider, FabricProviderListRequest } from "../protocol.js";
import { readChildToolAllowlist } from "../core/child-tool-allowlist.js";
import { throwIfAborted } from "../async-settlement.js";
import { validationMessage } from "../core/action-arguments.js";
import type { DurableShellBridge } from "../jev-fabric/bridge.js";
import type { JevFabricServe } from "../jev-fabric/serve.js";

const DAY_MS = 24 * 3_600_000;
const OWNER_RECEIPT_GRACE_MS = 250;
const TERMINAL_RECEIPT_LIMIT = 128;
const id = { type: "string", minLength: 1, maxLength: 128, description: "Session ID from sessions.open (`s-…` for session lifetime). Output and receipts require a child opened here; use tasks.* for durable batch output." };
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
  { name: "status", description: "Current state, or the final receipt, for a child opened here.", inputSchema: idOnly, risk: "read", effect: { kind: "none", ordering: "commutative" } },
  {
    name: "wait",
    description: "Wait for the final receipt of a child opened here, or return the running state at the ceiling (default 30 s). Waiting never stops the child.",
    inputSchema: { type: "object", properties: { id, timeoutMs: { type: "integer", minimum: 1, maximum: 300000 } }, required: ["id"], additionalProperties: false },
    risk: "read", effect: { kind: "none", ordering: "commutative" },
  },
  {
    name: "events",
    description: "Retained jev-fabric events after a sequence cursor, optionally long-polled.",
    inputSchema: { type: "object", properties: { id, after: { type: "integer", minimum: 0 }, waitMs }, required: ["id"], additionalProperties: false },
    risk: "read", effect: { kind: "none", ordering: "commutative" },
  },
  { name: "stop", description: "Stop a child opened here by ID (process group, then force), never by PID. Idempotent.", inputSchema: idOnly, risk: "execute", effect: { kind: "emission", ordering: "ordered" } },
  { name: "list", description: "Interactive children opened by this Pi session, with lifetime and state.", inputSchema: { type: "object", properties: {}, additionalProperties: false }, risk: "read", effect: { kind: "none", ordering: "commutative" } },
];

type Opened = { id: string; lifetime: "session" | "durable"; label?: string; owner?: string };

/**
 * Interactive children through jev-fabric's serve protocol: the same verbs,
 * records and lifetimes as the jev-fabric CLI and clients (docs/composition.md
 * in jev-fabric). Session launches use isolated connections so a withheld
 * receipt can be torn down without stopping another owner's children.
 */
export class SessionsProvider implements FabricProvider {
  readonly name = "sessions";
  readonly description = "Interactive jev-fabric children: open, write, read, wait and stop";
  #serve: Promise<JevFabricServe> | undefined;
  readonly #connections = new Set<Promise<JevFabricServe>>();
  readonly #launchConnections = new Map<string, Set<Promise<JevFabricServe>>>();
  readonly #jobConnections = new Map<string, JevFabricServe>();
  readonly #opened = new Map<string, Opened>();
  readonly #terminalReceipts = new Map<string, unknown>();
  #closed = false;
  readonly #allowedTools = readChildToolAllowlist();
  readonly #launches = new Map<Promise<unknown>, string>();
  readonly #retiredOwners = new Set<string>();

  constructor(
    readonly bridge: DurableShellBridge,
    readonly options: { cwd: string; shellOverride: () => boolean;
      admitShell?: (args: Record<string, unknown>, context: FabricInvocationContext) => Promise<Record<string, unknown>> },
  ) {}

  async list(request: FabricProviderListRequest): Promise<FabricActionDescriptor[]> {
    if (this.#allowedTools && !this.#allowedTools.has("bash")) return [];
    const query = request.query?.toLowerCase();
    return query ? descriptors.filter(d => `${d.name} ${d.description}`.toLowerCase().includes(query)) : descriptors;
  }

  async describe(name: string): Promise<FabricActionDescriptor | undefined> {
    if (this.#allowedTools && !this.#allowedTools.has("bash")) return undefined;
    return descriptors.find(d => d.name === name);
  }

  #connect(isolatedOwner?: string): Promise<JevFabricServe> {
    if (this.#closed) return Promise.reject(new Error("Sessions provider is closed"));
    if (isolatedOwner !== undefined && this.#retiredOwners.has(isolatedOwner)) return Promise.reject(new Error("Session launch owner ended"));
    if (isolatedOwner === undefined && this.#serve) return this.#serve;
    const pending = (async () => {
      const [resolution, { JevFabricServe }] = await Promise.all([this.bridge.resolve("sessions"), import("../jev-fabric/serve.js")]);
      const serve = await JevFabricServe.open(resolution.path, { home: this.bridge.home, cwd: this.options.cwd, timeoutMs: DAY_MS });
      // A lost connection took its session children with it; the next call reconnects.
      void serve.exited.then(() => {
        for (const [key, connection] of this.#jobConnections) if (connection === serve) {
          if (this.#opened.get(key)?.lifetime === "session") this.#opened.delete(key);
          this.#jobConnections.delete(key);
        }
        this.#connections.delete(pending);
        if (isolatedOwner !== undefined) {
          const connections = this.#launchConnections.get(isolatedOwner);
          connections?.delete(pending);
          if (!connections?.size) this.#launchConnections.delete(isolatedOwner);
        }
        if (this.#serve === pending) this.#serve = undefined;
      });
      return serve;
    })();
    this.#connections.add(pending);
    if (isolatedOwner === undefined) this.#serve = pending;
    else {
      let connections = this.#launchConnections.get(isolatedOwner);
      if (!connections) this.#launchConnections.set(isolatedOwner, connections = new Set());
      connections.add(pending);
    }
    void pending.catch(() => {
      this.#connections.delete(pending);
      if (isolatedOwner !== undefined) {
        const connections = this.#launchConnections.get(isolatedOwner);
        connections?.delete(pending);
        if (!connections?.size) this.#launchConnections.delete(isolatedOwner);
      }
      if (this.#serve === pending) this.#serve = undefined;
    });
    return pending;
  }

  async invoke(name: string, args: Record<string, unknown>, context: FabricInvocationContext): Promise<unknown> {
    if (this.#allowedTools && !this.#allowedTools.has("bash")) throw new Error("Pi tool bash is not permitted by this child's tool allowlist");
    const descriptor = await this.describe(name);
    if (!descriptor) throw new Error(`Unknown sessions action: ${name}`);
    const invalid = validationMessage(descriptor.inputSchema, args);
    if (invalid) throw new Error(`Invalid sessions.${name} arguments: ${invalid}`);
    if (name === "list") return [...this.#opened.values()].map(opened => ({ ...opened }));
    if (name === "open") {
      const launch = this.#open(args, context);
      this.#launches.set(launch, context.parentToolCallId);
      try { return await launch; } finally { this.#launches.delete(launch); }
    }
    const job = args.id as string;
    const returnsReceipt = name === "status" || name === "wait" || name === "stop";
    // Cached receipts belong to children opened here, even after owner teardown.
    if (returnsReceipt && this.#terminalReceipts.has(job)) return this.#terminalReceipts.get(job);
    // Terminal receipts contain stdout/stderr too. Batch output belongs
    // exclusively to tasks.*, with its launch-time filter; never connect to
    // the shared store to retrieve output for a foreign job through any verb.
    if ((name === "read" || name === "events" || returnsReceipt) && !this.#opened.has(job)) {
      throw new Error("Session output is available only for children opened here; use tasks for durable batch jobs");
    }
    const serve = this.#jobConnections.get(job) ?? await this.#connect();
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
    if ((args.argv === undefined) === (args.cmd === undefined)) throw new Error("sessions.open needs exactly one of argv or cmd");
    const cwd = path.resolve(this.options.cwd, typeof args.cwd === "string" ? args.cwd : ".");
    if (!fs.statSync(cwd, { throwIfNoEntry: false })?.isDirectory()) throw new Error(`Working directory does not exist: ${cwd}`);
    const argv = Array.isArray(args.argv) ? args.argv as string[] : [fs.existsSync("/bin/bash") ? "/bin/bash" : "bash", "-c", args.cmd as string];
    if (!this.options.admitShell) throw new Error("Interactive sessions require host shell admission");
    const command = typeof args.cmd === "string" ? args.cmd : argv.map(value => "'" + value.replaceAll("'", "'\\''") + "'").join(" ");
    const admitted = await this.options.admitShell({ command, cwd,
      ...(typeof args.timeoutMs === "number" ? { timeout: args.timeoutMs / 1000 } : {}),
    }, context);
    const timeoutMs = typeof admitted.timeout === "number" ? Math.min(DAY_MS, Math.ceil(admitted.timeout * 1000)) : (args.timeoutMs ?? 3_600_000);
    const durable = args.durable === true;
    const serve = await this.#connect(durable ? undefined : context.parentToolCallId);
    throwIfAborted(context.signal);
    if (this.#closed || this.#retiredOwners.has(context.parentToolCallId)) throw new Error("Session launch owner ended");
    if (this.options.shellOverride()) throw new Error("Interactive sessions would bypass bash shell protection");
    const fields = { argv, cwd, timeoutMs, ...pick(args, ["label"]) };
    // A spawn cannot be cancelled at the wire: keep its response so ownership
    // is never lost between dispatch and recording. Compensate before rejecting.
    // Abort/retirement closes this launch's isolated connection even if the
    // spawn response never arrives. close() rejects the pending receipt and
    // confirms backend exit; a late receipt still uses compensation below.
    const abortLaunch = (): void => { if (!durable) void serve.close(); };
    context.signal?.addEventListener("abort", abortLaunch, { once: true });
    let result: Record<string, unknown>;
    try {
      result = await serve.request<Record<string, unknown>>(durable ? "start" : "spawn", durable ? { ...fields, input: "pipe" } : fields);
    } finally { context.signal?.removeEventListener("abort", abortLaunch); }
    const opened: Opened = {
      id: String(result.id), lifetime: durable ? "durable" : "session",
      ...(typeof args.label === "string" ? { label: args.label } : {}),
      // A Jev program's session children end with the program; fabric_exec ones stay with Pi.
      ...(!durable && context.parentToolCallId.startsWith("jev:") ? { owner: context.parentToolCallId } : {}),
    };
    this.#opened.set(opened.id, opened);
    this.#jobConnections.set(opened.id, serve);
    if (!durable && (context.signal?.aborted || this.#closed || this.#retiredOwners.has(context.parentToolCallId))) {
      try { await serve.request("stop", { job: opened.id }, AbortSignal.timeout(OWNER_RECEIPT_GRACE_MS)); } catch { await serve.close(); }
      this.#opened.delete(opened.id);
      throwIfAborted(context.signal);
      throw new Error("Session launch owner ended");
    }
    return { ...result, lifetime: opened.lifetime };
  }

  async invocationEnded(parentToolCallId: string): Promise<void> {
    if (!parentToolCallId.startsWith("jev:")) return;
    this.#retiredOwners.add(parentToolCallId);
    const launches = [...this.#launches].filter(([, owner]) => owner === parentToolCallId).map(([pending]) => pending);
    let timer: ReturnType<typeof setTimeout> | undefined;
    await Promise.race([Promise.allSettled(launches), new Promise(resolve => { timer = setTimeout(resolve, OWNER_RECEIPT_GRACE_MS); })]);
    clearTimeout(timer);
    const owned = [...this.#opened.values()].filter(opened => opened.owner === parentToolCallId);
    await Promise.allSettled(owned.map(async opened => {
      const serve = this.#jobConnections.get(opened.id);
      try {
        if (serve) {
          const receipt = await serve.request("stop", { job: opened.id }, AbortSignal.timeout(OWNER_RECEIPT_GRACE_MS));
          this.#terminalReceipts.set(opened.id, receipt);
          if (this.#terminalReceipts.size > TERMINAL_RECEIPT_LIMIT) this.#terminalReceipts.delete(this.#terminalReceipts.keys().next().value!);
        }
      } catch { /* EOF below also handles an unresponsive child or lost receipt. */ }
    }));
    // Connection ownership outlives its spawn receipt. Always close every
    // isolated connection for this owner, and await confirmed backend exit;
    // neither settled launches nor missing receipts may retain a serve process.
    await Promise.allSettled([...(this.#launchConnections.get(parentToolCallId) ?? [])].map(async pending => (await pending).close()));
    for (const opened of owned) {
      this.#opened.delete(opened.id);
      this.#jobConnections.delete(opened.id);
    }
  }

  async close(): Promise<void> {
    this.#closed = true;
    // Initiate teardown before observing pending launches. A missing receipt
    // must never hold the connection (and its real children) open indefinitely.
    await Promise.allSettled([...this.#connections].map(async pending => (await pending).close()));
    await Promise.allSettled(this.#launches.keys());
    this.#opened.clear();
    this.#jobConnections.clear();
    this.#terminalReceipts.clear();
    this.#launchConnections.clear();
  }
}

const pick = (args: Record<string, unknown>, keys: string[]): Record<string, unknown> =>
  Object.fromEntries(keys.filter(key => args[key] !== undefined).map(key => [key, args[key]]));
