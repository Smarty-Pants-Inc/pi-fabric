import type { ExtensionToolContext, RegisteredTool } from "@earendil-works/pi-coding-agent";
import { runAbortable, throwIfAborted } from "../async-settlement.js";
import type { CapturedToolCatalog } from "../capture/catalog.js";
import { readChildToolAllowlist } from "../core/child-tool-allowlist.js";
import { nativeMcpIdentity } from "../core/native-mcp-identity.js";
import type { FabricActionDescriptor, FabricInvocationContext, FabricProviderListRequest } from "../protocol.js";
import { sanitizeMcpRefPart } from "../ref-names.js";
import { mcpToolDescriptor, normalizeMcpResult } from "./mcp-provider.js";

interface NativeTool {
  server: string;
  tool: string;
  registered: RegisteredTool;
}

interface NativeServerIndex {
  tools: NativeTool[];
  exact: Map<string, NativeTool[]>;
  aliases: Map<string, NativeTool[]>;
  invalid: RegisteredTool[];
}

/** Borrows Pi's registered tools. Never reads MCP config, connects, retries, or closes a server. */
export class PiNativeMcpTools {
  readonly #allowed = readChildToolAllowlist();
  readonly #calls = new Set<AbortController>();
  readonly #owners: ReadonlySet<string>;
  readonly #indexes = new Map<string, NativeServerIndex>();
  #snapshot: readonly RegisteredTool[] | undefined;
  #closed = false;

  constructor(
    readonly catalog: CapturedToolCatalog,
    readonly servers: readonly string[],
    readonly timeoutMs: number,
  ) {
    this.servers = Object.freeze([...servers]);
    this.#owners = new Set(this.servers);
  }

  owns(server: string): boolean {
    return this.#owners.has(server);
  }

  resolveServer(requested: string): string | undefined {
    if (this.owns(requested)) return requested;
    const matches = this.servers.filter(server => sanitizeMcpRefPart(server) === requested);
    if (matches.length > 1) throw new Error(`Ambiguous Pi MCP server alias: ${requested}; use an exact server name`);
    return matches[0];
  }

  close(): void {
    this.#closed = true;
    for (const call of this.#calls) call.abort(new Error("MCP provider is closed"));
    this.#calls.clear();
    this.#indexes.clear();
    this.#snapshot = undefined;
  }

  #refreshIndex(): void {
    if (this.#closed) throw new Error("MCP provider is closed");
    const snapshot = this.catalog.registeredTools();
    if (snapshot === this.#snapshot) return;
    this.#indexes.clear();
    for (const registered of snapshot) {
      const definition = registered.definition;
      const server = definition.namespace?.name.startsWith("mcp__")
        ? definition.namespace.name.slice(5) : undefined;
      if (!server || !this.owns(server)) continue;
      let index = this.#indexes.get(server);
      if (!index) {
        index = { tools: [], exact: new Map(), aliases: new Map(), invalid: [] };
        this.#indexes.set(server, index);
      }
      const identity = nativeMcpIdentity(definition);
      if (!identity) { index.invalid.push(registered); continue; }
      const entry = { ...identity, registered };
      index.tools.push(entry);
      const exact = index.exact.get(entry.tool) ?? [];
      exact.push(entry);
      index.exact.set(entry.tool, exact);
      const alias = sanitizeMcpRefPart(entry.tool);
      const aliases = index.aliases.get(alias) ?? [];
      aliases.push(entry);
      index.aliases.set(alias, aliases);
    }
    this.#snapshot = snapshot;
  }

  // Identity is indexed per registration snapshot, never by TTL. Exposure,
  // direct-tool activation and schemas remain live on every lookup.
  #visibility(): (registered: RegisteredTool) => boolean {
    let active: Set<string> | undefined;
    return ({ definition }) => {
      if (definition.exposure === "hidden" || definition.exposure === "model-only") return false;
      if (this.#allowed && !this.#allowed.has(definition.name)) return false;
      if (definition.exposure === undefined || definition.exposure === "direct") {
        active ??= new Set(this.catalog.runner?.getActiveTools() ?? []);
        return active.has(definition.name);
      }
      return true;
    };
  }

  #checkMetadata(index: NativeServerIndex, visible: (tool: RegisteredTool) => boolean): void {
    const invalid = index.invalid.find(visible);
    if (invalid) throw new Error(`Unsupported Pi MCP identity metadata for ${invalid.definition.name}; refusing to guess its original name`);
  }

  #tools(server?: string): NativeTool[] {
    this.#refreshIndex();
    const visible = this.#visibility();
    const indexes = server ? [this.#indexes.get(server)].filter(index => index !== undefined) : [...this.#indexes.values()];
    return indexes.flatMap(index => {
      this.#checkMetadata(index, visible);
      return index.tools.filter(entry => visible(entry.registered));
    });
  }

  #descriptor(entry: NativeTool): FabricActionDescriptor {
    const definition = entry.registered.definition;
    // Pi's output schema describes CallToolResult<T>, not the server's T.
    const envelope = definition.outputSchema as unknown as { properties?: { structuredContent?: Record<string, unknown> } } | undefined;
    const outputSchema = envelope?.properties?.structuredContent;
    return mcpToolDescriptor(entry.server, {
      name: entry.tool,
      description: definition.description,
      inputSchema: definition.parameters as unknown as Record<string, unknown>,
      ...(outputSchema ? { outputSchema } : {}),
      ...(definition.annotations ? { annotations: definition.annotations } : {}),
    });
  }

  list(request: FabricProviderListRequest = {}): FabricActionDescriptor[] {
    const server = request.namespace ? this.resolveServer(request.namespace) : undefined;
    if (request.namespace && !server) return [];
    const query = request.query?.toLowerCase();
    return this.#tools(server).map(entry => this.#descriptor(entry))
      .filter(entry => !query || `${entry.name} ${entry.description}`.toLowerCase().includes(query));
  }

  serverInfo(): Array<{ name: string; description: string | null; transport: string; tools: number; stale: boolean }> {
    let tools = this.#tools();
    const indexed = new Set(tools.map(tool => tool.server));
    // Discovery heals too: a server that connected after the snapshot must not
    // read as stale for the rest of the session.
    if (this.servers.some(name => !indexed.has(name)) && this.#pollLiveRegistrations()) {
      tools = this.#tools();
    }
    return this.servers.map(name => {
      const selected = tools.filter(tool => tool.server === name);
      return { name, description: selected[0]?.registered.definition.namespace?.description ?? null,
        transport: "pi", tools: selected.length, stale: selected.length === 0 };
    });
  }

  #resolve(server: string, requested: string): NativeTool {
    const attempt = (): NativeTool[] => {
      this.#refreshIndex();
      const visible = this.#visibility();
      const index = this.#indexes.get(server);
      if (index) this.#checkMetadata(index, visible);
      // A withdrawn exact name must not silently become another tool's alias.
      const candidates = index?.exact.get(requested) ?? index?.aliases.get(requested) ?? [];
      return candidates.filter(entry => visible(entry.registered));
    };
    let matches = attempt();
    // Pi keeps its registry live, but the capture catalog only refreshes when
    // something calls runner.getAllRegisteredTools(). A miss is exactly the
    // moment a late-connected server is in flight, so re-poll once: a session
    // must not pin its startup registration snapshot for native lookups.
    if (matches.length !== 1 && this.#pollLiveRegistrations()) matches = attempt();
    if (matches.length !== 1) {
      throw new Error(`Unknown or ambiguous Pi MCP tool: ${server}.${requested}. Check /mcp and the tool exposure; no mcporter fallback was attempted.`);
    }
    return matches[0]!;
  }

  // Mirrors Pi's "waits for the servers it names" semantics inside the borrow
  // layer: never polls on a timer, only once per failed lookup, and reports
  // change only when the capture catalog actually moved.
  #pollLiveRegistrations(): boolean {
    const runner = this.catalog.runner as
      | { getAllRegisteredTools?: () => readonly RegisteredTool[] }
      | undefined;
    if (typeof runner?.getAllRegisteredTools !== "function") return false;
    const before = this.catalog.registeredTools();
    try {
      runner.getAllRegisteredTools();
    } catch {
      return false;
    }
    return this.catalog.registeredTools() !== before;
  }

  describe(server: string, tool: string): FabricActionDescriptor {
    return this.#descriptor(this.#resolve(server, tool));
  }

  async invoke(server: string, tool: string, args: Record<string, unknown>, context: FabricInvocationContext): Promise<unknown> {
    throwIfAborted(context.signal);
    const entry = this.#resolve(server, tool);
    const host = context.extensionContext as Partial<ExtensionToolContext>;
    const name = entry.registered.definition.name;
    if (!host.executeTool || !host.tools?.some(candidate => candidate.name === name)) {
      throw new Error(`Pi MCP tool ${server}.${entry.tool} is not callable in this execution context`);
    }
    // Never invoke captured execute() directly: native middleware and accounting are authoritative.
    const deadline = new AbortController();
    this.#calls.add(deadline);
    const timer = setTimeout(() => deadline.abort(new Error(`MCP call timed out after ${this.timeoutMs}ms`)), this.timeoutMs);
    timer.unref?.();
    const signal = context.signal ? AbortSignal.any([context.signal, deadline.signal]) : deadline.signal;
    try {
      const outcome = await runAbortable(signal, () => host.executeTool!(name, args, {
        signal,
        onUpdate: partial => {
          if (signal.aborted) return;
          const text = partial.content.filter(part => part.type === "text").map(part => part.text).join("\n");
          if (text) context.update(`${server}.${entry.tool}: ${text.slice(0, 500)}`);
        },
      }));
      context.updateArguments?.(outcome.toolCall.arguments);
      const result = outcome.result;
      const images = result.content.filter(part => part.type === "image");
      if (images.length && !context.nativeToolResult) context.attachMedia?.(images);
      // A content-only redaction removes structuredContent in Pi. Never resurrect the raw result.
      const raw = result.structuredContent;
      const envelope = raw && typeof raw === "object" && Array.isArray((raw as Record<string, unknown>).content)
        ? raw as Record<string, unknown>
        : { content: result.content, ...(raw !== undefined ? { structuredContent: raw } : {}) };
      if (context.nativeToolResult && outcome.isError && result.structuredContent === undefined) {
        throw new Error(result.content.filter(part => part.type === "text").map(part => part.text).join("\n") || "MCP tool failed");
      }
      return normalizeMcpResult({ ...envelope, ...(outcome.isError ? { isError: true } : {}) }, context.nativeToolResult);
    } finally {
      this.#calls.delete(deadline);
      clearTimeout(timer);
    }
  }
}
